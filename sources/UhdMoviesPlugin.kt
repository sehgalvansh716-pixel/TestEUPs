package com.euthopiar.core.provider

import com.euthopiar.core.dsl.*
import com.euthopiar.core.model.*
import com.euthopiar.core.network.DohDns
import com.euthopiar.core.util.TmdbBridge
import com.euthopiar.core.util.parallelMapIsolated
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import okhttp3.FormBody
import okhttp3.OkHttpClient
import okhttp3.Request
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import java.net.URLDecoder
import java.net.URLEncoder
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * UHDMovies Universal Media Provider Implementation.
 *
 * Scrapes metadata and extracts direct Google Drive / Google CDN 10Gbps, 4K UHD, HDR,
 * 10Bit HEVC, and multi-language streams from https://uhdmovies.my/.
 *
 * Implements a complete 4-step LinkPilot / Naukriadda bypass engine and DriveSeed
 * stream resolver to extract unthrottled, direct-seekable video streams
 * (video-downloads.googleusercontent.com) while ignoring ad redirection and intermediate landing pages.
 *
 * Enriches missing metadata (16:9 backdrops, numerical ratings, content ratings, episode stills,
 * and actor profile photos) via TMDB API when not provided on the website.
 */
class UhdMoviesPlugin(
    private val client: OkHttpClient = DohDns.createOkHttpClient(
        connectTimeoutSeconds = 15,
        readTimeoutSeconds = 25
    )
) : UniversalPlugin {

    override val name: String = "UHDMovies"
    override val mainUrl: String = "https://uhdmovies.my"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE, MediaType.TV_SERIES)

    private val json = Json { ignoreUnknownKeys = true; isLenient = true }

    private val defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    private val tmdbApiKey = "1865f43a0549ca50d341dd9ab8b29f49"

    private val defaultHeaders = mapOf(
        "User-Agent" to defaultUserAgent,
        "Referer" to "https://uhdmovies.my/",
        "Origin" to "https://uhdmovies.my",
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    )

    // Dedicated client with redirection disabled to capture 302 redirects for CDN resolution
    private val noRedirectClient: OkHttpClient = client.newBuilder()
        .followRedirects(false)
        .followSslRedirects(false)
        .build()

    // Fast client for third-party mediator hops to prevent player hangs if upstream is unreachable
    private val fastMediatorClient: OkHttpClient = client.newBuilder()
        .connectTimeout(5, TimeUnit.SECONDS)
        .readTimeout(8, TimeUnit.SECONDS)
        .build()

    // Stream cache to provide instantaneous download link and stream source access
    private val streamCache = ConcurrentHashMap<String, Pair<Long, StreamResult>>()

    // ============================================================================
    // 1. Home Catalog & Search
    // ============================================================================

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        coroutineScope {
            val categories = listOf(
                "Latest Releases" to "$mainUrl/",
                "4K Ultra HD & 2160p HEVC" to "$mainUrl/2160p-hevc/",
                "4K HDR Masterpieces" to "$mainUrl/4k-hdr/",
                "1080p 10-Bit Full HD" to "$mainUrl/1080p-10bit/",
                "Dual Audio Movies" to "$mainUrl/movies/dual-audio-movies/",
                "English Movies" to "$mainUrl/movies/english-movies/",
                "Hindi & Bollywood Movies" to "$mainUrl/movies/dual-audio-movies/",
                "Web Series & TV Shows" to "$mainUrl/1080-x264/1080p-series/",
                "Latest Releases (Page 2)" to "$mainUrl/page/2/",
                "Latest Releases (Page 3)" to "$mainUrl/page/3/"
            )

            val deferred = categories.map { (title, url) ->
                async {
                    try {
                        val doc = client.getHtml(url, defaultHeaders)
                        val items = parseMovieCards(doc)
                        if (items.isNotEmpty()) CatalogRow(title, items) else null
                    } catch (_: Exception) {
                        null
                    }
                }
            }

            val rows = deferred.awaitAll().filterNotNull().toMutableList()

            // Ensure the first row has at least 8 items with official transparent PNG logos for the Home Hero Carousel
            if (rows.isNotEmpty()) {
                val firstRow = rows.first()
                val candidateItemsWithLogos = firstRow.items.take(10).map { item ->
                    async {
                        val logo = resolveLogo(item)
                        if (logo != null) item.copy(logoUrl = logo) else item
                    }
                }.awaitAll() + firstRow.items.drop(10)
                listOf(firstRow.copy(items = candidateItemsWithLogos)) + rows.drop(1)
            } else {
                rows
            }
        }
    }

    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        val encoded = URLEncoder.encode(query.trim(), "UTF-8")
        val searchUrl = "$mainUrl/?s=$encoded"
        try {
            val doc = client.getHtml(searchUrl, defaultHeaders)
            parseMovieCards(doc)
        } catch (_: Exception) {
            emptyList()
        }
    }

    /**
     * Cleans UHDMovies release titles into canonical movie/show titles suitable for
     * user display and TMDB lookups.
     */
    fun cleanTitleForDisplay(raw: String): String {
        return raw
            .replace(Regex("""(?i)^download\s+"""), "")
            .replace(Regex("""\[.*?\]"""), "")
            .replace(Regex("""\{.*?\}"""), "")
            .replace(Regex("""(?i)\((?:season|s\d|\d{4}).*?\)"""), "")
            .replace(Regex("""(?i)\b(1080p|2160p|4k|720p|480p|hevc|web-dl|bluray|esubs?|multi-audio|multi\s+audio|dual\s+audio|dual|org|remux|hdr|dovi|dv|sdr|dsnp|nf|amzn|x264|x265|english\s+movie|hindi\s+movie|movie|complete|all\s+episodes|all\s+seasons)\b.*"""), "")
            .replace("||", "")
            .trim(' ', '-', ':')
            .ifBlank { raw }
    }

    private fun parseMovieCards(doc: Document): List<MediaItem> {
        val items = mutableListOf<MediaItem>()
        val articles = doc.select("article")

        for (article in articles) {
            val link = article.selectFirst(".entry-image a, a[title]") ?: continue
            val rawHref = link.attr("href").trim()
            if (rawHref.isBlank()) continue

            val fullUrl = if (rawHref.startsWith("http")) rawHref else "$mainUrl${if (rawHref.startsWith("/")) "" else "/"}$rawHref"
            val rawTitle = link.attr("title").ifBlank { article.selectFirst(".entry-title, h2")?.text() ?: "" }.trim()
            if (rawTitle.isBlank()) continue

            val posterUrl = article.selectFirst("img")?.attr("src")?.trim()

            // Extract clean title and year
            val year = Regex("""\b(19\d\d|20\d\d)\b""").find(rawTitle)?.groupValues?.get(1)?.toIntOrNull()
            val cleanTitle = cleanTitleForDisplay(rawTitle)

            val isSeries = rawHref.contains("series", ignoreCase = true) ||
                    rawTitle.contains("Season", ignoreCase = true) ||
                    rawTitle.contains("Episode", ignoreCase = true) ||
                    Regex("""\bS\d{1,2}\b""", RegexOption.IGNORE_CASE).containsMatchIn(rawTitle)

            val mediaType = if (isSeries) MediaType.TV_SERIES else MediaType.MOVIE

            val qualityLabel = when {
                rawTitle.contains("2160p", ignoreCase = true) || rawTitle.contains("4K", ignoreCase = true) -> "4K 2160p"
                rawTitle.contains("1080p", ignoreCase = true) -> "1080p FHD"
                rawTitle.contains("720p", ignoreCase = true) -> "720p HD"
                else -> "HD"
            }

            val id = TmdbBridge.sanitizeSlug(fullUrl.removePrefix(mainUrl).trim('/'))

            items.add(
                MediaItem(
                    id = id,
                    title = cleanTitle,
                    url = fullUrl,
                    posterUrl = posterUrl,
                    backdropUrl = posterUrl,
                    type = mediaType,
                    year = year,
                    quality = qualityLabel,
                    provider = name
                )
            )
        }
        return items
    }

    // ============================================================================
    // 2. Media Details & Episode Structure (with TMDB Enrichment)
    // ============================================================================

    override suspend fun getDetails(mediaItem: MediaItem): MediaDetail = withContext(Dispatchers.IO) {
        val isTv = mediaItem.type == MediaType.TV_SERIES

        // 1. Resolve target URL
        var targetUrl = resolveTargetUrl(mediaItem)
        var doc: Document? = null

        if (targetUrl.isNotBlank()) {
            try {
                doc = client.getHtml(targetUrl, defaultHeaders)
            } catch (_: Exception) {}
        }

        // Search fallback if direct slug navigation failed
        if (doc == null && mediaItem.title.isNotBlank()) {
            try {
                val results = search(mediaItem.title)
                val targetClean = cleanTitleForDisplay(mediaItem.title).lowercase().trim()
                val match = results.firstOrNull {
                    it.title.equals(mediaItem.title, ignoreCase = true) ||
                            cleanTitleForDisplay(it.title).equals(targetClean, ignoreCase = true) ||
                            it.url.contains(mediaItem.id.trim('/'), ignoreCase = true)
                } ?: results.firstOrNull {
                    val candidateClean = cleanTitleForDisplay(it.title).lowercase().trim()
                    candidateClean.isNotBlank() && targetClean.isNotBlank() &&
                            (candidateClean == targetClean || candidateClean.startsWith(targetClean) || targetClean.startsWith(candidateClean))
                }

                if (match != null) {
                    targetUrl = match.url
                    doc = client.getHtml(targetUrl, defaultHeaders)
                }
            } catch (_: Exception) {}
        }

        // 2. Extract site metadata
        val rawPageTitle = doc?.selectFirst("h1.sanket, h1.entry-title, h1")?.text()?.trim()
            ?.ifBlank { null }
            ?: doc?.title()?.substringBefore(" - ")?.trim()?.ifBlank { null }
            ?: mediaItem.title

        val cleanTitle = cleanTitleForDisplay(rawPageTitle).ifBlank { mediaItem.title }

        val siteYear = Regex("""\b(19\d\d|20\d\d)\b""").find(rawPageTitle)?.groupValues?.get(1)?.toIntOrNull() ?: mediaItem.year

        val sitePoster = doc?.selectFirst(".entry-content img.size-full, .entry-content img[src*='uploads'], .entry-content img")?.attr("src")?.trim()
            ?.ifBlank { null }
            ?: mediaItem.posterUrl

        val siteSynopsis = doc?.select(".entry-content p")?.find { p ->
            val t = p.text().lowercase()
            t.contains("storyline") || t.contains("plot") || t.contains("synopsis")
        }?.text()?.substringAfter(":")?.trim()
            ?: doc?.selectFirst("meta[name='description']")?.attr("content")?.trim()
            ?: ""

        // 3. Search or reuse TMDB ID for metadata enrichment
        val resolvedTmdbId: String? = if (mediaItem.id.all { it.isDigit() } && mediaItem.id.isNotBlank()) {
            mediaItem.id
        } else {
            TmdbBridge.searchTmdbId(client, cleanTitle, siteYear, isTv, tmdbApiKey)
        }

        // 4. Enrich metadata via TMDB Bridge
        val enrichedMeta = if (!resolvedTmdbId.isNullOrBlank()) {
            TmdbBridge.fetchEnrichedDetails(client, resolvedTmdbId, isTv, name, tmdbApiKey)
        } else null

        val enrichedPoster = enrichedMeta?.posterUrl ?: sitePoster ?: mediaItem.posterUrl
        val enrichedBackdrop = enrichedMeta?.backdropUrl ?: mediaItem.backdropUrl ?: enrichedPoster
        val enrichedSynopsis = if (siteSynopsis.isNotBlank()) siteSynopsis else (enrichedMeta?.synopsis ?: siteSynopsis)
        val enrichedRating = enrichedMeta?.rating ?: mediaItem.rating
        val enrichedRtRating = enrichedMeta?.rottenTomatoesRating
        val enrichedContentRating = enrichedMeta?.contentRating ?: if (isTv) "TV-14" else "PG-13"
        val enrichedDuration = enrichedMeta?.duration
        val enrichedYear = if (isTv && enrichedMeta?.year != null) enrichedMeta.year else (siteYear ?: enrichedMeta?.year)
        val enrichedImdbId = enrichedMeta?.imdbId
        val enrichedLogo = enrichedMeta?.logoUrl ?: resolveLogo(mediaItem)

        // Prioritize authentic site-native related titles parsed directly from UHDMovies
        val siteRelatedItems = mutableListOf<MediaItem>()
        doc?.select("a[href]")?.forEach { a ->
            val href = a.attr("abs:href").ifBlank { a.attr("href") }.trim()
            val currentSlug = targetUrl.removePrefix(mainUrl).trim('/')
            val isPostLink = href.contains("/download-") &&
                    (currentSlug.isBlank() || !href.contains(currentSlug))
            if (isPostLink) {
                val fullUrl = if (href.startsWith("http")) href else "$mainUrl${if (href.startsWith("/")) "" else "/"}$href"
                val slug = TmdbBridge.sanitizeSlug(fullUrl.removePrefix(mainUrl).trim('/'))
                if (slug.isNotBlank() && siteRelatedItems.none { it.id == slug }) {
                    val rawCardTitle = a.selectFirst(".entry-title, h1, h2, h3, h4")?.text()?.trim()
                        ?: a.text().trim()
                    val cleanRecTitle = if (rawCardTitle.isNotBlank() && rawCardTitle.length > 3) {
                        cleanTitleForDisplay(rawCardTitle)
                    } else {
                        cleanTitleForDisplay(slug.replace("-", " "))
                    }
                    val poster = a.selectFirst("img")?.attr("src")?.trim()
                    val isSeries = href.contains("season", ignoreCase = true) || href.contains("series", ignoreCase = true)
                    siteRelatedItems.add(
                        MediaItem(
                            id = slug,
                            title = cleanRecTitle,
                            url = fullUrl,
                            posterUrl = poster,
                            backdropUrl = poster,
                            type = if (isSeries) MediaType.TV_SERIES else MediaType.MOVIE,
                            provider = name
                        )
                    )
                }
            }
        }
        val enrichedRecs = if (siteRelatedItems.isNotEmpty()) siteRelatedItems.take(12) else (enrichedMeta?.recommendations ?: emptyList())
        val enrichedTrailer = enrichedMeta?.trailerUrl
        val enrichedDirectors = enrichedMeta?.directors ?: emptyList()

        val finalCast = if (enrichedMeta?.cast?.isNotEmpty() == true) enrichedMeta.cast else emptyList()

        val seasonsToFetch = (enrichedMeta?.seasonNumbers ?: listOf(1)).distinct()
        val tmdbSeasonsData = if (isTv && !resolvedTmdbId.isNullOrBlank()) {
            TmdbBridge.fetchTmdbSeasons(client, resolvedTmdbId, seasonsToFetch, tmdbApiKey)
        } else emptyMap()

        // 5. Parse content download variants or TV episodes
        val episodes = mutableListOf<EpisodeItem>()

        if (!isTv) {
            val variants = doc?.let { parseMovieVariants(it, cleanTitle) } ?: emptyList()
            val payload = UhdMoviePayload(
                tmdbId = resolvedTmdbId,
                title = cleanTitle,
                url = targetUrl,
                variants = variants
            )
            val dataJson = json.encodeToString(UhdMoviePayload.serializer(), payload)
            episodes.add(
                EpisodeItem(
                    id = TmdbBridge.sanitizeSlug("${mediaItem.id}-movie"),
                    title = cleanTitle,
                    seasonNumber = 1,
                    episodeNumber = 1,
                    data = dataJson,
                    thumbnail = enrichedBackdrop ?: enrichedPoster ?: sitePoster,
                    description = enrichedSynopsis.ifBlank { null },
                    duration = enrichedDuration
                )
            )
        } else {
            val parsedEpisodes = doc?.let { parseSeriesEpisodes(it, resolvedTmdbId, cleanTitle, targetUrl) } ?: emptyList()

            // Merge with TMDB episode details
            val enrichedEpisodes = parsedEpisodes.map { ep ->
                val tmdbEp = tmdbSeasonsData[ep.seasonNumber]?.get(ep.episodeNumber)
                if (tmdbEp != null) {
                    ep.copy(
                        id = TmdbBridge.sanitizeSlug(ep.id),
                        title = tmdbEp.name.ifBlank { ep.title },
                        thumbnail = tmdbEp.stillUrl ?: ep.thumbnail ?: enrichedBackdrop ?: enrichedPoster ?: sitePoster,
                        description = tmdbEp.overview?.ifBlank { ep.description } ?: ep.description,
                        duration = tmdbEp.duration ?: ep.duration
                    )
                } else {
                    ep.copy(
                        id = TmdbBridge.sanitizeSlug(ep.id),
                        thumbnail = ep.thumbnail ?: enrichedBackdrop ?: enrichedPoster ?: sitePoster
                    )
                }
            }
            episodes.addAll(enrichedEpisodes)
        }

        MediaDetail(
            id = TmdbBridge.sanitizeSlug(mediaItem.id.ifBlank { targetUrl.removePrefix(mainUrl).trim('/') }),
            title = cleanTitle,
            url = targetUrl,
            posterUrl = enrichedPoster ?: sitePoster,
            backdropUrl = enrichedBackdrop ?: enrichedPoster ?: sitePoster,
            type = mediaItem.type,
            year = enrichedYear,
            synopsis = enrichedSynopsis.ifBlank { "High quality 4K UHD and 1080p streaming on UHDMovies." },
            genres = (enrichedMeta?.genres ?: emptyList()).distinct(),
            duration = enrichedDuration,
            rating = enrichedRating,
            contentRating = enrichedContentRating,
            episodes = episodes,
            provider = name,
            cast = finalCast,
            imdbId = enrichedImdbId,
            logoUrl = enrichedLogo,
            rottenTomatoesRating = enrichedRtRating,
            recommendations = enrichedRecs,
            trailerUrl = enrichedTrailer,
            directors = enrichedDirectors
        )
    }

    override suspend fun resolveLogo(mediaItem: MediaItem): String? = withContext(Dispatchers.IO) {
        if (!mediaItem.logoUrl.isNullOrBlank()) return@withContext mediaItem.logoUrl
        val isTv = mediaItem.type == MediaType.TV_SERIES
        val tmdbId = if (mediaItem.url.startsWith("tmdb://")) {
            mediaItem.url.removePrefix("tmdb://")
        } else if (mediaItem.id.all { it.isDigit() } && mediaItem.id.isNotBlank()) {
            mediaItem.id
        } else {
            TmdbBridge.searchTmdbId(client, mediaItem.title, mediaItem.year, isTv, tmdbApiKey)
        }
        if (!tmdbId.isNullOrBlank()) {
            TmdbBridge.resolveLogo(client, tmdbId, isTv, apiKey = tmdbApiKey)
        } else null
    }

    private fun resolveTargetUrl(mediaItem: MediaItem): String {
        return when {
            mediaItem.url.startsWith("http") -> mediaItem.url
            mediaItem.id.startsWith("http") -> mediaItem.id
            mediaItem.id.isNotBlank() && !mediaItem.id.all { it.isDigit() } -> {
                val cleanSlug = mediaItem.id.trim('/')
                "$mainUrl/$cleanSlug/"
            }
            else -> ""
        }
    }

    // ============================================================================
    // 3. Movie and TV Series Content Parser (Ad-Filtered)
    // ============================================================================

    private fun parseMovieVariants(doc: Document, fallbackTitle: String): List<UhdVariantLink> {
        val variants = mutableListOf<UhdVariantLink>()
        val entryContent = doc.selectFirst(".entry-content") ?: return emptyList()
        val elements = entryContent.children()

        var currentHeader = fallbackTitle

        for (el in elements) {
            val text = el.text().trim()

            // Update current release header if element describes resolution/format and doesn't contain sid links
            if (isReleaseHeader(text, el)) {
                currentHeader = text
            }

            // Look for download links
            val links = el.select("a[href*='sid=']")
            for (a in links) {
                val href = a.attr("href").trim()
                val btnText = a.text().trim()

                if (!isValidContentLink(href, btnText)) continue
                val lowerBtn = btnText.lowercase()
                if (!lowerBtn.contains("download") && !lowerBtn.contains("drive") && !lowerBtn.contains("direct") && !lowerBtn.contains("link")) {
                    continue
                }

                val quality = parseQualityLabel(currentHeader)
                val size = parseSizeLabel(currentHeader)

                variants.add(
                    UhdVariantLink(
                        title = currentHeader,
                        quality = quality,
                        size = size,
                        server = "Google Drive (DriveSeed)",
                        linkUrl = href
                    )
                )
            }
        }
        return variants.distinctBy { it.linkUrl }
    }

    private fun parseSeriesEpisodes(
        doc: Document,
        tmdbId: String?,
        seriesTitle: String,
        seriesUrl: String
    ): List<EpisodeItem> {
        val episodeMap = mutableMapOf<Pair<Int, Int>, MutableList<UhdVariantLink>>()
        val entryContent = doc.selectFirst(".entry-content") ?: return emptyList()
        val elements = entryContent.children()

        var currentSeason = 1
        var currentQualityHeader = seriesTitle

        for (el in elements) {
            val text = el.text().trim()

            // Update season if heading mentions Season X
            val seasonMatch = Regex("""\b(?:Season|S)\s*0?(\d+)\b""", RegexOption.IGNORE_CASE).find(text)
            if (seasonMatch != null && (text.length < 100 || isReleaseHeader(text, el))) {
                val sNum = seasonMatch.groupValues[1].toIntOrNull()
                if (sNum != null && sNum > 0) {
                    currentSeason = sNum
                }
            }

            // Update quality header
            if (isReleaseHeader(text, el)) {
                currentQualityHeader = text
                // Also check if release header explicitly specifies S01, S02, etc.
                val sMatch = Regex("""\bS0?(\d+)\b""", RegexOption.IGNORE_CASE).find(text)
                if (sMatch != null) {
                    val sNum = sMatch.groupValues[1].toIntOrNull()
                    if (sNum != null && sNum > 0) currentSeason = sNum
                }
            }

            // Find episode buttons
            val links = el.select("a[href*='sid=']")
            for (a in links) {
                val href = a.attr("href").trim()
                val btnText = a.text().trim()

                if (!isValidContentLink(href, btnText)) continue

                val epNum = extractEpisodeNumber(btnText) ?: continue
                val quality = parseQualityLabel(currentQualityHeader)
                val size = parseSizeLabel(currentQualityHeader)

                val key = Pair(currentSeason, epNum)
                val list = episodeMap.getOrPut(key) { mutableListOf() }
                list.add(
                    UhdVariantLink(
                        title = currentQualityHeader,
                        quality = quality,
                        size = size,
                        server = "Google Drive (DriveSeed)",
                        linkUrl = href
                    )
                )
            }
        }

        return episodeMap.entries
            .sortedWith(compareBy({ it.key.first }, { it.key.second }))
            .map { (key, variants) ->
                val (seasonNum, epNum) = key
                val payload = UhdTvPayload(
                    tmdbId = tmdbId,
                    season = seasonNum,
                    episode = epNum,
                    title = "$seriesTitle S${seasonNum}E$epNum",
                    variants = variants
                )
                val dataJson = json.encodeToString(UhdTvPayload.serializer(), payload)
                val firstVariantTitle = variants.firstOrNull()?.title ?: seriesTitle
                val audioTracks = parseAudioTracksFromTitle(firstVariantTitle)
                val releaseType = determineReleaseType(firstVariantTitle, audioTracks)

                EpisodeItem(
                    id = "$seriesUrl:S${seasonNum}E$epNum",
                    title = "Episode $epNum",
                    seasonNumber = seasonNum,
                    episodeNumber = epNum,
                    data = dataJson,
                    audioType = releaseType,
                    availableLanguages = audioTracks.map { it.languageName }.distinct()
                )
            }
    }

    private fun isReleaseHeader(text: String, el: Element): Boolean {
        if (el.selectFirst("a[href*='sid=']") != null) return false
        val lower = text.lowercase()
        val hasQuality = lower.contains("1080p") || lower.contains("2160p") || lower.contains("4k") ||
                lower.contains("720p") || lower.contains("480p") || lower.contains("remux") ||
                lower.contains("web-dl") || lower.contains("bluray") || lower.contains("hevc")
        val hasSizeOrGroup = lower.contains("gb") || lower.contains("mb") || lower.contains("season") ||
                lower.contains("flux") || lower.contains("darq") || lower.contains("kingsman") ||
                lower.contains("spidey") || lower.contains("nogrp") || lower.contains("norvine")
        return hasQuality && (hasSizeOrGroup || text.length > 25)
    }

    private fun isValidContentLink(href: String, btnText: String): Boolean {
        val sid = href.substringAfter("sid=").substringBefore("&")
        // Genuine encrypted media links have a long base64 SID (> 150 chars), while ad/tag links are short (< 100 chars)
        if (sid.length < 150) return false
        if (sid.startsWith("YTZTdj", ignoreCase = true)) return false

        val lower = btnText.lowercase()
        val isAd = lower.contains("1080p uhd") || lower.contains("4k hdr") || lower.contains("hevc") ||
                lower.contains("3d movies") || lower.contains("60fps") || lower.contains("uhdmovies") ||
                lower.contains("telegram") || lower.contains("whatsapp")
        return !isAd
    }

    private fun extractEpisodeNumber(btnText: String): Int? {
        val match = Regex("""(?:Episode|Ep|E)\s*0?(\d+)""", RegexOption.IGNORE_CASE).find(btnText)
        return match?.groupValues?.get(1)?.toIntOrNull()
    }

    private fun parseQualityLabel(text: String): String {
        return when {
            text.contains("2160p", ignoreCase = true) || text.contains("4K", ignoreCase = true) -> "4K 2160p"
            text.contains("1080p", ignoreCase = true) -> "1080p FHD"
            text.contains("720p", ignoreCase = true) -> "720p HD"
            text.contains("480p", ignoreCase = true) -> "480p SD"
            else -> "HD"
        }
    }

    private fun parseSizeLabel(text: String): String {
        val match = Regex("""(\d+(?:\.\d+)?\s*(?:GB|MB))""", RegexOption.IGNORE_CASE).find(text)
        return match?.groupValues?.get(1) ?: "HD"
    }

    private fun parseAudioTracksFromTitle(title: String): List<AudioTrackDescriptor> {
        val tracks = mutableListOf<AudioTrackDescriptor>()
        val lower = title.lowercase()

        val languages = listOf(
            "hindi" to Pair("Hindi", "hi"),
            "english" to Pair("English", "en"),
            "tamil" to Pair("Tamil", "ta"),
            "telugu" to Pair("Telugu", "te"),
            "kannada" to Pair("Kannada", "kn"),
            "malayalam" to Pair("Malayalam", "ml"),
            "bengali" to Pair("Bengali", "bn"),
            "marathi" to Pair("Marathi", "mr"),
            "punjabi" to Pair("Punjabi", "pa"),
            "spanish" to Pair("Spanish", "es"),
            "korean" to Pair("Korean", "ko"),
            "japanese" to Pair("Japanese", "ja"),
            "french" to Pair("French", "fr"),
            "german" to Pair("German", "de")
        )

        for ((key, pair) in languages) {
            if (lower.contains(key)) {
                tracks.add(
                    AudioTrackDescriptor(
                        languageName = pair.first,
                        isoCode = pair.second,
                        channels = if (lower.contains("5.1")) 6 else 2,
                        codec = if (lower.contains("ddp") || lower.contains("eac3")) "DDP 5.1" else "AAC"
                    )
                )
            }
        }

        if (tracks.isEmpty()) {
            if (lower.contains("dual") || lower.contains("multi")) {
                tracks.add(AudioTrackDescriptor("Hindi", "hi", 6, "DDP 5.1"))
                tracks.add(AudioTrackDescriptor("English", "en", 6, "DDP 5.1"))
            } else {
                tracks.add(AudioTrackDescriptor("Original", "und", 2, "AAC"))
            }
        }
        return tracks
    }

    private fun determineReleaseType(title: String, tracks: List<AudioTrackDescriptor>): AudioReleaseType {
        val lower = title.lowercase()
        return when {
            lower.contains("multi") || lower.contains("dual") || tracks.size > 1 -> AudioReleaseType.DUAL_AUDIO
            lower.contains("dub") -> AudioReleaseType.DUB
            lower.contains("sub") || lower.contains("esub") -> AudioReleaseType.SUB
            else -> AudioReleaseType.ORIGINAL
        }
    }

    // ============================================================================
    // 4. LinkPilot & DriveSeed Multi-Stage Redirection Bypass Engine
    // ============================================================================

    /**
     * Bypasses the 4-step LinkPilot / thenaukriadda mediator form chain:
     * Step 1: GET init_url -> extract form action 1 and hidden inputs.
     * Step 2: POST form 1 -> extract action 2 and hidden inputs (_lp_http2, _lp_token, _lp_chain).
     * Step 3: POST form 2 -> extract lp_go URL & document.cookie ("lp-<id>", "<val>").
     * Step 4: GET lp_go URL with Cookie -> extract destination DriveSeed redirect URL.
     */
    fun bypassLinkPilot(initUrl: String): String? {
        return try {
            val headers = mutableMapOf(
                "User-Agent" to defaultUserAgent,
                "Referer" to "https://uhdmovies.my/",
                "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            )

            // Step 1: GET init_url
            val req1 = Request.Builder().url(initUrl).apply { headers.forEach { (k, v) -> header(k, v) } }.build()
            val html1 = fastMediatorClient.newCall(req1).execute().use { resp ->
                if (!resp.isSuccessful) return null
                resp.body?.string() ?: ""
            }

            val doc1 = Jsoup.parse(html1, initUrl)
            val form1 = doc1.selectFirst("form:has(input[name]), form") ?: return null
            val action1 = form1.attr("abs:action").ifBlank { form1.attr("action") }
            val resolvedAction1 = if (action1.startsWith("http")) action1 else "https://en.thenaukriadda.in${if (action1.startsWith("/")) "" else "/"}$action1"

            val body1Builder = FormBody.Builder()
            form1.select("input[name]").forEach { input ->
                body1Builder.add(input.attr("name"), input.attr("value"))
            }

            // Step 2: POST form 1
            headers["Referer"] = initUrl
            val req2 = Request.Builder()
                .url(resolvedAction1)
                .apply { headers.forEach { (k, v) -> header(k, v) } }
                .post(body1Builder.build())
                .build()

            var step2Url = resolvedAction1
            val html2 = fastMediatorClient.newCall(req2).execute().use { resp ->
                step2Url = resp.request.url.toString()
                if (!resp.isSuccessful) return null
                resp.body?.string() ?: ""
            }

            val doc2 = Jsoup.parse(html2, step2Url)
            val form2 = doc2.selectFirst("form:has(input[name*='lp']), form[id*='lp'], form#lp-s1-form, form") ?: return null
            val action2 = form2.attr("abs:action").ifBlank { form2.attr("action") }
            val resolvedAction2 = if (action2.startsWith("http")) action2 else "https://en.thenaukriadda.in${if (action2.startsWith("/")) "" else "/"}$action2"

            val body2Builder = FormBody.Builder()
            form2.select("input[name]").forEach { input ->
                body2Builder.add(input.attr("name"), input.attr("value"))
            }

            // Step 3: POST form 2
            headers["Referer"] = step2Url
            val req3 = Request.Builder()
                .url(resolvedAction2)
                .apply { headers.forEach { (k, v) -> header(k, v) } }
                .post(body2Builder.build())
                .build()

            var step3Url = resolvedAction2
            val html3 = fastMediatorClient.newCall(req3).execute().use { resp ->
                step3Url = resp.request.url.toString()
                if (!resp.isSuccessful) return null
                resp.body?.string() ?: ""
            }

            val goMatch = Regex("""bg\.setAttribute\(\s*['"]href['"]\s*,\s*['"]([^'"]+)['"]""").find(html3) ?: return null
            val goUrl = goMatch.groupValues[1].replace("\\/", "/")

            val scMatch = Regex("""sc\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]""").find(html3)
            if (scMatch != null) {
                val cookieName = scMatch.groupValues[1]
                val cookieVal = scMatch.groupValues[2].replace("\\/", "/")
                headers["Cookie"] = "$cookieName=$cookieVal"
            }
            headers["Referer"] = step3Url

            // Step 4: GET lp_go URL with Cookie
            val req4 = Request.Builder().url(goUrl).apply { headers.forEach { (k, v) -> header(k, v) } }.build()
            var finalUrl4 = goUrl
            val html4 = fastMediatorClient.newCall(req4).execute().use { resp ->
                finalUrl4 = resp.request.url.toString()
                if (!resp.isSuccessful) return null
                resp.body?.string() ?: ""
            }

            if (finalUrl4.contains("driveseed.org") || finalUrl4.contains("hubcloud")) {
                return finalUrl4
            }

            val destMatch = Regex("""window\.location\.replace\(\s*['"]([^'"]+)['"]\s*\)""").find(html4)
                ?: Regex("""content=["']\d+;\s*url=([^"']+)["']""", RegexOption.IGNORE_CASE).find(html4)
                ?: Regex("""window\.location(?:\.href)?\s*=\s*['"]([^'"]+)['"]""").find(html4)
            destMatch?.groupValues?.get(1)?.replace("\\/", "/")?.replace("&amp;", "&")
        } catch (_: Exception) {
            null
        }
    }

    /**
     * Resolves a DriveSeed destination URL into direct Google CDN and Instant Download video streams:
     * Step A: GET driveseed.org/r?key=... -> extract window.location.replace("/file/<FILE_ID>").
     * Step B: GET driveseed.org/file/<FILE_ID> -> extract cdn.video-gen.xyz instant download link.
     * Step C: GET cdn.video-gen.xyz (without following redirects) -> capture HTTP 302 Location header.
     * Step D: Extract direct Google CDN URL (video-downloads.googleusercontent.com) from Location.
     */
    fun resolveDriveSeed(driveseedUrl: String): List<UhdDirectServer> {
        val servers = mutableListOf<UhdDirectServer>()
        try {
            val headers = mutableMapOf(
                "User-Agent" to defaultUserAgent,
                "Referer" to "https://en.thenaukriadda.in/"
            )

            // Step A: GET /r?key=...
            val reqR = Request.Builder().url(driveseedUrl).apply { headers.forEach { (k, v) -> header(k, v) } }.build()
            val htmlR = client.newCall(reqR).execute().use { resp ->
                if (!resp.isSuccessful) return emptyList()
                resp.body?.string() ?: ""
            }

            val fileMatch = Regex("""window\.location\.replace\(\s*['"]([^'"]+)['"]\s*\)""").find(htmlR) ?: return emptyList()
            val filePath = fileMatch.groupValues[1].replace("\\/", "/")
            val fileUrl = if (filePath.startsWith("http")) filePath else "https://driveseed.org${if (filePath.startsWith("/")) "" else "/"}$filePath"

            // Step B: GET /file/<FILE_ID>
            headers["Referer"] = driveseedUrl
            val reqFile = Request.Builder().url(fileUrl).apply { headers.forEach { (k, v) -> header(k, v) } }.build()
            val htmlFile = client.newCall(reqFile).execute().use { resp ->
                if (!resp.isSuccessful) return emptyList()
                resp.body?.string() ?: ""
            }

            val doc = Jsoup.parse(htmlFile, fileUrl)

            if (htmlFile.contains("file has been removed", ignoreCase = true) ||
                htmlFile.contains("file has been deleted", ignoreCase = true) ||
                htmlFile.contains("404 not found", ignoreCase = true)
            ) {
                return emptyList()
            }

            // Look for Instant Download or direct CDN links
            val instantLinks = mutableListOf<String>()

            // 1. Check all anchor tags for Instant / Direct download buttons
            doc.select("a").forEach { a ->
                val text = a.text().lowercase()
                val href = a.attr("abs:href").trim()
                if (href.startsWith("http") && (text.contains("instant") || text.contains("download") || text.contains("direct") || href.contains("video-"))) {
                    if (!href.contains("/login") && !href.contains("/zfile") && !href.contains("t.me") && !href.contains("driveseed.org")) {
                        instantLinks.add(href)
                    }
                }
            }

            // Fallback regex matching if Jsoup didn't find specific button
            if (instantLinks.isEmpty()) {
                val cdnMatch = Regex("""href=['"](https://[^'"]*(?:video-gen|video-plex|video-seed|video-downloads)[^'"]*)['"]""").find(htmlFile)
                cdnMatch?.groupValues?.get(1)?.let { instantLinks.add(it) }
            }

            for (cdnUrl in instantLinks.distinct()) {
                // Step C: GET cdnUrl with noRedirectClient to capture 302 Location
                headers["Referer"] = fileUrl
                val reqCdn = Request.Builder().url(cdnUrl).apply { headers.forEach { (k, v) -> header(k, v) } }.build()

                try {
                    noRedirectClient.newCall(reqCdn).execute().use { cdnResp ->
                        val location = cdnResp.header("Location")
                        if (location != null) {
                            val directUrl = if (location.contains("url=")) {
                                val after = location.substringAfter("url=")
                                if (after.startsWith("http%3A", ignoreCase = true) || after.startsWith("https%3A", ignoreCase = true)) {
                                    val enc = after.substringBefore("&")
                                    try { URLDecoder.decode(enc, "UTF-8") } catch (_: Exception) { enc }
                                } else {
                                    val plain = after.substringBefore("&")
                                    try { URLDecoder.decode(plain, "UTF-8") } catch (_: Exception) { plain }
                                }
                            } else {
                                location
                            }

                            val isCdn = directUrl.contains("googleusercontent.com") || directUrl.contains("drive.google.com")
                            val isSupported = isCdn || directUrl.contains("video-seed.dev") ||
                                    directUrl.contains("video-gen.xyz") || directUrl.contains("video-plex") ||
                                    directUrl.contains("video-downloads") || directUrl.contains("driveseed")

                            if (isSupported) {
                                servers.add(
                                    UhdDirectServer(
                                        name = if (isCdn) "DriveSeed (Google Server)" else "DriveSeed (Fast Cloud)",
                                        url = directUrl,
                                        isGoogleCdn = isCdn
                                    )
                                )
                            }
                        } else if (cdnResp.isSuccessful && (cdnUrl.contains("video-gen.xyz") || cdnUrl.contains("video-seed.dev"))) {
                            servers.add(
                                UhdDirectServer(
                                    name = "DriveSeed (Fast Cloud)",
                                    url = cdnUrl,
                                    isGoogleCdn = false
                                )
                            )
                        }
                    }
                } catch (_: Exception) {}
            }
        } catch (_: Exception) {}
        return servers.distinctBy { it.url }
    }

    // ============================================================================
    // 5. Reactive Stream Flow & Downloads
    // ============================================================================

    override fun getStreamFlow(episodeData: String): Flow<StreamEmission> = channelFlow {
        send(StreamEmission.StatusUpdate(name, "Resolving UHDMovies links and decrypting Google CDN / DriveSeed streams..."))

        val emittedUrls = ConcurrentHashMap.newKeySet<String>()
        val variants = mutableListOf<UhdVariantLink>()

        try {
            if (episodeData.contains("\"variants\"")) {
                val root = json.parseToJsonElement(episodeData).jsonObject
                val varArr = root["variants"]?.jsonArray
                varArr?.forEach { elem ->
                    try {
                        val v = json.decodeFromJsonElement(UhdVariantLink.serializer(), elem)
                        variants.add(v)
                    } catch (_: Exception) {}
                }
            }
        } catch (_: Exception) {}

        val topVariants = variants.distinctBy { it.linkUrl }.take(6)
        val jobs = topVariants.map { variant ->
            launch {
                try {
                    val driveseedUrl = bypassLinkPilot(variant.linkUrl) ?: return@launch
                    val directServers = resolveDriveSeed(driveseedUrl)

                    val audioTracks = parseAudioTracksFromTitle(variant.title)
                    val releaseType = determineReleaseType(variant.title, audioTracks)

                    for (server in directServers) {
                        if (emittedUrls.add(server.url)) {
                            val serverLabel = "${server.name} [${variant.quality}]"
                            val streamSource = StreamSource(
                                url = server.url,
                                serverName = serverLabel,
                                resolutionLabel = variant.quality,
                                quality = variant.quality,
                                isM3u8 = false,
                                releaseType = releaseType,
                                audioTracks = audioTracks,
                                headers = mapOf(
                                    "User-Agent" to defaultUserAgent,
                                    "Referer" to "https://driveseed.org/",
                                    "Accept-Ranges" to "bytes"
                                )
                            )
                            val existing = streamCache[episodeData]?.second?.streams ?: emptyList()
                            streamCache[episodeData] = System.currentTimeMillis() to StreamResult((existing + streamSource).distinctBy { it.url }, emptyList())
                            send(StreamEmission.SourceFound(streamSource))
                        }
                    }
                } catch (_: Exception) {}
            }
        }
        jobs.joinAll()

        if (emittedUrls.isEmpty()) {
            send(StreamEmission.StatusUpdate(name, "[Maintenance] UHDMovies upstream redirection is currently down. Please use 4KHDHub, MoviesLeech, or PvrPlay."))
        }
    }

    override suspend fun getStreamLinks(episodeData: String): StreamResult = withContext(Dispatchers.IO) {
        val cached = streamCache[episodeData]
        val now = System.currentTimeMillis()
        if (cached != null && (now - cached.first < 120_000L) && cached.second.streams.isNotEmpty()) {
            return@withContext cached.second
        }

        val streamSources = mutableListOf<StreamSource>()
        val subtitles = mutableListOf<SubtitleTrack>()

        getStreamFlow(episodeData).collect { emission ->
            when (emission) {
                is StreamEmission.SourceFound -> streamSources.add(emission.source)
                is StreamEmission.SubtitleFound -> subtitles.add(emission.track)
                else -> {}
            }
        }

        val sortedStreams = streamSources.sortedWith(
            compareBy<StreamSource> {
                when {
                    it.resolutionLabel.contains("2160") || it.resolutionLabel.contains("4K") -> 0
                    it.resolutionLabel.contains("1080") -> 1
                    it.resolutionLabel.contains("720") -> 2
                    it.resolutionLabel.contains("480") -> 3
                    else -> 4
                }
            }.thenBy { it.serverName }
        )

        val result = StreamResult(sortedStreams, subtitles)
        if (result.streams.isNotEmpty()) {
            streamCache[episodeData] = now to result
        }
        result
    }

    override suspend fun getDownloadLinks(episodeData: String): List<DownloadOption> = withContext(Dispatchers.IO) {
        // Fast path: use cached stream result if available
        val cached = streamCache[episodeData]
        val now = System.currentTimeMillis()
        val streams = if (cached != null && (now - cached.first < 120_000L) && cached.second.streams.isNotEmpty()) {
            cached.second.streams
        } else {
            try {
                getStreamLinks(episodeData).streams
            } catch (_: Exception) {
                emptyList()
            }
        }

        if (streams.isNotEmpty()) {
            return@withContext streams.map { stream ->
                val size = when {
                    stream.resolutionLabel.contains("2160") || stream.resolutionLabel.contains("4K") -> "~6.5 GB"
                    stream.resolutionLabel.contains("1080") -> "~2.5 GB"
                    stream.resolutionLabel.contains("720") -> "~1.2 GB"
                    stream.resolutionLabel.contains("480") -> "~450 MB"
                    else -> "~1.8 GB"
                }

                DownloadOption(
                    title = "${stream.quality} - UHDMovies Direct",
                    quality = stream.resolutionLabel,
                    size = size,
                    url = stream.url,
                    source = stream.serverName,
                    provider = name,
                    headers = mapOf(
                        "User-Agent" to defaultUserAgent,
                        "Referer" to "https://driveseed.org/",
                        "Accept-Ranges" to "bytes"
                    )
                )
            }
        }

        // Fallback: direct variant resolution
        val downloadOptions = mutableListOf<DownloadOption>()
        val variants = mutableListOf<UhdVariantLink>()

        try {
            if (episodeData.contains("\"variants\"")) {
                val root = json.parseToJsonElement(episodeData).jsonObject
                val varArr = root["variants"]?.jsonArray
                varArr?.forEach { elem ->
                    try {
                        val v = json.decodeFromJsonElement(UhdVariantLink.serializer(), elem)
                        variants.add(v)
                    } catch (_: Exception) {}
                }
            }
        } catch (_: Exception) {}

        val resolved = variants.distinctBy { it.linkUrl }.take(6).parallelMapIsolated { variant ->
            try {
                val driveseedUrl = bypassLinkPilot(variant.linkUrl) ?: return@parallelMapIsolated emptyList()
                val servers = resolveDriveSeed(driveseedUrl)
                servers.map { s ->
                    DownloadOption(
                        title = "${variant.title} - ${s.name}",
                        quality = variant.quality,
                        size = variant.size,
                        url = s.url,
                        source = s.name,
                        provider = name,
                        headers = mapOf(
                            "User-Agent" to defaultUserAgent,
                            "Referer" to "https://driveseed.org/",
                            "Accept-Ranges" to "bytes"
                        )
                    )
                }
            } catch (_: Exception) {
                emptyList()
            }
        }.flatten()

        downloadOptions.addAll(resolved)
        downloadOptions
    }

    // ============================================================================
    // Internal Serialized Models
    // ============================================================================

    @Serializable
    data class UhdVariantLink(
        val title: String,
        val quality: String,
        val size: String,
        val server: String,
        val linkUrl: String
    )

    @Serializable
    data class UhdMoviePayload(
        val tmdbId: String?,
        val title: String,
        val url: String,
        val variants: List<UhdVariantLink>
    )

    @Serializable
    data class UhdTvPayload(
        val tmdbId: String?,
        val season: Int,
        val episode: Int,
        val title: String,
        val variants: List<UhdVariantLink>
    )

    data class UhdDirectServer(
        val name: String,
        val url: String,
        val isGoogleCdn: Boolean = false
    )

    override suspend fun fetchCast(mediaId: String, imdbId: String?, type: MediaType): List<CastMember> = emptyList()
}

