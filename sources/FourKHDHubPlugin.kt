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
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import okhttp3.OkHttpClient
import okhttp3.Request
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import java.net.URLEncoder
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * 4KHDHub Universal Media Provider Implementation.
 *
 * Scrapes metadata and extracts direct UHD, 4K HDR, 1080p, REMUX, and multi-language streams
 * from https://4khdhub.one/ with a full multi-stage bypass engine for Greenmotors, HubCloud,
 * and Gamerxyt mediators to produce direct seekable Cloudflare R2, GPDL, and Buzz media streams.
 *
 * Enriches missing metadata (16:9 backdrops, numerical ratings, content ratings, episode stills,
 * and actor profile photos) via TMDB API when not provided on the website.
 */
class FourKHDHubPlugin(
    private val client: OkHttpClient = DohDns.createOkHttpClient(
        connectTimeoutSeconds = 15,
        readTimeoutSeconds = 25
    )
) : UniversalPlugin {

    override val name: String = "4KHDHub"
    override val mainUrl: String = "https://4khdhub.one"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE, MediaType.TV_SERIES)

    private val json = Json { ignoreUnknownKeys = true; isLenient = true }

    private val defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    private val tmdbApiKey = "1865f43a0549ca50d341dd9ab8b29f49"

    private val defaultHeaders = mapOf(
        "User-Agent" to defaultUserAgent,
        "Referer" to "https://4khdhub.one/",
        "Accept" to "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    )

    // ============================================================================
    // 1. Home Catalog & Search
    // ============================================================================

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        coroutineScope {
            val categories = listOf(
                "Latest Releases" to "$mainUrl/",
                "4K Ultra HD & 2160p HDR" to "$mainUrl/category/2160p-HDR/",
                "Movies (1080p & 4K)" to "$mainUrl/category/movies/",
                "Web Series & TV Shows" to "$mainUrl/category/series/",
                "Hindi Movies" to "$mainUrl/category/hindi-movies/",
                "English Movies" to "$mainUrl/category/english-movies/",
                "Hindi Web Series" to "$mainUrl/category/hindi-series/",
                "English Web Series" to "$mainUrl/category/english-series/",
                "Netflix Originals" to "$mainUrl/category/netflix/",
                "Amazon Prime Video" to "$mainUrl/category/amazon_prime_video/",
                "JioHotstar Specials" to "$mainUrl/category/jiohotstar/",
                "HBO Max" to "$mainUrl/category/hbo_max/",
                "Apple TV+" to "$mainUrl/category/apple_tv/",
                "Anime Series & Movies" to "$mainUrl/category/anime/",
                "Korean Drama & Series" to "$mainUrl/category/korean-series/",
                "IMDb Top Rated" to "$mainUrl/category/imdb/"
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
        } catch (e: Exception) {
            emptyList()
        }
    }

    /**
     * Cleans 4KHDHub release titles into canonical movie/show titles suitable for
     * user display and TMDB lookups.
     */
    fun cleanTitleForDisplay(raw: String): String {
        return raw
            .replace(Regex("""(?i)^download\s+"""), "")
            .replace(Regex("""\[.*?\]"""), "")
            .replace(Regex("""\{.*?\}"""), "")
            .replace(Regex("""(?i)\((?:season|s\d|\d{4}).*?\)"""), "")
            .replace(Regex("""(?i)\b(1080p|2160p|4k|720p|480p|hevc|web-dl|bluray|esubs?|multi-audio|multi\s+audio|dual\s+audio|dual|org|remux|hdr|dovi|dv|sdr|dsnp|nf|amzn|x264|x265|english\s+movie|hindi\s+movie|movie|complete|all\s+episodes|all\s+seasons|season\s*\d+.*|s\d+.*)\b.*"""), "")
            .replace("||", "")
            .trim(' ', '-', ':')
            .ifBlank { raw }
    }

    private fun parseMovieCards(doc: Document): List<MediaItem> {
        val items = mutableListOf<MediaItem>()
        val cards = doc.select("a.movie-card")

        for (card in cards) {
            val rawHref = card.attr("href").trim()
            if (rawHref.isBlank()) continue

            val fullUrl = if (rawHref.startsWith("http")) rawHref else "$mainUrl${if (rawHref.startsWith("/")) "" else "/"}$rawHref"
            val rawTitle = card.selectFirst(".movie-card-title")?.text()?.trim() ?: continue
            val title = cleanTitleForDisplay(rawTitle)
            val posterUrl = card.selectFirst("img")?.attr("src")?.trim()

            val metaText = card.selectFirst(".movie-card-meta")?.text()?.trim() ?: ""
            val year = Regex("""\b(19\d\d|20\d\d)\b""").find(metaText)?.groupValues?.get(1)?.toIntOrNull()

            val qualityBadges = card.select(".quality-badge").map { it.text().trim() }
            val formats = card.select(".movie-card-format").map { it.text().trim() }

            val isSeries = rawHref.contains("-series-", ignoreCase = true) ||
                    metaText.contains("EP", ignoreCase = true) ||
                    metaText.contains("S0", ignoreCase = true) ||
                    formats.any { it.equals("Series", ignoreCase = true) }

            val mediaType = if (isSeries) MediaType.TV_SERIES else MediaType.MOVIE

            val qualityLabel = if (qualityBadges.isNotEmpty()) {
                qualityBadges.joinToString(" • ")
            } else {
                formats.filter { it.contains("p") || it.contains("HDR") || it.contains("4K") }.joinToString(" • ").ifBlank { "HD" }
            }

            val id = TmdbBridge.sanitizeSlug(fullUrl.removePrefix(mainUrl).trim('/'))

            items.add(
                MediaItem(
                    id = id,
                    title = title,
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

        // 1. Resolve authentic 4KHDHub web URL
        var targetUrl = resolveTargetUrl(mediaItem)
        var doc: Document? = null

        if (targetUrl.isNotBlank()) {
            try {
                doc = client.getHtml(targetUrl, defaultHeaders)
            } catch (_: Exception) {}
        }

        // If direct slug URL was invalid or empty, fallback to searching the title on 4khdhub
        if (doc == null && mediaItem.title.isNotBlank()) {
            try {
                val searchResults = search(mediaItem.title)
                val targetClean = cleanTitleForDisplay(mediaItem.title).lowercase().trim()
                val match = searchResults.firstOrNull {
                    it.title.equals(mediaItem.title, ignoreCase = true) ||
                            cleanTitleForDisplay(it.title).equals(targetClean, ignoreCase = true) ||
                            it.url.contains(mediaItem.id.trim('/'), ignoreCase = true)
                } ?: searchResults.firstOrNull {
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

        // 2. Extract site-native metadata
        val rawTitle = doc?.selectFirst("h1.page-title, .page-title, h1, .movie-card-title")?.text()?.trim()
            ?.ifBlank { null }
            ?: doc?.title()?.substringBefore(" - ")?.trim()?.ifBlank { null }
            ?: mediaItem.title

        val cleanTitle = cleanTitleForDisplay(rawTitle).ifBlank { mediaItem.title }
        val siteTitle = cleanTitle

        val siteSynopsis = doc?.selectFirst(".content-section p.mt-4, p.mt-4")?.text()?.trim()
            ?: doc?.selectFirst("meta[name='description']")?.attr("content")?.trim()
            ?: ""

        val sitePoster = doc?.selectFirst(".poster-image img, img[src*='image.tmdb.org']")?.attr("src")?.trim()
            ?.ifBlank { null }
            ?: mediaItem.posterUrl

        val siteYear = Regex("""\b(19\d\d|20\d\d)\b""").find(rawTitle)?.groupValues?.get(1)?.toIntOrNull() ?: mediaItem.year
        val siteGenres = mutableListOf<String>()
        val siteStars = mutableListOf<CastMember>()

        doc?.select(".metadata-item")?.forEach { item ->
            val label = item.selectFirst(".metadata-label")?.text()?.trim() ?: ""
            val value = item.selectFirst(".metadata-value")?.text()?.trim() ?: ""

            when {
                label.contains("Stars", ignoreCase = true) -> {
                    value.split(",").forEach { nameWithRole ->
                        val cleanName = nameWithRole.substringBefore("(").trim()
                        val charName = if (nameWithRole.contains("(")) {
                            nameWithRole.substringAfter("(").substringBefore(")").trim()
                        } else null
                        if (cleanName.isNotBlank()) {
                            siteStars.add(CastMember(id = cleanName, name = cleanName, character = charName))
                        }
                    }
                }
                label.contains("Release", ignoreCase = true) || label.contains("Air", ignoreCase = true) -> {
                    val y = Regex("""\b(19\d\d|20\d\d)\b""").find(value)?.groupValues?.get(1)?.toIntOrNull()
                    if (y != null) {
                        // Keep siteYear from rawTitle if present, otherwise set
                    }
                }
            }
        }

        doc?.select(".badge-outline, .movie-card-format")?.forEach {
            val g = it.text().trim()
            if (g.isNotBlank() && !g.contains("p") && !g.contains("GB") && !g.contains("Audio") && !g.contains("DL")) {
                siteGenres.add(g)
            }
        }

        // 3. Extract or resolve TMDB ID
        val htmlString = doc?.html() ?: ""
        val tmdbMatch = Regex("""defaultVideoId\s*=\s*['"](\d+)['"]""").find(htmlString)
        var resolvedTmdbId: String? = tmdbMatch?.groupValues?.get(1)

        if (resolvedTmdbId == null && mediaItem.id.all { it.isDigit() } && mediaItem.id.isNotBlank()) {
            resolvedTmdbId = mediaItem.id
        }

        if (resolvedTmdbId == null && cleanTitle.isNotBlank()) {
            resolvedTmdbId = TmdbBridge.searchTmdbId(client, cleanTitle, siteYear, isTv, tmdbApiKey)
        }

        // 4. Enrich missing metadata via TMDB Bridge
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

        // Prioritize authentic site-native related titles parsed directly from 4khdhub page
        val siteRelatedItems = mutableListOf<MediaItem>()
        doc?.select("a[href]")?.forEach { a ->
            val href = a.attr("href").trim()
            val currentSlug = targetUrl.removePrefix(mainUrl).trim('/')
            val isPostLink = (href.contains("-movie-") || href.contains("-series-")) &&
                    (currentSlug.isBlank() || !href.contains(currentSlug))
            if (isPostLink) {
                val fullUrl = if (href.startsWith("http")) href else "$mainUrl${if (href.startsWith("/")) "" else "/"}$href"
                val slug = TmdbBridge.sanitizeSlug(fullUrl.removePrefix(mainUrl).trim('/'))
                if (slug.isNotBlank() && siteRelatedItems.none { it.id == slug }) {
                    val rawCardTitle = a.selectFirst(".movie-card-title, h1, h2, h3, h4")?.text()?.trim()
                    val cleanRecTitle = if (!rawCardTitle.isNullOrBlank()) cleanTitleForDisplay(rawCardTitle) else cleanTitleForDisplay(slug.replace("-", " "))
                    val poster = a.selectFirst("img")?.attr("src")?.trim()
                    val isSeries = href.contains("-series-")
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

        val finalCast = mutableListOf<CastMember>()
        if (siteStars.isNotEmpty() && enrichedMeta?.cast?.isNotEmpty() == true) {
            val tmdbActorMap = enrichedMeta.cast.associateBy { it.name.lowercase().trim() }
            for (siteActor in siteStars) {
                val matched = tmdbActorMap[siteActor.name.lowercase().trim()]
                if (matched != null) {
                    finalCast.add(
                        siteActor.copy(
                            profileUrl = matched.profileUrl ?: siteActor.profileUrl,
                            character = siteActor.character ?: matched.character
                        )
                    )
                } else {
                    finalCast.add(siteActor)
                }
            }
        } else if (enrichedMeta?.cast?.isNotEmpty() == true) {
            finalCast.addAll(enrichedMeta.cast)
        } else {
            finalCast.addAll(siteStars)
        }

        val seasonsToFetch = (enrichedMeta?.seasonNumbers ?: listOf(1)).distinct()
        val tmdbSeasonsData = if (isTv && !resolvedTmdbId.isNullOrBlank()) {
            TmdbBridge.fetchTmdbSeasons(client, resolvedTmdbId, seasonsToFetch, tmdbApiKey)
        } else emptyMap()

        // 5. Parse 4KHDHub stream/download mirrors
        val episodes = mutableListOf<EpisodeItem>()

        if (!isTv) {
            val variants = doc?.let { parseMovieDownloadVariants(it) } ?: emptyList()
            val payload = MoviePayload(
                tmdbId = resolvedTmdbId,
                title = siteTitle,
                url = targetUrl,
                variants = variants
            )
            val dataJson = json.encodeToString(MoviePayload.serializer(), payload)
            episodes.add(
                EpisodeItem(
                    id = TmdbBridge.sanitizeSlug("${mediaItem.id}-movie"),
                    title = siteTitle,
                    seasonNumber = 1,
                    episodeNumber = 1,
                    data = dataJson,
                    thumbnail = enrichedBackdrop ?: enrichedPoster ?: sitePoster,
                    description = enrichedSynopsis.ifBlank { null },
                    duration = enrichedDuration
                )
            )
        } else {
            val parsedEpisodes = doc?.let { parseSeriesEpisodes(it, resolvedTmdbId, siteTitle, targetUrl) } ?: emptyList()

            // Merge with TMDB episode names, stills, and descriptions
            val enrichedEpisodes = parsedEpisodes.map { ep ->
                val tmdbEp = tmdbSeasonsData[ep.seasonNumber]?.get(ep.episodeNumber)
                if (tmdbEp != null) {
                    ep.copy(
                        title = tmdbEp.name.ifBlank { ep.title },
                        thumbnail = tmdbEp.stillUrl ?: ep.thumbnail ?: enrichedBackdrop ?: enrichedPoster ?: sitePoster,
                        description = tmdbEp.overview?.ifBlank { ep.description } ?: ep.description,
                        duration = tmdbEp.duration ?: ep.duration
                    )
                } else {
                    ep.copy(
                        thumbnail = ep.thumbnail ?: enrichedBackdrop ?: enrichedPoster ?: sitePoster
                    )
                }
            }
            episodes.addAll(enrichedEpisodes)
        }

        MediaDetail(
            id = TmdbBridge.sanitizeSlug(mediaItem.id.ifBlank { targetUrl.removePrefix(mainUrl).trim('/') }),
            title = siteTitle,
            url = targetUrl,
            posterUrl = enrichedPoster ?: sitePoster,
            backdropUrl = enrichedBackdrop ?: enrichedPoster ?: sitePoster,
            type = mediaItem.type,
            year = enrichedYear,
            synopsis = enrichedSynopsis.ifBlank { "High quality streaming & downloads on 4KHDHub." },
            genres = (siteGenres + (enrichedMeta?.genres ?: emptyList())).distinct(),
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

    private fun parseMovieDownloadVariants(doc: Document): List<VariantLink> {
        val variants = mutableListOf<VariantLink>()
        val downloadItems = doc.select(".download-item")

        for (item in downloadItems) {
            val headerText = item.selectFirst(".download-header .flex-1")?.text()?.trim() ?: ""
            val fileTitle = item.selectFirst(".file-title")?.text()?.trim() ?: headerText

            val size = Regex("""(\d+(?:\.\d+)?\s*(?:GB|MB))""").find(headerText)?.groupValues?.get(1) ?: "Direct"
            val quality = parseQualityLabel(fileTitle.ifBlank { headerText })

            val mediaLinks = item.select("a[href*='greenmotors'], a[href*='hubcloud'], a[href*='gamerxyt'], a[href*='drive']")
            for (a in mediaLinks) {
                val linkUrl = a.attr("href").trim()
                val serverName = a.text().trim().replace("Download", "").trim().ifBlank { "HubCloud" }
                if (linkUrl.isNotBlank()) {
                    variants.add(
                        VariantLink(
                            title = fileTitle.ifBlank { headerText },
                            quality = quality,
                            size = size,
                            server = serverName,
                            greenmotorsUrl = linkUrl
                        )
                    )
                }
            }
        }
        return variants
    }

    private fun extractSeasonAndEpisode(fileTitle: String, badgeText: String, defaultSeason: Int = 1): Pair<Int, Int> {
        // 1. Try S01E02 / s1e2 pattern from fileTitle
        val seMatch = Regex("""[sS](\d{1,2})[eE](\d{1,3})""").find(fileTitle)
        if (seMatch != null) {
            val s = seMatch.groupValues[1].toIntOrNull() ?: defaultSeason
            val e = seMatch.groupValues[2].toIntOrNull() ?: 1
            return Pair(s, e)
        }

        // 2. Try 1x02 / 01x02 pattern
        val xMatch = Regex("""\b(\d{1,2})x(\d{1,3})\b""", RegexOption.IGNORE_CASE).find(fileTitle)
        if (xMatch != null) {
            val s = xMatch.groupValues[1].toIntOrNull() ?: defaultSeason
            val e = xMatch.groupValues[2].toIntOrNull() ?: 1
            return Pair(s, e)
        }

        // 3. Try Season X Episode Y pattern
        val wordsMatch = Regex("""Season\s*(\d{1,2})[\s._-]*Episode\s*(\d{1,3})""", RegexOption.IGNORE_CASE).find(fileTitle)
        if (wordsMatch != null) {
            val s = wordsMatch.groupValues[1].toIntOrNull() ?: defaultSeason
            val e = wordsMatch.groupValues[2].toIntOrNull() ?: 1
            return Pair(s, e)
        }

        // 4. Try badgeText (e.g. "Episode-02", "Episode 2", "EP 03")
        val badgeEpMatch = Regex("""(?:EP|Episode|E)[-_\s.]*(\d+)""", RegexOption.IGNORE_CASE).find(badgeText)
        if (badgeEpMatch != null) {
            val e = badgeEpMatch.groupValues[1].toIntOrNull() ?: 1
            return Pair(defaultSeason, e)
        }

        // 5. Try "Episode.2" or "Episode 2" or "EP02" from fileTitle
        val titleEpMatch = Regex("""(?:EP|Episode|E)[-_\s.]*(\d+)""", RegexOption.IGNORE_CASE).find(fileTitle)
        if (titleEpMatch != null) {
            val e = titleEpMatch.groupValues[1].toIntOrNull() ?: 1
            return Pair(defaultSeason, e)
        }

        return Pair(defaultSeason, 1)
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

        val foundLangs = mutableListOf<Pair<String, String>>()
        for ((key, pair) in languages) {
            if (lower.contains(key)) {
                foundLangs.add(pair)
            }
        }

        val channels = when {
            lower.contains("7.1") -> 8
            lower.contains("5.1") -> 6
            else -> 2
        }

        val codec = when {
            lower.contains("truehd") || lower.contains("atmos") -> "TrueHD Atmos"
            lower.contains("ddp5.1") || lower.contains("ddp 5.1") || lower.contains("eac3") -> "DDP 5.1"
            lower.contains("dts-hd") || lower.contains("dts") -> "DTS-HD"
            lower.contains("aac5.1") || lower.contains("aac 5.1") -> "AAC 5.1"
            lower.contains("aac2.0") || lower.contains("aac 2.0") || lower.contains("aac") -> "AAC Stereo"
            lower.contains("ac3") -> "AC3"
            else -> "AAC"
        }

        if (foundLangs.isNotEmpty()) {
            foundLangs.forEach { (display, iso) ->
                tracks.add(
                    AudioTrackDescriptor(
                        languageName = display,
                        isoCode = iso,
                        channels = channels,
                        codec = codec
                    )
                )
            }
        } else if (lower.contains("dual") || lower.contains("multi")) {
            tracks.add(AudioTrackDescriptor("Hindi", "hi", channels, codec))
            tracks.add(AudioTrackDescriptor("English", "en", channels, codec))
        } else {
            tracks.add(AudioTrackDescriptor("Default", "und", channels, codec))
        }

        return tracks
    }

    private fun determineReleaseType(title: String, tracks: List<AudioTrackDescriptor>): AudioReleaseType {
        val lower = title.lowercase()
        return when {
            lower.contains("multi") || lower.contains("dual") || tracks.size > 1 -> AudioReleaseType.DUAL_AUDIO
            lower.contains("dub") -> AudioReleaseType.DUB
            lower.contains("sub") || lower.contains("msubs") -> AudioReleaseType.SUB
            else -> AudioReleaseType.ORIGINAL
        }
    }

    private fun parseSeriesEpisodes(
        doc: Document,
        tmdbId: String?,
        seriesTitle: String,
        seriesUrl: String
    ): List<EpisodeItem> {
        val episodeMap = mutableMapOf<Pair<Int, Int>, MutableList<VariantLink>>()
        val seasonItems = doc.select(".season-item, .episode-item")

        for (seasonItem in seasonItems) {
            val seasonNumberText = seasonItem.selectFirst(".episode-number")?.text()?.trim() ?: "S01"
            val fallbackSeason = Regex("""\b(?:S|Season\s*)(\d+)\b""", RegexOption.IGNORE_CASE)
                .find(seasonNumberText)?.groupValues?.get(1)?.toIntOrNull() ?: 1

            val seasonQuality = seasonItem.selectFirst(".episode-title")?.text()?.trim() ?: "HD"
            val quality = parseQualityLabel(seasonQuality)

            val epDownloads = seasonItem.select(".episode-download-item")
            
            // First pass: extract raw season and episode numbers for this section
            val rawEps = epDownloads.map { epItem ->
                val fileTitle = epItem.selectFirst(".episode-file-title")?.text()?.trim() ?: ""
                val badgeText = epItem.selectFirst(".badge-psa")?.text()?.trim() ?: ""
                extractSeasonAndEpisode(fileTitle, badgeText, fallbackSeason)
            }

            // Detect if this section uses absolute numbering across seasons (e.g. S02 with Ep 13..25, S03 with Ep 48..59)
            val episodeNumbers = rawEps.map { it.second }.filter { it < 100 }
            val minEp = episodeNumbers.minOrNull() ?: 1
            val isAbsoluteCour = minEp > 1 && (minEp >= epDownloads.size || minEp >= 10)

            for ((idx, epItem) in epDownloads.withIndex()) {
                val fileTitle = epItem.selectFirst(".episode-file-title")?.text()?.trim() ?: ""
                val badgeText = epItem.selectFirst(".badge-psa")?.text()?.trim() ?: ""

                val (rawSeason, rawEp) = rawEps[idx]
                val seasonNumber = rawSeason
                val epNumber = when {
                    isAbsoluteCour -> rawEp - minEp + 1
                    rawEp > epDownloads.size + 10 -> idx + 1 // e.g. S01E75 recap episode placed at end of 12-ep season
                    else -> rawEp
                }

                val resolvedTitle = fileTitle.ifBlank { "$seriesTitle S${seasonNumber}E$epNumber" }
                val greenmotorsLinks = epItem.select(".episode-links a[href], a[href*='greenmotors'], a[href*='hubcloud'], a[href*='gamerxyt'], a[href*='drive'], a[href*='download'], a[href*='cloud']")
                    .filter { a ->
                        val h = a.attr("href").trim()
                        h.isNotBlank() && !h.startsWith("#") && !h.contains("telegram", ignoreCase = true) && !h.contains("whatsapp", ignoreCase = true)
                    }

                for (a in greenmotorsLinks) {
                    val linkUrl = a.attr("href").trim()
                    val server = a.text().trim().replace("Download", "").trim().ifBlank { "HubCloud" }
                    if (linkUrl.isNotBlank()) {
                        val key = Pair(seasonNumber, epNumber)
                        val list = episodeMap.getOrPut(key) { mutableListOf() }
                        list.add(
                            VariantLink(
                                title = resolvedTitle,
                                quality = quality,
                                size = "HD",
                                server = server,
                                greenmotorsUrl = linkUrl
                            )
                        )
                    }
                }
            }
        }

        // If no season items matched, scan directly for all .episode-download-item in #episodes
        if (episodeMap.isEmpty()) {
            val directDownloads = doc.select("#episodes .episode-download-item, .episode-downloads .episode-download-item")
            for (epItem in directDownloads) {
                val fileTitle = epItem.selectFirst(".episode-file-title")?.text()?.trim() ?: ""
                val badgeText = epItem.selectFirst(".badge-psa")?.text()?.trim() ?: ""
                val (seasonNumber, epNumber) = extractSeasonAndEpisode(fileTitle, badgeText, 1)
                val key = Pair(seasonNumber, epNumber)

                if (!episodeMap.containsKey(key)) {
                    val quality = parseQualityLabel(fileTitle)
                    val greenmotorsLinks = epItem.select(".episode-links a[href], a[href*='greenmotors'], a[href*='hubcloud'], a[href*='gamerxyt'], a[href*='drive'], a[href*='download'], a[href*='cloud']")
                        .filter { a ->
                            val h = a.attr("href").trim()
                            h.isNotBlank() && !h.startsWith("#") && !h.contains("telegram", ignoreCase = true) && !h.contains("whatsapp", ignoreCase = true)
                        }
                    for (a in greenmotorsLinks) {
                        val linkUrl = a.attr("href").trim()
                        val server = a.text().trim().replace("Download", "").trim().ifBlank { "HubCloud" }
                        if (linkUrl.isNotBlank()) {
                            val list = episodeMap.getOrPut(key) { mutableListOf() }
                            list.add(
                                VariantLink(
                                    title = fileTitle.ifBlank { "$seriesTitle S${seasonNumber}E$epNumber" },
                                    quality = quality,
                                    size = "HD",
                                    server = server,
                                    greenmotorsUrl = linkUrl
                                )
                            )
                        }
                    }
                }
            }
        }

        // If no per-episode items found, look for season zip packs
        if (episodeMap.isEmpty()) {
            val downloadItems = doc.select(".download-item")
            for ((idx, item) in downloadItems.withIndex()) {
                val headerText = item.selectFirst(".download-header .flex-1")?.text()?.trim() ?: "Season Pack"
                val quality = parseQualityLabel(headerText)
                val greenmotorsLinks = item.select("a[href*='greenmotors'], a[href*='hubcloud'], a[href*='gamerxyt'], a[href*='drive']")
                val variants = greenmotorsLinks.mapNotNull { a ->
                    val href = a.attr("href").trim()
                    if (href.isNotBlank()) {
                        VariantLink(
                            title = headerText,
                            quality = quality,
                            size = "Zip Pack",
                            server = a.text().trim(),
                            greenmotorsUrl = href
                        )
                    } else null
                }
                if (variants.isNotEmpty()) {
                    val key = Pair(1, idx + 1)
                    episodeMap[key] = variants.toMutableList()
                }
            }
        }

        // Parse global series audio tracks to ensure consistency
        val seriesAudioTracks = parseAudioTracksFromTitle(seriesTitle)
        val seriesReleaseType = determineReleaseType(seriesTitle, seriesAudioTracks)

        val seriesSlug = TmdbBridge.sanitizeSlug(seriesUrl.removePrefix(mainUrl).trim('/'))

        return episodeMap.entries
            .sortedWith(compareBy({ it.key.first }, { it.key.second }))
            .map { (key, variants) ->
                val (seasonNum, epNum) = key
                val payload = TvPayload(
                    tmdbId = tmdbId,
                    season = seasonNum,
                    episode = epNum,
                    title = "$seriesTitle S${seasonNum}E$epNum",
                    variants = variants
                )
                val dataJson = json.encodeToString(TvPayload.serializer(), payload)
                val allTitles = (variants.map { it.title } + seriesTitle).joinToString(" ")
                val audioTracks = parseAudioTracksFromTitle(allTitles).ifEmpty { seriesAudioTracks }
                val releaseType = if (seriesReleaseType == AudioReleaseType.DUAL_AUDIO) AudioReleaseType.DUAL_AUDIO else determineReleaseType(allTitles, audioTracks)

                EpisodeItem(
                    id = "${seriesSlug}:S${seasonNum}E$epNum",
                    title = "Episode $epNum",
                    seasonNumber = seasonNum,
                    episodeNumber = epNum,
                    data = dataJson,
                    audioType = releaseType,
                    availableLanguages = audioTracks.map { it.languageName }.distinct().ifEmpty { seriesAudioTracks.map { it.languageName }.distinct() }
                )
            }
    }

    private fun parseQualityLabel(text: String): String {
        return when {
            text.contains("2160p", ignoreCase = true) || text.contains("4K", ignoreCase = true) -> "4K 2160p"
            text.contains("1080p", ignoreCase = true) -> "1080p FHD"
            text.contains("720p", ignoreCase = true) -> "720p HD"
            text.contains("480p", ignoreCase = true) -> "480p SD"
            else -> "Auto"
        }
    }

    // ============================================================================
    // 4. Multi-Stage Greenmotors & HubCloud Redirection Bypass Engine
    // ============================================================================

    private fun rot13(input: String): String {
        val sb = StringBuilder(input.length)
        for (c in input) {
            when (c) {
                in 'a'..'z' -> sb.append(((c - 'a' + 13) % 26 + 'a'.code).toChar())
                in 'A'..'Z' -> sb.append(((c - 'A' + 13) % 26 + 'A'.code).toChar())
                else -> sb.append(c)
            }
        }
        return sb.toString()
    }

    private fun base64Decode(input: String): String {
        val clean = input.trim().replace("\n", "").replace("\r", "")
        val bytes = Base64.getDecoder().decode(clean)
        return String(bytes, Charsets.UTF_8)
    }

    /**
     * Bypasses the Greenmotors mediator page instantly without countdown:
     * Extracts `s('o', '<PAYLOAD>', 180*1000)` and decrypts via:
     * Base64Decode -> Base64Decode -> ROT13 -> Base64Decode -> JSON parse -> Base64Decode.
     */
    fun bypassGreenmotors(greenmotorsUrl: String): String? {
        val cleanUrl = greenmotorsUrl.trim().replace(" ", "+")
        if (cleanUrl.contains("hubcloud") || cleanUrl.contains("gamerxyt")) {
            return cleanUrl
        }
        return try {
            val req = Request.Builder()
                .url(cleanUrl)
                .header("User-Agent", defaultUserAgent)
                .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .build()

            val html = client.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return null
                resp.body?.string() ?: ""
            }

            // Direct HubCloud redirect or link in HTML
            val directHubMatch = Regex("""https?://[^'"\s]+hubcloud[^'"\s]+""").find(html)?.value
            if (directHubMatch != null && !directHubMatch.contains(".css") && !directHubMatch.contains(".js")) {
                return directHubMatch
            }

            val payloadRegex = Regex("""s\(\s*['"]o['"]\s*,\s*['"]([^'"]+)['"]\s*,\s*\d+""")
            val match = payloadRegex.find(html) ?: return null
            val rawPayload = match.groupValues[1]

            val s1 = base64Decode(rawPayload)
            val s2 = base64Decode(s1)
            val s3 = rot13(s2)
            val s4 = base64Decode(s3)

            val root = json.parseToJsonElement(s4).jsonObject
            val destB64 = root["o"]?.jsonPrimitive?.contentOrNull ?: return null
            base64Decode(destB64)
        } catch (_: Exception) {
            null
        }
    }

    /**
     * Fetches the HubCloud page and extracts the Gamerxyt intermediate resolution endpoint.
     */
    fun resolveHubcloud(hubcloudUrl: String): String? {
        val cleanUrl = hubcloudUrl.trim().replace(" ", "+")
        if (cleanUrl.contains("gamerxyt")) {
            return cleanUrl
        }
        return try {
            val req = Request.Builder()
                .url(cleanUrl)
                .header("User-Agent", defaultUserAgent)
                .header("Referer", "https://greenmotors.cc/")
                .build()

            val html = client.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return null
                resp.body?.string() ?: ""
            }

            val gamerRegex = Regex("""var\s+url\s*=\s*['"](https?://[^'"]+/hubcloud\.php[^'"]+)['"]""")
            val gamerLinkRegex = Regex("""href=['"](https?://[^'"]+/hubcloud\.php[^'"]+)['"]""")
            val genericGamer = Regex("""['"](https?://[^'"]*gamerxyt[^'"]*)['"]""")

            gamerRegex.find(html)?.groupValues?.get(1)
                ?: gamerLinkRegex.find(html)?.groupValues?.get(1)
                ?: genericGamer.find(html)?.groupValues?.get(1)
                ?: if (html.contains("r2.cloudflarestorage") || html.contains("pixeldrain") || html.contains("10Gbps")) cleanUrl else null
        } catch (_: Exception) {
            null
        }
    }

    /**
     * Fetches Gamerxyt and extracts direct download and media streaming servers.
     */
    fun resolveGamerxyt(gamerxytUrl: String): List<ResolvedDirectServer> {
        val servers = mutableListOf<ResolvedDirectServer>()
        try {
            val req = Request.Builder()
                .url(gamerxytUrl)
                .header("User-Agent", defaultUserAgent)
                .header("Referer", "https://hubcloud.ist/")
                .build()

            val html = client.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return emptyList()
                resp.body?.string() ?: ""
            }

            val doc = Jsoup.parse(html, gamerxytUrl)
            val links = doc.select("a[href]")

            for (a in links) {
                val href = a.attr("href").trim()
                val text = a.text().trim()

                val isIgnored = href.contains("telegram", ignoreCase = true) ||
                        href.contains("winexch", ignoreCase = true) ||
                        href.contains("tinyurl", ignoreCase = true) ||
                        href.contains("bonuscaf", ignoreCase = true) ||
                        href.contains("google.com", ignoreCase = true) ||
                        href.contains("snvhost.com", ignoreCase = true) ||
                        href.contains("fuckingfast.net", ignoreCase = true) ||
                        text.contains("Buzz Server", ignoreCase = true) ||
                        href.startsWith("#")

                if (isIgnored) continue

                val isDirectServer = href.contains("r2.cloudflarestorage.com", ignoreCase = true) ||
                        href.contains("gpdl.hubcloud.ist", ignoreCase = true) ||
                        href.contains("pixel.hubcloud.ist", ignoreCase = true) ||
                        href.contains("pixeldrain.", ignoreCase = true) ||
                        href.contains("hbplay.pages.dev", ignoreCase = true) ||
                        text.contains("Download [FSL Server]", ignoreCase = true) ||
                        text.contains("Download [Server : 10Gbps]", ignoreCase = true) ||
                        text.contains("Download [PixelServer", ignoreCase = true) ||
                        text.contains("Watch Online", ignoreCase = true)

                if (isDirectServer) {
                    val directUrl = when {
                        href.contains("hbplay.pages.dev/?u=") -> {
                            val uParam = href.substringAfter("?u=").substringBefore("&")
                            try { base64Decode(uParam) } catch (_: Exception) { href }
                        }
                        href.contains("pixeldrain.") && href.contains("/u/") -> {
                            val fileId = href.substringAfter("/u/").substringBefore("/").substringBefore("?")
                            "https://pixeldrain.com/api/file/$fileId"
                        }
                        href.contains("pixel.hubcloud.ist") || href.contains("gpdl.hubcloud.ist") || text.contains("10Gbps") -> {
                            resolveGoogleCdnLink(href) ?: href
                        }
                        else -> href
                    }

                    // Only emit media streams that are actual media files, never intermediate HTML pages
                    if (directUrl.contains("hubcloud.ist/?id=") && !directUrl.contains("video-downloads")) {
                        continue
                    }

                    val serverName = when {
                        directUrl.contains("googleusercontent.com") || text.contains("10Gbps") -> "HubCloud 10Gbps Google Direct"
                        href.contains("r2.cloudflarestorage.com") || text.contains("FSL") -> "HubCloud FSL (R2 Direct)"
                        directUrl.contains("pixeldrain.com/api/file") -> "Pixeldrain Direct"
                        href.contains("hbplay.pages.dev") || text.contains("Watch Online") -> "HubCloud Web Stream"
                        else -> text.ifBlank { "HubCloud Direct" }
                    }

                    servers.add(
                        ResolvedDirectServer(
                            name = serverName,
                            url = directUrl,
                            isDirectR2 = directUrl.contains("r2.cloudflarestorage.com")
                        )
                    )
                }
            }
        } catch (_: Exception) {}
        return servers.distinctBy { it.url }.sortedByDescending { server ->
            when {
                server.url.contains("googleusercontent.com") || server.name.contains("10Gbps") -> 5
                server.isDirectR2 || server.url.contains("r2.cloudflarestorage.com") || server.name.contains("R2") || server.name.contains("FSL") -> 4
                server.name.contains("Web Stream") -> 3
                server.url.contains("pixeldrain") -> 2
                else -> 1
            }
        }
    }

    private fun resolveGoogleCdnLink(hubcloudUrl: String): String? {
        return try {
            val req = Request.Builder()
                .url(hubcloudUrl)
                .header("User-Agent", defaultUserAgent)
                .header("Referer", "https://gamerxyt.com/")
                .build()
            client.newCall(req).execute().use { resp ->
                val finalUrl = resp.request.url.toString()
                if (finalUrl.contains("dl.php?link=")) {
                    val link = finalUrl.substringAfter("dl.php?link=").substringBefore("&")
                    java.net.URLDecoder.decode(link, "UTF-8")
                } else if (resp.isSuccessful && !finalUrl.contains("hubcloud") && !finalUrl.contains("gamerxyt")) {
                    finalUrl
                } else {
                    null
                }
            }
        } catch (_: Exception) {
            null
        }
    }

    // ============================================================================
    // 5. Reactive Stream Flow & Downloads
    // ============================================================================

    override fun getStreamFlow(episodeData: String): Flow<StreamEmission> = channelFlow {
        send(StreamEmission.StatusUpdate(name, "Resolving 4KHDHub streams and decrypting direct mirrors..."))

        val emittedUrls = ConcurrentHashMap.newKeySet<String>()

        // 1. Decode payload (movie or tv)
        var tmdbId: String? = null
        var seasonNum: Int? = null
        var epNum: Int? = null
        val variants = mutableListOf<VariantLink>()

        try {
            if (episodeData.contains("\"variants\"")) {
                val root = json.parseToJsonElement(episodeData).jsonObject
                tmdbId = root["tmdbId"]?.jsonPrimitive?.contentOrNull
                seasonNum = root["season"]?.jsonPrimitive?.intOrNull
                epNum = root["episode"]?.jsonPrimitive?.intOrNull
                val varArr = root["variants"]?.jsonArray
                varArr?.forEach { elem ->
                    try {
                        val v = json.decodeFromJsonElement(VariantLink.serializer(), elem)
                        variants.add(v)
                    } catch (_: Exception) {}
                }
            }
        } catch (_: Exception) {}

        if (variants.isEmpty()) {
            variants.addAll(resolveVariantsOnDemand(episodeData))
        }

        // 2. Concurrently bypass Greenmotors links and extract direct Cloudflare R2 / 10Gbps media
        val topVariants = variants.distinctBy { it.greenmotorsUrl }.take(6)
        topVariants.forEach { variant ->
            launch {
                try {
                    val hubcloudUrl = bypassGreenmotors(variant.greenmotorsUrl) ?: return@launch
                    val gamerxytUrl = resolveHubcloud(hubcloudUrl) ?: return@launch
                    val directServers = resolveGamerxyt(gamerxytUrl)

                    val audioTracks = parseAudioTracksFromTitle(variant.title)
                    val releaseType = determineReleaseType(variant.title, audioTracks)

                    for (server in directServers) {
                        if (emittedUrls.add(server.url)) {
                            val serverLabel = "${server.name} [${variant.quality}]"
                            val streamReferer = when {
                                server.url.contains("pixeldrain.com", ignoreCase = true) -> "https://pixeldrain.com/"
                                server.url.contains("r2.cloudflarestorage.com", ignoreCase = true) || server.url.contains("gamerxyt.com", ignoreCase = true) -> "https://gamerxyt.com/"
                                else -> "https://hubcloud.ist/"
                            }

                            send(
                                StreamEmission.SourceFound(
                                    StreamSource(
                                        url = server.url,
                                        serverName = serverLabel,
                                        resolutionLabel = variant.quality,
                                        quality = variant.quality,
                                        isM3u8 = server.url.contains(".m3u8", ignoreCase = true),
                                        audioTracks = audioTracks,
                                        releaseType = releaseType,
                                        headers = mapOf(
                                            "User-Agent" to defaultUserAgent,
                                            "Referer" to streamReferer,
                                            "Accept-Ranges" to "bytes"
                                        )
                                    )
                                )
                            )
                        }
                    }
                } catch (_: Exception) {}
            }
        }
    }

    override suspend fun getStreamLinks(episodeData: String): StreamResult = withContext(Dispatchers.IO) {
        val streamSources = mutableListOf<StreamSource>()
        val subtitleTracks = mutableListOf<SubtitleTrack>()
        getStreamFlow(episodeData).collect { emission ->
            when (emission) {
                is StreamEmission.SourceFound -> streamSources.add(emission.source)
                is StreamEmission.SubtitleFound -> subtitleTracks.add(emission.track)
                is StreamEmission.StatusUpdate -> {}
            }
        }
        StreamResult(streamSources, subtitleTracks)
    }

    override suspend fun getDownloadLinks(episodeData: String): List<DownloadOption> = withContext(Dispatchers.IO) {
        val downloadOptions = mutableListOf<DownloadOption>()
        val variants = mutableListOf<VariantLink>()

        try {
            if (episodeData.contains("\"variants\"")) {
                val root = json.parseToJsonElement(episodeData).jsonObject
                val varArr = root["variants"]?.jsonArray
                varArr?.forEach { elem ->
                    try {
                        val v = json.decodeFromJsonElement(VariantLink.serializer(), elem)
                        variants.add(v)
                    } catch (_: Exception) {}
                }
            }
        } catch (_: Exception) {}

        if (variants.isEmpty()) {
            variants.addAll(resolveVariantsOnDemand(episodeData))
        }

        // Resolve top variants for high-speed direct download options
        val resolved = variants.distinctBy { it.greenmotorsUrl }.take(6).parallelMapIsolated { variant ->
            try {
                val hubcloudUrl = bypassGreenmotors(variant.greenmotorsUrl) ?: return@parallelMapIsolated emptyList()
                val gamerxytUrl = resolveHubcloud(hubcloudUrl) ?: return@parallelMapIsolated emptyList()
                val servers = resolveGamerxyt(gamerxytUrl)
                servers.map { s ->
                    val streamReferer = when {
                        s.url.contains("pixeldrain.com", ignoreCase = true) -> "https://pixeldrain.com/"
                        s.url.contains("r2.cloudflarestorage.com", ignoreCase = true) || s.url.contains("gamerxyt.com", ignoreCase = true) -> "https://gamerxyt.com/"
                        else -> "https://hubcloud.ist/"
                    }
                    DownloadOption(
                        title = "${variant.title} - ${s.name}",
                        quality = variant.quality,
                        size = variant.size,
                        url = s.url,
                        source = s.name,
                        provider = name,
                        headers = mapOf(
                            "User-Agent" to defaultUserAgent,
                            "Referer" to streamReferer,
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

    private suspend fun resolveVariantsOnDemand(episodeData: String): List<VariantLink> = withContext(Dispatchers.IO) {
        val foundVariants = mutableListOf<VariantLink>()
        val trimmed = episodeData.trim()

        // 1. Direct intermediate / greenmotors / stream links
        if (trimmed.contains("greenmotors") || trimmed.contains("hubcloud") ||
            trimmed.contains("gamerxyt") || trimmed.contains("drive") || trimmed.contains("pixeldrain")) {
            foundVariants.add(
                VariantLink(
                    title = "Direct Media Link",
                    quality = parseQualityLabel(trimmed),
                    size = "Direct",
                    server = "HubCloud",
                    greenmotorsUrl = trimmed
                )
            )
            return@withContext foundVariants
        }

        // 2. Identify season and episode numbers if present
        var targetSeason = 1
        var targetEpisode = 1
        var hasSpecificEp = false

        val seMatch = Regex("""[:_\-\s]S(\d{1,2})E(\d{1,3})\b""", RegexOption.IGNORE_CASE).find(trimmed)
        val colonMatch = Regex("""[:](\d{1,2})[:](\d{1,3})\b""").find(trimmed)

        if (seMatch != null) {
            targetSeason = seMatch.groupValues[1].toIntOrNull() ?: 1
            targetEpisode = seMatch.groupValues[2].toIntOrNull() ?: 1
            hasSpecificEp = true
        } else if (colonMatch != null) {
            targetSeason = colonMatch.groupValues[1].toIntOrNull() ?: 1
            targetEpisode = colonMatch.groupValues[2].toIntOrNull() ?: 1
            hasSpecificEp = true
        } else if (trimmed.contains("\"season\"") && trimmed.contains("\"episode\"")) {
            try {
                val root = json.parseToJsonElement(trimmed).jsonObject
                val s = root["season"]?.jsonPrimitive?.intOrNull
                val e = root["episode"]?.jsonPrimitive?.intOrNull
                if (s != null && e != null) {
                    targetSeason = s
                    targetEpisode = e
                    hasSpecificEp = true
                }
            } catch (_: Exception) {}
        }

        // 3. Extract base URL, slug, or search term
        val normalized = trimmed
            .replace("https:__", "https://")
            .replace("http:__", "http://")
            .replace("4khdhub.one_", "4khdhub.one/")
            .replace("_:", ":")

        var baseTarget = normalized
            .substringBefore(":S")
            .substringBefore(":s")
            .substringBefore(":")
            .trim()
            .trimEnd('_')

        if (baseTarget.startsWith("{")) {
            try {
                val root = json.parseToJsonElement(baseTarget).jsonObject
                baseTarget = root["url"]?.jsonPrimitive?.contentOrNull
                    ?: root["id"]?.jsonPrimitive?.contentOrNull
                    ?: root["title"]?.jsonPrimitive?.contentOrNull
                    ?: ""
            } catch (_: Exception) {}
        }

        val targetUrl = when {
            baseTarget.startsWith("http") -> baseTarget
            baseTarget.contains("4khdhub.one/") -> {
                val cleanSlug = baseTarget.substringAfter("4khdhub.one/").trim('/')
                "$mainUrl/$cleanSlug/"
            }
            baseTarget.isNotBlank() && !baseTarget.all { it.isDigit() } -> {
                val cleanSlug = baseTarget.trim('/')
                "$mainUrl/$cleanSlug/"
            }
            else -> ""
        }

        var resolvedUrl = targetUrl
        val doc = if (targetUrl.isNotBlank()) {
            try { client.getHtml(targetUrl, defaultHeaders) } catch (_: Exception) { null }
        } else null

        val finalDoc = doc ?: run {
            // Search fallback
            val searchQuery = baseTarget.ifBlank { trimmed }
            if (searchQuery.isNotBlank() && !searchQuery.all { it.isDigit() }) {
                try {
                    val searchResults = search(cleanTitleForDisplay(searchQuery))
                    val match = searchResults.firstOrNull()
                    if (match != null) {
                        resolvedUrl = match.url
                        try { client.getHtml(match.url, defaultHeaders) } catch (_: Exception) { null }
                    } else null
                } catch (_: Exception) { null }
            } else null
        }

        if (finalDoc != null) {
            val parsedEpisodes = parseSeriesEpisodes(finalDoc, null, "", resolvedUrl)
            if (parsedEpisodes.isNotEmpty()) {
                val matchedEp = if (hasSpecificEp) {
                    parsedEpisodes.firstOrNull { it.seasonNumber == targetSeason && it.episodeNumber == targetEpisode }
                        ?: parsedEpisodes.firstOrNull { it.seasonNumber == targetSeason }
                        ?: parsedEpisodes.firstOrNull()
                } else {
                    parsedEpisodes.firstOrNull()
                }

                if (matchedEp != null && matchedEp.data.isNotBlank()) {
                    try {
                        val root = json.parseToJsonElement(matchedEp.data).jsonObject
                        val varArr = root["variants"]?.jsonArray
                        varArr?.forEach { elem ->
                            try {
                                val v = json.decodeFromJsonElement(VariantLink.serializer(), elem)
                                foundVariants.add(v)
                            } catch (_: Exception) {}
                        }
                    } catch (_: Exception) {}
                }
            } else {
                val movieVariants = parseMovieDownloadVariants(finalDoc)
                foundVariants.addAll(movieVariants)
            }
        }

        foundVariants
    }

    override suspend fun fetchCast(mediaId: String, imdbId: String?, type: MediaType): List<CastMember> = emptyList()
}

// ============================================================================
// Internal Serialized Models
// ============================================================================

@Serializable
data class VariantLink(
    val title: String,
    val quality: String,
    val size: String,
    val server: String,
    val greenmotorsUrl: String
)

@Serializable
data class MoviePayload(
    val tmdbId: String?,
    val title: String,
    val url: String,
    val variants: List<VariantLink>
)

@Serializable
data class TvPayload(
    val tmdbId: String?,
    val season: Int,
    val episode: Int,
    val title: String,
    val variants: List<VariantLink>
)

data class ResolvedDirectServer(
    val name: String,
    val url: String,
    val isDirectR2: Boolean = false
)

