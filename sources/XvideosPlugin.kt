package com.euthopiar.core.provider

import com.euthopiar.core.model.*
import com.euthopiar.core.network.DohDns
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import java.net.URLEncoder
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

/**
 * XVideos Universal Plugin Provider.
 *
 * Scrapes full-length scenes, categories, uploader channels, and high-definition
 * adaptive HLS master streams (1080p / 720p / 480p) from https://www.xvideos2.com.
 *
 * Uses built-in DNS-over-HTTPS (DoH) via [DohDns] to seamlessly bypass ISP DNS
 * tampering and connection blocks, serving 100% full-length uncut videos.
 */
class XvideosPlugin(
    private val client: OkHttpClient = DohDns.createOkHttpClient(
        connectTimeoutSeconds = 15,
        readTimeoutSeconds = 20
    )
) : UniversalPlugin {

    override val name: String = "XVideos"
    override val mainUrl: String = "https://www.xvideos2.com"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE)
    override val isSearchGlobalOnly: Boolean get() = false

    private val defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    private val headers = mapOf(
        "User-Agent" to defaultUserAgent,
        "Referer" to "$mainUrl/",
        "Accept-Language" to "en-US,en;q=0.9"
    )

    private val thumbBlockSplitter = Pattern.compile("""class="frame-block\s+thumb-block""")
    private val urlPattern = Pattern.compile("""href="(/video\.[a-zA-Z0-9_\-]+/[^"]*|/video\d+/[^"]*)"""")
    private val idPattern = Pattern.compile("""data-eid="([a-zA-Z0-9_\-]+)"""")
    private val dataIdPattern = Pattern.compile("""data-id="(\d+)"""")
    private val imgPattern = Pattern.compile("""(?:data-src|src)="([^"]+)"""")
    private val titlePattern = Pattern.compile("""<p class="title">\s*<a[^>]*title="([^"]+)"""")
    private val fallbackTitlePattern = Pattern.compile("""<p class="title">\s*<a[^>]*>([^<]+)""")
    private val durationPattern = Pattern.compile("""<span class="duration">([^<]+)</span>""")
    private val hdMarkPattern = Pattern.compile("""<span class="video-hd-mark">([^<]+)</span>""")
    private val uploaderPattern = Pattern.compile("""<a href="/([^"]+)"><span class="name">([^<]+)</span></a>""")

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        val rows = mutableListOf<CatalogRow>()

        coroutineScope {
            val mainDeferred = async { fetchHtml("$mainUrl/") }
            val desiDeferred = async { fetchHtml("$mainUrl/tags/desi") }
            val amateurDeferred = async { fetchHtml("$mainUrl/tags/amateur") }

            val mainHtml = mainDeferred.await()
            if (!mainHtml.isNullOrBlank()) {
                val allItems = parseVideoCards(mainHtml)
                if (allItems.isNotEmpty()) {
                    // 1. Trending & Featured
                    val trending = allItems.take(15)
                    rows.add(CatalogRow(title = "Trending & Best Videos", items = trending))

                    // 2. 1080p Full HD Scenes
                    val fullHdItems = allItems.filter { it.quality?.contains("1080", ignoreCase = true) == true }
                    if (fullHdItems.isNotEmpty()) {
                        rows.add(CatalogRow(title = "1080p Full HD Scenes", items = fullHdItems))
                    }

                    // 3. Latest Recommendations
                    val latest = allItems.drop(15)
                    if (latest.isNotEmpty()) {
                        rows.add(CatalogRow(title = "Recommended For You", items = latest))
                    }
                }
            }

            val desiHtml = desiDeferred.await()
            if (!desiHtml.isNullOrBlank()) {
                val desiItems = parseVideoCards(desiHtml).take(15)
                if (desiItems.isNotEmpty()) {
                    rows.add(CatalogRow(title = "Desi & Asian Exclusive", items = desiItems))
                }
            }

            val amateurHtml = amateurDeferred.await()
            if (!amateurHtml.isNullOrBlank()) {
                val amateurItems = parseVideoCards(amateurHtml).take(15)
                if (amateurItems.isNotEmpty()) {
                    rows.add(CatalogRow(title = "Amateur & Homemade", items = amateurItems))
                }
            }
        }

        rows
    }

    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        val encoded = URLEncoder.encode(query.trim(), "UTF-8")
        val searchUrl = "$mainUrl/?k=$encoded"
        val html = fetchHtml(searchUrl) ?: return@withContext emptyList()
        parseVideoCards(html)
    }

    override suspend fun getDetails(mediaItem: MediaItem): MediaDetail = withContext(Dispatchers.IO) {
        val targetUrl = when {
            mediaItem.url.startsWith("http") -> mediaItem.url
            mediaItem.id.startsWith("http") -> mediaItem.id
            mediaItem.id.startsWith("/") -> "$mainUrl${mediaItem.id}"
            else -> "$mainUrl/video.${mediaItem.id}/"
        }

        val html = fetchHtml(targetUrl) ?: return@withContext createFallbackDetail(mediaItem)

        // 1. Video Title
        var title = mediaItem.title
        val titleMatcher = Pattern.compile("""html5player\.setVideoTitle\('([^']+)'\)""").matcher(html)
        if (titleMatcher.find()) {
            title = titleMatcher.group(1)?.replace("&amp;", "&")?.trim() ?: mediaItem.title
        }

        // 2. Stream URLs: Master HLS (multi-resolution) & MP4 Fallbacks
        var masterHlsUrl: String? = null
        val hlsMatcher = Pattern.compile("""html5player\.setVideoHLS\('([^']+)'\)""").matcher(html)
        if (hlsMatcher.find()) {
            val rawHls = hlsMatcher.group(1)?.trim()
            if (!rawHls.isNullOrBlank()) {
                // If it's hls_low.m3u8, convert to full master playlist hls.m3u8 containing 1080p
                masterHlsUrl = if (rawHls.endsWith("hls_low.m3u8")) {
                    rawHls.replace("hls_low.m3u8", "hls.m3u8")
                } else {
                    rawHls
                }
            }
        }

        var mp4HighUrl: String? = null
        val highMatcher = Pattern.compile("""html5player\.setVideoUrlHigh\('([^']+)'\)""").matcher(html)
        if (highMatcher.find()) {
            mp4HighUrl = highMatcher.group(1)?.trim()
        }

        var mp4LowUrl: String? = null
        val lowMatcher = Pattern.compile("""html5player\.setVideoUrlLow\('([^']+)'\)""").matcher(html)
        if (lowMatcher.find()) {
            mp4LowUrl = lowMatcher.group(1)?.trim()
        }

        val primaryStreamUrl = masterHlsUrl ?: mp4HighUrl ?: mp4LowUrl

        // 3. Duration
        var durationFormatted = mediaItem.rating ?: "15:00"
        val durMatcher = Pattern.compile("""<span class="duration">([^<]+)</span>""").matcher(html)
        if (durMatcher.find()) {
            durationFormatted = durMatcher.group(1)?.trim() ?: durationFormatted
        }

        // 4. Tags / Categories
        val tagList = mutableListOf<String>()
        val tagMatcher = Pattern.compile("""href="/tags/([^/"]+)"""").matcher(html)
        while (tagMatcher.find()) {
            val rawTag = tagMatcher.group(1) ?: continue
            val cleanTag = rawTag.replace("-", " ")
                .split(" ")
                .joinToString(" ") { it.replaceFirstChar { c -> c.uppercase() } }
            if (cleanTag.isNotBlank() && cleanTag !in tagList) {
                tagList.add(cleanTag)
            }
        }

        // 5. Uploader / Channel
        var creatorName: String? = null
        var creatorSlug: String? = null
        var creatorAvatar: String? = null

        val uploaderMatcher = Pattern.compile("""<a\s+href="/([^/"]+)"\s+class="main-uploader">\s*<span class="name">([^<]+)</span>""").matcher(html)
        if (uploaderMatcher.find()) {
            creatorSlug = uploaderMatcher.group(1)?.trim()
            creatorName = uploaderMatcher.group(2)?.trim()
        }

        val packedCreator = if (!creatorSlug.isNullOrBlank()) {
            "${creatorAvatar ?: ""}|$creatorSlug|${creatorName ?: creatorSlug}|channels"
        } else {
            mediaItem.logoUrl
        }

        // 6. Episode items for player (Master HLS + High MP4 + Low MP4)
        val episodesList = mutableListOf<EpisodeItem>()
        if (!masterHlsUrl.isNullOrBlank()) {
            episodesList.add(
                EpisodeItem(
                    id = "${mediaItem.id}-auto",
                    title = "Auto (Adaptive HLS)",
                    seasonNumber = 1,
                    episodeNumber = 1,
                    data = masterHlsUrl,
                    thumbnail = mediaItem.posterUrl,
                    duration = durationFormatted
                )
            )
        }
        if (!mp4HighUrl.isNullOrBlank()) {
            episodesList.add(
                EpisodeItem(
                    id = "${mediaItem.id}-high",
                    title = "1080p / 720p (MP4)",
                    seasonNumber = 1,
                    episodeNumber = episodesList.size + 1,
                    data = mp4HighUrl,
                    thumbnail = mediaItem.posterUrl,
                    duration = durationFormatted
                )
            )
        }
        if (!mp4LowUrl.isNullOrBlank()) {
            episodesList.add(
                EpisodeItem(
                    id = "${mediaItem.id}-low",
                    title = "360p (MP4)",
                    seasonNumber = 1,
                    episodeNumber = episodesList.size + 1,
                    data = mp4LowUrl,
                    thumbnail = mediaItem.posterUrl,
                    duration = durationFormatted
                )
            )
        }

        MediaDetail(
            id = mediaItem.id,
            title = title,
            url = targetUrl,
            posterUrl = mediaItem.posterUrl,
            backdropUrl = mediaItem.backdropUrl ?: mediaItem.posterUrl,
            type = MediaType.MOVIE,
            year = 2026,
            synopsis = if (creatorName != null) "Uploaded by $creatorName. Full length video scene." else "Full length uncut video scene.",
            genres = tagList.take(12),
            duration = durationFormatted,
            episodes = episodesList,
            rating = mediaItem.rating,
            contentRating = "18+",
            provider = name,
            cast = emptyList(),
            logoUrl = packedCreator
        )
    }

    override suspend fun getStreamLinks(episodeData: String): StreamResult = withContext(Dispatchers.IO) {
        val streamHeaders = mapOf(
            "User-Agent" to defaultUserAgent,
            "Referer" to "$mainUrl/"
        )

        if (episodeData.startsWith("http://") || episodeData.startsWith("https://")) {
            val isM3u8 = episodeData.contains(".m3u8")
            val quality = if (isM3u8) "Auto" else if (episodeData.contains("1080")) "1080p" else if (episodeData.contains("720")) "720p" else "HD"
            return@withContext StreamResult(
                streams = listOf(
                    StreamSource(
                        url = episodeData,
                        serverName = if (isM3u8) "XVideos High-Speed Master HLS" else "XVideos Direct MP4",
                        resolutionLabel = if (isM3u8) "Adaptive 1080p (Master)" else quality,
                        quality = quality,
                        isM3u8 = isM3u8,
                        headers = streamHeaders
                    )
                )
            )
        }

        val detail = getDetails(MediaItem(id = episodeData, title = "", url = "", posterUrl = null, type = MediaType.MOVIE))
        val sources = detail.episodes.map { ep ->
            StreamSource(
                url = ep.data,
                serverName = "XVideos (${ep.title})",
                resolutionLabel = ep.title,
                quality = if (ep.data.contains(".m3u8")) "Auto" else ep.title,
                isM3u8 = ep.data.contains(".m3u8"),
                headers = streamHeaders
            )
        }
        StreamResult(sources)
    }

    /**
     * Fetches videos belonging to a specific tag slug (e.g. "desi", "amateur", "milf").
     */
    suspend fun getCategoryVideos(slug: String, sort: String = "popular", page: Int = 1): List<MediaItem> = withContext(Dispatchers.IO) {
        val cleanSlug = slug.trim().removePrefix("/tags/").removePrefix("/")
        val pagePath = if (page > 1) "/$page" else ""
        val url = "$mainUrl/tags/$cleanSlug$pagePath"
        val html = fetchHtml(url) ?: return@withContext emptyList()
        parseVideoCards(html)
    }

    /**
     * Fetches videos from a creator or uploader channel.
     */
    suspend fun getCreatorVideos(slug: String, type: String = "channels", page: Int = 1): List<MediaItem> = withContext(Dispatchers.IO) {
        val cleanSlug = slug.trim().removePrefix("/")
        val pagePath = if (page > 1) "/videos/best/$page" else "/videos/best"
        val url = "$mainUrl/$cleanSlug$pagePath"
        val html = fetchHtml(url) ?: return@withContext emptyList()
        parseVideoCards(html)
    }

    /**
     * Parses all video cards from a page's HTML body.
     */
    private fun parseVideoCards(html: String): List<MediaItem> {
        val items = mutableListOf<MediaItem>()
        val blocks = thumbBlockSplitter.split(html)

        for (i in 1 until blocks.size) {
            val block = if (blocks[i].length > 3000) blocks[i].substring(0, 3000) else blocks[i]

            // Video URL
            val urlM = urlPattern.matcher(block)
            if (!urlM.find()) continue
            val relativeUrl = urlM.group(1) ?: continue

            // Video ID
            val idM = idPattern.matcher(block)
            val vidId = if (idM.find()) {
                idM.group(1) ?: relativeUrl.substringAfter("/video.").substringBefore("/")
            } else {
                val dataIdM = dataIdPattern.matcher(block)
                if (dataIdM.find()) dataIdM.group(1) ?: relativeUrl else relativeUrl
            }

            // Thumbnail
            var posterUrl: String? = null
            val imgM = imgPattern.matcher(block)
            while (imgM.find()) {
                val candidate = imgM.group(1)
                if (!candidate.isNullOrBlank() && !candidate.contains("lightbox-blank.gif")) {
                    posterUrl = candidate.replace("THUMBNUM", "1")
                    break
                }
            }

            // Quality & Duration
            var quality = "HD"
            val qM = hdMarkPattern.matcher(block)
            if (qM.find()) {
                quality = qM.group(1)?.trim() ?: "1080p"
            }

            var duration: String? = null
            val dM = durationPattern.matcher(block)
            if (dM.find()) {
                duration = dM.group(1)?.trim()
            }

            // Title
            var title = ""
            val titleM = titlePattern.matcher(block)
            if (titleM.find()) {
                title = titleM.group(1)?.trim() ?: ""
            } else {
                val fallbackTitleM = fallbackTitlePattern.matcher(block)
                if (fallbackTitleM.find()) {
                    title = fallbackTitleM.group(1)?.trim() ?: ""
                }
            }
            if (title.isBlank()) continue

            // Uploader
            var uploaderSlug: String? = null
            var uploaderName: String? = null
            val uploaderM = uploaderPattern.matcher(block)
            if (uploaderM.find()) {
                uploaderSlug = uploaderM.group(1)?.trim()
                uploaderName = uploaderM.group(2)?.trim()
            }

            val packedCreator = if (!uploaderSlug.isNullOrBlank()) {
                "|$uploaderSlug|${uploaderName ?: uploaderSlug}|channels"
            } else null

            val fullUrl = if (relativeUrl.startsWith("http")) relativeUrl else "$mainUrl$relativeUrl"

            items.add(
                MediaItem(
                    id = vidId,
                    title = title,
                    url = fullUrl,
                    posterUrl = posterUrl,
                    backdropUrl = posterUrl,
                    type = MediaType.MOVIE,
                    quality = quality,
                    rating = duration,
                    provider = name,
                    logoUrl = packedCreator
                )
            )
        }
        return items
    }

    private fun createFallbackDetail(item: MediaItem): MediaDetail {
        return MediaDetail(
            id = item.id,
            title = item.title,
            url = item.url,
            posterUrl = item.posterUrl,
            backdropUrl = item.backdropUrl ?: item.posterUrl,
            type = MediaType.MOVIE,
            year = 2026,
            synopsis = "Full-length scene from XVideos.",
            genres = listOf("HD", "Full Length"),
            duration = item.rating ?: "15:00",
            episodes = emptyList(),
            rating = item.rating,
            provider = name,
            logoUrl = item.logoUrl
        )
    }

    private fun fetchHtml(url: String): String? {
        val mirrors = listOf("https://www.xvideos2.com", "https://www.xvideos3.com", "https://www.xvideos.com")
        for (mirror in mirrors) {
            val target = if (url.startsWith("http")) {
                url.replace("https://www.xvideos2.com", mirror)
                   .replace("https://www.xvideos3.com", mirror)
                   .replace("https://www.xvideos.com", mirror)
            } else {
                "$mirror$url"
            }
            try {
                val req = Request.Builder()
                    .url(target)
                    .apply { headers.forEach { (k, v) -> header(k, v) } }
                    .build()
                client.newCall(req).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val body = resp.body?.string()
                        if (!body.isNullOrBlank()) return body
                    }
                }
            } catch (_: Exception) {}
        }
        return null
    }
}
