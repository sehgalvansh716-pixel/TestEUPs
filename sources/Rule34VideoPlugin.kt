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
import java.util.regex.Pattern

/**
 * Rule34Video Universal Plugin Provider.
 *
 * Scrapes 3D animations, game characters, and anime scenes from https://rule34video.com
 * with direct high-definition MP4 streams (720p / 480p / 360p / 1080p).
 *
 * Uses DNS-over-HTTPS (DoH) via [DohDns] to bypass ISP blocking.
 */
class Rule34VideoPlugin(
    private val client: OkHttpClient = DohDns.createOkHttpClient(
        connectTimeoutSeconds = 15,
        readTimeoutSeconds = 25
    )
) : UniversalPlugin {

    override val name: String = "Rule34Video"
    override val mainUrl: String = "https://rule34video.com"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE)
    override val isSearchGlobalOnly: Boolean get() = false

    private val defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    private val cardRegex = Pattern.compile(
        """<a class="th js-open-popup"\s+href="(https://rule34video\.com/video/(\d+)/[^"]*)"\s+title="([^"]+)"[\s\S]*?(?:data-original|data-webp)="([^"]+)""""
    )
    private val durationRegex = Pattern.compile("""<span class="duration">\s*([^<]+)\s*</span>""")

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        val rows = mutableListOf<CatalogRow>()

        coroutineScope {
            val latestDeferred = async { fetchVideosFromUrl("$mainUrl/latest-updates/") }
            val popularDeferred = async { fetchVideosFromUrl("$mainUrl/most-popular/") }
            val topRatedDeferred = async { fetchVideosFromUrl("$mainUrl/top-rated/") }
            val threeDDeferred = async { fetchVideosFromUrl("$mainUrl/categories/3d/") }

            val latest = latestDeferred.await()
            if (latest.isNotEmpty()) {
                rows.add(CatalogRow(title = "Latest Updates & Animations", items = latest.take(16)))
            }

            val popular = popularDeferred.await()
            if (popular.isNotEmpty()) {
                rows.add(CatalogRow(title = "Most Popular Across All Time", items = popular.take(16)))
            }

            val topRated = topRatedDeferred.await()
            if (topRated.isNotEmpty()) {
                rows.add(CatalogRow(title = "Top Rated Masterpieces", items = topRated.take(16)))
            }

            val threeD = threeDDeferred.await()
            if (threeD.isNotEmpty()) {
                rows.add(CatalogRow(title = "3D & Blender Works", items = threeD.take(16)))
            }
        }

        rows
    }

    override suspend fun search(query: String): List<MediaItem> = search(query, page = 1)

    suspend fun search(query: String, page: Int = 1): List<MediaItem> = withContext(Dispatchers.IO) {
        val encoded = URLEncoder.encode(query.trim(), "UTF-8")
        val url = if (page <= 1) "$mainUrl/search/$encoded/" else "$mainUrl/search/$encoded/$page/"
        fetchVideosFromUrl(url)
    }

    suspend fun getCategoryVideos(slug: String, page: Int = 1): List<MediaItem> = withContext(Dispatchers.IO) {
        val targetUrl = if (page <= 1) "$mainUrl/categories/$slug/" else "$mainUrl/categories/$slug/$page/"
        fetchVideosFromUrl(targetUrl)
    }

    private fun fetchVideosFromUrl(url: String): List<MediaItem> {
        val html = fetchHtml(url) ?: return emptyList()
        val items = mutableListOf<MediaItem>()
        val matcher = cardRegex.matcher(html)

        while (matcher.find()) {
            val videoUrl = matcher.group(1) ?: continue
            val id = matcher.group(2) ?: videoUrl
            val title = matcher.group(3)?.trim() ?: "Animation"
            val imgUrl = matcher.group(4) ?: ""

            items.add(
                MediaItem(
                    id = id,
                    title = title,
                    url = videoUrl,
                    posterUrl = imgUrl.ifBlank { null },
                    backdropUrl = imgUrl.ifBlank { null },
                    type = MediaType.MOVIE,
                    quality = "HD",
                    rating = "HD",
                    provider = name
                )
            )
        }
        return items
    }

    override suspend fun getDetails(mediaItem: MediaItem): MediaDetail = withContext(Dispatchers.IO) {
        val videoPageUrl = when {
            mediaItem.url.startsWith("http") -> mediaItem.url
            mediaItem.url.isNotBlank() -> "$mainUrl/${mediaItem.url.removePrefix("/")}"
            else -> "$mainUrl/video/${mediaItem.id}/"
        }

        val html = fetchHtml(videoPageUrl)
        var resolvedTitle = mediaItem.title
        var duration = "10:00"
        val genres = mutableListOf<String>()
        val episodes = mutableListOf<EpisodeItem>()

        if (!html.isNullOrBlank()) {
            // Title from flashvars
            val titleMatcher = Pattern.compile("""video_title:\s*'([^']+)'""").matcher(html)
            if (titleMatcher.find()) {
                resolvedTitle = titleMatcher.group(1)?.trim() ?: resolvedTitle
            }

            // Tags
            val tagsMatcher = Pattern.compile("""video_tags:\s*'([^']+)'""").matcher(html)
            if (tagsMatcher.find()) {
                tagsMatcher.group(1)?.split(",")?.forEach { rawTag ->
                    val t = rawTag.trim().replaceFirstChar { it.uppercase() }
                    if (t.isNotBlank() && t !in genres) genres.add(t)
                }
            }

            // Video direct URLs from flashvars
            val patterns = listOf(
                """video_alt_url2:\s*'([^']+)'""" to """video_alt_url2_text:\s*'([^']+)'""",
                """video_alt_url:\s*'([^']+)'""" to """video_alt_url_text:\s*'([^']+)'""",
                """video_url:\s*'([^']+)'""" to """video_url_text:\s*'([^']+)'"""
            )

            var epIndex = 1
            for ((urlPat, textPat) in patterns) {
                val uMatch = Pattern.compile(urlPat).matcher(html)
                if (uMatch.find()) {
                    val streamUrl = uMatch.group(1) ?: continue
                    val tMatch = Pattern.compile(textPat).matcher(html)
                    val label = if (tMatch.find()) tMatch.group(1)?.trim() ?: "Direct Stream" else "Direct Stream"

                    episodes.add(
                        EpisodeItem(
                            id = "${mediaItem.id}_$epIndex",
                            seasonNumber = 1,
                            episodeNumber = epIndex,
                            title = if (label.contains("p", ignoreCase = true)) label else "${label}p",
                            data = streamUrl
                        )
                    )
                    epIndex++
                }
            }
        }

        MediaDetail(
            id = mediaItem.id,
            title = resolvedTitle,
            url = videoPageUrl,
            posterUrl = mediaItem.posterUrl,
            backdropUrl = mediaItem.backdropUrl ?: mediaItem.posterUrl,
            type = MediaType.MOVIE,
            year = 2026,
            synopsis = "High definition 3D animation scene from Rule34Video.",
            genres = genres.ifEmpty { listOf("3D Animation", "Hentai", "HD") },
            duration = duration,
            episodes = episodes,
            rating = "HD",
            contentRating = "18+",
            provider = name,
            cast = emptyList(),
            logoUrl = mediaItem.logoUrl
        )
    }

    override suspend fun getStreamLinks(episodeData: String): StreamResult = withContext(Dispatchers.IO) {
        val streamHeaders = mapOf(
            "User-Agent" to defaultUserAgent,
            "Referer" to "$mainUrl/"
        )

        val quality = when {
            episodeData.contains("1080") -> "1080p"
            episodeData.contains("720") -> "720p"
            episodeData.contains("480") -> "480p"
            episodeData.contains("360") -> "360p"
            else -> "HD"
        }

        StreamResult(
            streams = listOf(
                StreamSource(
                    url = episodeData,
                    serverName = "Rule34 High-Speed CDN ($quality)",
                    resolutionLabel = quality,
                    quality = quality,
                    isM3u8 = episodeData.contains(".m3u8"),
                    headers = streamHeaders
                )
            )
        )
    }

    private fun fetchHtml(url: String): String? {
        return try {
            val req = Request.Builder()
                .url(url)
                .header("User-Agent", defaultUserAgent)
                .header("Referer", "$mainUrl/")
                .build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) resp.body?.string() else null
            }
        } catch (_: Exception) {
            null
        }
    }
}
