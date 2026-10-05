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
 * HQPorner Universal Plugin Provider.
 *
 * Scrapes full-length 4K Ultra HD and 1080p studio scenes (Brazzers, Blacked, Vixen, Tushy, etc.)
 * directly from https://hqporner.com with pristine high-speed direct MP4 CDN playback.
 *
 * Employs DNS-over-HTTPS (DoH) via [DohDns] to ensure zero ISP interference.
 */
class HQPornerPlugin(
    private val client: OkHttpClient = DohDns.createOkHttpClient(
        connectTimeoutSeconds = 15,
        readTimeoutSeconds = 25
    )
) : UniversalPlugin {

    override val name: String = "HQPorner"
    override val mainUrl: String = "https://hqporner.com"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE)
    override val isSearchGlobalOnly: Boolean get() = false

    private val defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    private val cardRegex = Pattern.compile(
        """<a href="(/hdporn/(\d+)-[^"]+\.html)"[\s\S]*?<img id="cover_\d+" src="([^"]+)"[\s\S]*?<h3 class="meta-data-title"><a[^>]*>([^<]+)</a></h3>[\s\S]*?<span class="icon fa-clock-o meta-data">([^<]+)</span>"""
    )
    private val embedParamRegex = Pattern.compile("""url:\s*'/blocks/(?:nativeplayer|altplayer)\.php\?i=([^']+)'""")
    private val sourceRegex = Pattern.compile("""<source\s+src="([^"]+)"(?:\s+title="([^"]+)")?""")

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        val rows = mutableListOf<CatalogRow>()

        coroutineScope {
            val fourKDeferred = async { fetchVideosFromUrl("$mainUrl/category/4k-porn") }
            val topMonthDeferred = async { fetchVideosFromUrl("$mainUrl/top/month") }
            val brazzersDeferred = async { fetchVideosFromUrl("$mainUrl/studio/free-brazzers-videos") }
            val fullHdDeferred = async { fetchVideosFromUrl("$mainUrl/category/1080p-porn") }
            val asianDeferred = async { fetchVideosFromUrl("$mainUrl/category/asian") }
            val amateurDeferred = async { fetchVideosFromUrl("$mainUrl/category/amateur") }

            val fourK = fourKDeferred.await()
            if (fourK.isNotEmpty()) {
                rows.add(CatalogRow(title = "4K Ultra HD Masterpieces", items = fourK.take(16)))
            }

            val topMonth = topMonthDeferred.await()
            if (topMonth.isNotEmpty()) {
                rows.add(CatalogRow(title = "Most Viewed This Month", items = topMonth.take(16)))
            }

            val brazzers = brazzersDeferred.await()
            if (brazzers.isNotEmpty()) {
                rows.add(CatalogRow(title = "Brazzers & Premium Studios", items = brazzers.take(16)))
            }

            val fullHd = fullHdDeferred.await()
            if (fullHd.isNotEmpty()) {
                rows.add(CatalogRow(title = "1080p Full HD Studio Scenes", items = fullHd.take(16)))
            }

            val asian = asianDeferred.await()
            if (asian.isNotEmpty()) {
                rows.add(CatalogRow(title = "Desi & Asian Exclusive", items = asian.take(16)))
            }

            val amateur = amateurDeferred.await()
            if (amateur.isNotEmpty()) {
                rows.add(CatalogRow(title = "Amateur & Homemade", items = amateur.take(16)))
            }
        }

        rows
    }

    override suspend fun search(query: String): List<MediaItem> = search(query, page = 1)

    suspend fun search(query: String, page: Int = 1): List<MediaItem> = withContext(Dispatchers.IO) {
        val encoded = URLEncoder.encode(query.trim(), "UTF-8")
        val url = if (page <= 1) "$mainUrl/?q=$encoded" else "$mainUrl/?q=$encoded&p=$page"
        fetchVideosFromUrl(url)
    }

    suspend fun getCategoryVideos(slug: String, page: Int = 1): List<MediaItem> = withContext(Dispatchers.IO) {
        val targetPath = when {
            slug.startsWith("studio/") -> if (page <= 1) "$mainUrl/$slug" else "$mainUrl/$slug/$page"
            slug.startsWith("top/") -> if (page <= 1) "$mainUrl/$slug" else "$mainUrl/$slug/$page"
            slug.startsWith("category/") -> if (page <= 1) "$mainUrl/$slug" else "$mainUrl/$slug/$page"
            else -> if (page <= 1) "$mainUrl/category/$slug" else "$mainUrl/category/$slug/$page"
        }
        fetchVideosFromUrl(targetPath)
    }

    private fun fetchVideosFromUrl(url: String): List<MediaItem> {
        val html = fetchHtml(url) ?: return emptyList()
        val items = mutableListOf<MediaItem>()
        val matcher = cardRegex.matcher(html)

        while (matcher.find()) {
            val relativeUrl = matcher.group(1) ?: continue
            val id = matcher.group(2) ?: relativeUrl
            var imgUrl = matcher.group(3) ?: ""
            if (imgUrl.startsWith("//")) imgUrl = "https:$imgUrl"
            val title = matcher.group(4)?.trim() ?: "HQ Video"
            val duration = matcher.group(5)?.trim() ?: "20:00"

            val is4k = title.contains("4K", ignoreCase = true) || relativeUrl.contains("4K", ignoreCase = true)

            items.add(
                MediaItem(
                    id = id,
                    title = title,
                    url = if (relativeUrl.startsWith("http")) relativeUrl else "$mainUrl$relativeUrl",
                    posterUrl = imgUrl.ifBlank { null },
                    backdropUrl = imgUrl.ifBlank { null },
                    type = MediaType.MOVIE,
                    quality = if (is4k) "4K" else "1080p",
                    rating = duration,
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
            else -> "$mainUrl/hdporn/${mediaItem.id}.html"
        }

        val html = fetchHtml(videoPageUrl)
        var resolvedTitle = mediaItem.title
        var duration = mediaItem.rating ?: "20:00"
        val genres = mutableListOf<String>()
        val episodes = mutableListOf<EpisodeItem>()

        if (!html.isNullOrBlank()) {
            // Extract Meta description / duration
            val durMatcher = Pattern.compile("""Video duration is\s*([^.]+)\.""").matcher(html)
            if (durMatcher.find()) {
                duration = durMatcher.group(1)?.trim() ?: duration
            }

            // Extract tags
            val tagMatcher = Pattern.compile("""<a href="/\?q=[^"]*"\s+title="Search for tag:[^"]*"\s+class="tag-link">([^<]+)</a>""").matcher(html)
            while (tagMatcher.find()) {
                val tag = tagMatcher.group(1)?.trim()?.replaceFirstChar { it.uppercase() }
                if (!tag.isNullOrBlank() && tag !in genres) {
                    genres.add(tag)
                }
            }

            // Extract native/alt player embed parameter
            val embedMatcher = embedParamRegex.matcher(html)
            val embedParam = if (embedMatcher.find()) embedMatcher.group(1) else null

            if (!embedParam.isNullOrBlank()) {
                val embedUrl = if (embedParam.startsWith("//")) "https:$embedParam" else embedParam
                val embedHtml = fetchHtml(embedUrl, referer = "$mainUrl/")

                if (!embedHtml.isNullOrBlank()) {
                    val sMatcher = sourceRegex.matcher(embedHtml)
                    var epIndex = 1
                    while (sMatcher.find()) {
                        var streamSrc = sMatcher.group(1) ?: continue
                        if (streamSrc.startsWith("//")) streamSrc = "https:$streamSrc"
                        val streamTitle = sMatcher.group(2)?.trim() ?: "Direct Stream"

                        val label = when {
                            streamTitle.contains("2160", ignoreCase = true) || streamTitle.contains("4K", ignoreCase = true) -> "4K Ultra HD"
                            streamTitle.contains("1080", ignoreCase = true) -> "1080p Full HD"
                            streamTitle.contains("720", ignoreCase = true) -> "720p HD"
                            streamTitle.contains("360", ignoreCase = true) -> "360p Standard"
                            else -> streamTitle
                        }

                        episodes.add(
                            EpisodeItem(
                                id = "${mediaItem.id}_$epIndex",
                                seasonNumber = 1,
                                episodeNumber = epIndex,
                                title = label,
                                data = streamSrc
                            )
                        )
                        epIndex++
                    }
                }
            }
        }

        // Sort descending by quality (4K first, then 1080p, etc.)
        val sortedEpisodes = episodes.sortedByDescending { ep ->
            when {
                ep.title.contains("4K") -> 2160
                ep.title.contains("1080") -> 1080
                ep.title.contains("720") -> 720
                else -> 360
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
            synopsis = "Full-length Ultra HD studio scene from HQPorner.",
            genres = genres.ifEmpty { listOf("Full Length", "Studio", "Ultra HD") },
            duration = duration,
            episodes = sortedEpisodes,
            rating = mediaItem.rating,
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

        if (episodeData.startsWith("http://") || episodeData.startsWith("https://")) {
            val quality = when {
                episodeData.contains("2160") -> "4K"
                episodeData.contains("1080") -> "1080p"
                episodeData.contains("720") -> "720p"
                else -> "HD"
            }
            return@withContext StreamResult(
                streams = listOf(
                    StreamSource(
                        url = episodeData,
                        serverName = "HQPorner CDN ($quality)",
                        resolutionLabel = quality,
                        quality = quality,
                        isM3u8 = episodeData.contains(".m3u8"),
                        headers = streamHeaders
                    )
                )
            )
        }

        val detail = getDetails(MediaItem(id = episodeData, title = "", url = "", posterUrl = null, type = MediaType.MOVIE))
        val sources = detail.episodes.map { ep ->
            StreamSource(
                url = ep.data,
                serverName = "HQPorner CDN (${ep.title})",
                resolutionLabel = ep.title,
                quality = ep.title,
                isM3u8 = ep.data.contains(".m3u8"),
                headers = streamHeaders
            )
        }
        StreamResult(streams = sources)
    }

    private fun fetchHtml(url: String, referer: String = "$mainUrl/"): String? {
        return try {
            val req = Request.Builder()
                .url(url)
                .header("User-Agent", defaultUserAgent)
                .header("Referer", referer)
                .build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) resp.body?.string() else null
            }
        } catch (_: Exception) {
            null
        }
    }
}
