package com.euthopiar.core.provider

import com.euthopiar.core.model.*
import com.euthopiar.core.network.DohDns
import org.json.JSONObject
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import okhttp3.Cookie
import okhttp3.CookieJar
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import java.net.URLEncoder
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

/**
 * EPorner Universal Plugin Provider.
 *
 * Utilizes the official EPorner JSON API and high-speed MP4 video CDNs to deliver
 * pristine 4K / 1080p / 720p full-length adult video scenes.
 *
 * Backed by automatic DNS-over-HTTPS (DoH) via [DohDns] to avoid ISP DNS blocking.
 */
class EpornerPlugin(
    private val client: OkHttpClient = DohDns.createOkHttpClient(
        connectTimeoutSeconds = 15,
        readTimeoutSeconds = 20
    ).newBuilder().cookieJar(object : CookieJar {
        private val cookieStore = ConcurrentHashMap<String, String>()
        override fun saveFromResponse(url: HttpUrl, cookies: List<Cookie>) {
            cookies.forEach { cookieStore[it.name] = it.value }
        }
        override fun loadForRequest(url: HttpUrl): List<Cookie> {
            return cookieStore.map { (name, value) ->
                Cookie.Builder().name(name).value(value).domain(url.host).build()
            }
        }
    }).build()
) : UniversalPlugin {

    constructor() : this(DohDns.createOkHttpClient())

    override val name: String = "EPorner"
    override val mainUrl: String = "https://www.eporner.com"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE)
    override val isSearchGlobalOnly: Boolean get() = false

    private val defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    private val dloadPattern = Pattern.compile("""href="(/dload/[^"]+)"""")
    private val contentUrlPattern = Pattern.compile(""""contentUrl":\s*"([^"]+)"""")

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        val rows = mutableListOf<CatalogRow>()

        coroutineScope {
            val topWeeklyDeferred = async { fetchApiVideos(query = "", order = "top-weekly", count = 16) }
            val fourKDeferred = async { fetchApiVideos(query = "4k", order = "top-weekly", count = 16) }
            val desiDeferred = async { fetchApiVideos(query = "desi", order = "top-weekly", count = 16) }
            val amateurDeferred = async { fetchApiVideos(query = "amateur", order = "top-weekly", count = 16) }

            val topWeekly = topWeeklyDeferred.await()
            if (topWeekly.isNotEmpty()) {
                rows.add(CatalogRow(title = "Weekly Top Rated", items = topWeekly))
            }

            val fourK = fourKDeferred.await()
            if (fourK.isNotEmpty()) {
                rows.add(CatalogRow(title = "4K Ultra HD Masterpieces", items = fourK))
            }

            val desi = desiDeferred.await()
            if (desi.isNotEmpty()) {
                rows.add(CatalogRow(title = "Desi & Asian Exclusive", items = desi))
            }

            val amateur = amateurDeferred.await()
            if (amateur.isNotEmpty()) {
                rows.add(CatalogRow(title = "Amateur & Homemade", items = amateur))
            }
        }

        rows
    }

    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        fetchApiVideos(query = query, order = "top-weekly", count = 24)
    }

    override suspend fun getDetails(mediaItem: MediaItem): MediaDetail = withContext(Dispatchers.IO) {
        val videoId = mediaItem.id.removePrefix("video-").substringBefore("/").substringBefore("-")
        val videoPageUrl = "$mainUrl/video-$videoId/"

        var resolvedTitle = mediaItem.title
        var duration = mediaItem.rating ?: "15:00"
        val genres = mutableListOf<String>()
        var episodes = emptyList<EpisodeItem>()

        val html = fetchHtml(videoPageUrl)
        if (!html.isNullOrBlank()) {
            // Title
            val titleMatcher = Pattern.compile("""<h1[^>]*>(.*?)</h1>""", Pattern.DOTALL).matcher(html)
            if (titleMatcher.find()) {
                resolvedTitle = titleMatcher.group(1)?.replace(Regex("<[^>]+>"), "")?.trim() ?: resolvedTitle
            }

            // Extract tags
            val tagMatcher = Pattern.compile("""href="/cat/([^/"]+)"""").matcher(html)
            while (tagMatcher.find()) {
                val tag = tagMatcher.group(1)?.replace("-", " ")
                    ?.split(" ")?.joinToString(" ") { it.replaceFirstChar { c -> c.uppercase() } }
                if (!tag.isNullOrBlank() && tag !in genres) {
                    genres.add(tag)
                }
            }

            // Extract direct download redirect URLs for all available qualities
            val dloadMatcher = dloadPattern.matcher(html)
            val dloadPaths = mutableListOf<String>()
            while (dloadMatcher.find()) {
                val path = dloadMatcher.group(1)
                if (!path.isNullOrBlank() && path !in dloadPaths) {
                    dloadPaths.add(path)
                }
            }

            // Resolve available quality links in parallel
            val resolvedEpisodes = coroutineScope {
                dloadPaths.map { path ->
                    async {
                        val qualityLabel = when {
                            path.contains("1080") -> "1080p"
                            path.contains("720") -> "720p"
                            path.contains("480") -> "480p"
                            path.contains("360") -> "360p"
                            path.contains("240") -> "240p"
                            else -> "HD"
                        }
                        val resolved = resolveDloadRedirect("$mainUrl$path")
                        if (!resolved.isNullOrBlank()) {
                            qualityLabel to resolved
                        } else null
                    }
                }.awaitAll().filterNotNull()
            }

            val qualityOrder = mapOf("1080p" to 1, "720p" to 2, "480p" to 3, "360p" to 4, "240p" to 5)
            val sortedEpisodes = resolvedEpisodes
                .distinctBy { it.first }
                .sortedBy { qualityOrder[it.first] ?: 99 }
                .mapIndexed { index, (qualityLabel, streamLink) ->
                    EpisodeItem(
                        id = "$videoId-$qualityLabel",
                        title = qualityLabel,
                        seasonNumber = 1,
                        episodeNumber = index + 1,
                        data = streamLink,
                        thumbnail = mediaItem.posterUrl,
                        duration = duration
                    )
                }

            episodes = if (sortedEpisodes.isNotEmpty()) {
                sortedEpisodes
            } else {
                // Fallback: contentUrl
                var fallbackUrl: String? = null
                val contentM = contentUrlPattern.matcher(html)
                if (contentM.find()) {
                    fallbackUrl = contentM.group(1)?.replace("&amp;", "&")
                }
                if (!fallbackUrl.isNullOrBlank()) {
                    listOf(
                        EpisodeItem(
                            id = videoId,
                            title = "HD",
                            seasonNumber = 1,
                            episodeNumber = 1,
                            data = fallbackUrl,
                            thumbnail = mediaItem.posterUrl,
                            duration = duration
                        )
                    )
                } else emptyList()
            }
        }

        MediaDetail(
            id = videoId,
            title = resolvedTitle,
            url = videoPageUrl,
            posterUrl = mediaItem.posterUrl,
            backdropUrl = mediaItem.backdropUrl ?: mediaItem.posterUrl,
            type = MediaType.MOVIE,
            year = 2026,
            synopsis = "Full-length Ultra HD scene from EPorner.",
            genres = genres.take(10),
            duration = duration,
            episodes = episodes,
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
            val quality = if (episodeData.contains("1080")) "1080p" else if (episodeData.contains("720")) "720p" else "HD"
            return@withContext StreamResult(
                streams = listOf(
                    StreamSource(
                        url = episodeData,
                        serverName = "EPorner CDN ($quality)",
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
                serverName = "EPorner CDN (${ep.title})",
                resolutionLabel = ep.title,
                quality = ep.title,
                isM3u8 = ep.data.contains(".m3u8"),
                headers = streamHeaders
            )
        }
        StreamResult(sources)
    }

    /**
     * Fetches videos belonging to a category or search query via EPorner API.
     */
    suspend fun getCategoryVideos(slug: String, sort: String = "top-weekly", page: Int = 1): List<MediaItem> = withContext(Dispatchers.IO) {
        fetchApiVideos(query = slug, order = sort, page = page, count = 24)
    }

    private fun fetchApiVideos(query: String, order: String = "top-weekly", page: Int = 1, count: Int = 20): List<MediaItem> {
        val encoded = URLEncoder.encode(query.trim(), "UTF-8")
        val mirrors = listOf("https://www.eporner.com", "https://www.eporner.net")
        for (mirror in mirrors) {
            val url = "$mirror/api/v2/video/search/?query=$encoded&per_page=$count&page=$page&thumbsize=big&order=$order&format=json"
            try {
                val req = Request.Builder()
                    .url(url)
                    .header("User-Agent", defaultUserAgent)
                    .build()
                client.newCall(req).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val bodyStr = resp.body?.string()
                        if (!bodyStr.isNullOrBlank()) {
                            val items = parseApiJson(bodyStr)
                            if (items.isNotEmpty()) return items
                        }
                    }
                }
            } catch (_: Exception) {}
        }
        return emptyList()
    }

    private fun parseApiJson(jsonStr: String): List<MediaItem> {
        return try {
            val root = JSONObject(jsonStr)
            val videos = root.optJSONArray("videos") ?: return emptyList()

            val items = mutableListOf<MediaItem>()
            for (i in 0 until videos.length()) {
                val v = videos.optJSONObject(i) ?: continue
                val id = v.optString("id")
                val title = v.optString("title")
                val videoUrl = v.optString("url")
                val duration = v.optString("length_min")
                val posterUrl = v.optJSONObject("default_thumb")?.optString("src") ?: ""

                if (id.isNotBlank() && title.isNotBlank()) {
                    items.add(
                        MediaItem(
                            id = id,
                            title = title,
                            url = videoUrl.ifBlank { "$mainUrl/video-$id/" },
                            posterUrl = posterUrl.ifBlank { null },
                            backdropUrl = posterUrl.ifBlank { null },
                            type = MediaType.MOVIE,
                            quality = if (title.contains("4k", ignoreCase = true)) "4K" else "1080p",
                            rating = duration,
                            provider = name
                        )
                    )
                }
            }
            items
        } catch (_: Exception) {
            emptyList()
        }
    }

    private fun resolveDloadRedirect(dloadUrl: String): String? {
        return try {
            val noRedirectClient = client.newBuilder().followRedirects(false).build()
            val req = Request.Builder()
                .url(dloadUrl)
                .header("User-Agent", defaultUserAgent)
                .build()
            noRedirectClient.newCall(req).execute().use { resp ->
                resp.header("Location")
            }
        } catch (_: Exception) {
            null
        }
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
