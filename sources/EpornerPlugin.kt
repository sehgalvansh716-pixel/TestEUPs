package com.euthopiar.core.provider

import com.euthopiar.core.model.*
import com.euthopiar.core.network.DohDns
import org.json.JSONObject
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withContext
import okhttp3.Cookie
import okhttp3.CookieJar
import okhttp3.Dns
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import java.net.InetAddress
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
 * Backed by automatic DNS-over-HTTPS (DoH) via [DohDns] with intelligent ISP unblock
 * route prioritization.
 */
class EpornerPlugin(
    customClient: OkHttpClient? = null
) : UniversalPlugin {

    private val smartDns = object : Dns {
        override fun lookup(hostname: String): List<InetAddress> {
            val isWebHost = hostname.equals("www.eporner.com", ignoreCase = true) ||
                            hostname.equals("eporner.com", ignoreCase = true)
            val addresses = try {
                DohDns.DEFAULT.lookup(hostname)
            } catch (_: Throwable) {
                emptyList()
            }
            if (isWebHost) {
                val cleanIps = listOf("94.75.220.4", "94.75.220.7")
                val resolvedClean = addresses.filter { it.hostAddress in cleanIps }
                    .sortedByDescending { if (it.hostAddress == "94.75.220.4") 100 else 90 }
                val fallbackAddresses = cleanIps.mapNotNull { ip ->
                    try {
                        val bytes = ip.split(".").map { it.toInt().toByte() }.toByteArray()
                        InetAddress.getByAddress(hostname, bytes)
                    } catch (_: Throwable) { null }
                }
                val otherResolved = addresses.filterNot { it.hostAddress in cleanIps }
                return (resolvedClean + fallbackAddresses + otherResolved).distinctBy { it.hostAddress }
            }
            return addresses.ifEmpty {
                try { Dns.SYSTEM.lookup(hostname) } catch (_: Throwable) { emptyList() }
            }
        }
    }

    private val client: OkHttpClient = (customClient ?: DohDns.createOkHttpClient(
        connectTimeoutSeconds = 15,
        readTimeoutSeconds = 20,
        dns = smartDns
    )).newBuilder()
        .dns(smartDns)
        .retryOnConnectionFailure(true)
        .cookieJar(object : CookieJar {
            private val cookieStore = ConcurrentHashMap<String, String>()
            override fun saveFromResponse(url: HttpUrl, cookies: List<Cookie>) {
                cookies.forEach { cookieStore[it.name] = it.value }
            }
            override fun loadForRequest(url: HttpUrl): List<Cookie> {
                return cookieStore.mapNotNull { (name, value) ->
                    try {
                        Cookie.Builder().name(name).value(value).domain(url.host).build()
                    } catch (_: Throwable) { null }
                }
            }
        }).build()

    constructor() : this(null)

    override val name: String = "EPorner"
    override val mainUrl: String = "https://www.eporner.com"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE)
    override val isSearchGlobalOnly: Boolean get() = false

    private val defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    private val dloadPattern = Pattern.compile("""href="(/dload/[^"]+)"""")
    private val contentUrlPattern = Pattern.compile(""""contentUrl":\s*"([^"]+)"""")

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        val rows = mutableListOf<CatalogRow>()

        supervisorScope {
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
        val videoId = mediaItem.id.removePrefix("video-").substringBefore("/")
        val videoPageUrl = when {
            mediaItem.url.startsWith("http") -> mediaItem.url
            mediaItem.url.isNotBlank() -> "$mainUrl/${mediaItem.url.removePrefix("/")}"
            else -> "$mainUrl/video-$videoId/"
        }

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

            // Prioritize standard MP4 over AV1 since standard MP4 is hardware decoded on all Android devices
            val standardPaths = dloadPaths.filterNot { it.contains("-av1", ignoreCase = true) }
            val candidatePaths = if (standardPaths.isNotEmpty()) standardPaths else dloadPaths

            // Sort candidate paths descending by resolution (1080p -> 720p -> 480p -> 360p -> 240p)
            val qualityOrder = mapOf("1080p" to 1, "720p" to 2, "480p" to 3, "360p" to 4, "240p" to 5)
            val sortedCandidates = candidatePaths.sortedBy { path ->
                when {
                    path.contains("1080") -> 1
                    path.contains("720") -> 2
                    path.contains("480") -> 3
                    path.contains("360") -> 4
                    path.contains("240") -> 5
                    else -> 99
                }
            }

            // Resolve available quality links in parallel safely with supervisorScope
            val resolvedEpisodes = supervisorScope {
                sortedCandidates.map { path ->
                    async {
                        try {
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
                        } catch (_: Throwable) {
                            null
                        }
                    }
                }.awaitAll().filterNotNull()
            }

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
                emptyList()
            }
        }

        MediaDetail(
            id = mediaItem.id,
            title = resolvedTitle,
            url = mediaItem.url.ifBlank { videoPageUrl },
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

        val cleanUrl = episodeData.substringBefore("?dload=")
        val resolvedUrl = if (cleanUrl.contains("/dload/")) {
            resolveDloadRedirect(cleanUrl)
        } else {
            cleanUrl
        }

        if (!resolvedUrl.isNullOrBlank() && !resolvedUrl.contains("/login", ignoreCase = true) && (resolvedUrl.startsWith("http://") || resolvedUrl.startsWith("https://"))) {
            val quality = when {
                resolvedUrl.contains("1440") -> "1440p 2K"
                resolvedUrl.contains("1080") -> "1080p"
                resolvedUrl.contains("720") -> "720p"
                resolvedUrl.contains("480") -> "480p"
                resolvedUrl.contains("360") -> "360p"
                resolvedUrl.contains("240") -> "240p"
                else -> "HD"
            }
            return@withContext StreamResult(
                streams = listOf(
                    StreamSource(
                        url = resolvedUrl,
                        serverName = "EPorner High-Speed CDN ($quality)",
                        resolutionLabel = quality,
                        quality = quality,
                        isM3u8 = resolvedUrl.contains(".m3u8"),
                        headers = streamHeaders
                    )
                )
            )
        }

        val detail = getDetails(MediaItem(id = cleanUrl, title = "", url = "", posterUrl = null, type = MediaType.MOVIE))
        val sources = detail.episodes.map { ep ->
            val cleanEpUrl = ep.data.substringBefore("?dload=")
            val q = ep.title
            StreamSource(
                url = cleanEpUrl,
                serverName = "EPorner High-Speed CDN ($q)",
                resolutionLabel = q,
                quality = q,
                isM3u8 = cleanEpUrl.contains(".m3u8"),
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
        val url = "$mainUrl/api/v2/video/search/?query=$encoded&per_page=$count&page=$page&thumbsize=big&order=$order&format=json"
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
            val noRedirectClient = client.newBuilder()
                .followRedirects(false)
                .followSslRedirects(false)
                .build()
            val req = Request.Builder()
                .url(dloadUrl)
                .header("User-Agent", defaultUserAgent)
                .header("Referer", "$mainUrl/")
                .build()
            noRedirectClient.newCall(req).execute().use { resp ->
                val loc = resp.header("Location") ?: return null
                if (loc.contains("/login", ignoreCase = true) || loc.contains("login.", ignoreCase = true)) {
                    return null
                }
                val fullUrl = if (loc.startsWith("/")) "$mainUrl$loc" else loc
                if (!fullUrl.contains(".mp4") && !fullUrl.contains(".m3u8")) {
                    return null
                }
                fullUrl.substringBefore("?dload=")
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
