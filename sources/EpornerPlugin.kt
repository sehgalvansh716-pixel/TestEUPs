package com.euthopiar.core.provider

import com.euthopiar.core.browser.BrowserResolveRequest
import com.euthopiar.core.model.*
import com.euthopiar.core.network.DohDns
import org.json.JSONObject
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
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
import java.util.regex.Pattern

/**
 * EPorner Universal Plugin Provider.
 *
 * Utilizes the official EPorner JSON API, the direct dynamic web player endpoint (/xhr/video/),
 * and Headless WebResolver fallback to deliver 1080p / 720p full-length adult video scenes
 * without requiring account authentication or redirects.
 *
 * Backed by automatic DNS-over-HTTPS (DoH) via [DohDns] with intelligent ISP unblock
 * route prioritization.
 */
class EpornerPlugin(
    customClient: OkHttpClient? = null
) : UniversalPlugin {

    private var hostApi: HostApi? = null

    override fun init(host: HostApi) {
        this.hostApi = host
    }

    private val relatedCache = ConcurrentHashMap<String, List<MediaItem>>()

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

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        val rows = mutableListOf<CatalogRow>()

        supervisorScope {
            val topWeeklyDeferred = async { fetchApiVideos(query = "", order = "top-weekly", count = 16) }
            val fourKDeferred = async { fetchApiVideos(query = "4k", order = "top-weekly", count = 16) }
            val desiDeferred = async { fetchApiVideos(query = "desi", order = "top-weekly", count = 16) }
            val amateurDeferred = async { fetchApiVideos(query = "amateur", order = "top-weekly", count = 16) }

            val topWeekly = try { topWeeklyDeferred.await() } catch (_: Throwable) { emptyList() }
            if (topWeekly.isNotEmpty()) {
                rows.add(CatalogRow(title = "Weekly Top Rated", items = topWeekly))
            }

            val fourK = try { fourKDeferred.await() } catch (_: Throwable) { emptyList() }
            if (fourK.isNotEmpty()) {
                rows.add(CatalogRow(title = "4K Ultra HD Masterpieces", items = fourK))
            }

            val desi = try { desiDeferred.await() } catch (_: Throwable) { emptyList() }
            if (desi.isNotEmpty()) {
                rows.add(CatalogRow(title = "Desi & Asian Exclusive", items = desi))
            }

            val amateur = try { amateurDeferred.await() } catch (_: Throwable) { emptyList() }
            if (amateur.isNotEmpty()) {
                rows.add(CatalogRow(title = "Amateur & Homemade", items = amateur))
            }
        }

        rows
    }

    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        val cleanQuery = query.trim()
        if (cleanQuery.isBlank()) {
            return@withContext fetchApiVideos(query = "", order = "top-weekly", count = 24)
        }

        // 1. Direct hit in related cache (for instant recommendation load)
        if (relatedCache.containsKey(cleanQuery)) {
            val cached = relatedCache[cleanQuery]
            if (!cached.isNullOrEmpty()) return@withContext cached
        }

        // 2. Perform official API search
        var results = fetchApiVideos(query = cleanQuery, order = "top-weekly", count = 24)

        // 3. Fallback for long multi-word scene titles
        if (results.isEmpty() && cleanQuery.contains(" ")) {
            val keywords = cleanQuery.split(Regex("""\s+"""))
                .filter { it.length >= 4 && !it.startsWith("http", ignoreCase = true) }
                .take(2)
                .joinToString(" ")
            if (keywords.isNotBlank()) {
                results = fetchApiVideos(query = keywords, order = "top-weekly", count = 24)
            }
        }

        // 4. Recommendation guarantee: fall back to cached items or top-weekly if still empty
        if (results.isEmpty()) {
            val anyCached = relatedCache.values.flatten().distinctBy { it.id }.take(16)
            if (anyCached.isNotEmpty()) {
                return@withContext anyCached
            }
            results = fetchApiVideos(query = "", order = "top-weekly", count = 24)
        }

        results
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

            // Extract tags/genres
            val tagMatcher = Pattern.compile("""href="/cat/([^/"]+)"""").matcher(html)
            while (tagMatcher.find()) {
                val tag = tagMatcher.group(1)?.replace("-", " ")
                    ?.split(" ")?.joinToString(" ") { it.replaceFirstChar { c -> c.uppercase() } }
                if (!tag.isNullOrBlank() && tag !in genres) {
                    genres.add(tag)
                }
            }

            // Extract real related videos from page HTML and cache them for recommendations
            val relatedFromPage = parseRelatedVideosFromHtml(html)
            if (relatedFromPage.isNotEmpty()) {
                relatedCache[videoId] = relatedFromPage
                relatedCache[mediaItem.id] = relatedFromPage
                genres.firstOrNull()?.let { relatedCache[it] = relatedFromPage }
            }

            // Primary Stream Resolution Strategy: Official Web Player JSON endpoint (/xhr/video/)
            val vidMatcher = Pattern.compile("""EP\.video\.player\.vid\s*=\s*['"]([^'"]+)['"]""").matcher(html)
            val hashMatcher = Pattern.compile("""EP\.video\.player\.hash\s*=\s*['"]([^'"]+)['"]""").matcher(html)
            val extractedVid = if (vidMatcher.find()) vidMatcher.group(1) else videoId
            val extractedHash = if (hashMatcher.find()) hashMatcher.group(1) else null

            var resolvedQualities: List<Pair<String, String>> = emptyList()

            if (!extractedHash.isNullOrBlank()) {
                resolvedQualities = fetchXhrVideoSources(extractedVid, extractedHash, videoPageUrl)
            }

            // Secondary Fallback: Headless WebResolver (sniffing / DOM evaluation)
            if (resolvedQualities.isEmpty() && hostApi?.browserResolver != null) {
                resolvedQualities = resolveViaWebResolver(videoPageUrl)
            }

            // Tertiary Fallback: /dload/ redirects (filtering out /login redirects)
            if (resolvedQualities.isEmpty()) {
                val dloadMatcher = dloadPattern.matcher(html)
                val dloadPaths = mutableListOf<String>()
                while (dloadMatcher.find()) {
                    val path = dloadMatcher.group(1)
                    if (!path.isNullOrBlank() && path !in dloadPaths) {
                        dloadPaths.add(path)
                    }
                }
                val standardPaths = dloadPaths.filterNot { it.contains("-av1", ignoreCase = true) }
                val candidatePaths = if (standardPaths.isNotEmpty()) standardPaths else dloadPaths
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

                resolvedQualities = supervisorScope {
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
            }

            val qualityOrder = mapOf("1080p" to 1, "720p" to 2, "480p" to 3, "360p" to 4, "240p" to 5)
            val sortedEpisodes = resolvedQualities
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

            episodes = sortedEpisodes
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
            genres = genres.ifEmpty { listOf("Popular", "4K Ultra HD") }.take(10),
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
     * Encodes 32-character hexadecimal player hash into the Base36 format expected by /xhr/video/.
     */
    private fun encodeHash(hash: String): String? {
        if (hash.length != 32) return null
        return try {
            val h1 = hash.substring(0, 8).toLong(16).toString(36)
            val h2 = hash.substring(8, 16).toLong(16).toString(36)
            val h3 = hash.substring(16, 24).toLong(16).toString(36)
            val h4 = hash.substring(24, 32).toLong(16).toString(36)
            "$h1$h2$h3$h4"
        } catch (_: Throwable) {
            null
        }
    }

    /**
     * Queries the official dynamic player endpoint (/xhr/video/) to retrieve direct,
     * high-speed signed CDN MP4 streams without login gates.
     */
    private fun fetchXhrVideoSources(vid: String, hash: String, referer: String): List<Pair<String, String>> {
        val encodedHash = encodeHash(hash) ?: return emptyList()
        val url = "$mainUrl/xhr/video/$vid?hash=$encodedHash&domain=www.eporner.com&fallback=false&supportedFormats=mp4&_=${System.currentTimeMillis()}"
        try {
            val req = Request.Builder()
                .url(url)
                .header("User-Agent", defaultUserAgent)
                .header("Referer", referer)
                .header("X-Requested-With", "XMLHttpRequest")
                .header("Accept", "application/json, text/javascript, */*; q=0.01")
                .build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    val body = resp.body?.string() ?: return emptyList()
                    val json = JSONObject(body)
                    val sources = json.optJSONObject("sources")?.optJSONObject("mp4") ?: return emptyList()
                    val result = mutableListOf<Pair<String, String>>()
                    val keys = sources.keys()
                    while (keys.hasNext()) {
                        val key = keys.next()
                        val obj = sources.optJSONObject(key) ?: continue
                        val src = obj.optString("src")
                        val labelShort = obj.optString("labelShort", key)
                        if (src.isNotBlank() && (src.startsWith("http://") || src.startsWith("https://"))) {
                            val q = when {
                                labelShort.contains("1080") || key.contains("1080") -> "1080p"
                                labelShort.contains("720") || key.contains("720") -> "720p"
                                labelShort.contains("480") || key.contains("480") -> "480p"
                                labelShort.contains("360") || key.contains("360") -> "360p"
                                labelShort.contains("240") || key.contains("240") -> "240p"
                                else -> labelShort
                            }
                            result.add(q to src.substringBefore("?dload="))
                        }
                    }
                    return result
                }
            }
        } catch (_: Throwable) {}
        return emptyList()
    }

    /**
     * Resolves the video stream via the Host Application's Headless WebResolver.
     */
    private suspend fun resolveViaWebResolver(url: String): List<Pair<String, String>> {
        val resolver = hostApi?.browserResolver ?: return emptyList()
        return try {
            val req = BrowserResolveRequest(
                url = url,
                timeoutMs = 15000L,
                urlSniffRegex = Regex(""".*(\.mp4|/xhr/video/).*"""),
                evaluateJsAfterLoad = """
                    (function() {
                        try {
                            var vid = (window.EP && EP.video && EP.video.player && EP.video.player.vid) || '';
                            var hash = (window.EP && EP.video && EP.video.player && EP.video.player.hash) || '';
                            return JSON.stringify({ vid: vid, hash: hash });
                        } catch(e) { return ''; }
                    })()
                """.trimIndent()
            )
            val res = resolver.resolve(req)
            val result = mutableListOf<Pair<String, String>>()

            val mp4Urls = res.sniffedUrls.filter { it.contains(".mp4") && !it.contains("/login") }
            for (mp4 in mp4Urls) {
                val q = when {
                    mp4.contains("1080") -> "1080p"
                    mp4.contains("720") -> "720p"
                    mp4.contains("480") -> "480p"
                    mp4.contains("360") -> "360p"
                    mp4.contains("240") -> "240p"
                    else -> "HD"
                }
                result.add(q to mp4.substringBefore("?dload="))
            }

            if (result.isNotEmpty()) return result

            if (!res.jsResult.isNullOrBlank()) {
                try {
                    val jsObj = JSONObject(res.jsResult)
                    val v = jsObj.optString("vid")
                    val h = jsObj.optString("hash")
                    if (v.isNotBlank() && h.isNotBlank()) {
                        val sources = fetchXhrVideoSources(v, h, url)
                        if (sources.isNotEmpty()) return sources
                    }
                } catch (_: Throwable) {}
            }

            emptyList()
        } catch (_: Throwable) {
            emptyList()
        }
    }

    /**
     * Extracts related video cards directly from the HTML page for immediate recommendations.
     */
    private fun parseRelatedVideosFromHtml(html: String): List<MediaItem> {
        val items = mutableListOf<MediaItem>()
        try {
            val cardPattern = Pattern.compile(
                """href="(/video-([^"/]+)/[^"]*)".*?(?:data-src|src)="([^"]+\.(?:jpg|png|webp)[^"]*)".*?alt="([^"]*)"""",
                Pattern.DOTALL
            )
            val matcher = cardPattern.matcher(html)
            while (matcher.find()) {
                val pagePath = matcher.group(1) ?: continue
                val vid = matcher.group(2) ?: continue
                val thumb = matcher.group(3)
                val title = matcher.group(4)?.trim() ?: ""
                if (vid.isNotBlank() && title.isNotBlank() && !thumb.isNullOrBlank() && !thumb.contains("data:image")) {
                    items.add(
                        MediaItem(
                            id = vid,
                            title = title,
                            url = "$mainUrl$pagePath",
                            posterUrl = thumb,
                            backdropUrl = thumb,
                            type = MediaType.MOVIE,
                            quality = if (title.contains("4k", ignoreCase = true)) "4K" else "1080p",
                            rating = "15:00",
                            provider = name
                        )
                    )
                }
            }
        } catch (_: Throwable) {}
        return items.distinctBy { it.id }
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
        } catch (_: Throwable) {}
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
        } catch (_: Throwable) {
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
        } catch (_: Throwable) {
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
        } catch (_: Throwable) {
            null
        }
    }
}
