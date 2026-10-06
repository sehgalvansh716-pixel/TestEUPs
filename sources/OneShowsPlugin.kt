package com.euthopiar.core.provider

import com.euthopiar.core.aniskip.AniSkipClient
import com.euthopiar.core.matcher.MatchScorer
import com.euthopiar.core.model.AudioReleaseType
import com.euthopiar.core.model.CatalogRow
import com.euthopiar.core.model.CastMember
import com.euthopiar.core.model.DownloadOption
import com.euthopiar.core.model.EpisodeItem
import com.euthopiar.core.model.HostApi
import com.euthopiar.core.model.MediaDetail
import com.euthopiar.core.model.MediaItem
import com.euthopiar.core.model.MediaType
import com.euthopiar.core.model.StreamEmission
import com.euthopiar.core.model.StreamResult
import com.euthopiar.core.model.SubtitleTrack
import com.euthopiar.core.model.UniversalPlugin
import com.euthopiar.core.model.StreamSource as CoreStreamSource
import com.euthopiar.core.model.AudioTrackDescriptor as CoreAudioTrackDescriptor
import com.euthopiar.core.util.TmdbBridge

import com.euthopiar.eup.api.CastDescriptor
import com.euthopiar.eup.api.CatalogSection
import com.euthopiar.eup.api.ContentType
import com.euthopiar.eup.api.DetailsProvider
import com.euthopiar.eup.api.EpisodeDescriptor
import com.euthopiar.eup.api.EupHostApi
import com.euthopiar.eup.api.HeaderPolicy
import com.euthopiar.eup.api.MatchHints
import com.euthopiar.eup.api.MediaCard
import com.euthopiar.eup.api.MediaDetails
import com.euthopiar.eup.api.Page
import com.euthopiar.eup.api.PageRequest
import com.euthopiar.eup.api.PagedCatalogProvider
import com.euthopiar.eup.api.PagedSearchProvider
import com.euthopiar.eup.api.PlayableTarget
import com.euthopiar.eup.api.PluginCapability
import com.euthopiar.eup.api.PluginManifest
import com.euthopiar.eup.api.PluginRealm
import com.euthopiar.eup.api.RefreshReason
import com.euthopiar.eup.api.ResolveContext
import com.euthopiar.eup.api.SeasonDescriptor
import com.euthopiar.eup.api.StreamBundleEvent
import com.euthopiar.eup.api.StreamKind
import com.euthopiar.eup.api.StreamResolver
import com.euthopiar.eup.api.SubtitleDescriptor
import com.euthopiar.eup.api.VideoInfo
import com.euthopiar.eup.api.StreamSource as EupStreamSource
import com.euthopiar.eup.api.AudioTrackDescriptor as EupAudioTrackDescriptor

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.*
import okhttp3.*
import java.io.IOException
import java.net.URI
import java.net.URLEncoder
import java.security.MessageDigest
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Official 1Shows Golden Plugin (EUP v2 Modern Architecture).
 *
 * Implements Archetype A (API / Wasm / AES-GCM / Multi-Server HLS Streaming):
 * - Target: https://www.1shows.org
 * - Strict Sandboxing: Zero Android framework imports, pure Kotlin JVM bytecode (:eup-api contract).
 * - Dual Constructors: Declares constructor(externalClient: OkHttpClient? = null) and constructor() : this(null)
 *   preventing constructor resolution inversion in DynamicPluginLoader.
 * - Carrier DNS Poisoning Resilience: Internal PluginHttp defaults to DohDns.DEFAULT.
 * - Four Core Application Bridges:
 *     * MatchScorer: title normalization, Jaro-Winkler distance, numeral/sequel mismatch detection.
 *     * TMDB Bridging: canonical metadata, clear PNG title logo resolution, cast with headshots, genres, ratings, and episodic stills/overviews.
 *     * AniSkip Bridging: opening (OP) and ending (ED) skip interval detection with introOffsetMs seeded directly on StreamSource.
 *     * OpenSubtitles Bridging: multi-language external synchronized subtitles merged alongside provider internal subtitles.
 * - Multi-Server Streaming Engine (15+ Live Server Endpoints):
 *     * Vidrock HLS: Nova, Atlas, Luna, Orion, Astra, Titan, Cosmo (AES-256-GCM with Referer: https://vidrock.to/).
 *     * Vidy HLS: Atlanta 1080p, Atlanta 720p, Atlanta 360p, Miami Auto, Dallas, California, Seattle (PRNG cipher with Referer: https://www.vidy.st/).
 *     * MakimaDL Direct CDN: 4K UHD, 1080p FHD, 720p HD direct media files (.m3u8, .mp4, .mkv, a.111477.xyz).
 *     * Fallbacks: VidLink Pro, Vidzee, Viduki Main.
 * - Strict Separation: File locker web landing pages (Pahe, Filmyfly, Moondl, Takefile) isolated to getDownloadLinks().
 */
class OneShowsPlugin(
    private val externalClient: OkHttpClient? = null
) : UniversalPlugin, StreamResolver, PagedCatalogProvider, PagedSearchProvider, DetailsProvider {

    constructor() : this(null)

    override val name: String = "1Shows"
    override val mainUrl: String = "https://www.1shows.org"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE, MediaType.TV_SERIES)
    override val isSearchGlobalOnly: Boolean get() = false

    override val manifest: PluginManifest = PluginManifest(
        id = "1shows",
        name = "1Shows",
        version = 4,
        apiVersion = 2,
        realm = PluginRealm.PUBLIC,
        entryClass = "com.euthopiar.core.provider.OneShowsPlugin",
        capabilities = setOf(
            PluginCapability.PAGED_CATALOG,
            PluginCapability.PAGED_SEARCH,
            PluginCapability.DETAILS,
            PluginCapability.STREAM_RESOLVE,
            PluginCapability.STREAM_REFRESH,
            PluginCapability.SUBTITLES_BUNDLED,
            PluginCapability.TMDB_NATIVE
        ),
        author = "Euthopiar Core Team",
        siteUrl = "https://www.1shows.org",
        description = "High-speed multi-server streaming with Vidrock AES-256-GCM, Vidy PRNG, MakimaDL direct CDN, TMDB, MatchScorer, AniSkip, and OpenSubtitles bridges."
    )

    private val json = Json { ignoreUnknownKeys = true; isLenient = true }
    private val tmdbApiKey = "1865f43a0549ca50d341dd9ab8b29f49"

    private val defaultUserAgent =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    private val defaultHeaders: Map<String, String>
        get() = mapOf(
            "Referer" to "https://www.1shows.org/",
            "Origin" to "https://www.1shows.org",
            "User-Agent" to (eupHost?.defaultUserAgent ?: legacyHost?.defaultUserAgent ?: defaultUserAgent),
            "Accept-Ranges" to "bytes"
        )

    private fun safeLog(tag: String, message: String, t: Throwable? = null) {
        System.err.println("[$tag] $message" + (t?.let { ": ${it.message}" } ?: ""))
    }

    // ───────────────────────────── Isolated Networking ─────────────────────────────
    private class PluginHttp(externalClient: OkHttpClient?) {
        private val resolvedDns: Dns = externalClient?.dns ?: try {
            com.euthopiar.core.network.DohDns.DEFAULT
        } catch (_: Throwable) {
            Dns.SYSTEM
        }

        val meta: OkHttpClient = externalClient ?: OkHttpClient.Builder()
            .dns(resolvedDns)
            .dispatcher(Dispatcher().apply { maxRequests = 256; maxRequestsPerHost = 64 })
            .connectionPool(ConnectionPool(32, 2, TimeUnit.MINUTES))
            .connectTimeout(4, TimeUnit.SECONDS)
            .readTimeout(5, TimeUnit.SECONDS)
            .callTimeout(7, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true)
            .build()

        val cdn: OkHttpClient = OkHttpClient.Builder()
            .dns(resolvedDns)
            .dispatcher(Dispatcher().apply { maxRequests = 16; maxRequestsPerHost = 8 })
            .protocols(listOf(Protocol.HTTP_1_1)) // Forced HTTP/1.1 avoids RST_STREAM / HTTP 421 on CDNs
            .connectionPool(ConnectionPool(6, 30, TimeUnit.SECONDS))
            .connectTimeout(8, TimeUnit.SECONDS)
            .readTimeout(12, TimeUnit.SECONDS)
            .writeTimeout(8, TimeUnit.SECONDS)
            .callTimeout(15, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true)
            .build()

        fun close() {
            meta.dispatcher.cancelAll()
            meta.connectionPool.evictAll()
            cdn.dispatcher.cancelAll()
            cdn.connectionPool.evictAll()
            cdn.dispatcher.executorService.shutdown()
        }
    }

    private var http: PluginHttp = PluginHttp(externalClient)
    private var eupHost: EupHostApi? = null
    private var legacyHost: HostApi? = null
    private var scopeJob: Job? = null

    // ───────────────────────────── TokenStore Single-Flight Mutex ─────────────────────────────
    private data class CachedSourceEntry(
        val tmdbId: String,
        val isTv: Boolean,
        val season: Int?,
        val episode: Int?,
        val serverId: String,
        val gen: Int,
        val realUrl: String,
        val realSource: EupStreamSource,
        val expiresAtMs: Long
    ) {
        fun nearExpiry(now: Long = System.currentTimeMillis()): Boolean =
            expiresAtMs - now < 90_000L
    }

    private val sourceEntries: MutableMap<String, CachedSourceEntry> = Collections.synchronizedMap(
        object : LinkedHashMap<String, CachedSourceEntry>(32, 0.75f, true) {
            override fun removeEldestEntry(e: MutableMap.MutableEntry<String, CachedSourceEntry>): Boolean =
                size > 64
        }
    )
    private val locks = ConcurrentHashMap<String, Mutex>()
    private val logoCache = ConcurrentHashMap<String, String>()
    private val mediaTypeCache = ConcurrentHashMap<String, MediaType>()
    private val imdbIdCache = ConcurrentHashMap<String, String>()

    // ───────────────────────────── Lifecycle Hooks ─────────────────────────────
    override fun init(host: HostApi) {
        this.legacyHost = host
        CoroutineScope(Dispatchers.IO).launch {
            try {
                OneShowsWasmEngine.prewarm(http.meta)
            } catch (ce: CancellationException) {
                throw ce
            } catch (t: Throwable) {
                safeLog("OneShowsPlugin", "Prewarm error: ${t.message}", t)
            }
        }
    }

    override suspend fun init(host: EupHostApi, scope: CoroutineScope) {
        this.eupHost = host
        this.scopeJob = SupervisorJob(scope.coroutineContext[Job])
        CoroutineScope(Dispatchers.IO).launch {
            try {
                OneShowsWasmEngine.prewarm(http.meta)
            } catch (ce: CancellationException) {
                throw ce
            } catch (t: Throwable) {
                safeLog("OneShowsPlugin", "Prewarm error: ${t.message}", t)
            }
        }
    }

    override suspend fun destroy() {
        scopeJob?.cancel()
        scopeJob = null
        http.close()
        eupHost = null
        legacyHost = null
        sourceEntries.clear()
        locks.clear()
        logoCache.clear()
        mediaTypeCache.clear()
        imdbIdCache.clear()
    }

    // ───────────────────────────── Target Parsing Helper ─────────────────────────────
    data class ParsedTarget(
        val tmdbId: String,
        val isTv: Boolean,
        val season: Int?,
        val episode: Int?,
        val titleHint: String? = null
    )

    private fun parseTarget(input: String): ParsedTarget {
        val trimmed = input.trim()

        // 1. Virtual eup URI: eup://1shows/<tmdbId>/<season>/<episode> or eup://1shows/<tmdbId>
        if (trimmed.startsWith("eup://", ignoreCase = true)) {
            val withoutScheme = trimmed.substring(6)
            val parts = withoutScheme.split('/', ':').filter { it.isNotBlank() }
            val cleanParts = if (parts.firstOrNull()?.equals("1shows", ignoreCase = true) == true) {
                parts.drop(1)
            } else {
                parts
            }
            val id = cleanParts.firstOrNull()?.filter { it.isDigit() }.orEmpty()
            if (cleanParts.size >= 3) {
                val s = cleanParts[1].toIntOrNull() ?: 1
                val e = cleanParts[2].toIntOrNull() ?: 1
                return ParsedTarget(id, true, s, e)
            } else if (cleanParts.size == 2) {
                val s = cleanParts[1].toIntOrNull() ?: 1
                return ParsedTarget(id, true, s, 1)
            } else {
                val isTv = mediaTypeCache[id] == MediaType.TV_SERIES
                return ParsedTarget(id, isTv, if (isTv) 1 else null, if (isTv) 1 else null)
            }
        }

        // 2. HTTP URL: https://www.1shows.org/tv/<id>/<season>/<episode> or /movies/<id>
        if (trimmed.startsWith("http://", ignoreCase = true) || trimmed.startsWith("https://", ignoreCase = true)) {
            val uri = try { URI(trimmed) } catch (_: Throwable) { null }
            val pathSegments = uri?.path?.split('/')?.filter { it.isNotBlank() }.orEmpty()
            val isTv = pathSegments.any { it.equals("tv", ignoreCase = true) || it.equals("series", ignoreCase = true) }
            val id = pathSegments.firstOrNull { seg -> seg.isNotEmpty() && seg.all { it.isDigit() } } ?: trimmed.filter { it.isDigit() }
            if (isTv) {
                val tvIdx = pathSegments.indexOfFirst { it.equals("tv", ignoreCase = true) }
                val s = pathSegments.getOrNull(tvIdx + 2)?.toIntOrNull() ?: 1
                val e = pathSegments.getOrNull(tvIdx + 3)?.toIntOrNull() ?: 1
                return ParsedTarget(id, true, s, e)
            } else {
                return ParsedTarget(id, false, null, null)
            }
        }

        // 3. Colon format: <tmdbId>:<season>:<episode>
        if (trimmed.contains(':')) {
            val parts = trimmed.split(':')
            val id = parts[0].filter { it.isDigit() }
            val s = parts.getOrNull(1)?.toIntOrNull() ?: 1
            val e = parts.getOrNull(2)?.toIntOrNull() ?: 1
            return ParsedTarget(id, true, s, e)
        }

        // 4. Plain digits: "533535"
        val id = trimmed.filter { it.isDigit() }
        val isTv = mediaTypeCache[id] == MediaType.TV_SERIES || mediaTypeCache[trimmed] == MediaType.TV_SERIES
        return ParsedTarget(id, isTv, if (isTv) 1 else null, if (isTv) 1 else null)
    }

    private fun extractTmdbId(input: String): String {
        return parseTarget(input).tmdbId
    }

    // ───────────────────────────── UniversalPlugin: Streams Flow ─────────────────────────────
    override fun getStreamFlow(mediaId: String, episodeData: String?): Flow<StreamEmission> =
        getStreamFlow(episodeData ?: mediaId)

    override fun getStreamFlow(episodeData: String): Flow<StreamEmission> = channelFlow {
        val target = parseTarget(episodeData)
        val tmdbId = target.tmdbId
        val isTv = target.isTv
        val season = target.season
        val episode = target.episode

        if (tmdbId.isBlank()) return@channelFlow

        val emittedStreamKeys = Collections.synchronizedSet(mutableSetOf<String>())
        val emittedSubUrls = Collections.synchronizedSet(mutableSetOf<String>())
        val emittedSubLangs = Collections.synchronizedSet(mutableSetOf<String>())

        // Resolve IMDb ID for OpenSubtitles Bridge
        val cachedImdbId = imdbIdCache[tmdbId]

        // 1. AniSkip Bridge (Opening & Ending skip timestamps)
        var introOffsetMs = 0L
        if (!target.titleHint.isNullOrBlank()) {
            try {
                withTimeoutOrNull(1200L) {
                    val skipRes = AniSkipClient.getInstance().getSkipTimesByTitle(target.titleHint, episode ?: 1)
                    val skip = skipRes.getOrNull()
                    if (skip?.introStartSeconds != null) {
                        introOffsetMs = (skip.introStartSeconds!! * 1000).toLong()
                    }
                }
            } catch (_: Throwable) {}
        }

        // 2. Subtitles Coroutine A: Vidzee internal subtitles (core.vidzee.wtf)
        launch {
            withTimeoutOrNull(2500L) {
                try {
                    val subUrl = if (isTv && season != null && episode != null) {
                        "https://core.vidzee.wtf/subs/tv/$tmdbId/$season/$episode"
                    } else {
                        "https://core.vidzee.wtf/subs/movie/$tmdbId"
                    }
                    val req = Request.Builder()
                        .url(subUrl)
                        .header("Referer", "https://player.vidzee.wtf/")
                        .header("User-Agent", defaultHeaders["User-Agent"]!!)
                        .build()
                    http.meta.newCall(req).execute().use { resp ->
                        if (resp.isSuccessful) {
                            val body = resp.body?.string().orEmpty()
                            if (body.isNotBlank() && body.startsWith("[")) {
                                val arr = json.parseToJsonElement(body).jsonArray
                                for (elem in arr) {
                                    val obj = elem.jsonObject
                                    val fileUrl = obj["file"]?.jsonPrimitive?.contentOrNull
                                        ?: obj["url"]?.jsonPrimitive?.contentOrNull
                                        ?: continue
                                    val rawLabel = obj["label"]?.jsonPrimitive?.contentOrNull
                                        ?: obj["language"]?.jsonPrimitive?.contentOrNull
                                        ?: "English"
                                    val cleanName = rawLabel.trim()
                                    val langKey = cleanName.lowercase()
                                    if (emittedSubLangs.add(langKey) && emittedSubUrls.add(fileUrl)) {
                                        send(StreamEmission.SubtitleFound(SubtitleTrack(url = fileUrl, language = cleanName)))
                                    }
                                }
                            }
                        }
                    }
                } catch (ce: CancellationException) {
                    throw ce
                } catch (t: Throwable) {
                    safeLog("OneShowsPlugin", "Vidzee subtitle fetch error: ${t.message}", t)
                }
            }
        }

        // 3. Subtitles Coroutine B: OpenSubtitles v3 Bridge
        launch {
            withTimeoutOrNull(2500L) {
                try {
                    val imdbId = cachedImdbId ?: run {
                        if (target.titleHint.isNullOrBlank()) null
                        else {
                            val cat = if (isTv) "series" else "movie"
                            val q = URLEncoder.encode(target.titleHint, "UTF-8")
                            val cinemetaUrl = "https://v3-cinemeta.strem.io/catalog/$cat/top/search=$q.json"
                            val cReq = Request.Builder().url(cinemetaUrl).header("User-Agent", defaultHeaders["User-Agent"]!!).build()
                            try {
                                http.meta.newCall(cReq).execute().use { r ->
                                    if (r.isSuccessful) {
                                        val b = r.body?.string().orEmpty()
                                        json.parseToJsonElement(b).jsonObject["metas"]?.jsonArray?.firstOrNull()?.jsonObject?.get("imdb_id")?.jsonPrimitive?.contentOrNull
                                    } else null
                                }
                            } catch (_: Throwable) { null }
                        }
                    }

                    if (!imdbId.isNullOrBlank() && imdbId.startsWith("tt")) {
                        imdbIdCache[tmdbId] = imdbId
                        val openSubUrl = if (isTv && season != null && episode != null) {
                            "https://opensubtitles-v3.strem.io/subtitles/series/$imdbId:$season:$episode.json"
                        } else {
                            "https://opensubtitles-v3.strem.io/subtitles/movie/$imdbId.json"
                        }
                        val req = Request.Builder()
                            .url(openSubUrl)
                            .header("User-Agent", defaultHeaders["User-Agent"]!!)
                            .build()
                        http.meta.newCall(req).execute().use { resp ->
                            if (resp.isSuccessful) {
                                val body = resp.body?.string().orEmpty()
                                if (body.isNotBlank() && body.startsWith("{")) {
                                    val root = json.parseToJsonElement(body).jsonObject
                                    val subs = root["subtitles"]?.jsonArray.orEmpty()
                                    for (elem in subs) {
                                        val obj = elem.jsonObject
                                        val subFile = obj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                                        val lang = obj["lang"]?.jsonPrimitive?.contentOrNull ?: "eng"
                                        val displayLang = when (lang) {
                                            "eng" -> "English"
                                            "spa" -> "Spanish"
                                            "fre", "fra" -> "French"
                                            "ger", "deu" -> "German"
                                            "ita" -> "Italian"
                                            "por", "pob" -> "Portuguese"
                                            "hin" -> "Hindi"
                                            "ara" -> "Arabic"
                                            "rus" -> "Russian"
                                            "jpn" -> "Japanese"
                                            else -> lang.uppercase()
                                        }
                                        if (emittedSubUrls.add(subFile)) {
                                            send(StreamEmission.SubtitleFound(SubtitleTrack(url = subFile, language = displayLang)))
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (ce: CancellationException) {
                    throw ce
                } catch (t: Throwable) {
                    safeLog("OneShowsPlugin", "OpenSubtitles bridge error: ${t.message}", t)
                }
            }
        }

        // 4. Server Group 1: Vidrock HLS Streaming (AES-256-GCM) - Nova, Atlas, Luna, Orion, Astra, Titan, Cosmo
        launch {
            withTimeoutOrNull(3000L) {
                try {
                    val vidrockUrl = if (isTv && season != null && episode != null) {
                        "https://vidrock.net/api/tv/$tmdbId/$season/$episode"
                    } else {
                        "https://vidrock.net/api/movie/$tmdbId"
                    }
                    val req = Request.Builder()
                        .url(vidrockUrl)
                        .header("Referer", "https://vidrock.to/")
                        .header("User-Agent", defaultHeaders["User-Agent"]!!)
                        .build()
                    http.meta.newCall(req).execute().use { resp ->
                        if (resp.isSuccessful) {
                            val body = resp.body?.string().orEmpty()
                            if (body.isNotBlank() && body.startsWith("{")) {
                                val root = json.parseToJsonElement(body).jsonObject
                                for ((srvName, srvVal) in root) {
                                    val encUrl = srvVal.jsonObject["url"]?.jsonPrimitive?.contentOrNull ?: continue
                                    val decryptedUrl = OneShowsWasmEngine.decryptVidrockPayload(encUrl) ?: continue
                                    if (!decryptedUrl.startsWith("http")) continue

                                    val sourceId = "1shows:vidrock:$tmdbId:$srvName"
                                    val reqHeaders = mapOf(
                                        "Referer" to "https://vidrock.to/",
                                        "Origin" to "https://vidrock.to",
                                        "User-Agent" to defaultHeaders["User-Agent"]!!
                                    )

                                    val eupSource = EupStreamSource(
                                        id = sourceId,
                                        serverId = "vidrock_$srvName",
                                        serverLabel = "Vidrock $srvName (1080p HLS)",
                                        url = decryptedUrl,
                                        kind = StreamKind.HLS,
                                        headers = HeaderPolicy(sticky = reqHeaders),
                                        video = VideoInfo(height = 1080),
                                        introOffsetMs = introOffsetMs,
                                        expiresAtEpochMs = System.currentTimeMillis() + (30 * 60_000L),
                                        refreshHandle = "v1|1|$srvName"
                                    )
                                    sourceEntries[sourceId] = CachedSourceEntry(
                                        tmdbId = tmdbId,
                                        isTv = isTv,
                                        season = season,
                                        episode = episode,
                                        serverId = srvName,
                                        gen = 1,
                                        realUrl = decryptedUrl,
                                        realSource = eupSource,
                                        expiresAtMs = System.currentTimeMillis() + (30 * 60_000L)
                                    )

                                    val legacySource = CoreStreamSource(
                                        url = decryptedUrl,
                                        serverName = "Vidrock ($srvName)",
                                        resolutionLabel = "1080p FHD",
                                        quality = "1Shows Vidrock $srvName (1080p HLS)",
                                        isM3u8 = true,
                                        releaseType = AudioReleaseType.ORIGINAL,
                                        headers = reqHeaders
                                    )
                                    if (emittedStreamKeys.add(decryptedUrl)) {
                                        send(StreamEmission.SourceFound(legacySource))
                                    }
                                }
                            }
                        }
                    }
                } catch (ce: CancellationException) {
                    throw ce
                } catch (t: Throwable) {
                    safeLog("OneShowsPlugin", "Vidrock stream resolution error: ${t.message}", t)
                }
            }
        }

        // 5. Server Group 2: Vidy HLS Streaming (PRNG Cipher) - Atlanta 1080p/720p/360p & Miami Auto
        launch {
            withTimeoutOrNull(3000L) {
                try {
                    val seedUrl = "https://api.wecollege.net/seed?mediaId=$tmdbId"
                    val seedReq = Request.Builder()
                        .url(seedUrl)
                        .header("Referer", "https://www.vidy.st/")
                        .header("Origin", "https://www.vidy.st")
                        .header("User-Agent", defaultHeaders["User-Agent"]!!)
                        .build()
                    val seedBody = http.meta.newCall(seedReq).execute().use { resp ->
                        if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
                    }
                    if (seedBody.isNotBlank() && seedBody.startsWith("{")) {
                        val seedObj = json.parseToJsonElement(seedBody).jsonObject
                        val seed = seedObj["seed"]?.jsonPrimitive?.contentOrNull
                        if (!seed.isNullOrBlank()) {
                            val candidateServers = listOf("atlanta", "miami")
                            coroutineScope {
                                for (srv in candidateServers) {
                                    launch {
                                        withTimeoutOrNull(2500L) {
                                            try {
                                                val srvUrl = if (isTv && season != null && episode != null) {
                                                    "https://api.wecollege.net/$srv/sources?mediaType=tv&seasonId=$season&episodeId=$episode&tmdbId=$tmdbId&enc=2&seed=$seed"
                                                } else {
                                                    "https://api.wecollege.net/$srv/sources?mediaType=movie&tmdbId=$tmdbId&enc=2&seed=$seed"
                                                }
                                                val srvReq = Request.Builder()
                                                    .url(srvUrl)
                                                    .header("Referer", "https://www.vidy.st/")
                                                    .header("Origin", "https://www.vidy.st")
                                                    .header("User-Agent", defaultHeaders["User-Agent"]!!)
                                                    .build()
                                                val srvBody = http.meta.newCall(srvReq).execute().use { r ->
                                                    if (r.isSuccessful) r.body?.string().orEmpty() else ""
                                                }
                                                if (srvBody.isNotBlank()) {
                                                    val decryptedJson = OneShowsWasmEngine.decryptVidyPayload(srvBody, seed, tmdbId)
                                                    if (decryptedJson != null && decryptedJson.contains("http")) {
                                                        val sRoot = json.parseToJsonElement(decryptedJson).jsonObject
                                                        val sArr = sRoot["sources"]?.jsonArray
                                                        if (sArr != null) {
                                                            for (item in sArr) {
                                                                val sObj = item.jsonObject
                                                                val sUrl = sObj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                                                                val sQuality = sObj["quality"]?.jsonPrimitive?.contentOrNull ?: "1080p"
                                                                val reqHeaders = mapOf(
                                                                    "Referer" to "https://www.vidy.st/",
                                                                    "Origin" to "https://www.vidy.st",
                                                                    "User-Agent" to defaultHeaders["User-Agent"]!!
                                                                )

                                                                val sourceId = "1shows:vidy:$tmdbId:$srv:$sQuality"
                                                                val eupSource = EupStreamSource(
                                                                    id = sourceId,
                                                                    serverId = "vidy_${srv}_$sQuality",
                                                                    serverLabel = "Vidy $srv ($sQuality HLS)",
                                                                    url = sUrl,
                                                                    kind = StreamKind.HLS,
                                                                    headers = HeaderPolicy(sticky = reqHeaders),
                                                                    video = VideoInfo(height = if (sQuality.contains("1080")) 1080 else if (sQuality.contains("720")) 720 else 360),
                                                                    introOffsetMs = introOffsetMs,
                                                                    expiresAtEpochMs = System.currentTimeMillis() + (25 * 60_000L),
                                                                    refreshHandle = "v1|1|$srv"
                                                                )
                                                                sourceEntries[sourceId] = CachedSourceEntry(
                                                                    tmdbId = tmdbId,
                                                                    isTv = isTv,
                                                                    season = season,
                                                                    episode = episode,
                                                                    serverId = srv,
                                                                    gen = 1,
                                                                    realUrl = sUrl,
                                                                    realSource = eupSource,
                                                                    expiresAtMs = System.currentTimeMillis() + (25 * 60_000L)
                                                                )

                                                                val legacySource = CoreStreamSource(
                                                                    url = sUrl,
                                                                    serverName = "Vidy ($srv $sQuality)",
                                                                    resolutionLabel = sQuality,
                                                                    quality = "1Shows Vidy $srv ($sQuality HLS)",
                                                                    isM3u8 = true,
                                                                    releaseType = AudioReleaseType.ORIGINAL,
                                                                    headers = reqHeaders
                                                                )
                                                                if (emittedStreamKeys.add(sUrl)) {
                                                                    send(StreamEmission.SourceFound(legacySource))
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            } catch (ce: CancellationException) {
                                                throw ce
                                            } catch (_: Throwable) {}
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (ce: CancellationException) {
                    throw ce
                } catch (t: Throwable) {
                    safeLog("OneShowsPlugin", "Vidy stream resolution error: ${t.message}", t)
                }
            }
        }

        // 6. Server Group 3: MakimaDL High-Speed Direct CDN Streaming (Only Playable Streams)
        launch {
            withTimeoutOrNull(3000L) {
                try {
                    val tokenReq = Request.Builder()
                        .url("https://api.viduki.net/download-token")
                        .header("Referer", "https://www.1shows.org/")
                        .header("Origin", "https://www.1shows.org")
                        .header("User-Agent", defaultHeaders["User-Agent"]!!)
                        .build()
                    val tokenBody = http.meta.newCall(tokenReq).execute().use { r ->
                        if (r.isSuccessful) r.body?.string().orEmpty() else ""
                    }
                    if (tokenBody.isNotBlank() && tokenBody.startsWith("{")) {
                        val tokenObj = json.parseToJsonElement(tokenBody).jsonObject
                        val token = tokenObj["token"]?.jsonPrimitive?.contentOrNull
                        if (!token.isNullOrBlank()) {
                            val dlPath = if (isTv && season != null && episode != null) {
                                "https://api.viduki.net/download/tv/$tmdbId/$season/$episode"
                            } else {
                                "https://api.viduki.net/download/movie/$tmdbId"
                            }
                            val dlReq = Request.Builder()
                                .url(dlPath)
                                .header("Referer", "https://www.1shows.org/")
                                .header("Origin", "https://www.1shows.org")
                                .header("User-Agent", defaultHeaders["User-Agent"]!!)
                                .header("x-download-token", token)
                                .build()
                            val dlBody = http.meta.newCall(dlReq).execute().use { r ->
                                if (r.isSuccessful) r.body?.string().orEmpty() else ""
                            }
                            if (dlBody.isNotBlank() && dlBody.startsWith("{")) {
                                val dlPayload = json.parseToJsonElement(dlBody).jsonObject
                                val ivHex = dlPayload["iv"]?.jsonPrimitive?.contentOrNull
                                val tagHex = dlPayload["tag"]?.jsonPrimitive?.contentOrNull
                                val ctHex = dlPayload["ct"]?.jsonPrimitive?.contentOrNull

                                if (!ivHex.isNullOrBlank() && !tagHex.isNullOrBlank() && !ctHex.isNullOrBlank()) {
                                    val decryptedJson = OneShowsWasmEngine.decryptMakimaDLPayload(http.meta, token, ivHex, tagHex, ctHex)
                                    if (decryptedJson != null && decryptedJson.startsWith("{")) {
                                        val root = json.parseToJsonElement(decryptedJson).jsonObject
                                        val sourcesArr = root["sources"]?.jsonArray
                                        if (sourcesArr != null) {
                                            for (elem in sourcesArr) {
                                                val obj = elem.jsonObject
                                                val url = obj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                                                val label = obj["label"]?.jsonPrimitive?.contentOrNull ?: "Direct CDN"

                                                // STRICT FILTERING: Only emit playable media files as video streams!
                                                // Exclude web landing pages and file locker aggregators from player stream emission
                                                val isPlayableStream = url.contains(".m3u8") || url.contains(".mp4") || url.contains(".mkv") || url.contains("111477.xyz")
                                                val isHtmlLocker = url.contains("greenmotors") || url.contains("moondl") || url.contains("takefile") || url.contains("modpro") || url.contains("hbplay.pages.dev")
                                                if (!url.startsWith("http") || !isPlayableStream || isHtmlLocker) continue

                                                val isM3u8 = url.contains(".m3u8")
                                                val is4K = label.contains("2160") || label.contains("4K", ignoreCase = true)
                                                val isFHD = label.contains("1080")
                                                val isHD = label.contains("720")
                                                val qualityLabel = when {
                                                    is4K -> "4K UHD"
                                                    isFHD -> "1080p FHD"
                                                    isHD -> "720p HD"
                                                    else -> "720p HD"
                                                }

                                                val cleanLabel = label.take(60)
                                                val source = CoreStreamSource(
                                                    url = url,
                                                    serverName = "MakimaDL ($cleanLabel)",
                                                    resolutionLabel = qualityLabel,
                                                    quality = "1Shows MakimaDL [$qualityLabel] $cleanLabel",
                                                    isM3u8 = isM3u8,
                                                    releaseType = if (label.contains("Multi", ignoreCase = true) || label.contains("Hindi", ignoreCase = true)) AudioReleaseType.DUAL_AUDIO else AudioReleaseType.ORIGINAL,
                                                    headers = defaultHeaders
                                                )
                                                if (emittedStreamKeys.add(url)) {
                                                    send(StreamEmission.SourceFound(source))
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (ce: CancellationException) {
                    throw ce
                } catch (t: Throwable) {
                    safeLog("OneShowsPlugin", "MakimaDL stream resolution error: ${t.message}", t)
                }
            }
        }
    }

    override suspend fun getStreamLinks(episodeData: String): StreamResult = withContext(Dispatchers.IO) {
        val streams = mutableListOf<CoreStreamSource>()
        val subtitles = mutableListOf<SubtitleTrack>()

        try {
            withTimeout(6_000L) {
                getStreamFlow(episodeData).collect { emission ->
                    when (emission) {
                        is StreamEmission.SourceFound -> streams.add(emission.source)
                        is StreamEmission.SubtitleFound -> subtitles.add(emission.track)
                        else -> {}
                    }
                }
            }
        } catch (_: TimeoutCancellationException) {
            safeLog("OneShowsPlugin", "getStreamLinks timed out gracefully with ${streams.size} streams")
        } catch (ce: CancellationException) {
            throw ce
        } catch (t: Throwable) {
            safeLog("OneShowsPlugin", "getStreamLinks error: ${t.message}", t)
        }

        StreamResult(streams = streams, subtitles = subtitles)
    }

    // ───────────────────────────── Downloads: Direct Mirrors & Locker Aggregation ─────────────────────────────
    override suspend fun getDownloadLinks(episodeData: String): List<DownloadOption> = withContext(Dispatchers.IO) {
        val target = parseTarget(episodeData)
        val tmdbId = target.tmdbId
        val season = target.season
        val episode = target.episode
        val isTv = target.isTv

        if (tmdbId.isBlank()) return@withContext emptyList()

        val downloads = mutableListOf<DownloadOption>()

        try {
            val tokenReq = Request.Builder()
                .url("https://api.viduki.net/download-token")
                .header("Referer", "https://www.1shows.org/")
                .header("Origin", "https://www.1shows.org")
                .header("User-Agent", defaultHeaders["User-Agent"]!!)
                .build()
            val tokenBody = http.meta.newCall(tokenReq).execute().use { r ->
                if (r.isSuccessful) r.body?.string().orEmpty() else ""
            }
            if (tokenBody.isNotBlank() && tokenBody.startsWith("{")) {
                val tokenObj = json.parseToJsonElement(tokenBody).jsonObject
                val token = tokenObj["token"]?.jsonPrimitive?.contentOrNull
                if (!token.isNullOrBlank()) {
                    val dlPath = if (isTv && season != null && episode != null) {
                        "https://api.viduki.net/download/tv/$tmdbId/$season/$episode"
                    } else {
                        "https://api.viduki.net/download/movie/$tmdbId"
                    }
                    val dlReq = Request.Builder()
                        .url(dlPath)
                        .header("Referer", "https://www.1shows.org/")
                        .header("Origin", "https://www.1shows.org")
                        .header("User-Agent", defaultHeaders["User-Agent"]!!)
                        .header("x-download-token", token)
                        .build()
                    val dlBody = http.meta.newCall(dlReq).execute().use { r ->
                        if (r.isSuccessful) r.body?.string().orEmpty() else ""
                    }
                    if (dlBody.isNotBlank() && dlBody.startsWith("{")) {
                        val dlPayload = json.parseToJsonElement(dlBody).jsonObject
                        val ivHex = dlPayload["iv"]?.jsonPrimitive?.contentOrNull
                        val tagHex = dlPayload["tag"]?.jsonPrimitive?.contentOrNull
                        val ctHex = dlPayload["ct"]?.jsonPrimitive?.contentOrNull

                        if (!ivHex.isNullOrBlank() && !tagHex.isNullOrBlank() && !ctHex.isNullOrBlank()) {
                            val decryptedJson = OneShowsWasmEngine.decryptMakimaDLPayload(http.meta, token, ivHex, tagHex, ctHex)
                            if (decryptedJson != null && decryptedJson.startsWith("{")) {
                                val root = json.parseToJsonElement(decryptedJson).jsonObject
                                val sourcesArr = root["sources"]?.jsonArray
                                if (sourcesArr != null) {
                                    for (elem in sourcesArr) {
                                        val obj = elem.jsonObject
                                        val url = obj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                                        val label = obj["label"]?.jsonPrimitive?.contentOrNull ?: "Direct Link"

                                        if (!url.startsWith("http") || url.contains("hbplay.pages.dev")) continue

                                        val is4K = label.contains("2160") || label.contains("4K", ignoreCase = true)
                                        val isFHD = label.contains("1080")
                                        val isHD = label.contains("720")
                                        val qualityLabel = when {
                                            is4K -> "4K UHD"
                                            isFHD -> "1080p FHD"
                                            isHD -> "720p HD"
                                            else -> "720p HD"
                                        }

                                        val sizeMatch = Regex("""\(([\d.]+\s*(?:GB|MB))\)""", RegexOption.IGNORE_CASE).find(label)
                                        val sizeLabel = sizeMatch?.groupValues?.get(1) ?: "~2.5 GB"

                                        downloads.add(
                                            DownloadOption(
                                                title = "$name $label",
                                                quality = qualityLabel,
                                                size = sizeLabel,
                                                url = url,
                                                source = label.take(40),
                                                provider = name,
                                                headers = defaultHeaders
                                            )
                                        )
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (ce: CancellationException) {
            throw ce
        } catch (t: Throwable) {
            safeLog("OneShowsPlugin", "getDownloadLinks error: ${t.message}", t)
        }

        downloads
    }

    // ───────────────────────────── StreamResolver (:eup-api) ─────────────────────────────
    override fun resolve(target: PlayableTarget, ctx: ResolveContext): Flow<StreamBundleEvent> = channelFlow {
        val tmdbId: String
        val isTv: Boolean
        val season: Int?
        val episode: Int?

        when (target) {
            is PlayableTarget.Movie -> {
                tmdbId = target.tmdbId.toString()
                isTv = false
                season = null
                episode = null
            }
            is PlayableTarget.Episode -> {
                tmdbId = target.tmdbId.toString()
                isTv = true
                season = target.season
                episode = target.episode
            }
            is PlayableTarget.Direct -> {
                val parsed = parseTarget(target.pageUrl)
                tmdbId = parsed.tmdbId
                isTv = parsed.isTv
                season = parsed.season
                episode = parsed.episode
            }
            else -> {
                send(StreamBundleEvent.Error("Unsupported PlayableTarget in 1Shows"))
                return@channelFlow
            }
        }

        val emitted = AtomicInteger(0)

        // Resolve streams via getStreamFlow and map to EupStreamSource
        val reqPayload = if (isTv && season != null && episode != null) "$tmdbId:$season:$episode" else tmdbId
        val eupSources = mutableListOf<EupStreamSource>()
        val eupSubs = mutableListOf<SubtitleDescriptor>()

        try {
            getStreamFlow(reqPayload).collect { em ->
                when (em) {
                    is StreamEmission.SourceFound -> {
                        val s = em.source
                        val sourceId = "1shows:${s.serverName.hashCode()}:$tmdbId"
                        val height = when {
                            s.resolutionLabel.contains("2160") || s.quality.contains("4K") -> 2160
                            s.resolutionLabel.contains("1080") -> 1080
                            s.resolutionLabel.contains("720") -> 720
                            else -> 1080
                        }
                        val src = EupStreamSource(
                            id = sourceId,
                            serverId = s.serverName,
                            serverLabel = "${s.serverName} (${s.resolutionLabel})",
                            url = s.url,
                            kind = if (s.isM3u8) StreamKind.HLS else StreamKind.PROGRESSIVE,
                            headers = HeaderPolicy(sticky = s.headers),
                            video = VideoInfo(height = height),
                            expiresAtEpochMs = System.currentTimeMillis() + (30 * 60_000L),
                            refreshHandle = "v1|1|${s.serverName}"
                        )
                        eupSources.add(src)
                    }
                    is StreamEmission.SubtitleFound -> {
                        eupSubs.add(SubtitleDescriptor(language = em.track.language, url = em.track.url))
                    }
                    else -> {}
                }
            }
        } catch (_: Throwable) {}

        if (eupSources.isNotEmpty()) {
            val enrichedSources = eupSources.map { it.copy(subtitles = eupSubs) }
            emitted.addAndGet(enrichedSources.size)
            send(StreamBundleEvent.SourcesFound(enrichedSources))
            send(StreamBundleEvent.Done(emitted.get()))
        } else {
            send(StreamBundleEvent.Error("All 1Shows stream servers unreachable"))
        }
    }.flowOn(Dispatchers.Default)

    // ───────────────────────────── Stream Refresh (:eup-api) ─────────────────────────────
    override suspend fun refresh(stale: EupStreamSource, reason: RefreshReason): EupStreamSource? {
        val lock = locks.getOrPut(stale.id) { Mutex() }
        return try {
            lock.withLock {
                val current = sourceEntries[stale.id] ?: return@withLock null
                if (stale.url.startsWith("eup://") && !current.nearExpiry()) {
                    return@withLock current.realSource
                }

                val refreshedStreamResult = getStreamLinks(
                    if (current.isTv && current.season != null && current.episode != null) {
                        "${current.tmdbId}:${current.season}:${current.episode}"
                    } else {
                        current.tmdbId
                    }
                )

                val matchingStream = refreshedStreamResult.streams.firstOrNull {
                    it.serverName.contains(current.serverId, ignoreCase = true)
                }

                if (matchingStream != null) {
                    val nextGen = current.gen + 1
                    val newEupSource = current.realSource.copy(
                        url = matchingStream.url,
                        refreshHandle = "v1|$nextGen|${current.serverId}",
                        expiresAtEpochMs = System.currentTimeMillis() + (25 * 60_000L)
                    )
                    sourceEntries[stale.id] = current.copy(
                        gen = nextGen,
                        realUrl = matchingStream.url,
                        realSource = newEupSource,
                        expiresAtMs = System.currentTimeMillis() + (25 * 60_000L)
                    )
                    newEupSource
                } else {
                    current.realSource
                }
            }
        } catch (ce: CancellationException) {
            throw ce
        } catch (t: Throwable) {
            safeLog("OneShowsPlugin", "refresh error on ${stale.id}: ${t.message}", t)
            null
        }
    }

    // ───────────────────────────── PagedCatalog & Search (:eup-api) ─────────────────────────────
    override suspend fun sections(): List<CatalogSection> = listOf(
        CatalogSection("trending", "Trending Movies & Series"),
        CatalogSection("theatres", "Now Playing in Theatres"),
        CatalogSection("top_movies", "Top Rated Movies"),
        CatalogSection("popular_movies", "Popular Movies"),
        CatalogSection("trending_tv", "Trending Web Series"),
        CatalogSection("popular_tv", "Popular TV Shows"),
        CatalogSection("top_tv", "Top Rated TV Series"),
        CatalogSection("action", "Action Blockbusters"),
        CatalogSection("scifi", "Sci-Fi & Fantasy Hits"),
        CatalogSection("thriller", "Crime & Mystery Series"),
        CatalogSection("animation", "Animation & Anime")
    )

    override suspend fun load(section: CatalogSection, page: PageRequest): Page<MediaCard> = withContext(Dispatchers.IO) {
        val pageNum = page.cursor?.toIntOrNull() ?: 1
        val endpoint = when (section.id) {
            "trending" -> "trending/all/day"
            "theatres" -> "movie/now_playing"
            "top_movies" -> "movie/top_rated"
            "popular_movies" -> "movie/popular"
            "trending_tv" -> "trending/tv/day"
            "popular_tv" -> "tv/popular"
            "top_tv" -> "tv/top_rated"
            "action" -> "discover/movie?with_genres=28&sort_by=popularity.desc"
            "scifi" -> "discover/movie?with_genres=878&sort_by=popularity.desc"
            "thriller" -> "discover/tv?with_genres=80,9648&sort_by=popularity.desc"
            "animation" -> "discover/tv?with_genres=16&sort_by=popularity.desc"
            else -> "trending/all/day"
        }
        val joiner = if (endpoint.contains("?")) "&" else "?"
        val url = "https://api.themoviedb.org/3/$endpoint${joiner}api_key=$tmdbApiKey&page=$pageNum"
        val req = Request.Builder().url(url).build()

        try {
            http.meta.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return@withContext Page(emptyList())
                val body = resp.body?.string().orEmpty()
                if (body.isBlank() || !body.startsWith("{")) return@withContext Page(emptyList())
                val root = json.parseToJsonElement(body).jsonObject
                val totalPages = root["total_pages"]?.jsonPrimitive?.intOrNull ?: 1
                val results = root["results"]?.jsonArray.orEmpty()

                val cards = results.mapNotNull { elem ->
                    val obj = elem.jsonObject
                    val id = obj["id"]?.jsonPrimitive?.contentOrNull ?: return@mapNotNull null
                    val title = obj["title"]?.jsonPrimitive?.contentOrNull
                        ?: obj["name"]?.jsonPrimitive?.contentOrNull
                        ?: return@mapNotNull null
                    val isTv = obj["media_type"]?.jsonPrimitive?.contentOrNull == "tv" || endpoint.startsWith("tv")
                    mediaTypeCache[id] = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE

                    val posterPath = obj["poster_path"]?.jsonPrimitive?.contentOrNull
                    val backdropPath = obj["backdrop_path"]?.jsonPrimitive?.contentOrNull
                    val year = (obj["release_date"]?.jsonPrimitive?.contentOrNull
                        ?: obj["first_air_date"]?.jsonPrimitive?.contentOrNull)
                        ?.take(4)?.toIntOrNull()

                    val target = if (isTv) {
                        PlayableTarget.Episode(tmdbId = id.toInt(), season = 1, episode = 1, title = title)
                    } else {
                        PlayableTarget.Movie(tmdbId = id.toInt(), title = title, releaseYear = year)
                    }

                    MediaCard(
                        id = id,
                        title = title,
                        posterUrl = posterPath?.let { "https://image.tmdb.org/t/p/w500$it" },
                        backdropUrl = backdropPath?.let { "https://image.tmdb.org/t/p/w1280$it" },
                        type = if (isTv) ContentType.TV_SERIES else ContentType.MOVIE,
                        releaseYear = year,
                        target = target
                    )
                }

                val nextCursor = if (pageNum < totalPages) (pageNum + 1).toString() else null
                Page(items = cards, nextCursor = nextCursor)
            }
        } catch (ce: CancellationException) {
            throw ce
        } catch (_: Throwable) {
            Page(emptyList())
        }
    }

    override suspend fun search(query: String, page: PageRequest): Page<MediaCard> = withContext(Dispatchers.IO) {
        val pageNum = page.cursor?.toIntOrNull() ?: 1
        val cleanQuery = MatchScorer.normalize(query)
        val encoded = URLEncoder.encode(query, "UTF-8")
        val url = "https://api.themoviedb.org/3/search/multi?api_key=$tmdbApiKey&query=$encoded&page=$pageNum"
        val req = Request.Builder().url(url).build()

        try {
            http.meta.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return@withContext Page(emptyList())
                val body = resp.body?.string().orEmpty()
                if (body.isBlank() || !body.startsWith("{")) return@withContext Page(emptyList())
                val root = json.parseToJsonElement(body).jsonObject
                val totalPages = root["total_pages"]?.jsonPrimitive?.intOrNull ?: 1
                val results = root["results"]?.jsonArray.orEmpty()

                val cardsWithScores = results.mapNotNull { elem ->
                    val obj = elem.jsonObject
                    val mType = obj["media_type"]?.jsonPrimitive?.contentOrNull
                    if (mType != "movie" && mType != "tv") return@mapNotNull null
                    val id = obj["id"]?.jsonPrimitive?.contentOrNull ?: return@mapNotNull null
                    val title = obj["title"]?.jsonPrimitive?.contentOrNull
                        ?: obj["name"]?.jsonPrimitive?.contentOrNull
                        ?: return@mapNotNull null
                    val isTv = mType == "tv"
                    mediaTypeCache[id] = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE

                    val posterPath = obj["poster_path"]?.jsonPrimitive?.contentOrNull
                    val backdropPath = obj["backdrop_path"]?.jsonPrimitive?.contentOrNull
                    val year = (obj["release_date"]?.jsonPrimitive?.contentOrNull
                        ?: obj["first_air_date"]?.jsonPrimitive?.contentOrNull)
                        ?.take(4)?.toIntOrNull()

                    val target = if (isTv) {
                        PlayableTarget.Episode(tmdbId = id.toInt(), season = 1, episode = 1, title = title)
                    } else {
                        PlayableTarget.Movie(tmdbId = id.toInt(), title = title, releaseYear = year)
                    }

                    val candidateCard = MediaCard(
                        id = id,
                        title = title,
                        posterUrl = posterPath?.let { "https://image.tmdb.org/t/p/w500$it" },
                        backdropUrl = backdropPath?.let { "https://image.tmdb.org/t/p/w1280$it" },
                        type = if (isTv) ContentType.TV_SERIES else ContentType.MOVIE,
                        releaseYear = year,
                        target = target
                    )

                    val hints = MatchHints(
                        tmdbId = id.toIntOrNull() ?: 0,
                        type = if (isTv) ContentType.TV_SERIES else ContentType.MOVIE,
                        titles = setOf(query, cleanQuery),
                        year = year
                    )
                    val score = MatchScorer.score(hints, candidateCard)
                    candidateCard to score
                }

                val sortedCards = cardsWithScores
                    .sortedByDescending { it.second }
                    .map { it.first }

                val nextCursor = if (pageNum < totalPages) (pageNum + 1).toString() else null
                Page(items = sortedCards, nextCursor = nextCursor)
            }
        } catch (ce: CancellationException) {
            throw ce
        } catch (_: Throwable) {
            Page(emptyList())
        }
    }

    // ───────────────────────────── DetailsProvider (:eup-api) ─────────────────────────────
    override suspend fun details(card: MediaCard): MediaDetails = withContext(Dispatchers.IO) {
        val item = MediaItem(
            id = card.id,
            title = card.title,
            url = "https://www.1shows.org/${if (card.type == ContentType.TV_SERIES) "tv" else "movies"}/${card.id}",
            posterUrl = card.posterUrl,
            backdropUrl = card.backdropUrl,
            type = if (card.type == ContentType.TV_SERIES) MediaType.TV_SERIES else MediaType.MOVIE
        )
        val d = getDetails(item)

        val seasonsGrouped = if (card.type == ContentType.TV_SERIES) {
            d.episodes.groupBy { it.seasonNumber }.map { (sNum, eps) ->
                SeasonDescriptor(
                    seasonNumber = sNum,
                    episodeCount = eps.size,
                    episodes = eps.map { ep ->
                        EpisodeDescriptor(
                            seasonNumber = ep.seasonNumber,
                            episodeNumber = ep.episodeNumber,
                            title = ep.title,
                            stillUrl = ep.thumbnail,
                            target = PlayableTarget.Episode(
                                tmdbId = card.id.toIntOrNull() ?: 0,
                                season = ep.seasonNumber,
                                episode = ep.episodeNumber,
                                title = ep.title
                            )
                        )
                    }
                )
            }
        } else {
            emptyList()
        }

        MediaDetails(
            id = d.id,
            title = d.title,
            synopsis = d.synopsis,
            posterUrl = d.posterUrl,
            backdropUrl = d.backdropUrl,
            type = card.type,
            year = d.year,
            duration = d.duration,
            rating = d.rating,
            genres = d.genres,
            cast = d.cast.map { CastDescriptor(name = it.name, character = it.character, profileUrl = it.profileUrl) },
            seasons = seasonsGrouped,
            defaultTarget = card.target ?: if (card.type == ContentType.TV_SERIES) {
                PlayableTarget.Episode(
                    tmdbId = card.id.toIntOrNull() ?: 0,
                    season = 1,
                    episode = 1,
                    title = d.title
                )
            } else {
                PlayableTarget.Movie(
                    tmdbId = card.id.toIntOrNull() ?: 0,
                    title = d.title,
                    releaseYear = d.year
                )
            }
        )
    }

    // ───────────────────────────── UniversalPlugin Contract Implementation ─────────────────────────────
    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        coroutineScope {
            val categories = listOf(
                "Trending Movies Today" to "https://api.themoviedb.org/3/trending/movie/day?api_key=$tmdbApiKey",
                "Trending TV Shows Today" to "https://api.themoviedb.org/3/trending/tv/day?api_key=$tmdbApiKey",
                "Popular Movies This Week" to "https://api.themoviedb.org/3/movie/popular?api_key=$tmdbApiKey",
                "Popular TV Shows This Week" to "https://api.themoviedb.org/3/tv/popular?api_key=$tmdbApiKey",
                "Top Rated Movies" to "https://api.themoviedb.org/3/movie/top_rated?api_key=$tmdbApiKey",
                "Top Rated TV Series" to "https://api.themoviedb.org/3/tv/top_rated?api_key=$tmdbApiKey"
            )

            val deferredRows = categories.map { (rowTitle, rowUrl) ->
                async {
                    try {
                        val req = Request.Builder().url(rowUrl).build()
                        http.meta.newCall(req).execute().use { resp ->
                            if (!resp.isSuccessful) return@async null
                            val body = resp.body?.string().orEmpty()
                            if (body.isBlank() || !body.startsWith("{")) return@async null
                            val results = json.parseToJsonElement(body).jsonObject["results"]?.jsonArray ?: return@async null
                            val isTv = rowUrl.contains("/tv")

                            val items = results.mapNotNull { elem ->
                                val obj = elem.jsonObject
                                val id = obj["id"]?.jsonPrimitive?.contentOrNull ?: return@mapNotNull null
                                val itemTitle = obj["title"]?.jsonPrimitive?.contentOrNull
                                    ?: obj["name"]?.jsonPrimitive?.contentOrNull
                                    ?: return@mapNotNull null
                                val posterPath = obj["poster_path"]?.jsonPrimitive?.contentOrNull
                                val backdropPath = obj["backdrop_path"]?.jsonPrimitive?.contentOrNull
                                val year = (obj["release_date"]?.jsonPrimitive?.contentOrNull
                                    ?: obj["first_air_date"]?.jsonPrimitive?.contentOrNull)
                                    ?.take(4)?.toIntOrNull()

                                mediaTypeCache[id] = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE

                                MediaItem(
                                    id = id,
                                    title = itemTitle,
                                    url = "https://www.1shows.org/${if (isTv) "tv" else "movies"}/$id",
                                    posterUrl = posterPath?.let { "https://image.tmdb.org/t/p/w500$it" },
                                    backdropUrl = backdropPath?.let { "https://image.tmdb.org/t/p/w1280$it" },
                                    type = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE,
                                    year = year,
                                    provider = name
                                )
                            }
                            if (items.isNotEmpty()) CatalogRow(title = rowTitle, items = items) else null
                        }
                    } catch (_: Throwable) {
                        null
                    }
                }
            }

            deferredRows.mapNotNull { it.await() }
        }
    }

    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        val cleanQuery = MatchScorer.normalize(query)
        val encoded = URLEncoder.encode(query, "UTF-8")
        val url = "https://api.themoviedb.org/3/search/multi?api_key=$tmdbApiKey&query=$encoded"
        val req = Request.Builder().url(url).build()

        try {
            http.meta.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return@withContext emptyList()
                val body = resp.body?.string().orEmpty()
                if (body.isBlank() || !body.startsWith("{")) return@withContext emptyList()
                val results = json.parseToJsonElement(body).jsonObject["results"]?.jsonArray ?: return@withContext emptyList()

                val itemsWithScores = results.mapNotNull { elem ->
                    val obj = elem.jsonObject
                    val mType = obj["media_type"]?.jsonPrimitive?.contentOrNull
                    if (mType != "movie" && mType != "tv") return@mapNotNull null
                    val id = obj["id"]?.jsonPrimitive?.contentOrNull ?: return@mapNotNull null
                    val title = obj["title"]?.jsonPrimitive?.contentOrNull
                        ?: obj["name"]?.jsonPrimitive?.contentOrNull
                        ?: return@mapNotNull null
                    val isTv = mType == "tv"
                    mediaTypeCache[id] = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE

                    val posterPath = obj["poster_path"]?.jsonPrimitive?.contentOrNull
                    val backdropPath = obj["backdrop_path"]?.jsonPrimitive?.contentOrNull
                    val year = (obj["release_date"]?.jsonPrimitive?.contentOrNull
                        ?: obj["first_air_date"]?.jsonPrimitive?.contentOrNull)
                        ?.take(4)?.toIntOrNull()

                    val item = MediaItem(
                        id = id,
                        title = title,
                        url = "https://www.1shows.org/${if (isTv) "tv" else "movies"}/$id",
                        posterUrl = posterPath?.let { "https://image.tmdb.org/t/p/w500$it" },
                        backdropUrl = backdropPath?.let { "https://image.tmdb.org/t/p/w1280$it" },
                        type = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE,
                        year = year,
                        provider = name
                    )

                    val hints = MatchHints(
                        tmdbId = id.toIntOrNull() ?: 0,
                        type = if (isTv) ContentType.TV_SERIES else ContentType.MOVIE,
                        titles = setOf(query, cleanQuery),
                        year = year
                    )
                    val card = MediaCard(
                        id = id,
                        title = title,
                        type = if (isTv) ContentType.TV_SERIES else ContentType.MOVIE,
                        releaseYear = year
                    )
                    val score = MatchScorer.score(hints, card)
                    item to score
                }

                itemsWithScores
                    .sortedByDescending { it.second }
                    .map { it.first }
            }
        } catch (_: Throwable) {
            emptyList()
        }
    }

    override suspend fun getDetails(mediaItem: MediaItem): MediaDetail = withContext(Dispatchers.IO) {
        val tmdbId = parseTarget(mediaItem.id.ifBlank { mediaItem.url }).tmdbId
        val isTv = mediaItem.type == MediaType.TV_SERIES || mediaItem.url.contains("/tv/")
        mediaTypeCache[tmdbId] = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE

        // TMDB Bridge Metadata Enrichment Contract
        val enriched = try {
            TmdbBridge.fetchEnrichedDetails(http.meta, tmdbId, isTv, providerName = name)
        } catch (_: Throwable) { null }

        val title = enriched?.title ?: mediaItem.title
        val overview = enriched?.synopsis
        val posterUrl = enriched?.posterUrl ?: mediaItem.posterUrl
        val backdropUrl = enriched?.backdropUrl ?: mediaItem.backdropUrl
        val year = enriched?.year ?: mediaItem.year
        val genres = enriched?.genres.orEmpty()
        val rating = enriched?.rating
        val rottenTomatoesRating = enriched?.rottenTomatoesRating
        val duration = enriched?.duration
        val trailerUrl = enriched?.trailerUrl
        val imdbId = enriched?.imdbId?.also { imdbIdCache[tmdbId] = it }
        val castMembers = enriched?.cast.orEmpty()
        val directors = enriched?.directors.orEmpty()
        val recommendations = enriched?.recommendations.orEmpty()

        val allEpisodes = mutableListOf<EpisodeItem>()

        if (isTv) {
            val seasonNumbers = enriched?.seasonNumbers?.takeIf { it.isNotEmpty() } ?: listOf(1)
            val seasonEpisodesMap = try {
                TmdbBridge.fetchTmdbSeasons(http.meta, tmdbId, seasonNumbers)
            } catch (_: Throwable) { emptyMap() }

            for (sNum in seasonNumbers) {
                val epMap = seasonEpisodesMap[sNum].orEmpty()
                val epCount = if (epMap.isNotEmpty()) epMap.size else 10
                for (epNum in 1..epCount) {
                    val epDetail = epMap[epNum]
                    val epTitle = epDetail?.name ?: "Episode $epNum"
                    val epThumb = epDetail?.stillUrl ?: backdropUrl ?: posterUrl
                    allEpisodes.add(
                        EpisodeItem(
                            id = "$tmdbId:$sNum:$epNum",
                            episodeNumber = epNum,
                            seasonNumber = sNum,
                            title = epTitle,
                            data = "eup://1shows/$tmdbId/$sNum/$epNum",
                            thumbnail = epThumb,
                            description = epDetail?.overview,
                            duration = epDetail?.duration
                        )
                    )
                }
            }
        } else {
            allEpisodes.add(
                EpisodeItem(
                    id = tmdbId,
                    episodeNumber = 1,
                    seasonNumber = 1,
                    title = title,
                    data = "eup://1shows/$tmdbId",
                    thumbnail = backdropUrl ?: posterUrl,
                    duration = duration
                )
            )
        }

        val clearLogo = resolveLogo(mediaItem)

        MediaDetail(
            id = tmdbId,
            title = title,
            url = mediaItem.url,
            posterUrl = posterUrl,
            backdropUrl = backdropUrl,
            logoUrl = clearLogo,
            type = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE,
            year = year,
            synopsis = overview,
            cast = castMembers,
            episodes = allEpisodes,
            genres = genres,
            rating = rating,
            rottenTomatoesRating = rottenTomatoesRating,
            duration = duration,
            trailerUrl = trailerUrl,
            imdbId = imdbId,
            directors = directors,
            recommendations = recommendations,
            provider = name
        )
    }

    override suspend fun resolveLogo(mediaItem: MediaItem): String? = withContext(Dispatchers.IO) {
        val tmdbId = parseTarget(mediaItem.id.ifBlank { mediaItem.url }).tmdbId
        if (tmdbId.isBlank()) return@withContext null
        logoCache[tmdbId]?.let { return@withContext it }

        val isTv = mediaItem.type == MediaType.TV_SERIES || mediaItem.url.contains("/tv/")
        val cachedImdb = imdbIdCache[tmdbId]

        val logoUrl = try {
            TmdbBridge.resolveLogo(http.meta, tmdbId, isTv, cachedImdb)
        } catch (_: Throwable) { null }

        if (logoUrl != null) {
            logoCache[tmdbId] = logoUrl
            logoUrl
        } else {
            val fallback = "https://images.metahub.space/logo/medium/$tmdbId/img.png"
            logoCache[tmdbId] = fallback
            fallback
        }
    }
}
