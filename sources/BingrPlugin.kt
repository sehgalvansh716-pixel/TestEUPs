package com.euthopiar.core.provider

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

import com.euthopiar.eup.api.CatalogSection
import com.euthopiar.eup.api.ContentType
import com.euthopiar.eup.api.DetailsProvider
import com.euthopiar.eup.api.EpisodeDescriptor
import com.euthopiar.eup.api.EupHostApi
import com.euthopiar.eup.api.HeaderPolicy
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
import com.euthopiar.eup.api.CastDescriptor
import com.euthopiar.eup.api.StreamSource as EupStreamSource
import com.euthopiar.eup.api.AudioTrackDescriptor as EupAudioTrackDescriptor

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.*
import okhttp3.*
import java.net.URLEncoder
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Official Bingr Golden Plugin (EUP v2 Modern Architecture - Multi-Cluster Streaming).
 *
 * Implements high-speed discovery, comprehensive cataloging, and robust multi-server resolution
 * of Movies, TV Series, and Anime from Bingr (https://bingr.one):
 * - REST API Discovery: Live Trending Movies, Trending TV Shows, Trending Anime, and Discover.
 * - Deep Details: Full synopsis, backdrops, ratings, genres, and complete episodic season trees.
 * - Multi-Server Streaming Clusters:
 *     1. Vidrift Orion Multi-Audio Cluster:
 *        * Original, Hindi Dub, French Dub, Russian, Spanish, Latin American Spanish,
 *          Brazilian Portuguese, and Ukrainian streams.
 *        * Master HLS quality ladder (1080p FHD, 720p HD, 480p SD, 360p SD) & Progressive MP4 containers.
 *        * Vidrift Relay Proxy master stream.
 *     2. Vidy High-Speed CDN Cluster:
 *        * In-memory pure-JVM PRNG keystream cipher (61-element S-box & "mvm1" magic).
 *        * Multi-city CDN servers (Atlanta, Miami, Dallas, Phoenix).
 *        * Individual quality renditions (1080p FHD, 720p HD, 360p SD, Auto HLS).
 *     3. Native VDRK Subtitle Engine:
 *        * 40+ language subtitles with ISO 639-1 tags and descriptive labels.
 * - Isolated Dual OkHttp Stacks (PluginHttp):
 *     * `meta`: HTTP/2 enabled, 4 conn / 2 min pool, TMDB & Bingr REST API queries.
 *     * `cdn`: Strictly forced HTTP/1.1, 6 conn / 30s pool, liveness probes & video segment transport.
 * - Playback Resilience:
 *     * Single-flight mutex TokenStore protecting against 403 token expiry races.
 *     * Non-blocking coroutines with bounded timeouts preventing UI hangs.
 */
class BingrPlugin(
    private val externalClient: OkHttpClient? = null
) : UniversalPlugin, StreamResolver, PagedCatalogProvider, PagedSearchProvider, DetailsProvider {

    constructor() : this(null)

    override val name: String = "Bingr"
    override val mainUrl: String = "https://bingr.one"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE, MediaType.TV_SERIES, MediaType.ANIME)
    override val isSearchGlobalOnly: Boolean get() = false

    override val manifest: PluginManifest = PluginManifest(
        id = "bingr",
        name = "Bingr",
        version = 1,
        apiVersion = 2,
        realm = PluginRealm.PUBLIC,
        entryClass = "com.euthopiar.core.provider.BingrPlugin",
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
        siteUrl = "https://bingr.one",
        description = "High-speed multi-server streaming with Vidrift (HLS/MP4 multi-audio & multi-quality), Vidy PRNG cipher (1080p/720p/360p), and TMDB bridging (Decoupled EUP)."
    )

    private val json = Json { ignoreUnknownKeys = true; isLenient = true; coerceInputValues = true }
    private val apiBaseUrl = "https://api.bingr.one/api"
    private val vidriftBaseUrl = "https://embed.vidrift.in"
    private val vidyApiBaseUrl = "https://api.wecollege.net"

    private val defaultUserAgent =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    private class PluginHttp(externalClient: OkHttpClient?) {
        private val resolvedDns: Dns = externalClient?.dns ?: try {
            com.euthopiar.core.network.DohDns.DEFAULT
        } catch (_: Throwable) {
            Dns.SYSTEM
        }

        val meta: OkHttpClient = externalClient ?: OkHttpClient.Builder()
            .dns(resolvedDns)
            .dispatcher(Dispatcher().apply { maxRequests = 32; maxRequestsPerHost = 8 })
            .connectionPool(ConnectionPool(4, 2, TimeUnit.MINUTES))
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(20, TimeUnit.SECONDS)
            .callTimeout(30, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true)
            .build()

        val cdn: OkHttpClient = OkHttpClient.Builder()
            .dns(resolvedDns)
            .dispatcher(Dispatcher().apply { maxRequests = 16; maxRequestsPerHost = 8 })
            .protocols(listOf(Protocol.HTTP_1_1))
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
    )

    private val sourceEntries: MutableMap<String, CachedSourceEntry> = Collections.synchronizedMap(
        object : LinkedHashMap<String, CachedSourceEntry>(32, 0.75f, true) {
            override fun removeEldestEntry(e: MutableMap.MutableEntry<String, CachedSourceEntry>): Boolean =
                size > 64
        }
    )
    private val refreshLocks = ConcurrentHashMap<String, Mutex>()

    private fun safeLog(tag: String, message: String, throwable: Throwable? = null) {
        try {
            val text = if (throwable != null) "$message: ${throwable.message}" else message
            eupHost?.log(tag, text)
        } catch (_: Throwable) {}
    }

    // ───────────────────────────── Lifecycle Hooks ─────────────────────────────
    override fun init(host: HostApi) {
        this.legacyHost = host
    }

    override suspend fun init(host: EupHostApi, scope: CoroutineScope) {
        this.eupHost = host
        this.scopeJob = SupervisorJob(scope.coroutineContext[Job])
    }

    override suspend fun destroy() {
        scopeJob?.cancel()
        scopeJob = null
        http.close()
        eupHost = null
        legacyHost = null
        sourceEntries.clear()
        refreshLocks.clear()
    }

    // ───────────────────────────── PagedCatalogProvider ─────────────────────────────
    override suspend fun sections(): List<CatalogSection> = listOf(
        CatalogSection(id = "trending_movies", title = "Trending Movies"),
        CatalogSection(id = "trending_tv", title = "Trending TV Shows"),
        CatalogSection(id = "trending_anime", title = "Trending Anime"),
        CatalogSection(id = "discover_anime", title = "Discover Anime")
    )

    override suspend fun load(section: CatalogSection, page: PageRequest): Page<MediaCard> = withContext(Dispatchers.IO) {
        val pageNum = page.cursor?.toIntOrNull() ?: 1
        val url = when (section.id) {
            "trending_movies" -> "$apiBaseUrl/trending/movie?page=$pageNum"
            "trending_tv" -> "$apiBaseUrl/trending/tv?page=$pageNum"
            "trending_anime" -> "$apiBaseUrl/anime/trending?page=$pageNum"
            "discover_anime" -> "$apiBaseUrl/anime/discover?page=$pageNum"
            else -> "$apiBaseUrl/trending/movie?page=$pageNum"
        }

        val req = Request.Builder()
            .url(url)
            .header("User-Agent", eupHost?.defaultUserAgent ?: defaultUserAgent)
            .header("Referer", "https://bingr.one/")
            .header("Accept", "application/json")
            .build()

        try {
            val body = http.meta.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
            }
            if (body.isBlank()) return@withContext Page(emptyList(), null)

            val root = json.parseToJsonElement(body).jsonObject
            val results = root["results"]?.jsonArray ?: return@withContext Page(emptyList(), null)
            val totalPages = root["total_pages"]?.jsonPrimitive?.intOrNull ?: 1

            val cards = mutableListOf<MediaCard>()
            for (item in results) {
                val obj = item.jsonObject
                val idStr = obj["id"]?.jsonPrimitive?.contentOrNull ?: continue
                val title = obj["title"]?.jsonPrimitive?.contentOrNull
                    ?: obj["name"]?.jsonPrimitive?.contentOrNull
                    ?: continue
                val typeStr = obj["type"]?.jsonPrimitive?.contentOrNull.orEmpty()
                val contentType = when {
                    section.id.contains("anime") || typeStr.equals("anime", ignoreCase = true) -> ContentType.ANIME
                    typeStr.equals("tv", ignoreCase = true) -> ContentType.TV_SERIES
                    else -> ContentType.MOVIE
                }
                val poster = obj["poster"]?.jsonPrimitive?.contentOrNull
                    ?: obj["poster_path"]?.jsonPrimitive?.contentOrNull?.let { "https://image.tmdb.org/t/p/w500$it" }
                val year = obj["year"]?.jsonPrimitive?.contentOrNull
                    ?: obj["release_date"]?.jsonPrimitive?.contentOrNull?.take(4)

                cards.add(
                    MediaCard(
                        id = idStr,
                        title = title,
                        posterUrl = poster,
                        releaseYear = year?.toIntOrNull(),
                        type = contentType
                    )
                )
            }

            val nextCursor = if (pageNum < totalPages) (pageNum + 1).toString() else null
            Page(cards, nextCursor)
        } catch (t: Throwable) {
            safeLog("BingrPlugin", "Error loading catalog section ${section.id}: ${t.message}", t)
            Page(emptyList(), null)
        }
    }

    // ───────────────────────────── PagedSearchProvider ─────────────────────────────
    override suspend fun search(query: String, page: PageRequest): Page<MediaCard> = withContext(Dispatchers.IO) {
        val pageNum = page.cursor?.toIntOrNull() ?: 1
        val encodedQuery = URLEncoder.encode(query.trim(), "UTF-8")
        val mainSearchUrl = "$apiBaseUrl/search?q=$encodedQuery&page=$pageNum"
        val animeSearchUrl = "$apiBaseUrl/anime/search?q=$encodedQuery&page=$pageNum"

        val cards = mutableListOf<MediaCard>()
        val seenIds = mutableSetOf<String>()

        coroutineScope {
            val mainDeferred = async {
                try {
                    val req = Request.Builder()
                        .url(mainSearchUrl)
                        .header("User-Agent", eupHost?.defaultUserAgent ?: defaultUserAgent)
                        .header("Referer", "https://bingr.one/")
                        .header("Accept", "application/json")
                        .build()
                    http.meta.newCall(req).execute().use { resp ->
                        if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
                    }
                } catch (_: Throwable) { "" }
            }

            val animeDeferred = async {
                try {
                    val req = Request.Builder()
                        .url(animeSearchUrl)
                        .header("User-Agent", eupHost?.defaultUserAgent ?: defaultUserAgent)
                        .header("Referer", "https://bingr.one/")
                        .header("Accept", "application/json")
                        .build()
                    http.meta.newCall(req).execute().use { resp ->
                        if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
                    }
                } catch (_: Throwable) { "" }
            }

            val mainBody = mainDeferred.await()
            val animeBody = animeDeferred.await()

            if (mainBody.isNotBlank()) {
                try {
                    val root = json.parseToJsonElement(mainBody).jsonObject
                    val results = root["results"]?.jsonArray
                    if (results != null) {
                        for (item in results) {
                            val obj = item.jsonObject
                            val id = obj["id"]?.jsonPrimitive?.contentOrNull ?: continue
                            val title = obj["title"]?.jsonPrimitive?.contentOrNull ?: continue
                            val typeStr = obj["type"]?.jsonPrimitive?.contentOrNull.orEmpty()
                            val cType = if (typeStr.equals("tv", ignoreCase = true)) ContentType.TV_SERIES else ContentType.MOVIE
                            val poster = obj["poster"]?.jsonPrimitive?.contentOrNull
                            val year = obj["year"]?.jsonPrimitive?.contentOrNull?.toIntOrNull()

                            if (seenIds.add("$cType:$id")) {
                                cards.add(MediaCard(id = id, title = title, posterUrl = poster, releaseYear = year, type = cType))
                            }
                        }
                    }
                } catch (_: Throwable) {}
            }

            if (animeBody.isNotBlank()) {
                try {
                    val root = json.parseToJsonElement(animeBody).jsonObject
                    val results = root["results"]?.jsonArray
                    if (results != null) {
                        for (item in results) {
                            val obj = item.jsonObject
                            val id = obj["id"]?.jsonPrimitive?.contentOrNull ?: continue
                            val title = obj["title"]?.jsonPrimitive?.contentOrNull ?: continue
                            val poster = obj["poster"]?.jsonPrimitive?.contentOrNull
                            val year = obj["year"]?.jsonPrimitive?.contentOrNull?.toIntOrNull()

                            if (seenIds.add("ANIME:$id")) {
                                cards.add(MediaCard(id = id, title = title, posterUrl = poster, releaseYear = year, type = ContentType.ANIME))
                            }
                        }
                    }
                } catch (_: Throwable) {}
            }
        }

        val nextCursor = if (cards.isNotEmpty()) (pageNum + 1).toString() else null
        Page(cards, nextCursor)
    }

    // ───────────────────────────── DetailsProvider ─────────────────────────────
    override suspend fun details(card: MediaCard): MediaDetails = withContext(Dispatchers.IO) {
        val endpointType = when (card.type) {
            ContentType.TV_SERIES -> "tv"
            ContentType.ANIME -> "anime"
            else -> "movie"
        }

        val url = if (card.type == ContentType.ANIME) {
            "$apiBaseUrl/anime/${card.id}?v=1"
        } else {
            "$apiBaseUrl/details/$endpointType/${card.id}?v=1"
        }

        val req = Request.Builder()
            .url(url)
            .header("User-Agent", eupHost?.defaultUserAgent ?: defaultUserAgent)
            .header("Referer", "https://bingr.one/")
            .header("Accept", "application/json")
            .build()

        try {
            val body = http.meta.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
            }
            if (body.isBlank()) {
                return@withContext MediaDetails(id = card.id, title = card.title, type = card.type)
            }

            val root = json.parseToJsonElement(body).jsonObject
            val title = root["title"]?.jsonPrimitive?.contentOrNull
                ?: root["name"]?.jsonPrimitive?.contentOrNull
                ?: card.title
            val overview = root["overview"]?.jsonPrimitive?.contentOrNull
            val poster = root["poster"]?.jsonPrimitive?.contentOrNull ?: card.posterUrl
            val backdrop = root["backdrop"]?.jsonPrimitive?.contentOrNull ?: card.backdropUrl
            val rating = root["rating"]?.jsonPrimitive?.contentOrNull
            val year = root["year"]?.jsonPrimitive?.contentOrNull?.toIntOrNull() ?: card.releaseYear
            val runtime = root["runtime"]?.jsonPrimitive?.contentOrNull?.let { "$it mins" }

            val genres = root["genres"]?.jsonArray?.mapNotNull {
                it.jsonObject["name"]?.jsonPrimitive?.contentOrNull
                    ?: it.jsonPrimitive.contentOrNull
            } ?: emptyList()

            val castList = root["cast"]?.jsonArray?.mapNotNull {
                val cObj = it.jsonObject
                val cName = cObj["name"]?.jsonPrimitive?.contentOrNull ?: return@mapNotNull null
                CastDescriptor(
                    id = cObj["id"]?.jsonPrimitive?.contentOrNull,
                    name = cName,
                    character = cObj["character"]?.jsonPrimitive?.contentOrNull,
                    profileUrl = cObj["profile_path"]?.jsonPrimitive?.contentOrNull?.let { "https://image.tmdb.org/t/p/w185$it" }
                )
            } ?: emptyList()

            val seasons = mutableListOf<SeasonDescriptor>()
            val rawSeasons = root["seasons"]?.jsonArray
            if (rawSeasons != null) {
                for (sItem in rawSeasons) {
                    val sObj = sItem.jsonObject
                    val sNum = sObj["season"]?.jsonPrimitive?.intOrNull ?: continue
                    if (sNum <= 0 && rawSeasons.size > 1) continue

                    val sName = sObj["name"]?.jsonPrimitive?.contentOrNull ?: "Season $sNum"
                    val epCount = sObj["episodes"]?.jsonPrimitive?.intOrNull ?: 1

                    val epList = mutableListOf<EpisodeDescriptor>()
                    for (epIdx in 1..epCount) {
                        epList.add(
                            EpisodeDescriptor(
                                seasonNumber = sNum,
                                episodeNumber = epIdx,
                                title = "Episode $epIdx",
                                target = PlayableTarget.Episode(
                                    tmdbId = card.id.toIntOrNull() ?: 0,
                                    season = sNum,
                                    episode = epIdx,
                                    title = title
                                )
                            )
                        )
                    }

                    seasons.add(
                        SeasonDescriptor(
                            seasonNumber = sNum,
                            name = sName,
                            episodeCount = epCount,
                            episodes = epList
                        )
                    )
                }
            }

            MediaDetails(
                id = card.id,
                title = title,
                type = card.type,
                synopsis = overview,
                posterUrl = poster,
                backdropUrl = backdrop,
                rating = rating,
                year = year,
                genres = genres,
                duration = runtime,
                cast = castList,
                seasons = seasons,
                defaultTarget = if (card.type == ContentType.TV_SERIES) {
                    PlayableTarget.Episode(
                        tmdbId = card.id.toIntOrNull() ?: 0,
                        season = 1,
                        episode = 1,
                        title = title
                    )
                } else {
                    PlayableTarget.Movie(
                        tmdbId = card.id.toIntOrNull() ?: 0,
                        title = title,
                        releaseYear = year
                    )
                }
            )
        } catch (t: Throwable) {
            safeLog("BingrPlugin", "Error fetching details for ${card.id}: ${t.message}", t)
            MediaDetails(id = card.id, title = card.title, type = card.type)
        }
    }

    // ───────────────────────────── StreamResolver ─────────────────────────────
    override fun resolve(target: PlayableTarget, ctx: ResolveContext): Flow<StreamBundleEvent> = channelFlow {
        val tmdbId: Int
        val isTv: Boolean
        val season: Int?
        val episode: Int?

        when (target) {
            is PlayableTarget.Movie -> {
                tmdbId = target.tmdbId
                isTv = false
                season = null
                episode = null
            }
            is PlayableTarget.Episode -> {
                tmdbId = target.tmdbId
                isTv = true
                season = target.season
                episode = target.episode
            }
            else -> {
                send(StreamBundleEvent.Error("Unsupported playable target: ${target::class.java.simpleName}"))
                return@channelFlow
            }
        }

        val emittedCount = AtomicInteger(0)
        val emittedKeys = Collections.synchronizedSet(HashSet<String>())
        val defaultUa = eupHost?.defaultUserAgent ?: defaultUserAgent

        // 1. Cluster 1: Vidrift Orion Multi-Audio & Warm Streams
        val vidriftJob = launch {
            withTimeoutOrNull(4500L) {
                try {
                    val vidriftUrl = if (isTv && season != null && episode != null) {
                        "$vidriftBaseUrl/embed/tv/$tmdbId/$season/$episode"
                    } else {
                        "$vidriftBaseUrl/embed/movie/$tmdbId"
                    }

                    val req = Request.Builder()
                        .url(vidriftUrl)
                        .header("User-Agent", defaultUa)
                        .header("Referer", "https://bingr.one/")
                        .build()

                    val html = http.meta.newCall(req).execute().use { resp ->
                        if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
                    }

                    if (html.isNotBlank()) {
                        parseVidriftHtml(html, tmdbId, isTv, season, episode, emittedKeys) { eupSrc, _ ->
                            if (emittedCount.incrementAndGet() > 0) {
                                send(StreamBundleEvent.SourcesFound(listOf(eupSrc)))
                            }
                        }
                    }
                } catch (ce: CancellationException) {
                    throw ce
                } catch (t: Throwable) {
                    safeLog("BingrPlugin", "Vidrift resolution error: ${t.message}", t)
                }
            }
        }

        // 2. Cluster 2: Vidy High-Speed CDN Streaming (Pure-JVM PRNG Keystream Cipher)
        val vidyJob = launch {
            withTimeoutOrNull(4500L) {
                try {
                    val seedUrl = "$vidyApiBaseUrl/seed?mediaId=$tmdbId"
                    val seedReq = Request.Builder()
                        .url(seedUrl)
                        .header("Referer", "https://www.vidy.st/")
                        .header("Origin", "https://www.vidy.st")
                        .header("User-Agent", defaultUa)
                        .build()

                    val seedBody = http.meta.newCall(seedReq).execute().use { resp ->
                        if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
                    }

                    if (seedBody.isNotBlank() && seedBody.startsWith("{")) {
                        val seedObj = json.parseToJsonElement(seedBody).jsonObject
                        val seed = seedObj["seed"]?.jsonPrimitive?.contentOrNull
                        if (!seed.isNullOrBlank()) {
                            val candidateServers = listOf("atlanta", "miami", "dallas", "phoenix")
                            coroutineScope {
                                for (srv in candidateServers) {
                                    launch {
                                        withTimeoutOrNull(3000L) {
                                            try {
                                                val srvUrl = if (isTv && season != null && episode != null) {
                                                    "$vidyApiBaseUrl/$srv/sources?mediaType=tv&seasonId=$season&episodeId=$episode&tmdbId=$tmdbId&enc=2&seed=$seed"
                                                } else {
                                                    "$vidyApiBaseUrl/$srv/sources?mediaType=movie&tmdbId=$tmdbId&enc=2&seed=$seed"
                                                }

                                                val srvReq = Request.Builder()
                                                    .url(srvUrl)
                                                    .header("Referer", "https://www.vidy.st/")
                                                    .header("Origin", "https://www.vidy.st")
                                                    .header("User-Agent", defaultUa)
                                                    .build()

                                                val srvBody = http.meta.newCall(srvReq).execute().use { r ->
                                                    if (r.isSuccessful) r.body?.string().orEmpty() else ""
                                                }

                                                if (srvBody.isNotBlank()) {
                                                    val decryptedJson = BingrCipher.decryptVidyPayload(srvBody, seed, tmdbId.toString())
                                                    if (decryptedJson != null && decryptedJson.contains("http")) {
                                                        parseVidyJson(decryptedJson, tmdbId, isTv, season, episode, srv, emittedKeys) { eupSrc, _ ->
                                                            if (emittedCount.incrementAndGet() > 0) {
                                                                send(StreamBundleEvent.SourcesFound(listOf(eupSrc)))
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
                    safeLog("BingrPlugin", "Vidy resolution error: ${t.message}", t)
                }
            }
        }

        joinAll(vidriftJob, vidyJob)

        val total = emittedCount.get()
        if (total == 0) {
            send(StreamBundleEvent.Error("No playable streaming sources available from Bingr mirrors"))
        } else {
            send(StreamBundleEvent.Done(total))
        }
    }.flowOn(Dispatchers.Default)

    override suspend fun refresh(stale: EupStreamSource, reason: RefreshReason): EupStreamSource? = withContext(Dispatchers.IO) {
        val entry = sourceEntries[stale.id] ?: return@withContext null
        val mutex = refreshLocks.computeIfAbsent(stale.id) { Mutex() }
        mutex.withLock {
            val current = sourceEntries[stale.id] ?: return@withContext null
            if (current.gen > entry.gen && System.currentTimeMillis() < current.expiresAtMs) {
                return@withLock current.realSource
            }

            val isHealthy = try {
                val req = Request.Builder()
                    .url(current.realUrl)
                    .headers(Headers.headersOf(*current.realSource.headers.sticky.flatMap { listOf(it.key, it.value) }.toTypedArray()))
                    .header("Range", "bytes=0-1024")
                    .build()
                http.cdn.newCall(req).execute().use { it.isSuccessful || it.code == 206 }
            } catch (_: Throwable) { false }

            if (isHealthy) {
                val renewed = current.realSource.copy(
                    expiresAtEpochMs = System.currentTimeMillis() + (25 * 60_000L)
                )
                sourceEntries[stale.id] = current.copy(
                    gen = current.gen + 1,
                    realSource = renewed,
                    expiresAtMs = System.currentTimeMillis() + (25 * 60_000L)
                )
                renewed
            } else {
                null
            }
        }
    }

    // ───────────────────────────── Stream Parsing Helpers ─────────────────────────────
    private suspend fun parseVidriftHtml(
        html: String,
        tmdbId: Int,
        isTv: Boolean,
        season: Int?,
        episode: Int?,
        emittedKeys: MutableSet<String>,
        onEmit: suspend (EupStreamSource, CoreStreamSource) -> Unit
    ) {
        val metaRegex = Regex("""var\s+embedMeta\s*=\s*(\{.*?\});\s*(?:var|let|const|</script>)""", RegexOption.DOT_MATCHES_ALL)
        val match = metaRegex.find(html) ?: return
        val rawJson = match.groupValues[1]

        val root = try {
            json.parseToJsonElement(rawJson).jsonObject
        } catch (_: Throwable) { return }

        val vdrkSubs = fetchVdrkSubtitles(tmdbId, isTv, season, episode)

        // 1. Parse warmStreams (Relay HLS)
        val warmStreams = root["warmStreams"]?.jsonArray
        if (warmStreams != null) {
            for (wItem in warmStreams) {
                val wObj = wItem.jsonObject
                val proxyUrl = wObj["proxyUrl"]?.jsonPrimitive?.contentOrNull
                if (!proxyUrl.isNullOrBlank() && proxyUrl.contains("http") && emittedKeys.add(proxyUrl)) {
                    val sourceId = "bingr:vidrift:relay:$tmdbId:${if (isTv) "s${season}e$episode" else "movie"}"
                    val headers = mapOf(
                        "Referer" to "https://embed.vidrift.in/",
                        "Origin" to "https://embed.vidrift.in",
                        "User-Agent" to (eupHost?.defaultUserAgent ?: defaultUserAgent)
                    )

                    val audioTracks = listOf(
                        EupAudioTrackDescriptor(
                            label = "English (Stereo AAC)",
                            language = "en",
                            isDefault = true,
                            codec = "aac",
                            channelCount = 2
                        )
                    )

                    val coreAudio = listOf(
                        CoreAudioTrackDescriptor(
                            languageName = "English",
                            isoCode = "en",
                            channels = 2,
                            codec = "AAC"
                        )
                    )

                    val eupSource = EupStreamSource(
                        id = sourceId,
                        serverId = "vidrift_relay",
                        serverLabel = "Vidrift Relay (Auto HLS)",
                        url = proxyUrl,
                        kind = StreamKind.HLS,
                        headers = HeaderPolicy(sticky = headers),
                        video = VideoInfo(height = 1080),
                        audioTracks = audioTracks,
                        subtitles = vdrkSubs.first,
                        expiresAtEpochMs = System.currentTimeMillis() + (25 * 60_000L),
                        refreshHandle = "v1|vidrift_relay"
                    )

                    val coreSource = CoreStreamSource(
                        url = proxyUrl,
                        serverName = "Vidrift (Relay Auto HLS)",
                        resolutionLabel = "Auto (HLS)",
                        quality = "Bingr Vidrift Relay [Auto HLS]",
                        isM3u8 = true,
                        audioTracks = coreAudio,
                        releaseType = AudioReleaseType.ORIGINAL,
                        headers = headers
                    )

                    sourceEntries[sourceId] = CachedSourceEntry(
                        tmdbId = tmdbId.toString(),
                        isTv = isTv,
                        season = season,
                        episode = episode,
                        serverId = "vidrift_relay",
                        gen = 1,
                        realUrl = proxyUrl,
                        realSource = eupSource,
                        expiresAtMs = System.currentTimeMillis() + (25 * 60_000L)
                    )

                    onEmit(eupSource, coreSource)
                }
            }
        }

        // 2. Parse orionStreams (Multi-Language, Multi-Quality, HLS & MP4)
        val orionStreams = root["orionStreams"]?.jsonArray
        if (orionStreams != null) {
            for (oItem in orionStreams) {
                val oObj = oItem.jsonObject
                val rawUrl = oObj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                if (rawUrl.isBlank()) continue

                val streamUrl = if (rawUrl.startsWith("/")) "https://mb.vidrift.net$rawUrl" else rawUrl
                if (!emittedKeys.add(streamUrl)) continue

                val streamType = oObj["type"]?.jsonPrimitive?.contentOrNull ?: "hls"
                val streamName = oObj["name"]?.jsonPrimitive?.contentOrNull ?: "Orion"
                val isMp4 = streamType.equals("mp4", ignoreCase = true) || streamUrl.contains(".mp4")

                var rungHeight = if (isMp4) 1080 else 1080
                val rungs = oObj["rungs"]?.jsonArray
                if (rungs != null && rungs.isNotEmpty()) {
                    val maxH = rungs.mapNotNull { it.jsonObject["height"]?.jsonPrimitive?.intOrNull }.maxOrNull()
                    if (maxH != null && maxH > 0) rungHeight = maxH
                }

                val resolutionLabel = when {
                    rungHeight >= 2160 -> "4K UHD"
                    rungHeight >= 1080 -> "1080p FHD"
                    rungHeight >= 720 -> "720p HD"
                    rungHeight >= 480 -> "480p SD"
                    else -> "360p SD"
                }

                val (audioLang, audioIso, relType) = when {
                    streamName.contains("Hindi", ignoreCase = true) -> Triple("Hindi", "hi", AudioReleaseType.DUB)
                    streamName.contains("French", ignoreCase = true) -> Triple("French", "fr", AudioReleaseType.DUB)
                    streamName.contains("Russian", ignoreCase = true) -> Triple("Russian", "ru", AudioReleaseType.DUB)
                    streamName.contains("Latin", ignoreCase = true) -> Triple("Spanish (LatAm)", "es", AudioReleaseType.DUB)
                    streamName.contains("Spanish", ignoreCase = true) -> Triple("Spanish", "es", AudioReleaseType.DUB)
                    streamName.contains("Portuguese", ignoreCase = true) || streamName.contains("Brazilian", ignoreCase = true) -> Triple("Portuguese (Brazil)", "pt", AudioReleaseType.DUB)
                    streamName.contains("Ukrainian", ignoreCase = true) -> Triple("Ukrainian", "uk", AudioReleaseType.DUB)
                    else -> Triple("English", "en", AudioReleaseType.ORIGINAL)
                }

                val cleanServerTag = streamName.replace("Orion", "").replace("·", "").trim().ifBlank { "Original" }
                val sourceId = "bingr:vidrift:orion:${cleanServerTag.lowercase()}:$tmdbId:${if (isTv) "s${season}e$episode" else "movie"}"
                val headers = mapOf(
                    "Referer" to "https://embed.vidrift.in/",
                    "Origin" to "https://embed.vidrift.in",
                    "User-Agent" to (eupHost?.defaultUserAgent ?: defaultUserAgent)
                )

                val audioTracks = listOf(
                    EupAudioTrackDescriptor(
                        label = "$audioLang ($cleanServerTag)",
                        language = audioIso,
                        isDefault = relType == AudioReleaseType.ORIGINAL,
                        codec = if (isMp4) "aac" else "mp4a.40.2",
                        channelCount = 2
                    )
                )

                val coreAudio = listOf(
                    CoreAudioTrackDescriptor(
                        languageName = audioLang,
                        isoCode = audioIso,
                        channels = 2,
                        codec = if (isMp4) "AAC" else "AAC"
                    )
                )

                val eupSource = EupStreamSource(
                    id = sourceId,
                    serverId = "orion_${cleanServerTag.filter { it.isLetterOrDigit() }.lowercase()}",
                    serverLabel = "Vidrift $streamName ($resolutionLabel ${if (isMp4) "Direct MP4" else "Master HLS"})",
                    url = streamUrl,
                    kind = if (isMp4) StreamKind.PROGRESSIVE else StreamKind.HLS,
                    headers = HeaderPolicy(sticky = headers),
                    video = VideoInfo(height = rungHeight),
                    audioTracks = audioTracks,
                    subtitles = vdrkSubs.first,
                    expiresAtEpochMs = System.currentTimeMillis() + (25 * 60_000L),
                    refreshHandle = "v1|orion|$cleanServerTag"
                )

                val coreSource = CoreStreamSource(
                    url = streamUrl,
                    serverName = "Vidrift ($streamName $resolutionLabel)",
                    resolutionLabel = resolutionLabel,
                    quality = "Bingr Vidrift $streamName [$resolutionLabel]",
                    isM3u8 = !isMp4,
                    audioTracks = coreAudio,
                    releaseType = relType,
                    headers = headers
                )

                sourceEntries[sourceId] = CachedSourceEntry(
                    tmdbId = tmdbId.toString(),
                    isTv = isTv,
                    season = season,
                    episode = episode,
                    serverId = "orion_${cleanServerTag.lowercase()}",
                    gen = 1,
                    realUrl = streamUrl,
                    realSource = eupSource,
                    expiresAtMs = System.currentTimeMillis() + (25 * 60_000L)
                )

                onEmit(eupSource, coreSource)
            }
        }
    }

    private suspend fun parseVidyJson(
        jsonStr: String,
        tmdbId: Int,
        isTv: Boolean,
        season: Int?,
        episode: Int?,
        serverTag: String,
        emittedKeys: MutableSet<String>,
        onEmit: suspend (EupStreamSource, CoreStreamSource) -> Unit
    ) {
        val root = try {
            json.parseToJsonElement(jsonStr).jsonObject
        } catch (_: Throwable) { return }

        val sources = root["sources"]?.jsonArray ?: return
        val vdrkSubs = fetchVdrkSubtitles(tmdbId, isTv, season, episode)

        for (item in sources) {
            val sObj = item.jsonObject
            val sUrl = sObj["url"]?.jsonPrimitive?.contentOrNull ?: continue
            if (!emittedKeys.add(sUrl)) continue

            val sQuality = sObj["quality"]?.jsonPrimitive?.contentOrNull ?: "1080p"
            val cleanResolution = when {
                sQuality.contains("2160") || sQuality.contains("4k", ignoreCase = true) -> "4K UHD"
                sQuality.contains("1080") -> "1080p FHD"
                sQuality.contains("720") -> "720p HD"
                sQuality.contains("480") -> "480p SD"
                sQuality.contains("360") -> "360p SD"
                sQuality.contains("auto", ignoreCase = true) -> "Auto (HLS)"
                else -> "$sQuality HD"
            }

            val sourceHeight = when {
                sQuality.contains("2160") || sQuality.contains("4k", ignoreCase = true) -> 2160
                sQuality.contains("1080") -> 1080
                sQuality.contains("720") -> 720
                sQuality.contains("480") -> 480
                else -> 360
            }

            val sourceId = "bingr:vidy:$serverTag:${sQuality.filter { it.isLetterOrDigit() }}:$tmdbId:${if (isTv) "s${season}e$episode" else "movie"}"
            val headers = mapOf(
                "Referer" to "https://www.vidy.st/",
                "Origin" to "https://www.vidy.st",
                "User-Agent" to (eupHost?.defaultUserAgent ?: defaultUserAgent)
            )

            val audioTracks = listOf(
                EupAudioTrackDescriptor(
                    label = "English (Original Stereo)",
                    language = "en",
                    isDefault = true,
                    codec = "aac",
                    channelCount = 2
                )
            )

            val coreAudio = listOf(
                CoreAudioTrackDescriptor(
                    languageName = "English",
                    isoCode = "en",
                    channels = 2,
                    codec = "AAC"
                )
            )

            val eupSource = EupStreamSource(
                id = sourceId,
                serverId = "vidy_${serverTag}_${sQuality.filter { it.isLetterOrDigit() }}",
                serverLabel = "Vidy ${serverTag.replaceFirstChar { it.uppercase() }} ($cleanResolution HLS)",
                url = sUrl,
                kind = StreamKind.HLS,
                headers = HeaderPolicy(sticky = headers),
                video = VideoInfo(height = sourceHeight),
                audioTracks = audioTracks,
                subtitles = vdrkSubs.first,
                expiresAtEpochMs = System.currentTimeMillis() + (25 * 60_000L),
                refreshHandle = "v1|vidy|$serverTag"
            )

            val coreSource = CoreStreamSource(
                url = sUrl,
                serverName = "Vidy (${serverTag.replaceFirstChar { it.uppercase() }} $cleanResolution)",
                resolutionLabel = cleanResolution,
                quality = "Bingr Vidy ${serverTag.replaceFirstChar { it.uppercase() }} [$cleanResolution]",
                isM3u8 = true,
                audioTracks = coreAudio,
                releaseType = AudioReleaseType.ORIGINAL,
                headers = headers
            )

            sourceEntries[sourceId] = CachedSourceEntry(
                tmdbId = tmdbId.toString(),
                isTv = isTv,
                season = season,
                episode = episode,
                serverId = "vidy_$serverTag",
                gen = 1,
                realUrl = sUrl,
                realSource = eupSource,
                expiresAtMs = System.currentTimeMillis() + (25 * 60_000L)
            )

            onEmit(eupSource, coreSource)
        }
    }

    private suspend fun fetchVdrkSubtitles(
        tmdbId: Int,
        isTv: Boolean,
        season: Int?,
        episode: Int?
    ): Pair<List<SubtitleDescriptor>, List<SubtitleTrack>> = withContext(Dispatchers.IO) {
        val eupSubs = mutableListOf<SubtitleDescriptor>()
        val coreSubs = mutableListOf<SubtitleTrack>()

        val url = if (isTv && season != null && episode != null) {
            "$apiBaseUrl/subtitles/vdrk/tv/$tmdbId?season=$season&ep=$episode"
        } else {
            "$apiBaseUrl/subtitles/vdrk/movie/$tmdbId"
        }

        try {
            val req = Request.Builder()
                .url(url)
                .header("User-Agent", eupHost?.defaultUserAgent ?: defaultUserAgent)
                .header("Accept", "application/json")
                .header("Referer", "https://bingr.one/")
                .build()

            val body = http.meta.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
            }

            if (body.isNotBlank() && body.startsWith("{")) {
                val root = json.parseToJsonElement(body).jsonObject
                val subArr = root["subtitles"]?.jsonArray
                if (subArr != null) {
                    for (item in subArr) {
                        val sObj = item.jsonObject
                        val sUrl = sObj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                        val lang = sObj["lang"]?.jsonPrimitive?.contentOrNull ?: "en"
                        val label = sObj["label"]?.jsonPrimitive?.contentOrNull ?: lang.uppercase()

                        eupSubs.add(
                            SubtitleDescriptor(
                                language = lang,
                                url = sUrl,
                                mimeType = "text/vtt"
                            )
                        )
                        coreSubs.add(
                            SubtitleTrack(
                                language = label,
                                url = sUrl
                            )
                        )
                    }
                }
            }
        } catch (_: Throwable) {}

        Pair(eupSubs, coreSubs)
    }

    // ───────────────────────────── UniversalPlugin Legacy Overrides ─────────────────────────────
    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        val rows = mutableListOf<CatalogRow>()
        for (sec in sections()) {
            val page = load(sec, PageRequest())
            if (page.items.isNotEmpty()) {
                val mediaItems = page.items.map { card ->
                    val isTv = card.type == ContentType.TV_SERIES
                    MediaItem(
                        id = card.id,
                        title = card.title,
                        url = "https://bingr.one/${if (isTv) "tv" else "movies"}/${card.id}",
                        posterUrl = card.posterUrl,
                        backdropUrl = card.backdropUrl,
                        type = when (card.type) {
                            ContentType.TV_SERIES -> MediaType.TV_SERIES
                            ContentType.ANIME -> MediaType.ANIME
                            else -> MediaType.MOVIE
                        },
                        year = card.releaseYear,
                        provider = name
                    )
                }
                rows.add(CatalogRow(title = sec.title, items = mediaItems))
            }
        }
        rows
    }

    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        val page = search(query, PageRequest())
        page.items.map { card ->
            val isTv = card.type == ContentType.TV_SERIES
            MediaItem(
                id = card.id,
                title = card.title,
                url = "https://bingr.one/${if (isTv) "tv" else "movies"}/${card.id}",
                posterUrl = card.posterUrl,
                backdropUrl = card.backdropUrl,
                type = when (card.type) {
                    ContentType.TV_SERIES -> MediaType.TV_SERIES
                    ContentType.ANIME -> MediaType.ANIME
                    else -> MediaType.MOVIE
                },
                year = card.releaseYear,
                provider = name
            )
        }
    }

    override suspend fun getDetails(mediaItem: MediaItem): MediaDetail = withContext(Dispatchers.IO) {
        val cType = when (mediaItem.type) {
            MediaType.TV_SERIES -> ContentType.TV_SERIES
            MediaType.ANIME -> ContentType.ANIME
            else -> ContentType.MOVIE
        }
        val card = MediaCard(
            id = mediaItem.id,
            title = mediaItem.title,
            posterUrl = mediaItem.posterUrl,
            backdropUrl = mediaItem.backdropUrl,
            type = cType,
            releaseYear = mediaItem.year
        )
        val d = details(card)

        val epItems = mutableListOf<EpisodeItem>()
        d.seasons.forEach { s ->
            s.episodes.forEach { ep ->
                epItems.add(
                    EpisodeItem(
                        id = "${mediaItem.id}:${s.seasonNumber}:${ep.episodeNumber}",
                        title = ep.title ?: "Episode ${ep.episodeNumber}",
                        seasonNumber = s.seasonNumber,
                        episodeNumber = ep.episodeNumber,
                        data = "eup://bingr/${mediaItem.id}/${s.seasonNumber}/${ep.episodeNumber}"
                    )
                )
            }
        }

        MediaDetail(
            id = d.id,
            title = d.title,
            url = mediaItem.url,
            posterUrl = d.posterUrl,
            backdropUrl = d.backdropUrl,
            type = mediaItem.type,
            year = d.year,
            synopsis = d.synopsis,
            genres = d.genres,
            episodes = epItems,
            cast = d.cast.map { CastMember(id = it.id ?: it.name, name = it.name, character = it.character, profileUrl = it.profileUrl) },
            rating = d.rating
        )
    }

    override suspend fun getStreamLinks(episodeData: String): StreamResult =
        StreamResult(emptyList())

    override fun getStreamFlow(mediaId: String, episodeData: String?): Flow<StreamEmission> =
        getStreamFlow(episodeData ?: mediaId)

    override fun getStreamFlow(episodeData: String): Flow<StreamEmission> = channelFlow {
        val parts = episodeData.removePrefix("eup://bingr/").removePrefix("eup://").split("/")
        val tmdbId = parts.getOrNull(parts.size - if (parts.size >= 3) 3 else 1)?.toIntOrNull()
            ?: parts.firstOrNull()?.toIntOrNull()
            ?: return@channelFlow

        val isTv = parts.size >= 3
        val season = if (isTv) parts[parts.size - 2].toIntOrNull() else null
        val episode = if (isTv) parts[parts.size - 1].toIntOrNull() else null

        val emittedKeys = Collections.synchronizedSet(HashSet<String>())
        val defaultUa = eupHost?.defaultUserAgent ?: defaultUserAgent

        val vidriftJob = launch {
            withTimeoutOrNull(4500L) {
                try {
                    val vidriftUrl = if (isTv && season != null && episode != null) {
                        "$vidriftBaseUrl/embed/tv/$tmdbId/$season/$episode"
                    } else {
                        "$vidriftBaseUrl/embed/movie/$tmdbId"
                    }

                    val req = Request.Builder()
                        .url(vidriftUrl)
                        .header("User-Agent", defaultUa)
                        .header("Referer", "https://bingr.one/")
                        .build()

                    val html = http.meta.newCall(req).execute().use { resp ->
                        if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
                    }

                    if (html.isNotBlank()) {
                        parseVidriftHtml(html, tmdbId, isTv, season, episode, emittedKeys) { _, coreSrc ->
                            send(StreamEmission.SourceFound(coreSrc))
                        }
                    }
                } catch (ce: CancellationException) {
                    throw ce
                } catch (t: Throwable) {
                    safeLog("BingrPlugin", "Vidrift legacy resolution error: ${t.message}", t)
                }
            }
        }

        val vidyJob = launch {
            withTimeoutOrNull(4500L) {
                try {
                    val seedUrl = "$vidyApiBaseUrl/seed?mediaId=$tmdbId"
                    val seedReq = Request.Builder()
                        .url(seedUrl)
                        .header("Referer", "https://www.vidy.st/")
                        .header("Origin", "https://www.vidy.st")
                        .header("User-Agent", defaultUa)
                        .build()

                    val seedBody = http.meta.newCall(seedReq).execute().use { resp ->
                        if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
                    }

                    if (seedBody.isNotBlank() && seedBody.startsWith("{")) {
                        val seedObj = json.parseToJsonElement(seedBody).jsonObject
                        val seed = seedObj["seed"]?.jsonPrimitive?.contentOrNull
                        if (!seed.isNullOrBlank()) {
                            val candidateServers = listOf("atlanta", "miami", "dallas", "phoenix")
                            coroutineScope {
                                for (srv in candidateServers) {
                                    launch {
                                        withTimeoutOrNull(3000L) {
                                            try {
                                                val srvUrl = if (isTv && season != null && episode != null) {
                                                    "$vidyApiBaseUrl/$srv/sources?mediaType=tv&seasonId=$season&episodeId=$episode&tmdbId=$tmdbId&enc=2&seed=$seed"
                                                } else {
                                                    "$vidyApiBaseUrl/$srv/sources?mediaType=movie&tmdbId=$tmdbId&enc=2&seed=$seed"
                                                }

                                                val srvReq = Request.Builder()
                                                    .url(srvUrl)
                                                    .header("Referer", "https://www.vidy.st/")
                                                    .header("Origin", "https://www.vidy.st")
                                                    .header("User-Agent", defaultUa)
                                                    .build()

                                                val srvBody = http.meta.newCall(srvReq).execute().use { r ->
                                                    if (r.isSuccessful) r.body?.string().orEmpty() else ""
                                                }

                                                if (srvBody.isNotBlank()) {
                                                    val decryptedJson = BingrCipher.decryptVidyPayload(srvBody, seed, tmdbId.toString())
                                                    if (decryptedJson != null && decryptedJson.contains("http")) {
                                                        parseVidyJson(decryptedJson, tmdbId, isTv, season, episode, srv, emittedKeys) { _, coreSrc ->
                                                            send(StreamEmission.SourceFound(coreSrc))
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
                    safeLog("BingrPlugin", "Vidy legacy resolution error: ${t.message}", t)
                }
            }
        }

        joinAll(vidriftJob, vidyJob)
    }.flowOn(Dispatchers.Default)
}

/**
 * Pure-JVM Bitwise PRNG Keystream Decryption Engine for Vidy stream payloads.
 */
object BingrCipher {
    private val VIDY_U = byteArrayOf(109, 118, 109, 49) // "mvm1"

    private fun vidyM(value: Int): Int {
        var e = value
        e = e xor (e ushr 16)
        e = (e.toLong() * 2246822507L).toInt()
        e = e xor (e ushr 13)
        e = (e.toLong() * 3266489909L).toInt()
        e = e xor (e ushr 16)
        return e
    }

    private fun vidyP(e: Int, t: Int): Int {
        return Integer.rotateLeft(e, t and 31)
    }

    fun decryptVidyPayload(ciphertextB64: String, seed: String, mediaIdStr: String): String? {
        return try {
            val clean = ciphertextB64.trim().replace('-', '+').replace('_', '/')
            val padded = clean.padEnd(4 * ((clean.length + 3) / 4), '=')
            val o = java.util.Base64.getDecoder().decode(padded)
            if (o.size < VIDY_U.size) return null

            val mediaIdNum = mediaIdStr.toLongOrNull()?.toInt() ?: 0
            var t1 = 2166136261L.toInt()
            for (i in seed.indices) {
                val code = seed[i].code
                t1 = ((t1 xor code).toLong() * 16777619L).toInt()
            }
            val t2 = vidyM(mediaIdNum xor 2654435769L.toInt())
            var s = vidyM(vidyM(t1) xor t2)

            val a = IntArray(61)
            val setIndices = BooleanArray(61)
            for (e in 0 until 8) {
                val t = ((s.toLong() and 0xFFFFFFFFL) % 61).toInt()
                s = vidyP((s.toLong() + 2654435769L).toInt(), 7 + (7 and e))
                a[t] = s xor vidyM(s)
                setIndices[t] = true
                s = vidyM(s + t)
            }

            var acc = vidyM(2779096485L.toInt() xor s)

            val n = ByteArray(o.size)
            var step = 0
            var byteIdx = 0
            while (byteIdx < o.size) {
                val d = ((acc.toLong() and 0xFFFFFFFFL) % 61).toInt()
                val r = if (setIndices[d]) -1 else 0
                val iVal = a[d]

                val term = iVal xor ((2654435769L * (step + 1)).toInt())
                val lVal = (acc xor term) or (acc and term and r)

                val rot1 = vidyP(lVal + acc, 31 and d)
                val rot2 = vidyP(acc, 31 and (d * 7))
                val newAcc = vidyM((rot1 xor rot2) + 2654435769L.toInt())

                a[d] = newAcc
                setIndices[d] = true
                acc = newAcc
                val tGen = newAcc

                step++

                n[byteIdx++] = (tGen and 0xFF).toByte()
                if (byteIdx < o.size) n[byteIdx++] = ((tGen ushr 8) and 0xFF).toByte()
                if (byteIdx < o.size) n[byteIdx++] = ((tGen ushr 16) and 0xFF).toByte()
                if (byteIdx < o.size) n[byteIdx++] = ((tGen ushr 24) and 0xFF).toByte()
            }

            for (i in o.indices) {
                o[i] = (o[i].toInt() xor n[i].toInt()).toByte()
            }

            for (i in VIDY_U.indices) {
                if (o[i] != VIDY_U[i]) {
                    return null
                }
            }

            String(o, VIDY_U.size, o.size - VIDY_U.size, Charsets.UTF_8)
        } catch (_: Throwable) {
            null
        }
    }
}
