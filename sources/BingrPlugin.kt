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
import com.euthopiar.core.browser.BrowserResolveRequest
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
import java.io.Closeable
import java.net.URLEncoder
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Flagship Golden EUP v2 & UniversalPlugin Provider for Bingr (https://bingr.one).
 *
 * Capabilities:
 *  - 31 Rich Catalog Sections: Home Hero Carousel Spotlight, 10 Movies, 10 TV Shows, 10 Anime.
 *  - Seamless TMDB Bridging via TmdbBridge & MatchScorer fuzzy matching.
 *  - Full AniSkip Integration: auto-skipping intro/outro timestamps.
 *  - Dual Subtitle Clusters: Internal VDRK (43+ tracks) + External OpenSubtitles v3 Bridge.
 *  - Complete Video Clusters:
 *      * Vidrift Orion Multi-Audio Cluster (Original, Hindi Dub, French Dub, Russian MP4, Spanish MP4, etc.)
 *      * Vidrift Warm Relay Cluster (Master HLS)
 *      * Vidy Multi-Server Cluster (Atlanta, Miami, Dallas, Phoenix) via pure-JVM PRNG cipher (BingrCipher)
 *      * WebResolver Bridge: Headless sniffing for Filmu, Cinezo, Vidbolt embeds
 *  - Direct Fast-Download Locker Integration via Streamrip.fun (Showbox, Bollyflix, etc.)
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
        version = 2,
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
        author = "Euthopiar Core",
        description = "High-speed multi-server streaming with Vidrift Multi-Audio (Hindi/French/Russian/Spanish), Vidy PRNG cipher, 31 rich catalogs, Home Hero Carousel, AniSkip, OpenSubtitles, and direct Streamrip downloads."
    )

    private val apiBaseUrl = "https://api.bingr.one/api"
    private val vidriftBaseUrl = "https://embed.vidrift.in"
    private val vidyApiBaseUrl = "https://api.wecollege.net"
    private val downloadApiBaseUrl = "https://streamrip.fun/api/download"
    private val defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    private val defaultHeaders = mapOf(
        "User-Agent" to defaultUserAgent,
        "Referer" to "https://bingr.one/",
        "Accept" to "application/json"
    )

    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
        coerceInputValues = true
    }

    private class PluginHttp(externalClient: OkHttpClient?) : Closeable {
        private val sharedPool = ConnectionPool(16, 5, TimeUnit.MINUTES)

        val meta: OkHttpClient = (externalClient?.newBuilder() ?: OkHttpClient.Builder())
            .connectionPool(sharedPool)
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(10, TimeUnit.SECONDS)
            .writeTimeout(10, TimeUnit.SECONDS)
            .followRedirects(true)
            .followSslRedirects(true)
            .build()

        val cdn: OkHttpClient = (externalClient?.newBuilder() ?: OkHttpClient.Builder())
            .connectionPool(sharedPool)
            .protocols(listOf(Protocol.HTTP_1_1))
            .connectTimeout(8, TimeUnit.SECONDS)
            .readTimeout(8, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true)
            .followRedirects(true)
            .followSslRedirects(true)
            .build()

        override fun close() {
            meta.dispatcher.executorService.shutdown()
            cdn.dispatcher.executorService.shutdown()
            sharedPool.evictAll()
        }
    }

    private val http = PluginHttp(externalClient)
    private var eupHost: EupHostApi? = null
    private var legacyHost: HostApi? = null
    private var scopeJob: CompletableJob? = null

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

    private val sourceEntries = ConcurrentHashMap<String, CachedSourceEntry>()
    private val refreshLocks = ConcurrentHashMap<String, Mutex>()
    private val imdbIdCache = ConcurrentHashMap<String, String>()
    private val tmdbIdAnimeCache = ConcurrentHashMap<String, String>()

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
        imdbIdCache.clear()
        tmdbIdAnimeCache.clear()
    }

    // ───────────────────────────── PagedCatalogProvider (31 Sections) ─────────────────────────────
    override suspend fun sections(): List<CatalogSection> = listOf(
        // Flagship Home Hero Carousel & Spotlight
        CatalogSection(id = "featured_spotlight", title = "Featured Spotlight"),

        // Movies (10 Catalogs)
        CatalogSection(id = "trending_movies", title = "Trending Movies"),
        CatalogSection(id = "popular_movies", title = "Popular Movies"),
        CatalogSection(id = "top_rated_movies", title = "Top Rated Movies"),
        CatalogSection(id = "upcoming_movies", title = "Upcoming Movies"),
        CatalogSection(id = "action_movies", title = "Blockbuster Action Movies"),
        CatalogSection(id = "scifi_movies", title = "Sci-Fi & Fantasy Movies"),
        CatalogSection(id = "horror_movies", title = "Spine-Chilling Horror"),
        CatalogSection(id = "romance_movies", title = "Heartwarming Romance"),
        CatalogSection(id = "comedy_movies", title = "Comedy Hits"),
        CatalogSection(id = "animation_movies", title = "Animation Feature Films"),

        // TV Shows (10 Catalogs)
        CatalogSection(id = "trending_tv", title = "Trending TV Shows"),
        CatalogSection(id = "popular_tv", title = "Popular TV Shows"),
        CatalogSection(id = "top_rated_tv", title = "Top Rated TV Shows"),
        CatalogSection(id = "upcoming_tv", title = "Upcoming TV Series"),
        CatalogSection(id = "crime_tv", title = "Crime & Thriller Series"),
        CatalogSection(id = "comedy_tv", title = "Binge-Worthy Comedy Series"),
        CatalogSection(id = "action_adventure_tv", title = "Action & Adventure Series"),
        CatalogSection(id = "scifi_tv", title = "Sci-Fi & Fantasy TV"),
        CatalogSection(id = "animation_tv", title = "Animation TV Shows"),
        CatalogSection(id = "drama_tv", title = "Drama Series"),

        // Anime (10 Catalogs)
        CatalogSection(id = "trending_anime", title = "Trending Anime"),
        CatalogSection(id = "discover_anime", title = "Discover Anime"),
        CatalogSection(id = "popular_anime", title = "Popular Anime"),
        CatalogSection(id = "top_rated_anime", title = "Top Rated Anime"),
        CatalogSection(id = "action_anime", title = "Action Anime"),
        CatalogSection(id = "adventure_anime", title = "Adventure Anime"),
        CatalogSection(id = "fantasy_anime", title = "Fantasy Anime"),
        CatalogSection(id = "comedy_anime", title = "Comedy Anime"),
        CatalogSection(id = "scifi_anime", title = "Sci-Fi Anime"),
        CatalogSection(id = "anime_movies", title = "Top Rated Anime Movies")
    )

    override suspend fun load(section: CatalogSection, page: PageRequest): Page<MediaCard> = withContext(Dispatchers.IO) {
        val pageNum = page.cursor?.toIntOrNull() ?: 1
        val endpoint = when (section.id) {
            "featured_spotlight" -> "/trending/all?page=$pageNum"

            // Movies
            "trending_movies" -> "/trending/movie?page=$pageNum"
            "popular_movies" -> "/discover/movie?sort_by=popularity.desc&page=$pageNum"
            "top_rated_movies" -> "/discover/movie?sort_by=vote_average.desc&min_votes=1000&page=$pageNum"
            "upcoming_movies" -> "/discover/movie?upcoming=true&sort_by=popularity.desc&page=$pageNum"
            "action_movies" -> "/discover/movie?genre=28&sort_by=popularity.desc&page=$pageNum"
            "scifi_movies" -> "/discover/movie?genre=878&sort_by=popularity.desc&page=$pageNum"
            "horror_movies" -> "/discover/movie?genre=27&sort_by=popularity.desc&page=$pageNum"
            "romance_movies" -> "/discover/movie?genre=10749&sort_by=popularity.desc&page=$pageNum"
            "comedy_movies" -> "/discover/movie?genre=35&sort_by=popularity.desc&page=$pageNum"
            "animation_movies" -> "/discover/movie?genre=16&sort_by=popularity.desc&page=$pageNum"

            // TV Shows
            "trending_tv" -> "/trending/tv?page=$pageNum"
            "popular_tv" -> "/discover/tv?sort_by=popularity.desc&page=$pageNum"
            "top_rated_tv" -> "/discover/tv?sort_by=vote_average.desc&min_votes=500&page=$pageNum"
            "upcoming_tv" -> "/discover/tv?upcoming=true&sort_by=popularity.desc&page=$pageNum"
            "crime_tv" -> "/discover/tv?with_genres=80,9648&sort_by=popularity.desc&page=$pageNum"
            "comedy_tv" -> "/discover/tv?with_genres=35&sort_by=popularity.desc&page=$pageNum"
            "action_adventure_tv" -> "/discover/tv?with_genres=10759&sort_by=popularity.desc&page=$pageNum"
            "scifi_tv" -> "/discover/tv?with_genres=10765&sort_by=popularity.desc&page=$pageNum"
            "animation_tv" -> "/discover/tv?genre=16&page=$pageNum"
            "drama_tv" -> "/discover/tv?with_genres=18&sort_by=popularity.desc&page=$pageNum"

            // Anime
            "trending_anime" -> "/anime/trending?page=$pageNum"
            "discover_anime" -> "/anime/discover?page=$pageNum"
            "popular_anime" -> "/anime/discover?sort=POPULARITY_DESC&isAdult=false&page=$pageNum"
            "top_rated_anime" -> "/anime/discover?sort=SCORE_DESC&isAdult=false&page=$pageNum"
            "action_anime" -> "/anime/discover?genre=Action&sort=POPULARITY_DESC&isAdult=false&page=$pageNum"
            "adventure_anime" -> "/anime/discover?genre=Adventure&sort=POPULARITY_DESC&isAdult=false&page=$pageNum"
            "fantasy_anime" -> "/anime/discover?genre=Fantasy&sort=SCORE_DESC&isAdult=false&page=$pageNum"
            "comedy_anime" -> "/anime/discover?genre=Comedy&sort=POPULARITY_DESC&isAdult=false&page=$pageNum"
            "scifi_anime" -> "/anime/discover?genre=Sci-Fi&sort=POPULARITY_DESC&isAdult=false&page=$pageNum"
            "anime_movies" -> "/anime/discover?sort=SCORE_DESC&format=MOVIE&page=$pageNum"

            else -> "/trending/movie?page=$pageNum"
        }

        val url = "$apiBaseUrl$endpoint"
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
                    typeStr.equals("tv", ignoreCase = true) || section.id.contains("_tv") -> ContentType.TV_SERIES
                    else -> ContentType.MOVIE
                }
                val poster = obj["poster"]?.jsonPrimitive?.contentOrNull
                    ?: obj["poster_path"]?.jsonPrimitive?.contentOrNull?.let { "https://image.tmdb.org/t/p/w500$it" }
                val backdrop = obj["backdrop"]?.jsonPrimitive?.contentOrNull
                    ?: obj["backdrop_original"]?.jsonPrimitive?.contentOrNull
                    ?: obj["backdrop_path"]?.jsonPrimitive?.contentOrNull?.let { "https://image.tmdb.org/t/p/original$it" }
                val year = obj["year"]?.jsonPrimitive?.contentOrNull
                    ?: obj["release_date"]?.jsonPrimitive?.contentOrNull?.take(4)

                cards.add(
                    MediaCard(
                        id = idStr,
                        title = title,
                        posterUrl = poster,
                        backdropUrl = backdrop,
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

    // ───────────────────────────── PagedSearchProvider (MatchScorer Guided) ─────────────────────────────
    override suspend fun search(query: String, page: PageRequest): Page<MediaCard> = withContext(Dispatchers.IO) {
        val pageNum = page.cursor?.toIntOrNull() ?: 1
        val cleanQuery = MatchScorer.normalize(query)
        val encodedQuery = URLEncoder.encode(cleanQuery.ifBlank { query }.trim(), "UTF-8")
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

            fun parseIntoCards(body: String, defaultType: ContentType?) {
                if (body.isBlank()) return
                val root = try { json.parseToJsonElement(body).jsonObject } catch (_: Throwable) { return }
                val results = root["results"]?.jsonArray ?: return
                for (item in results) {
                    val obj = item.jsonObject
                    val idStr = obj["id"]?.jsonPrimitive?.contentOrNull ?: continue
                    if (!seenIds.add(idStr)) continue

                    val title = obj["title"]?.jsonPrimitive?.contentOrNull
                        ?: obj["name"]?.jsonPrimitive?.contentOrNull
                        ?: continue
                    val typeStr = obj["type"]?.jsonPrimitive?.contentOrNull.orEmpty()
                    val cType = when {
                        defaultType != null -> defaultType
                        typeStr.equals("anime", ignoreCase = true) -> ContentType.ANIME
                        typeStr.equals("tv", ignoreCase = true) -> ContentType.TV_SERIES
                        else -> ContentType.MOVIE
                    }
                    val poster = obj["poster"]?.jsonPrimitive?.contentOrNull
                        ?: obj["poster_path"]?.jsonPrimitive?.contentOrNull?.let { "https://image.tmdb.org/t/p/w500$it" }
                    val backdrop = obj["backdrop"]?.jsonPrimitive?.contentOrNull
                        ?: obj["backdrop_original"]?.jsonPrimitive?.contentOrNull
                        ?: obj["backdrop_path"]?.jsonPrimitive?.contentOrNull?.let { "https://image.tmdb.org/t/p/original$it" }
                    val year = obj["year"]?.jsonPrimitive?.contentOrNull
                        ?: obj["release_date"]?.jsonPrimitive?.contentOrNull?.take(4)

                    cards.add(
                        MediaCard(
                            id = idStr,
                            title = title,
                            posterUrl = poster,
                            backdropUrl = backdrop,
                            releaseYear = year?.toIntOrNull(),
                            type = cType
                        )
                    )
                }
            }

            parseIntoCards(mainBody, null)
            parseIntoCards(animeBody, ContentType.ANIME)
        }

        // Apply MatchScorer ranking to prioritize highest quality title matches
        val scoredCards = cards.map { card ->
            val hints = MatchHints(
                tmdbId = 0,
                imdbId = null,
                type = card.type,
                titles = setOf(cleanQuery, query),
                year = card.releaseYear,
                runtimeMin = null,
                season = null,
                episode = null
            )
            val score = MatchScorer.score(hints, card)
            card to score
        }.sortedByDescending { it.second }.map { it.first }

        val nextCursor = if (scoredCards.size >= 15) (pageNum + 1).toString() else null
        Page(scoredCards, nextCursor)
    }

    // ───────────────────────────── DetailsProvider (TMDB Enriched) ─────────────────────────────
    override suspend fun details(card: MediaCard): MediaDetails = withContext(Dispatchers.IO) {
        val isAnime = card.type == ContentType.ANIME
        val isTv = card.type == ContentType.TV_SERIES

        val endpoint = when {
            isAnime -> "$apiBaseUrl/anime/${card.id}?v=1"
            isTv -> "$apiBaseUrl/details/tv/${card.id}?v=1"
            else -> "$apiBaseUrl/details/movie/${card.id}?v=1"
        }

        val req = Request.Builder()
            .url(endpoint)
            .header("User-Agent", eupHost?.defaultUserAgent ?: defaultUserAgent)
            .header("Referer", "https://bingr.one/")
            .header("Accept", "application/json")
            .build()

        try {
            val body = http.meta.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
            }
            if (body.isBlank()) return@withContext fallbackDetails(card)

            val root = json.parseToJsonElement(body).jsonObject
            val title = root["title"]?.jsonPrimitive?.contentOrNull
                ?: root["name"]?.jsonPrimitive?.contentOrNull
                ?: card.title
            val overview = root["overview"]?.jsonPrimitive?.contentOrNull
            val poster = root["poster"]?.jsonPrimitive?.contentOrNull
                ?: root["poster_path"]?.jsonPrimitive?.contentOrNull?.let { "https://image.tmdb.org/t/p/w500$it" }
                ?: card.posterUrl
            val backdrop = root["backdrop"]?.jsonPrimitive?.contentOrNull
                ?: root["backdrop_original"]?.jsonPrimitive?.contentOrNull
                ?: root["backdrop_path"]?.jsonPrimitive?.contentOrNull?.let { "https://image.tmdb.org/t/p/original$it" }
                ?: card.backdropUrl
            val rating = root["rating"]?.jsonPrimitive?.floatOrNull

            // Cache TMDB and IMDb IDs for bridge lookups
            val tmdbIdStr = root["tmdb_id"]?.jsonPrimitive?.contentOrNull ?: card.id
            if (isAnime) {
                tmdbIdAnimeCache[card.id] = tmdbIdStr
            }
            root["imdb_id"]?.jsonPrimitive?.contentOrNull?.let { imdbId ->
                imdbIdCache[tmdbIdStr] = imdbId
                imdbIdCache[card.id] = imdbId
            }

            val genresList = root["genres"]?.jsonArray?.mapNotNull {
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
                                    tmdbId = tmdbIdStr.toIntOrNull() ?: card.id.toIntOrNull() ?: 0,
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
                rating = rating?.toString(),
                genres = genresList,
                cast = castList,
                seasons = seasons,
                year = card.releaseYear
            )
        } catch (t: Throwable) {
            safeLog("BingrPlugin", "Error loading details for ${card.id}: ${t.message}", t)
            fallbackDetails(card)
        }
    }

    private fun fallbackDetails(card: MediaCard): MediaDetails {
        return MediaDetails(
            id = card.id,
            title = card.title,
            type = card.type,
            posterUrl = card.posterUrl,
            backdropUrl = card.backdropUrl,
            year = card.releaseYear
        )
    }

    // ───────────────────────────── StreamResolver (Multi-Audio, Multi-Server, AniSkip) ─────────────────────────────
    override fun resolve(target: PlayableTarget, ctx: ResolveContext): Flow<StreamBundleEvent> = channelFlow {
        val isTv = target is PlayableTarget.Episode
        val tmdbId = when (target) {
            is PlayableTarget.Movie -> target.tmdbId
            is PlayableTarget.Episode -> target.tmdbId
            else -> 0
        }
        val season = if (isTv) (target as PlayableTarget.Episode).season else null
        val episode = if (isTv) (target as PlayableTarget.Episode).episode else null
        val titleHint = when (target) {
            is PlayableTarget.Movie -> target.title.orEmpty()
            is PlayableTarget.Episode -> target.title.orEmpty()
            else -> ""
        }

        if (tmdbId <= 0) {
            send(StreamBundleEvent.Error("Invalid target TMDB identifier for Bingr: $tmdbId"))
            return@channelFlow
        }

        val emittedKeys = Collections.synchronizedSet(HashSet<String>())
        val defaultUa = eupHost?.defaultUserAgent ?: defaultUserAgent
        val emittedCount = AtomicInteger(0)

        // 1. AniSkip Bridge: Auto-Skip Intro/Outro Bounded Lookup
        var introOffsetMs = 0L
        if (titleHint.isNotBlank()) {
            try {
                withTimeoutOrNull(1200L) {
                    val skipRes = AniSkipClient.getInstance().getSkipTimesByTitle(titleHint, episode ?: 1)
                    val skip = skipRes.getOrNull()
                    if (skip?.introStartSeconds != null) {
                        introOffsetMs = (skip.introStartSeconds!! * 1000).toLong()
                    }
                }
            } catch (_: Throwable) {}
        }

        // 2. Subtitles: VDRK Multi-Lingual Cluster + OpenSubtitles v3 Bridge
        val (eupSubs, _) = fetchCombinedSubtitles(tmdbId, isTv, season, episode, titleHint)

        // 3. Cluster 1: Vidrift Orion Multi-Audio & Multi-Container Streaming
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
                        parseVidriftHtml(html, tmdbId, isTv, season, episode, introOffsetMs, eupSubs, emittedKeys) { eupSrc, _ ->
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

        // 4. Cluster 2: Vidy High-Speed CDN Streaming (Pure-JVM PRNG Keystream Cipher)
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
                                                        parseVidyJson(decryptedJson, tmdbId, isTv, season, episode, srv, introOffsetMs, eupSubs, emittedKeys) { eupSrc, _ ->
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

        // 5. Cluster 3: WebResolver Headless Sniffing for Filmu / Cinezo / Vidbolt Embeds
        val webResolverJob = launch {
            legacyHost?.browserResolver?.let { resolver ->
                withTimeoutOrNull(6000L) {
                    val embeds = listOf(
                        "filmu" to if (isTv && season != null && episode != null) "https://embed.filmu.in/tv/$tmdbId/$season/$episode" else "https://embed.filmu.in/movie/$tmdbId",
                        "cinezo" to if (isTv && season != null && episode != null) "https://player.cinezo.live/embed/tv/$tmdbId/$season/$episode" else "https://player.cinezo.live/embed/movie/$tmdbId"
                    )
                    coroutineScope {
                        for ((srvTag, embedUrl) in embeds) {
                            launch {
                                try {
                                    val bReq = BrowserResolveRequest(
                                        url = embedUrl,
                                        headers = mapOf("Referer" to "https://bingr.one/"),
                                        userAgent = defaultUa,
                                        timeoutMs = 5000L,
                                        urlSniffRegex = Regex(""".*\.(?:m3u8|mp4).*""")
                                    )
                                    val bRes = resolver.resolve(bReq)
                                    for (sniffed in bRes.sniffedUrls) {
                                        if (sniffed.startsWith("http") && emittedKeys.add(sniffed)) {
                                            val isM3u8 = sniffed.contains(".m3u8")
                                            val sourceId = "bingr:$srvTag:$tmdbId:${if (isTv) "s${season}e$episode" else "movie"}"
                                            val eupSource = EupStreamSource(
                                                id = sourceId,
                                                serverId = srvTag,
                                                serverLabel = "${srvTag.replaceFirstChar { it.uppercase() }} (Auto ${if (isM3u8) "HLS" else "MP4"})",
                                                url = sniffed,
                                                kind = if (isM3u8) StreamKind.HLS else StreamKind.PROGRESSIVE,
                                                headers = HeaderPolicy(sticky = mapOf("Referer" to embedUrl, "User-Agent" to defaultUa)),
                                                video = VideoInfo(height = 1080),
                                                introOffsetMs = introOffsetMs,
                                                subtitles = eupSubs,
                                                expiresAtEpochMs = System.currentTimeMillis() + (25 * 60_000L)
                                            )
                                            if (emittedCount.incrementAndGet() > 0) {
                                                send(StreamBundleEvent.SourcesFound(listOf(eupSource)))
                                            }
                                        }
                                    }
                                } catch (_: Throwable) {}
                            }
                        }
                    }
                }
            }
        }

        joinAll(vidriftJob, vidyJob, webResolverJob)

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
                current.realSource
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
        introOffsetMs: Long,
        eupSubs: List<SubtitleDescriptor>,
        emittedKeys: MutableSet<String>,
        onEmit: suspend (EupStreamSource, CoreStreamSource) -> Unit
    ) {
        val metaRegex = Regex("""var\s+embedMeta\s*=\s*(\{.*?\});\s*(?:var|let|const|</script>)""", RegexOption.DOT_MATCHES_ALL)
        val match = metaRegex.find(html) ?: return
        val rawJson = match.groupValues[1]

        val root = try {
            json.parseToJsonElement(rawJson).jsonObject
        } catch (_: Throwable) { return }

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
                        serverLabel = "Vidrift Relay (English Stereo AAC Master HLS)",
                        url = proxyUrl,
                        kind = StreamKind.HLS,
                        headers = HeaderPolicy(sticky = headers),
                        video = VideoInfo(height = 1080),
                        audioTracks = audioTracks,
                        subtitles = eupSubs,
                        introOffsetMs = introOffsetMs,
                        expiresAtEpochMs = System.currentTimeMillis() + (25 * 60_000L),
                        refreshHandle = "v1|relay"
                    )

                    val coreSource = CoreStreamSource(
                        url = proxyUrl,
                        serverName = "Vidrift Relay (English Stereo AAC)",
                        resolutionLabel = "1080p FHD",
                        quality = "Bingr Vidrift Relay [1080p Master HLS]",
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

        // 2. Parse orionStreams (Multi-Audio & Multi-Quality Variants)
        val orionStreams = root["orionStreams"]?.jsonArray
        if (orionStreams != null) {
            for (oItem in orionStreams) {
                val oObj = oItem.jsonObject
                val streamName = oObj["name"]?.jsonPrimitive?.contentOrNull ?: "Orion"
                val rawUrl = oObj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                val streamType = oObj["type"]?.jsonPrimitive?.contentOrNull?.lowercase() ?: "hls"
                val isMp4 = streamType == "mp4"

                val streamUrl = if (rawUrl.startsWith("/")) "https://mb.vidrift.net$rawUrl" else rawUrl
                if (!streamUrl.startsWith("http") || !emittedKeys.add(streamUrl)) continue

                var rungHeight = 1080
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
                        codec = "AAC"
                    )
                )

                val displayServerName = "Vidrift Orion [$audioLang Dub] ($resolutionLabel ${if (isMp4) "Direct MP4" else "Master HLS"})"

                val eupSource = EupStreamSource(
                    id = sourceId,
                    serverId = "orion_${cleanServerTag.filter { it.isLetterOrDigit() }.lowercase()}",
                    serverLabel = displayServerName,
                    url = streamUrl,
                    kind = if (isMp4) StreamKind.PROGRESSIVE else StreamKind.HLS,
                    headers = HeaderPolicy(sticky = headers),
                    video = VideoInfo(height = rungHeight),
                    audioTracks = audioTracks,
                    subtitles = eupSubs,
                    introOffsetMs = introOffsetMs,
                    expiresAtEpochMs = System.currentTimeMillis() + (25 * 60_000L),
                    refreshHandle = "v1|orion|$cleanServerTag"
                )

                val coreSource = CoreStreamSource(
                    url = streamUrl,
                    serverName = displayServerName,
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
        decryptedJson: String,
        tmdbId: Int,
        isTv: Boolean,
        season: Int?,
        episode: Int?,
        serverRegion: String,
        introOffsetMs: Long,
        eupSubs: List<SubtitleDescriptor>,
        emittedKeys: MutableSet<String>,
        onEmit: suspend (EupStreamSource, CoreStreamSource) -> Unit
    ) {
        val root = try {
            json.parseToJsonElement(decryptedJson).jsonObject
        } catch (_: Throwable) { return }

        val sources = root["sources"]?.jsonArray ?: return
        for (srcItem in sources) {
            val srcObj = srcItem.jsonObject
            val streamUrl = srcObj["url"]?.jsonPrimitive?.contentOrNull ?: continue
            val qualityRaw = srcObj["quality"]?.jsonPrimitive?.contentOrNull ?: "auto"

            if (!streamUrl.startsWith("http") || !emittedKeys.add(streamUrl)) continue

            val (rungHeight, cleanResLabel) = when {
                qualityRaw.contains("2160") || qualityRaw.contains("4k", ignoreCase = true) -> Pair(2160, "4K UHD")
                qualityRaw.contains("1080") -> Pair(1080, "1080p FHD")
                qualityRaw.contains("720") -> Pair(720, "720p HD")
                qualityRaw.contains("480") -> Pair(480, "480p SD")
                qualityRaw.contains("360") -> Pair(360, "360p SD")
                else -> Pair(1080, "Auto HLS")
            }

            val sourceId = "bingr:vidy:$serverRegion:$qualityRaw:$tmdbId:${if (isTv) "s${season}e$episode" else "movie"}"
            val headers = mapOf(
                "Referer" to "https://www.vidy.st/",
                "Origin" to "https://www.vidy.st",
                "User-Agent" to (eupHost?.defaultUserAgent ?: defaultUserAgent)
            )

            val audioTracks = listOf(
                EupAudioTrackDescriptor(
                    label = "English (Stereo AAC)",
                    language = "en",
                    isDefault = true,
                    codec = "mp4a.40.2",
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

            val displayServerName = "Vidy ${serverRegion.replaceFirstChar { it.uppercase() }} [$cleanResLabel Master HLS]"

            val eupSource = EupStreamSource(
                id = sourceId,
                serverId = "vidy_${serverRegion}_$qualityRaw",
                serverLabel = displayServerName,
                url = streamUrl,
                kind = StreamKind.HLS,
                headers = HeaderPolicy(sticky = headers),
                video = VideoInfo(height = rungHeight),
                audioTracks = audioTracks,
                subtitles = eupSubs,
                introOffsetMs = introOffsetMs,
                expiresAtEpochMs = System.currentTimeMillis() + (25 * 60_000L),
                refreshHandle = "v1|vidy|$serverRegion|$qualityRaw"
            )

            val coreSource = CoreStreamSource(
                url = streamUrl,
                serverName = displayServerName,
                resolutionLabel = cleanResLabel,
                quality = "Bingr Vidy ${serverRegion.replaceFirstChar { it.uppercase() }} [$cleanResLabel]",
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
                serverId = "vidy_${serverRegion}_$qualityRaw",
                gen = 1,
                realUrl = streamUrl,
                realSource = eupSource,
                expiresAtMs = System.currentTimeMillis() + (25 * 60_000L)
            )

            onEmit(eupSource, coreSource)
        }
    }

    // ───────────────────────────── Subtitle Resolution (VDRK + OpenSubtitles v3) ─────────────────────────────
    private suspend fun fetchCombinedSubtitles(
        tmdbId: Int,
        isTv: Boolean,
        season: Int?,
        episode: Int?,
        titleHint: String
    ): Pair<List<SubtitleDescriptor>, List<SubtitleTrack>> = withContext(Dispatchers.IO) {
        val eupSubs = mutableListOf<SubtitleDescriptor>()
        val coreSubs = mutableListOf<SubtitleTrack>()
        val seenUrls = mutableSetOf<String>()

        // 1. Native VDRK Subtitles
        try {
            val subEndpoint = if (isTv && season != null && episode != null) {
                "$apiBaseUrl/subtitles/vdrk/tv/$tmdbId/$season/$episode"
            } else {
                "$apiBaseUrl/subtitles/vdrk/movie/$tmdbId"
            }

            val req = Request.Builder()
                .url(subEndpoint)
                .header("User-Agent", eupHost?.defaultUserAgent ?: defaultUserAgent)
                .header("Referer", "https://bingr.one/")
                .build()

            val body = http.meta.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
            }

            if (body.isNotBlank() && body.startsWith("{")) {
                val root = json.parseToJsonElement(body).jsonObject
                val subArray = root["subtitles"]?.jsonArray
                if (subArray != null) {
                    for (item in subArray) {
                        val obj = item.jsonObject
                        val lang = obj["language"]?.jsonPrimitive?.contentOrNull ?: "English"
                        val url = obj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                        if (!url.startsWith("http") || !seenUrls.add(url)) continue

                        eupSubs.add(SubtitleDescriptor(language = lang, url = url))
                        coreSubs.add(SubtitleTrack(language = lang, url = url))
                    }
                }
            }
        } catch (_: Throwable) {}

        // 2. OpenSubtitles v3 Bridge via IMDb ID
        try {
            withTimeoutOrNull(2500L) {
                val imdbId = imdbIdCache[tmdbId.toString()] ?: run {
                    if (titleHint.isBlank()) null
                    else {
                        val cat = if (isTv) "series" else "movie"
                        val q = URLEncoder.encode(titleHint, "UTF-8")
                        val cinemetaUrl = "https://v3-cinemeta.strem.io/catalog/$cat/top/search=$q.json"
                        val cReq = Request.Builder().url(cinemetaUrl).header("User-Agent", defaultUserAgent).build()
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
                    imdbIdCache[tmdbId.toString()] = imdbId
                    val openSubUrl = if (isTv && season != null && episode != null) {
                        "https://opensubtitles-v3.strem.io/subtitles/series/$imdbId:$season:$episode.json"
                    } else {
                        "https://opensubtitles-v3.strem.io/subtitles/movie/$imdbId.json"
                    }
                    val req = Request.Builder()
                        .url(openSubUrl)
                        .header("User-Agent", defaultUserAgent)
                        .build()
                    http.meta.newCall(req).execute().use { resp ->
                        if (resp.isSuccessful) {
                            val body = resp.body?.string().orEmpty()
                            if (body.isNotBlank() && body.startsWith("{")) {
                                val root = json.parseToJsonElement(body).jsonObject
                                val subs = root["subtitles"]?.jsonArray.orEmpty()
                                for (s in subs) {
                                    val sObj = s.jsonObject
                                    val lang = sObj["lang"]?.jsonPrimitive?.contentOrNull ?: "English"
                                    val subUrl = sObj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                                    if (subUrl.startsWith("http") && seenUrls.add(subUrl)) {
                                        val displayLang = "OpenSubtitles $lang"
                                        eupSubs.add(SubtitleDescriptor(language = displayLang, url = subUrl))
                                        coreSubs.add(SubtitleTrack(language = displayLang, url = subUrl))
                                    }
                                }
                            }
                        }
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

        val tmdbId = tmdbIdAnimeCache[mediaItem.id] ?: mediaItem.id
        val isTv = mediaItem.type == MediaType.TV_SERIES

        // TMDB Bridge Metadata & Logo Enrichment Contract
        val enriched = try {
            TmdbBridge.fetchEnrichedDetails(http.meta, tmdbId, isTv, providerName = name)
        } catch (_: Throwable) { null }

        val logoUrl = try {
            TmdbBridge.resolveLogo(http.meta, tmdbId, isTv)
        } catch (_: Throwable) { null }

        val title = enriched?.title ?: d.title
        val synopsis = enriched?.synopsis ?: d.synopsis
        val poster = enriched?.posterUrl ?: d.posterUrl
        val backdrop = enriched?.backdropUrl ?: d.backdropUrl
        val rating = enriched?.rating ?: d.rating
        val genres = (enriched?.genres.orEmpty() + d.genres).distinct()

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
            title = title,
            url = mediaItem.url,
            posterUrl = poster,
            backdropUrl = backdrop,
            type = mediaItem.type,
            year = d.year,
            synopsis = synopsis,
            genres = genres,
            episodes = epItems,
            cast = d.cast.map { CastMember(id = it.id ?: it.name, name = it.name, character = it.character, profileUrl = it.profileUrl) },
            rating = rating,
            logoUrl = logoUrl
        )
    }

    override suspend fun getStreamLinks(episodeData: String): StreamResult =
        StreamResult(emptyList())

    override fun getStreamFlow(mediaId: String, episodeData: String?): Flow<StreamEmission> =
        getStreamFlow(episodeData ?: mediaId)

    override fun getStreamFlow(episodeData: String): Flow<StreamEmission> = channelFlow {
        val parts = episodeData.removePrefix("eup://bingr/").removePrefix("eup://").split("/")
        val rawId = parts.getOrNull(parts.size - if (parts.size >= 3) 3 else 1)?.toIntOrNull()
            ?: parts.firstOrNull()?.toIntOrNull()
            ?: return@channelFlow

        val isTv = parts.size >= 3
        val season = if (isTv) parts[parts.size - 2].toIntOrNull() else null
        val episode = if (isTv) parts[parts.size - 1].toIntOrNull() else null

        // If anime ID was passed, translate to TMDB ID if present in cache
        val tmdbId = tmdbIdAnimeCache[rawId.toString()]?.toIntOrNull() ?: rawId

        val emittedKeys = Collections.synchronizedSet(HashSet<String>())
        val defaultUa = eupHost?.defaultUserAgent ?: defaultUserAgent

        // 1. AniSkip Bridge & Skip Offset
        var introOffsetMs = 0L

        // 2. Fetch and Emit Subtitles (Native VDRK + OpenSubtitles v3)
        val (_, coreSubs) = fetchCombinedSubtitles(tmdbId, isTv, season, episode, "")
        for (sub in coreSubs) {
            send(StreamEmission.SubtitleFound(sub))
        }

        // 3. Cluster 1: Vidrift Orion Multi-Audio
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
                        parseVidriftHtml(html, tmdbId, isTv, season, episode, introOffsetMs, emptyList(), emittedKeys) { _, coreSrc ->
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

        // 4. Cluster 2: Vidy PRNG CDN Cipher
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
                                                        parseVidyJson(decryptedJson, tmdbId, isTv, season, episode, srv, introOffsetMs, emptyList(), emittedKeys) { _, coreSrc ->
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

        // 5. Cluster 3: WebResolver Embed Sniffing for Filmu / Cinezo / Vidbolt
        val webResolverJob = launch {
            legacyHost?.browserResolver?.let { resolver ->
                withTimeoutOrNull(6000L) {
                    val embeds = listOf(
                        "filmu" to if (isTv && season != null && episode != null) "https://embed.filmu.in/tv/$tmdbId/$season/$episode" else "https://embed.filmu.in/movie/$tmdbId",
                        "cinezo" to if (isTv && season != null && episode != null) "https://player.cinezo.live/embed/tv/$tmdbId/$season/$episode" else "https://player.cinezo.live/embed/movie/$tmdbId"
                    )
                    coroutineScope {
                        for ((srvTag, embedUrl) in embeds) {
                            launch {
                                try {
                                    val bReq = BrowserResolveRequest(
                                        url = embedUrl,
                                        headers = mapOf("Referer" to "https://bingr.one/"),
                                        userAgent = defaultUa,
                                        timeoutMs = 5000L,
                                        urlSniffRegex = Regex(""".*\.(?:m3u8|mp4).*""")
                                    )
                                    val bRes = resolver.resolve(bReq)
                                    for (sniffed in bRes.sniffedUrls) {
                                        if (sniffed.startsWith("http") && emittedKeys.add(sniffed)) {
                                            val isM3u8 = sniffed.contains(".m3u8")
                                            val coreSource = CoreStreamSource(
                                                url = sniffed,
                                                serverName = "${srvTag.replaceFirstChar { it.uppercase() }} (Auto ${if (isM3u8) "HLS" else "MP4"})",
                                                resolutionLabel = "1080p FHD",
                                                quality = "Bingr $srvTag [Auto]",
                                                isM3u8 = isM3u8,
                                                audioTracks = listOf(CoreAudioTrackDescriptor("English", "en", 2, "AAC")),
                                                releaseType = AudioReleaseType.ORIGINAL,
                                                headers = mapOf("Referer" to embedUrl, "User-Agent" to defaultUa)
                                            )
                                            send(StreamEmission.SourceFound(coreSource))
                                        }
                                    }
                                } catch (_: Throwable) {}
                            }
                        }
                    }
                }
            }
        }

        joinAll(vidriftJob, vidyJob, webResolverJob)
    }.flowOn(Dispatchers.Default)

    // ───────────────────────────── Direct Download Link Resolver ─────────────────────────────
    override suspend fun getDownloadLinks(episodeData: String): List<DownloadOption> = withContext(Dispatchers.IO) {
        val parts = episodeData.removePrefix("eup://bingr/").removePrefix("eup://").split("/")
        val rawId = parts.getOrNull(parts.size - if (parts.size >= 3) 3 else 1)?.toIntOrNull()
            ?: parts.firstOrNull()?.toIntOrNull()
            ?: return@withContext emptyList()

        val isTv = parts.size >= 3
        val season = if (isTv) parts[parts.size - 2].toIntOrNull() else null
        val episode = if (isTv) parts[parts.size - 1].toIntOrNull() else null

        val tmdbId = tmdbIdAnimeCache[rawId.toString()]?.toIntOrNull() ?: rawId

        val dlUrl = if (isTv && season != null && episode != null) {
            "$downloadApiBaseUrl/tv/$tmdbId?season=$season&episode=$episode"
        } else {
            "$downloadApiBaseUrl/movie/$tmdbId"
        }

        val options = mutableListOf<DownloadOption>()
        try {
            val req = Request.Builder()
                .url(dlUrl)
                .header("User-Agent", eupHost?.defaultUserAgent ?: defaultUserAgent)
                .header("Referer", "https://bingr.one/")
                .build()

            val body = http.meta.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
            }

            if (body.isNotBlank() && body.startsWith("{")) {
                val root = json.parseToJsonElement(body).jsonObject
                val downloads = root["downloads"]?.jsonArray
                if (downloads != null) {
                    for (item in downloads) {
                        val obj = item.jsonObject
                        val server = obj["server"]?.jsonPrimitive?.contentOrNull ?: "Direct"
                        val url = obj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                        val qualityNum = obj["quality"]?.jsonPrimitive?.intOrNull ?: 1080
                        val sizeStr = obj["size"]?.jsonPrimitive?.contentOrNull ?: ""
                        val sourceName = obj["source"]?.jsonPrimitive?.contentOrNull ?: "Bingr Streamrip"

                        val qualityLabel = when {
                            qualityNum >= 2160 -> "4K UHD"
                            qualityNum >= 1080 -> "1080p FHD"
                            qualityNum >= 720 -> "720p HD"
                            qualityNum >= 480 -> "480p SD"
                            else -> "HD"
                        }

                        options.add(
                            DownloadOption(
                                title = "$sourceName - $server",
                                quality = qualityLabel,
                                size = sizeStr,
                                url = url,
                                source = sourceName,
                                provider = name,
                                headers = mapOf("Referer" to "https://bingr.one/", "User-Agent" to (eupHost?.defaultUserAgent ?: defaultUserAgent))
                            )
                        )
                    }
                }
            }
        } catch (t: Throwable) {
            safeLog("BingrPlugin", "Error fetching download links: ${t.message}", t)
        }

        options
    }
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

            val rBytes = ByteArray(o.size)
            for (i in o.indices) {
                rBytes[i] = (o[i].toInt() xor n[i].toInt()).toByte()
            }

            for (i in 0 until 4) {
                if (rBytes[i] != VIDY_U[i]) {
                    return null
                }
            }

            val textBytes = rBytes.copyOfRange(4, rBytes.size)
            String(textBytes, Charsets.UTF_8)
        } catch (_: Throwable) {
            null
        }
    }
}
