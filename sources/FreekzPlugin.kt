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
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.*
import okhttp3.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.net.URLEncoder
import java.security.MessageDigest
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * Flagship Golden EUP v2 & UniversalPlugin Provider for Freekz / bCine (https://freekz.to).
 *
 * Capabilities:
 *  - 30 Rich Catalog Sections: Home Hero Carousel Spotlight, 11 Movies, 12 TV Series, 5 Watch Providers.
 *  - Multi-server streaming with Orion, Centaurus, Andromeda, Atlas, and Ursa (15+ working sources).
 *  - Multi-Audio dubbing support (Original English 5.1/AAC, Hindi Dub, French Dub, Spanish Dub, German Dub, etc.).
 *  - Multi-HLS and MPEG-DASH master stream resolution.
 *  - TMDB Native metadata bridging & MatchScorer fuzzy matching.
 *  - Dual Subtitle Clusters: Vidstuck captions + OpenSubtitles v3 Bridge.
 *  - Dual Intro-Skip Clusters: Vidstuck episode intervals + AniSkip client.
 */
class FreekzPlugin(
    private val externalClient: OkHttpClient? = null
) : UniversalPlugin, StreamResolver, PagedCatalogProvider, PagedSearchProvider, DetailsProvider {

    constructor() : this(null)

    override val name: String = "Freekz"
    override val mainUrl: String = "https://freekz.to"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE, MediaType.TV_SERIES)
    override val isSearchGlobalOnly: Boolean get() = false

    override val manifest: PluginManifest = PluginManifest(
        id = "freekz",
        name = "Freekz",
        version = 3,
        apiVersion = 2,
        realm = PluginRealm.PUBLIC,
        entryClass = "com.euthopiar.core.provider.FreekzPlugin",
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
        siteUrl = "https://freekz.to",
        description = "High-speed multi-server streaming with Orion (Multi-Audio & Multi-Dub), Centaurus (Multi-Audio), Andromeda (HD), Atlas (Multi-HLS), Ursa/Meow, home carousel hero spotlights, 30+ categorized catalogs, full season/episode extraction, TMDB bridging, AniSkip, and OpenSubtitles."
    )

    private val tmdbApiKey = "3e20e76d6d210b6cb128d17d233b64dc"
    private val tmdbBaseUrl = "https://api.themoviedb.org/3"
    private val vidstuckBaseUrl = "https://vidstuck.xyz"
    private val cryptoPassphrase = "7f4c9e2a81d63b05c4f7a9e8126d3b50e1a8c7f23d9465ab0c6e9f1d4a7b832c"
    private val defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    // Vidstuck obfuscated param names
    private val paramTmdbId = "a7f39c821d604e5b9c71f36e1547b"
    private val paramMediaType = "c285f91ab306d28147a35632e816b"
    private val paramServer = "6b491e7253ad84d392e7561a9384c"
    private val paramSeason = "d8427b59ce30684a2f957c3613e85b"
    private val paramEpisode = "91c6e4a728503d1f785c92346b713d"
    private val paramTs = "61d9a5274c8e3b29afd6384c291e6"
    private val paramToken = "c492f7a183d6502b1e7436c538a716d"
    private val paramTitle = "5e28c9147a306d1e829f3674b392a1"
    private val paramYear = "b731e6c94f08269d725f8341c306e"
    private val paramDate = "e164932c50216a39e5814b3027"
    private val paramImdb = "f35a8c19d674b3265e871c4933a725f"

    private val defaultHeaders = mapOf(
        "User-Agent" to defaultUserAgent,
        "Referer" to "https://freekz.to/",
        "Accept" to "application/json"
    )

    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
        coerceInputValues = true
    }

    private class PluginHttp(externalClient: OkHttpClient?) : Closeable {
        private val sharedPool = ConnectionPool(32, 5, TimeUnit.MINUTES)

        private val resolvedDns: Dns = externalClient?.dns ?: try {
            com.euthopiar.core.network.DohDns.DEFAULT
        } catch (_: Throwable) {
            Dns.SYSTEM
        }

        val meta: OkHttpClient = (externalClient?.newBuilder() ?: OkHttpClient.Builder())
            .dns(resolvedDns)
            .connectionPool(sharedPool)
            .connectTimeout(12, TimeUnit.SECONDS)
            .readTimeout(12, TimeUnit.SECONDS)
            .writeTimeout(12, TimeUnit.SECONDS)
            .followRedirects(true)
            .followSslRedirects(true)
            .retryOnConnectionFailure(true)
            .build()

        val stream: OkHttpClient = (externalClient?.newBuilder() ?: OkHttpClient.Builder())
            .dns(resolvedDns)
            .connectionPool(sharedPool)
            .connectTimeout(15, TimeUnit.SECONDS)
            .readTimeout(15, TimeUnit.SECONDS)
            .writeTimeout(15, TimeUnit.SECONDS)
            .followRedirects(true)
            .followSslRedirects(true)
            .retryOnConnectionFailure(true)
            .build()

        override fun close() {
            meta.dispatcher.executorService.shutdown()
            stream.dispatcher.executorService.shutdown()
            sharedPool.evictAll()
        }
    }

    private var http = PluginHttp(externalClient)
    private var eupHost: EupHostApi? = null
    private var hostApi: HostApi? = null

    // In-memory caches for fast resolution
    private val mediaTypeCache = ConcurrentHashMap<String, MediaType>()
    private val imdbIdCache = ConcurrentHashMap<String, String>()
    private val metaDetailsCache = ConcurrentHashMap<String, TmdbDetailsMeta>()

    private data class TmdbDetailsMeta(
        val title: String,
        val year: String,
        val releaseDate: String,
        val imdbId: String?
    )

    // ───────────────────────────── Lifecycle ─────────────────────────────
    override fun init(host: HostApi) {
        this.hostApi = host
    }

    override suspend fun init(host: EupHostApi, scope: CoroutineScope) {
        this.eupHost = host
    }

    override suspend fun destroy() {
        http.close()
        mediaTypeCache.clear()
        imdbIdCache.clear()
        metaDetailsCache.clear()
    }

    private fun safeLog(tag: String, msg: String, t: Throwable? = null) {
        try {
            val logClass = Class.forName("android.util.Log")
            if (t != null) {
                val method = logClass.getMethod("w", String::class.java, String::class.java, Throwable::class.java)
                method.invoke(null, tag, msg, t)
            } else {
                val method = logClass.getMethod("d", String::class.java, String::class.java)
                method.invoke(null, tag, msg)
            }
        } catch (_: Throwable) {
            println("[$tag] $msg")
            t?.printStackTrace()
        }
    }

    // ───────────────────────────── CryptoJS AES Decryptor ─────────────────────────────
    private object CryptoJsAes {
        fun decrypt(cipherTextB64: String, passphrase: String): String {
            val raw = try {
                java.util.Base64.getDecoder().decode(cipherTextB64.trim())
            } catch (_: Throwable) {
                try {
                    val base64Class = Class.forName("android.util.Base64")
                    val decodeMethod = base64Class.getMethod("decode", String::class.java, Int::class.javaPrimitiveType)
                    decodeMethod.invoke(null, cipherTextB64.trim(), 0) as ByteArray
                } catch (e: Throwable) {
                    throw IllegalArgumentException("Base64 decoding failed: ${e.message}")
                }
            }

            if (raw.size < 16) throw IllegalArgumentException("Ciphertext too short")
            val prefix = String(raw.copyOfRange(0, 8), Charsets.US_ASCII)
            if (prefix != "Salted__") {
                throw IllegalArgumentException("Invalid CryptoJS salt header")
            }
            val salt = raw.copyOfRange(8, 16)
            val ciphertext = raw.copyOfRange(16, raw.size)

            val passBytes = passphrase.toByteArray(Charsets.UTF_8)
            val md = MessageDigest.getInstance("MD5")
            val keyIv = ByteArrayOutputStream()
            var prev = ByteArray(0)

            while (keyIv.size() < 48) {
                md.reset()
                if (prev.isNotEmpty()) md.update(prev)
                md.update(passBytes)
                md.update(salt)
                prev = md.digest()
                keyIv.write(prev)
            }

            val allBytes = keyIv.toByteArray()
            val keyBytes = allBytes.copyOfRange(0, 32)
            val ivBytes = allBytes.copyOfRange(32, 48)

            val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
            val keySpec = SecretKeySpec(keyBytes, "AES")
            val ivSpec = IvParameterSpec(ivBytes)
            cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec)

            val decrypted = cipher.doFinal(ciphertext)
            return String(decrypted, Charsets.UTF_8)
        }
    }

    // ───────────────────────────── Catalog Sections (30 Catalogs) ─────────────────────────────
    override suspend fun sections(): List<CatalogSection> = listOf(
        // Flagship Home Hero Carousel & Spotlight
        CatalogSection(id = "featured_spotlight", title = "Featured Spotlight & Carousel"),

        // Movies (11 Catalogs)
        CatalogSection(id = "trending_movies", title = "Trending Movies Today"),
        CatalogSection(id = "popular_movies", title = "Popular Movies"),
        CatalogSection(id = "top_rated_movies", title = "Top Rated Movies"),
        CatalogSection(id = "now_playing_movies", title = "Now Playing in Theatres"),
        CatalogSection(id = "upcoming_movies", title = "Upcoming Movies"),
        CatalogSection(id = "action_movies", title = "Action & Adventure Blockbusters"),
        CatalogSection(id = "scifi_movies", title = "Sci-Fi & Fantasy Hits"),
        CatalogSection(id = "horror_movies", title = "Spine-Chilling Horror"),
        CatalogSection(id = "comedy_movies", title = "Comedy Hits"),
        CatalogSection(id = "animation_movies", title = "Animated Feature Films"),
        CatalogSection(id = "romance_movies", title = "Heartwarming Romance & Drama"),

        // TV Shows (12 Catalogs)
        CatalogSection(id = "trending_tv", title = "Trending Web Series Today"),
        CatalogSection(id = "popular_tv", title = "Popular TV Shows"),
        CatalogSection(id = "top_rated_tv", title = "Top Rated TV Series"),
        CatalogSection(id = "airing_today_tv", title = "Airing Today Episodes"),
        CatalogSection(id = "on_the_air_tv", title = "On The Air Current Series"),
        CatalogSection(id = "crime_tv", title = "Crime & Mystery Thrillers"),
        CatalogSection(id = "scifi_tv", title = "Sci-Fi & Fantasy Series"),
        CatalogSection(id = "drama_tv", title = "Gripping Drama Series"),
        CatalogSection(id = "comedy_tv", title = "Comedy & Sitcom Hits"),
        CatalogSection(id = "anime_series", title = "Anime & Animation Series"),
        CatalogSection(id = "action_tv", title = "Action & Adventure Series"),
        CatalogSection(id = "documentary_tv", title = "Documentaries & Real World"),

        // Watch Providers & Streaming Networks (5 Catalogs)
        CatalogSection(id = "netflix", title = "Popular on Netflix"),
        CatalogSection(id = "disney", title = "Disney+ Exclusives"),
        CatalogSection(id = "prime", title = "Amazon Prime Video"),
        CatalogSection(id = "appletv", title = "Apple TV+ Originals"),
        CatalogSection(id = "hbo_max", title = "Max / HBO Hits")
    )

    override suspend fun load(section: CatalogSection, page: PageRequest): Page<MediaCard> = withContext(Dispatchers.IO) {
        val pageNum = page.cursor?.toIntOrNull() ?: 1
        val endpoint = when (section.id) {
            "featured_spotlight" -> "trending/all/day?page=$pageNum"
            "trending_movies" -> "trending/movie/day?page=$pageNum"
            "popular_movies" -> "movie/popular?page=$pageNum"
            "top_rated_movies" -> "movie/top_rated?page=$pageNum"
            "now_playing_movies" -> "movie/now_playing?page=$pageNum"
            "upcoming_movies" -> "movie/upcoming?page=$pageNum"
            "action_movies" -> "discover/movie?with_genres=28,12&sort_by=popularity.desc&page=$pageNum"
            "scifi_movies" -> "discover/movie?with_genres=878,14&sort_by=popularity.desc&page=$pageNum"
            "horror_movies" -> "discover/movie?with_genres=27,9648&sort_by=popularity.desc&page=$pageNum"
            "comedy_movies" -> "discover/movie?with_genres=35&sort_by=popularity.desc&page=$pageNum"
            "animation_movies" -> "discover/movie?with_genres=16&sort_by=popularity.desc&page=$pageNum"
            "romance_movies" -> "discover/movie?with_genres=10749,18&sort_by=popularity.desc&page=$pageNum"

            "trending_tv" -> "trending/tv/day?page=$pageNum"
            "popular_tv" -> "tv/popular?page=$pageNum"
            "top_rated_tv" -> "tv/top_rated?page=$pageNum"
            "airing_today_tv" -> "tv/airing_today?page=$pageNum"
            "on_the_air_tv" -> "tv/on_the_air?page=$pageNum"
            "crime_tv" -> "discover/tv?with_genres=80,9648&sort_by=popularity.desc&page=$pageNum"
            "scifi_tv" -> "discover/tv?with_genres=10765&sort_by=popularity.desc&page=$pageNum"
            "drama_tv" -> "discover/tv?with_genres=18&sort_by=popularity.desc&page=$pageNum"
            "comedy_tv" -> "discover/tv?with_genres=35&sort_by=popularity.desc&page=$pageNum"
            "anime_series" -> "discover/tv?with_genres=16&sort_by=popularity.desc&page=$pageNum"
            "action_tv" -> "discover/tv?with_genres=10759&sort_by=popularity.desc&page=$pageNum"
            "documentary_tv" -> "discover/tv?with_genres=99&sort_by=popularity.desc&page=$pageNum"

            "netflix" -> "discover/movie?with_watch_providers=8&watch_region=US&sort_by=popularity.desc&page=$pageNum"
            "disney" -> "discover/movie?with_watch_providers=337&watch_region=US&sort_by=popularity.desc&page=$pageNum"
            "prime" -> "discover/movie?with_watch_providers=119&watch_region=US&sort_by=popularity.desc&page=$pageNum"
            "appletv" -> "discover/movie?with_watch_providers=350&watch_region=US&sort_by=popularity.desc&page=$pageNum"
            "hbo_max" -> "discover/movie?with_watch_providers=1899&watch_region=US&sort_by=popularity.desc&page=$pageNum"
            else -> "trending/all/day?page=$pageNum"
        }

        val sep = if (endpoint.contains("?")) "&" else "?"
        val url = "$tmdbBaseUrl/$endpoint${sep}api_key=$tmdbApiKey"
        val req = Request.Builder()
            .url(url)
            .header("User-Agent", defaultUserAgent)
            .header("Accept", "application/json")
            .build()

        try {
            val body = http.meta.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
            }
            if (body.isBlank() || !body.startsWith("{")) return@withContext Page(emptyList(), null)

            val root = json.parseToJsonElement(body).jsonObject
            val results = root["results"]?.jsonArray ?: return@withContext Page(emptyList(), null)
            val totalPages = root["total_pages"]?.jsonPrimitive?.intOrNull ?: 1

            val isTvSection = section.id.endsWith("_tv") || section.id == "anime_series"

            val cards = results.mapNotNull { elem ->
                val obj = elem.jsonObject
                val id = obj["id"]?.jsonPrimitive?.contentOrNull ?: return@mapNotNull null
                val title = obj["title"]?.jsonPrimitive?.contentOrNull
                    ?: obj["name"]?.jsonPrimitive?.contentOrNull
                    ?: return@mapNotNull null

                val mediaTypeStr = obj["media_type"]?.jsonPrimitive?.contentOrNull
                val isTv = if (mediaTypeStr != null) mediaTypeStr == "tv" else isTvSection

                val posterPath = obj["poster_path"]?.jsonPrimitive?.contentOrNull
                val backdropPath = obj["backdrop_path"]?.jsonPrimitive?.contentOrNull
                val posterUrl = posterPath?.let { "https://image.tmdb.org/t/p/w500$it" }
                val backdropUrl = backdropPath?.let { "https://image.tmdb.org/t/p/original$it" }

                val dateStr = obj["release_date"]?.jsonPrimitive?.contentOrNull
                    ?: obj["first_air_date"]?.jsonPrimitive?.contentOrNull
                val year = dateStr?.take(4)?.toIntOrNull()
                val rating = obj["vote_average"]?.jsonPrimitive?.floatOrNull

                MediaCard(
                    id = id,
                    title = title,
                    type = if (isTv) ContentType.TV_SERIES else ContentType.MOVIE,
                    posterUrl = posterUrl,
                    backdropUrl = backdropUrl,
                    releaseYear = year,
                    rating = rating?.toString()
                )
            }

            val nextCursor = if (pageNum < totalPages) (pageNum + 1).toString() else null
            Page(cards, nextCursor)
        } catch (ce: CancellationException) {
            throw ce
        } catch (t: Throwable) {
            safeLog("FreekzPlugin", "Load section ${section.id} error: ${t.message}", t)
            Page(emptyList(), null)
        }
    }

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        val rows = mutableListOf<CatalogRow>()
        // Load the top 15 flagship sections for the home screen catalog
        val targetSections = sections().take(15)
        for (sec in targetSections) {
            val page = load(sec, PageRequest())
            if (page.items.isNotEmpty()) {
                val mediaItems = page.items.map { card ->
                    val isTv = card.type == ContentType.TV_SERIES
                    MediaItem(
                        id = card.id,
                        title = card.title,
                        url = "https://freekz.to/${if (isTv) "tv" else "movie"}/${card.id}",
                        posterUrl = card.posterUrl,
                        backdropUrl = card.backdropUrl,
                        type = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE,
                        year = card.releaseYear,
                        provider = name
                    )
                }
                rows.add(CatalogRow(title = sec.title, items = mediaItems))
            }
        }
        rows
    }

    // ───────────────────────────── PagedSearchProvider (Fuzzy Scored) ─────────────────────────────
    override suspend fun search(query: String, page: PageRequest): Page<MediaCard> = withContext(Dispatchers.IO) {
        val pageNum = page.cursor?.toIntOrNull() ?: 1
        val encodedQuery = URLEncoder.encode(query.trim(), "UTF-8")
        val url = "$tmdbBaseUrl/search/multi?api_key=$tmdbApiKey&query=$encodedQuery&page=$pageNum"

        val req = Request.Builder()
            .url(url)
            .header("User-Agent", defaultUserAgent)
            .header("Accept", "application/json")
            .build()

        try {
            val body = http.meta.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
            }
            if (body.isBlank() || !body.startsWith("{")) return@withContext Page(emptyList(), null)

            val root = json.parseToJsonElement(body).jsonObject
            val results = root["results"]?.jsonArray ?: return@withContext Page(emptyList(), null)
            val totalPages = root["total_pages"]?.jsonPrimitive?.intOrNull ?: 1

            val normQuery = MatchScorer.normalize(query)
            val scoredCards = mutableListOf<Pair<MediaCard, Double>>()

            for (elem in results) {
                val obj = elem.jsonObject
                val mediaTypeStr = obj["media_type"]?.jsonPrimitive?.contentOrNull ?: continue
                if (mediaTypeStr != "movie" && mediaTypeStr != "tv") continue

                val id = obj["id"]?.jsonPrimitive?.contentOrNull ?: continue
                val title = obj["title"]?.jsonPrimitive?.contentOrNull
                    ?: obj["name"]?.jsonPrimitive?.contentOrNull
                    ?: continue

                val isTv = mediaTypeStr == "tv"
                val posterPath = obj["poster_path"]?.jsonPrimitive?.contentOrNull
                val backdropPath = obj["backdrop_path"]?.jsonPrimitive?.contentOrNull
                val dateStr = obj["release_date"]?.jsonPrimitive?.contentOrNull
                    ?: obj["first_air_date"]?.jsonPrimitive?.contentOrNull
                val year = dateStr?.take(4)?.toIntOrNull()
                val rating = obj["vote_average"]?.jsonPrimitive?.floatOrNull

                val card = MediaCard(
                    id = id,
                    title = title,
                    type = if (isTv) ContentType.TV_SERIES else ContentType.MOVIE,
                    posterUrl = posterPath?.let { "https://image.tmdb.org/t/p/w500$it" },
                    backdropUrl = backdropPath?.let { "https://image.tmdb.org/t/p/original$it" },
                    releaseYear = year,
                    rating = rating?.toString()
                )
                val score = MatchScorer.jaroWinkler(MatchScorer.normalize(title), normQuery)
                scoredCards.add(card to score)
            }

            // Rank results by match score descending
            val sortedCards = scoredCards.sortedByDescending { it.second }.map { it.first }
            val nextCursor = if (pageNum < totalPages) (pageNum + 1).toString() else null
            Page(sortedCards, nextCursor)
        } catch (ce: CancellationException) {
            throw ce
        } catch (t: Throwable) {
            safeLog("FreekzPlugin", "Search error: ${t.message}", t)
            Page(emptyList(), null)
        }
    }

    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        val page = search(query, PageRequest())
        page.items.map { card ->
            val isTv = card.type == ContentType.TV_SERIES
            MediaItem(
                id = card.id,
                title = card.title,
                url = "https://freekz.to/${if (isTv) "tv" else "movie"}/${card.id}",
                posterUrl = card.posterUrl,
                backdropUrl = card.backdropUrl,
                type = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE,
                year = card.releaseYear,
                provider = name
            )
        }
    }

    // ───────────────────────────── DetailsProvider (Enriched TMDB Details) ─────────────────────────────
    override suspend fun details(card: MediaCard): MediaDetails = withContext(Dispatchers.IO) {
        val isTv = card.type == ContentType.TV_SERIES
        val item = MediaItem(
            id = card.id,
            title = card.title,
            url = "https://freekz.to/${if (isTv) "tv" else "movie"}/${card.id}",
            posterUrl = card.posterUrl,
            backdropUrl = card.backdropUrl,
            type = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE
        )
        val d = getDetails(item)

        val seasonsGrouped = if (isTv) {
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
            logoUrl = d.logoUrl,
            defaultTarget = card.target ?: if (isTv) {
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

    override suspend fun getDetails(mediaItem: MediaItem): MediaDetail = withContext(Dispatchers.IO) {
        val rawTarget = parseTarget(mediaItem.id.ifBlank { mediaItem.url })
        val tmdbId = rawTarget.tmdbId.ifBlank { mediaItem.id.filter { it.isDigit() } }
        val isTv = mediaItem.type == MediaType.TV_SERIES || mediaItem.url.contains("/tv/") || rawTarget.mediaType == "tv"
        val mediaType = if (isTv) "tv" else "movie"
        mediaTypeCache[tmdbId] = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE

        val url = "$tmdbBaseUrl/$mediaType/$tmdbId?api_key=$tmdbApiKey&append_to_response=credits,external_ids,images,videos"
        val req = Request.Builder()
            .url(url)
            .header("User-Agent", defaultUserAgent)
            .header("Accept", "application/json")
            .build()

        try {
            val body = http.meta.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
            }
            if (body.isBlank() || !body.startsWith("{")) return@withContext fallbackMediaDetail(mediaItem, tmdbId, isTv)

            val root = json.parseToJsonElement(body).jsonObject
            val title = root["title"]?.jsonPrimitive?.contentOrNull
                ?: root["name"]?.jsonPrimitive?.contentOrNull
                ?: mediaItem.title
            val overview = root["overview"]?.jsonPrimitive?.contentOrNull
            val posterPath = root["poster_path"]?.jsonPrimitive?.contentOrNull
            val poster = posterPath?.let { "https://image.tmdb.org/t/p/w500$it" } ?: mediaItem.posterUrl
            val backdropPath = root["backdrop_path"]?.jsonPrimitive?.contentOrNull
            val backdrop = backdropPath?.let { "https://image.tmdb.org/t/p/original$it" } ?: mediaItem.backdropUrl
            val rating = root["vote_average"]?.jsonPrimitive?.floatOrNull?.toString()

            val releaseDate = root["release_date"]?.jsonPrimitive?.contentOrNull
                ?: root["first_air_date"]?.jsonPrimitive?.contentOrNull
                ?: ""
            val year = releaseDate.take(4).toIntOrNull() ?: mediaItem.year

            // Cache IMDb ID and Metadata for rapid stream resolution
            val extIds = root["external_ids"]?.jsonObject
            val imdbId = extIds?.get("imdb_id")?.jsonPrimitive?.contentOrNull
                ?: root["imdb_id"]?.jsonPrimitive?.contentOrNull
            if (!imdbId.isNullOrBlank()) {
                imdbIdCache[tmdbId] = imdbId
            }
            metaDetailsCache[tmdbId] = TmdbDetailsMeta(title, year?.toString() ?: "", releaseDate, imdbId)

            val genresList = root["genres"]?.jsonArray?.mapNotNull {
                it.jsonObject["name"]?.jsonPrimitive?.contentOrNull
            } ?: emptyList()

            // Resolve logo
            val logos = root["images"]?.jsonObject?.get("logos")?.jsonArray
            val logoPath = logos?.firstOrNull {
                it.jsonObject["iso_639_1"]?.jsonPrimitive?.contentOrNull == "en"
            }?.jsonObject?.get("file_path")?.jsonPrimitive?.contentOrNull
                ?: logos?.firstOrNull()?.jsonObject?.get("file_path")?.jsonPrimitive?.contentOrNull
            val logoUrl = logoPath?.let { "https://image.tmdb.org/t/p/original$it" }

            val castList = root["credits"]?.jsonObject?.get("cast")?.jsonArray?.take(15)?.mapNotNull {
                val cObj = it.jsonObject
                val cName = cObj["name"]?.jsonPrimitive?.contentOrNull ?: return@mapNotNull null
                CastMember(
                    id = cObj["id"]?.jsonPrimitive?.contentOrNull ?: cName,
                    name = cName,
                    character = cObj["character"]?.jsonPrimitive?.contentOrNull,
                    profileUrl = cObj["profile_path"]?.jsonPrimitive?.contentOrNull?.let { p -> "https://image.tmdb.org/t/p/w185$p" }
                )
            } ?: emptyList()

            val episodeItems = mutableListOf<EpisodeItem>()
            if (isTv) {
                val rawSeasons = root["seasons"]?.jsonArray.orEmpty()
                val seasonFetches = rawSeasons.mapNotNull { sItem ->
                    val sObj = sItem.jsonObject
                    val sNum = sObj["season_number"]?.jsonPrimitive?.intOrNull ?: return@mapNotNull null
                    if (sNum <= 0 && rawSeasons.size > 1) return@mapNotNull null
                    val epCount = sObj["episode_count"]?.jsonPrimitive?.intOrNull ?: 10
                    sNum to epCount
                }

                // Fetch season episodes in parallel for instant loading of all seasons & episodes
                val fetchedSeasons = coroutineScope {
                    seasonFetches.map { (sNum, fallbackEpCount) ->
                        async {
                            val eps = mutableListOf<EpisodeItem>()
                            try {
                                val sReq = Request.Builder()
                                    .url("$tmdbBaseUrl/tv/$tmdbId/season/$sNum?api_key=$tmdbApiKey")
                                    .header("User-Agent", defaultUserAgent)
                                    .header("Accept", "application/json")
                                    .build()
                                http.meta.newCall(sReq).execute().use { sResp ->
                                    if (sResp.isSuccessful) {
                                        val sBody = sResp.body?.string().orEmpty()
                                        if (sBody.startsWith("{")) {
                                            val sJson = json.parseToJsonElement(sBody).jsonObject
                                            val sEps = sJson["episodes"]?.jsonArray.orEmpty()
                                            for (ep in sEps) {
                                                val epObj = ep.jsonObject
                                                val epNum = epObj["episode_number"]?.jsonPrimitive?.intOrNull ?: continue
                                                val epName = epObj["name"]?.jsonPrimitive?.contentOrNull ?: "Episode $epNum"
                                                val stillPath = epObj["still_path"]?.jsonPrimitive?.contentOrNull
                                                val epOverview = epObj["overview"]?.jsonPrimitive?.contentOrNull
                                                val epDuration = epObj["runtime"]?.jsonPrimitive?.intOrNull

                                                eps.add(
                                                    EpisodeItem(
                                                        id = "$tmdbId:$sNum:$epNum",
                                                        title = epName,
                                                        seasonNumber = sNum,
                                                        episodeNumber = epNum,
                                                        data = "eup://freekz/$tmdbId/$sNum/$epNum",
                                                        thumbnail = stillPath?.let { "https://image.tmdb.org/t/p/w300$it" } ?: backdrop ?: poster,
                                                        description = epOverview,
                                                        duration = epDuration?.let { "${it}m" }
                                                    )
                                                )
                                            }
                                        }
                                    }
                                }
                            } catch (_: Throwable) {}

                            // Fallback guarantee: if season details endpoint fails, synthesize episodes from episode_count!
                            if (eps.isEmpty() && fallbackEpCount > 0) {
                                for (epNum in 1..fallbackEpCount) {
                                    eps.add(
                                        EpisodeItem(
                                            id = "$tmdbId:$sNum:$epNum",
                                            title = "Episode $epNum",
                                            seasonNumber = sNum,
                                            episodeNumber = epNum,
                                            data = "eup://freekz/$tmdbId/$sNum/$epNum",
                                            thumbnail = backdrop ?: poster,
                                            description = "Season $sNum Episode $epNum"
                                        )
                                    )
                                }
                            }
                            eps
                        }
                    }.awaitAll().flatten()
                }

                episodeItems.addAll(fetchedSeasons)
            } else {
                // Movie: add playable single episode item
                episodeItems.add(
                    EpisodeItem(
                        id = tmdbId,
                        title = title,
                        seasonNumber = 1,
                        episodeNumber = 1,
                        data = "eup://freekz/$tmdbId",
                        thumbnail = backdrop ?: poster
                    )
                )
            }

            MediaDetail(
                id = tmdbId,
                title = title,
                url = "https://freekz.to/${if (isTv) "tv" else "movie"}/$tmdbId",
                posterUrl = poster,
                backdropUrl = backdrop,
                type = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE,
                year = year,
                synopsis = overview,
                genres = genresList,
                episodes = episodeItems,
                rating = rating,
                cast = castList,
                logoUrl = logoUrl,
                imdbId = imdbId,
                provider = name
            )
        } catch (ce: CancellationException) {
            throw ce
        } catch (t: Throwable) {
            safeLog("FreekzPlugin", "getDetails error: ${t.message}", t)
            fallbackMediaDetail(mediaItem, tmdbId, isTv)
        }
    }

    private fun fallbackMediaDetail(mediaItem: MediaItem, tmdbId: String, isTv: Boolean): MediaDetail {
        val epList = if (isTv) {
            (1..10).map { epNum ->
                EpisodeItem(
                    id = "$tmdbId:1:$epNum",
                    title = "Episode $epNum",
                    seasonNumber = 1,
                    episodeNumber = epNum,
                    data = "eup://freekz/$tmdbId/1/$epNum",
                    thumbnail = mediaItem.backdropUrl ?: mediaItem.posterUrl
                )
            }
        } else {
            listOf(
                EpisodeItem(
                    id = tmdbId,
                    title = mediaItem.title,
                    seasonNumber = 1,
                    episodeNumber = 1,
                    data = "eup://freekz/$tmdbId",
                    thumbnail = mediaItem.backdropUrl ?: mediaItem.posterUrl
                )
            )
        }
        return MediaDetail(
            id = tmdbId,
            title = mediaItem.title,
            url = mediaItem.url,
            posterUrl = mediaItem.posterUrl,
            backdropUrl = mediaItem.backdropUrl,
            type = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE,
            year = mediaItem.year,
            synopsis = null,
            episodes = epList,
            provider = name
        )
    }

    override suspend fun resolveLogo(mediaItem: MediaItem): String? = withContext(Dispatchers.IO) {
        try {
            val isTv = mediaItem.type == MediaType.TV_SERIES
            val mediaType = if (isTv) "tv" else "movie"
            val url = "$tmdbBaseUrl/$mediaType/${mediaItem.id}/images?api_key=$tmdbApiKey"
            val req = Request.Builder().url(url).header("User-Agent", defaultUserAgent).build()
            http.meta.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return@use null
                val body = resp.body?.string().orEmpty()
                val root = json.parseToJsonElement(body).jsonObject
                val logos = root["logos"]?.jsonArray ?: return@use null
                val path = logos.firstOrNull {
                    it.jsonObject["iso_639_1"]?.jsonPrimitive?.contentOrNull == "en"
                }?.jsonObject?.get("file_path")?.jsonPrimitive?.contentOrNull
                    ?: logos.firstOrNull()?.jsonObject?.get("file_path")?.jsonPrimitive?.contentOrNull
                path?.let { "https://image.tmdb.org/t/p/original$it" }
            }
        } catch (_: Throwable) {
            null
        }
    }

    // ───────────────────────────── Target Resolution Parsing ─────────────────────────────
    data class ParsedPlayable(
        val tmdbId: String,
        val mediaType: String, // "movie" or "tv"
        val season: Int?,
        val episode: Int?,
        val title: String = "",
        val year: String = "",
        val releaseDate: String = "",
        val imdbId: String? = null
    )

    private fun parseTarget(mediaId: String, episodeData: String? = null): ParsedPlayable {
        val payload = (episodeData ?: mediaId).trim()

        // 1. JSON payload
        if (payload.startsWith("{") && payload.contains("tmdbId")) {
            try {
                val obj = json.parseToJsonElement(payload).jsonObject
                val tmdbId = obj["tmdbId"]?.jsonPrimitive?.contentOrNull ?: ""
                val type = obj["type"]?.jsonPrimitive?.contentOrNull ?: if (mediaTypeCache[tmdbId] == MediaType.TV_SERIES) "tv" else "movie"
                val season = obj["season"]?.jsonPrimitive?.intOrNull
                val episode = obj["episode"]?.jsonPrimitive?.intOrNull
                val title = obj["title"]?.jsonPrimitive?.contentOrNull ?: ""
                val year = obj["year"]?.jsonPrimitive?.contentOrNull ?: ""
                val releaseDate = obj["releaseDate"]?.jsonPrimitive?.contentOrNull ?: ""
                val imdbId = obj["imdbId"]?.jsonPrimitive?.contentOrNull ?: imdbIdCache[tmdbId]
                return ParsedPlayable(tmdbId, type, season, episode, title, year, releaseDate, imdbId)
            } catch (_: Throwable) {}
        }

        // 2. Virtual eup:// URI: eup://freekz/<tmdbId>/<season>/<episode> or eup://freekz/<tmdbId>
        if (payload.startsWith("eup://", ignoreCase = true)) {
            val withoutScheme = payload.substring(6)
            val parts = withoutScheme.split('/', ':').filter { it.isNotBlank() }
            val cleanParts = if (parts.firstOrNull()?.equals("freekz", ignoreCase = true) == true) {
                parts.drop(1)
            } else {
                parts
            }
            val id = cleanParts.firstOrNull()?.filter { it.isDigit() }.orEmpty()
            if (cleanParts.size >= 3) {
                val s = cleanParts[1].toIntOrNull() ?: 1
                val e = cleanParts[2].toIntOrNull() ?: 1
                return ParsedPlayable(id, "tv", s, e)
            } else if (cleanParts.size == 2) {
                val s = cleanParts[1].toIntOrNull() ?: 1
                return ParsedPlayable(id, "tv", s, 1)
            } else {
                val isTv = mediaTypeCache[id] == MediaType.TV_SERIES
                return ParsedPlayable(id, if (isTv) "tv" else "movie", if (isTv) 1 else null, if (isTv) 1 else null)
            }
        }

        // 3. HTTP(S) URL
        if (payload.startsWith("http://", ignoreCase = true) || payload.startsWith("https://", ignoreCase = true)) {
            val uri = try { java.net.URI(payload) } catch (_: Throwable) { null }
            val pathSegments = uri?.path?.split('/')?.filter { it.isNotBlank() }.orEmpty()
            val isTv = pathSegments.any { it.equals("tv", ignoreCase = true) || it.equals("series", ignoreCase = true) }
            val id = pathSegments.firstOrNull { seg -> seg.isNotEmpty() && seg.all { it.isDigit() } }
                ?: payload.filter { it.isDigit() }
            if (isTv) {
                val tvIdx = pathSegments.indexOfFirst { it.equals("tv", ignoreCase = true) }
                val s = pathSegments.getOrNull(tvIdx + 2)?.toIntOrNull() ?: 1
                val e = pathSegments.getOrNull(tvIdx + 3)?.toIntOrNull() ?: 1
                return ParsedPlayable(id, "tv", s, e)
            } else {
                return ParsedPlayable(id, "movie", null, null)
            }
        }

        // 4. Colon format: <tmdbId>:<season>:<episode> or <tmdbId>:<season>
        if (payload.contains(':')) {
            val parts = payload.split(':')
            val id = parts[0].filter { it.isDigit() }
            val s = parts.getOrNull(1)?.toIntOrNull() ?: 1
            val e = parts.getOrNull(2)?.toIntOrNull() ?: 1
            return ParsedPlayable(id, "tv", s, e)
        }

        // 5. Dash format: <tmdbId>-s<season>-e<episode>
        val sRegex = Regex("""(\d+)[-_]s(\d+)[-_]e(\d+)""", RegexOption.IGNORE_CASE)
        val sMatch = sRegex.find(payload)
        if (sMatch != null) {
            val id = sMatch.groupValues[1]
            val s = sMatch.groupValues[2].toIntOrNull() ?: 1
            val e = sMatch.groupValues[3].toIntOrNull() ?: 1
            return ParsedPlayable(id, "tv", s, e)
        }

        // 6. Plain numeric ID
        val id = payload.filter { it.isDigit() }
        val isTv = mediaTypeCache[id] == MediaType.TV_SERIES || mediaTypeCache[payload] == MediaType.TV_SERIES
        val meta = metaDetailsCache[id]
        return ParsedPlayable(
            tmdbId = id,
            mediaType = if (isTv) "tv" else "movie",
            season = if (isTv) 1 else null,
            episode = if (isTv) 1 else null,
            title = meta?.title ?: "",
            year = meta?.year ?: "",
            releaseDate = meta?.releaseDate ?: "",
            imdbId = meta?.imdbId ?: imdbIdCache[id]
        )
    }

    // ───────────────────────────── Stream Resolvers (UniversalPlugin & StreamResolver) ─────────────────────────────
    override fun getStreamFlow(mediaId: String, episodeData: String?): Flow<StreamEmission> =
        getStreamFlow(episodeData ?: mediaId)

    override fun getStreamFlow(episodeData: String): Flow<StreamEmission> = channelFlow {
        val parsed = parseTarget(episodeData, episodeData)
        resolveInternal(parsed,
            onStreamFound = { s -> send(StreamEmission.SourceFound(s)) },
            onSubFound = { sub -> send(StreamEmission.SubtitleFound(sub)) },
            onStatus = { srv, msg -> send(StreamEmission.StatusUpdate(srv, msg)) })
    }.flowOn(Dispatchers.IO)

    override fun resolve(target: PlayableTarget, ctx: ResolveContext): Flow<StreamBundleEvent> = channelFlow {
        val parsed = when (target) {
            is PlayableTarget.Movie -> {
                ParsedPlayable(
                    tmdbId = target.tmdbId.toString(),
                    mediaType = "movie",
                    season = null,
                    episode = null,
                    title = target.title ?: "",
                    year = "",
                    releaseDate = "",
                    imdbId = null
                )
            }
            is PlayableTarget.Episode -> {
                ParsedPlayable(
                    tmdbId = target.tmdbId.toString(),
                    mediaType = "tv",
                    season = target.season,
                    episode = target.episode,
                    title = target.title ?: "",
                    year = "",
                    releaseDate = "",
                    imdbId = null
                )
            }
            is PlayableTarget.Direct -> {
                parseTarget(target.pageUrl, null)
            }
            is PlayableTarget.Opaque -> {
                parseTarget(target.payload.toString(), null)
            }
            else -> {
                ParsedPlayable("", "movie", null, null, "", "", "", null)
            }
        }

        val collectedSources = Collections.synchronizedList(mutableListOf<EupStreamSource>())

        resolveInternal(parsed,
            onV2StreamFound = { src ->
                collectedSources.add(src)
                send(StreamBundleEvent.SourcesFound(listOf(src)))
            },
            onSubFound = { /* Handled via StreamSource.subtitles */ },
            onStatus = { _, _ -> })

        send(StreamBundleEvent.Done(collectedSources.size))
    }.flowOn(Dispatchers.IO)

    override suspend fun refresh(stale: EupStreamSource, reason: RefreshReason): EupStreamSource? {
        return null
    }

    override suspend fun getStreamLinks(episodeData: String): StreamResult = withContext(Dispatchers.IO) {
        val parsed = parseTarget(episodeData, episodeData)
        val streams = mutableListOf<CoreStreamSource>()
        val subs = mutableListOf<SubtitleTrack>()

        resolveInternal(parsed,
            onStreamFound = { streams.add(it) },
            onSubFound = { subs.add(it) },
            onStatus = { _, _ -> })
        StreamResult(streams, subs)
    }

    // ───────────────────────────── Core Multi-Server Streaming Engine ─────────────────────────────
    private suspend fun resolveInternal(
        parsed: ParsedPlayable,
        onStreamFound: suspend (CoreStreamSource) -> Unit = {},
        onV2StreamFound: suspend (EupStreamSource) -> Unit = {},
        onSubFound: suspend (SubtitleTrack) -> Unit = {},
        onStatus: suspend (String, String) -> Unit = { _, _ -> }
    ) = coroutineScope {
        val tmdbId = parsed.tmdbId.ifBlank { return@coroutineScope }
        val isTv = parsed.mediaType == "tv" || mediaTypeCache[tmdbId] == MediaType.TV_SERIES
        val mediaType = if (isTv) "tv" else "movie"
        val season = parsed.season ?: 1
        val episode = parsed.episode ?: 1

        // 1. Ensure Metadata is loaded (Title, Year, ReleaseDate, IMDb ID)
        val meta = if (parsed.title.isBlank() || parsed.releaseDate.isBlank() || parsed.imdbId.isNullOrBlank()) {
            metaDetailsCache[tmdbId] ?: try {
                val detailUrl = "$tmdbBaseUrl/$mediaType/$tmdbId?api_key=$tmdbApiKey&append_to_response=external_ids"
                val dReq = Request.Builder().url(detailUrl).header("User-Agent", defaultUserAgent).build()
                http.meta.newCall(dReq).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val body = resp.body?.string().orEmpty()
                        if (body.startsWith("{")) {
                            val root = json.parseToJsonElement(body).jsonObject
                            val title = root["title"]?.jsonPrimitive?.contentOrNull
                                ?: root["name"]?.jsonPrimitive?.contentOrNull ?: "Media $tmdbId"
                            val relDate = root["release_date"]?.jsonPrimitive?.contentOrNull
                                ?: root["first_air_date"]?.jsonPrimitive?.contentOrNull ?: ""
                            val yr = relDate.take(4)
                            val imdb = root["external_ids"]?.jsonObject?.get("imdb_id")?.jsonPrimitive?.contentOrNull
                                ?: root["imdb_id"]?.jsonPrimitive?.contentOrNull
                            if (imdb != null) imdbIdCache[tmdbId] = imdb
                            val m = TmdbDetailsMeta(title, yr, relDate, imdb)
                            metaDetailsCache[tmdbId] = m
                            m
                        } else null
                    } else null
                }
            } catch (_: Throwable) { null }
        } else {
            TmdbDetailsMeta(parsed.title, parsed.year, parsed.releaseDate, parsed.imdbId)
        }

        val effectiveTitle = meta?.title ?: parsed.title.ifBlank { "Media $tmdbId" }
        val effectiveYear = meta?.year ?: parsed.year
        val effectiveDate = meta?.releaseDate ?: parsed.releaseDate
        val effectiveImdb = meta?.imdbId ?: parsed.imdbId ?: imdbIdCache[tmdbId]

        val embedReferer = if (isTv) {
            "$vidstuckBaseUrl/embed/tv/$tmdbId/$season/$episode"
        } else {
            "$vidstuckBaseUrl/embed/movie/$tmdbId"
        }

        val streamHeaders = mapOf(
            "User-Agent" to defaultUserAgent,
            "Referer" to embedReferer,
            "Origin" to vidstuckBaseUrl
        )

        val emittedSubUrls = Collections.synchronizedSet(mutableSetOf<String>())
        val collectedV2Subs = Collections.synchronizedList(mutableListOf<SubtitleDescriptor>())
        var introOffsetMs = 0L

        // ─── Parallel Task A: Intro & AniSkip Bridging ───
        val introJob = launch {
            try {
                // 1. Vidstuck intro interval
                if (!effectiveImdb.isNullOrBlank()) {
                    val introUrl = "$vidstuckBaseUrl/backend/intro?imdbId=$effectiveImdb&season=$season&episode=$episode&tmdbId=$tmdbId"
                    val iReq = Request.Builder().url(introUrl).headers(Headers.Builder().apply {
                        streamHeaders.forEach { (k, v) -> add(k, v) }
                    }.build()).build()
                    http.meta.newCall(iReq).execute().use { resp ->
                        if (resp.isSuccessful) {
                            val b = resp.body?.string().orEmpty()
                            if (b.startsWith("{")) {
                                val root = json.parseToJsonElement(b).jsonObject
                                val introObj = root["intro"]?.jsonObject
                                val startSec = introObj?.get("start")?.jsonPrimitive?.doubleOrNull
                                if (startSec != null && startSec > 0.0) {
                                    introOffsetMs = (startSec * 1000).toLong()
                                }
                            }
                        }
                    }
                }
            } catch (_: Throwable) {}

            // 2. AniSkip client fallback
            if (introOffsetMs == 0L && effectiveTitle.isNotBlank()) {
                try {
                    val skipRes = AniSkipClient.getInstance().getSkipTimesByTitle(effectiveTitle, episode)
                    val skip = skipRes.getOrNull()
                    if (skip?.introStartSeconds != null) {
                        introOffsetMs = (skip.introStartSeconds!! * 1000).toLong()
                    }
                } catch (_: Throwable) {}
            }
        }

        // ─── Parallel Task B: Subtitles (Vidstuck + OpenSubtitles v3) ───
        launch {
            // Subtitles Part 1: Vidstuck captions
            try {
                val tokenBody = buildJsonObject {
                    put(paramTmdbId, tmdbId)
                    put(paramMediaType, mediaType)
                    put(paramServer, "subtitle")
                    if (isTv) {
                        put(paramSeason, season)
                        put(paramEpisode, episode)
                    }
                }.toString()

                val tReq = Request.Builder()
                    .url("$vidstuckBaseUrl/backend/fuckoffniggawtaf")
                    .headers(Headers.Builder().apply { streamHeaders.forEach { (k, v) -> add(k, v) } }.build())
                    .post(tokenBody.toRequestBody("application/json".toMediaType()))
                    .build()

                http.meta.newCall(tReq).execute().use { tResp ->
                    if (tResp.isSuccessful) {
                        val tJson = json.parseToJsonElement(tResp.body?.string().orEmpty()).jsonObject
                        val ts = tJson["ts"]?.jsonPrimitive?.longOrNull ?: System.currentTimeMillis()
                        val token = tJson["token"]?.jsonPrimitive?.contentOrNull ?: ""

                        val sUrl = HttpUrl.Builder()
                            .scheme("https")
                            .host("vidstuck.xyz")
                            .addPathSegments("backend/subtitle")
                            .addQueryParameter(paramTmdbId, tmdbId)
                            .addQueryParameter(paramMediaType, mediaType)
                            .addQueryParameter(paramServer, "subtitle")
                            .addQueryParameter(paramTs, ts.toString())
                            .addQueryParameter(paramToken, token)
                            .addQueryParameter(paramTitle, effectiveTitle)
                            .addQueryParameter(paramYear, effectiveYear)
                            .addQueryParameter(paramDate, effectiveDate)
                            .apply {
                                if (isTv) {
                                    addQueryParameter(paramSeason, season.toString())
                                    addQueryParameter(paramEpisode, episode.toString())
                                }
                            }.build()

                        val subReq = Request.Builder().url(sUrl)
                            .headers(Headers.Builder().apply { streamHeaders.forEach { (k, v) -> add(k, v) } }.build())
                            .build()

                        http.meta.newCall(subReq).execute().use { sResp ->
                            if (sResp.isSuccessful) {
                                val sRoot = json.parseToJsonElement(sResp.body?.string().orEmpty()).jsonObject
                                val captions = sRoot["captions"]?.jsonArray.orEmpty()
                                for (cap in captions) {
                                    val cObj = cap.jsonObject
                                    val fileUrl = cObj["file"]?.jsonPrimitive?.contentOrNull ?: continue
                                    val display = cObj["display"]?.jsonPrimitive?.contentOrNull ?: "English"
                                    if (emittedSubUrls.add(fileUrl)) {
                                        onSubFound(SubtitleTrack(url = fileUrl, language = display))
                                        collectedV2Subs.add(SubtitleDescriptor(language = display, url = fileUrl, mimeType = "application/x-subrip"))
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (ce: CancellationException) {
                throw ce
            } catch (t: Throwable) {
                safeLog("FreekzPlugin", "Vidstuck subtitle error: ${t.message}", t)
            }

            // Subtitles Part 2: OpenSubtitles v3 Bridge
            try {
                if (!effectiveImdb.isNullOrBlank() && effectiveImdb.startsWith("tt")) {
                    val openSubUrl = if (isTv) {
                        "https://opensubtitles-v3.strem.io/subtitles/series/$effectiveImdb:$season:$episode.json"
                    } else {
                        "https://opensubtitles-v3.strem.io/subtitles/movie/$effectiveImdb.json"
                    }
                    val req = Request.Builder().url(openSubUrl).header("User-Agent", defaultUserAgent).build()
                    http.meta.newCall(req).execute().use { resp ->
                        if (resp.isSuccessful) {
                            val body = resp.body?.string().orEmpty()
                            if (body.startsWith("{")) {
                                val root = json.parseToJsonElement(body).jsonObject
                                val subs = root["subtitles"]?.jsonArray.orEmpty()
                                for (elem in subs) {
                                    val subObj = elem.jsonObject
                                    val subUrl = subObj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                                    val lang = subObj["lang"]?.jsonPrimitive?.contentOrNull ?: "eng"
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
                                    if (emittedSubUrls.add(subUrl)) {
                                        onSubFound(SubtitleTrack(url = subUrl, language = displayLang))
                                        collectedV2Subs.add(SubtitleDescriptor(language = displayLang, url = subUrl, mimeType = "application/x-subrip"))
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (ce: CancellationException) {
                throw ce
            } catch (t: Throwable) {
                safeLog("FreekzPlugin", "OpenSubtitles bridge error: ${t.message}", t)
            }
        }

        // ─── Parallel Task C: Servers Stream Resolvers (Instant Zero-Delay Emission) ───
        // Run all servers concurrently with zero throttling so Atlas & all streams emit within < 400ms!

        // 1. FAST FLAGSHIP HLS: Atlas (Multi-HLS High-Speed CDN Mirrors) - Queried immediately!
        launch {
            try {
                onStatus("Atlas", "Connecting to Atlas Multi-HLS cluster...")
                val sData = fetchServerData("atlas", tmdbId, mediaType, isTv, season, episode, effectiveTitle, effectiveYear, effectiveDate, effectiveImdb, streamHeaders)
                if (sData != null) {
                    val links = sData["links"]?.jsonArray.orEmpty()
                    var mirrorIndex = 1
                    for (l in links) {
                        val encLink = l.jsonObject["link"]?.jsonPrimitive?.contentOrNull ?: continue
                        try {
                            val decUrl = CryptoJsAes.decrypt(encLink, cryptoPassphrase)
                            val fullUrl = if (decUrl.startsWith("http")) decUrl else "$vidstuckBaseUrl$decUrl"
                            val taggedUrl = if (fullUrl.contains("#")) fullUrl else "$fullUrl#hls.m3u8"

                            val label = "Freekz - Atlas [HLS Mirror $mirrorIndex - 1080p]"
                            mirrorIndex++
                            emitResolvedSource(
                                sourceId = "freekz_atlas_${mirrorIndex}_${fullUrl.hashCode().toString(16)}",
                                serverName = label,
                                url = taggedUrl,
                                quality = "1080p",
                                isHls = true,
                                audioTracks = listOf(CoreAudioTrackDescriptor("Original Audio", "en", 2, "aac")),
                                v2AudioTracks = listOf(EupAudioTrackDescriptor("Original Audio", "en", true, "aac", 2)),
                                streamHeaders = streamHeaders,
                                introOffsetMs = introOffsetMs,
                                subtitles = collectedV2Subs,
                                onStreamFound = onStreamFound,
                                onV2StreamFound = onV2StreamFound
                            )
                        } catch (e: Throwable) {
                            safeLog("FreekzPlugin", "Atlas decrypt error: ${e.message}", e)
                        }
                    }
                }
            } catch (ce: CancellationException) {
                throw ce
            } catch (t: Throwable) {
                safeLog("FreekzPlugin", "Atlas error: ${t.message}", t)
            }
        }

        // 2. FAST HLS: Ursa / Meow (Edge HLS CDN) - Queried concurrently
        launch {
            try {
                onStatus("Ursa", "Connecting to Ursa Edge cluster...")
                val sData = fetchServerData("meow", tmdbId, mediaType, isTv, season, episode, effectiveTitle, effectiveYear, effectiveDate, effectiveImdb, streamHeaders)
                if (sData != null) {
                    val links = sData["links"]?.jsonArray.orEmpty()
                    for (l in links) {
                        val encLink = l.jsonObject["link"]?.jsonPrimitive?.contentOrNull ?: continue
                        try {
                            val decUrl = CryptoJsAes.decrypt(encLink, cryptoPassphrase)
                            val fullUrl = if (decUrl.startsWith("http")) decUrl else "$vidstuckBaseUrl$decUrl"
                            val taggedUrl = if (fullUrl.contains("#")) fullUrl else "$fullUrl#hls.m3u8"

                            emitResolvedSource(
                                sourceId = "freekz_ursa_${fullUrl.hashCode().toString(16)}",
                                serverName = "Freekz - Ursa [HLS Stream]",
                                url = taggedUrl,
                                quality = "1080p",
                                isHls = true,
                                audioTracks = listOf(CoreAudioTrackDescriptor("Original Audio", "en", 2, "aac")),
                                v2AudioTracks = listOf(EupAudioTrackDescriptor("Original Audio", "en", true, "aac", 2)),
                                streamHeaders = streamHeaders,
                                introOffsetMs = introOffsetMs,
                                subtitles = collectedV2Subs,
                                onStreamFound = onStreamFound,
                                onV2StreamFound = onV2StreamFound
                            )
                        } catch (e: Throwable) {
                            safeLog("FreekzPlugin", "Ursa decrypt error: ${e.message}", e)
                        }
                    }
                }
            } catch (ce: CancellationException) {
                throw ce
            } catch (t: Throwable) {
                safeLog("FreekzPlugin", "Ursa error: ${t.message}", t)
            }
        }

        // 3. Orion (Original Audio + parallel background Dubs)
        launch {
            try {
                onStatus("Orion", "Connecting to Orion cluster...")
                val sData = fetchServerData("orion", tmdbId, mediaType, isTv, season, episode, effectiveTitle, effectiveYear, effectiveDate, effectiveImdb, streamHeaders)
                if (sData != null) {
                    val links = sData["links"]?.jsonArray.orEmpty()
                    for (l in links) {
                        val encLink = l.jsonObject["link"]?.jsonPrimitive?.contentOrNull ?: continue
                        val res = l.jsonObject["resolution"]?.jsonPrimitive?.contentOrNull ?: "1080p"
                        val type = l.jsonObject["type"]?.jsonPrimitive?.contentOrNull ?: "dash"
                        try {
                            val decUrl = CryptoJsAes.decrypt(encLink, cryptoPassphrase)
                            val fullUrl = if (decUrl.startsWith("http")) decUrl else "$vidstuckBaseUrl$decUrl"
                            val isHls = type == "hls" || fullUrl.contains(".m3u8")
                            val taggedUrl = if (isHls) {
                                if (fullUrl.contains("#")) fullUrl else "$fullUrl#hls.m3u8"
                            } else {
                                if (fullUrl.contains("#")) fullUrl else "$fullUrl#dash.mpd"
                            }

                            emitResolvedSource(
                                sourceId = "freekz_orion_orig_${fullUrl.hashCode().toString(16)}",
                                serverName = "Freekz - Orion [Original Audio]",
                                url = taggedUrl,
                                quality = if (res == "0" || res == "null") "1080p" else res,
                                isHls = isHls,
                                audioTracks = listOf(CoreAudioTrackDescriptor("English", "en", 6, "aac")),
                                v2AudioTracks = listOf(EupAudioTrackDescriptor("English 5.1", "en", true, "aac", 6)),
                                streamHeaders = streamHeaders,
                                introOffsetMs = introOffsetMs,
                                subtitles = collectedV2Subs,
                                onStreamFound = onStreamFound,
                                onV2StreamFound = onV2StreamFound
                            )
                        } catch (e: Throwable) {
                            safeLog("FreekzPlugin", "Orion decrypt error: ${e.message}", e)
                        }
                    }

                    // Background dub fetching without blocking primary stream playback
                    val dubs = sData["dubs"]?.jsonArray.orEmpty()
                    for (d in dubs.take(6)) {
                        val dObj = d.jsonObject
                        if (dObj["original"]?.jsonPrimitive?.booleanOrNull == true) continue
                        val lanCode = dObj["lanCode"]?.jsonPrimitive?.contentOrNull ?: continue
                        val lanName = dObj["lanName"]?.jsonPrimitive?.contentOrNull ?: lanCode.uppercase()
                        val dubType = dObj["type"]?.jsonPrimitive?.contentOrNull ?: "0"

                        launch {
                            try {
                                val dubData = fetchServerData("orion", tmdbId, mediaType, isTv, season, episode, effectiveTitle, effectiveYear, effectiveDate, effectiveImdb, streamHeaders, dubCode = lanCode, dubType = dubType)
                                val dubLinks = dubData?.get("links")?.jsonArray.orEmpty()
                                for (dl in dubLinks) {
                                    val encDubLink = dl.jsonObject["link"]?.jsonPrimitive?.contentOrNull ?: continue
                                    val decDubUrl = CryptoJsAes.decrypt(encDubLink, cryptoPassphrase)
                                    val fullDubUrl = if (decDubUrl.startsWith("http")) decDubUrl else "$vidstuckBaseUrl$decDubUrl"
                                    val taggedDubUrl = if (fullDubUrl.contains("#")) fullDubUrl else "$fullDubUrl#dash.mpd"

                                    emitResolvedSource(
                                        sourceId = "freekz_orion_dub_${lanCode}_${fullDubUrl.hashCode().toString(16)}",
                                        serverName = "Freekz - Orion [$lanName Dub]",
                                        url = taggedDubUrl,
                                        quality = "1080p",
                                        isHls = false,
                                        audioTracks = listOf(CoreAudioTrackDescriptor(lanName, lanCode, 2, "aac")),
                                        v2AudioTracks = listOf(EupAudioTrackDescriptor(lanName, lanCode, false, "aac", 2)),
                                        streamHeaders = streamHeaders,
                                        introOffsetMs = introOffsetMs,
                                        subtitles = collectedV2Subs,
                                        onStreamFound = onStreamFound,
                                        onV2StreamFound = onV2StreamFound
                                    )
                                }
                            } catch (_: Throwable) {}
                        }
                    }
                }
            } catch (ce: CancellationException) {
                throw ce
            } catch (t: Throwable) {
                safeLog("FreekzPlugin", "Orion error: ${t.message}", t)
            }
        }

        // 4. Centaurus (Original Audio + parallel background Dubs)
        launch {
            try {
                onStatus("Centaurus", "Connecting to Centaurus cluster...")
                val sData = fetchServerData("centaurus", tmdbId, mediaType, isTv, season, episode, effectiveTitle, effectiveYear, effectiveDate, effectiveImdb, streamHeaders)
                if (sData != null) {
                    val links = sData["links"]?.jsonArray.orEmpty()
                    for (l in links) {
                        val encLink = l.jsonObject["link"]?.jsonPrimitive?.contentOrNull ?: continue
                        val res = l.jsonObject["resolution"]?.jsonPrimitive?.contentOrNull ?: "1080p"
                        try {
                            val decUrl = CryptoJsAes.decrypt(encLink, cryptoPassphrase)
                            val fullUrl = if (decUrl.startsWith("http")) decUrl else "$vidstuckBaseUrl$decUrl"
                            val taggedUrl = if (fullUrl.contains("#")) fullUrl else "$fullUrl#dash.mpd"

                            emitResolvedSource(
                                sourceId = "freekz_cent_orig_${fullUrl.hashCode().toString(16)}",
                                serverName = "Freekz - Centaurus [Original Audio]",
                                url = taggedUrl,
                                quality = if (res == "0" || res == "null") "1080p" else res,
                                isHls = false,
                                audioTracks = listOf(CoreAudioTrackDescriptor("English", "en", 6, "aac")),
                                v2AudioTracks = listOf(EupAudioTrackDescriptor("English 5.1", "en", true, "aac", 6)),
                                streamHeaders = streamHeaders,
                                introOffsetMs = introOffsetMs,
                                subtitles = collectedV2Subs,
                                onStreamFound = onStreamFound,
                                onV2StreamFound = onV2StreamFound
                            )
                        } catch (e: Throwable) {
                            safeLog("FreekzPlugin", "Centaurus decrypt error: ${e.message}", e)
                        }
                    }

                    // Background dub fetching
                    val dubs = sData["dubs"]?.jsonArray.orEmpty()
                    for (d in dubs.take(6)) {
                        val dObj = d.jsonObject
                        if (dObj["original"]?.jsonPrimitive?.booleanOrNull == true) continue
                        val lanCode = dObj["lanCode"]?.jsonPrimitive?.contentOrNull ?: continue
                        val lanName = dObj["lanName"]?.jsonPrimitive?.contentOrNull ?: lanCode.uppercase()
                        val dubType = dObj["type"]?.jsonPrimitive?.contentOrNull ?: "0"

                        launch {
                            try {
                                val dubData = fetchServerData("centaurus", tmdbId, mediaType, isTv, season, episode, effectiveTitle, effectiveYear, effectiveDate, effectiveImdb, streamHeaders, dubCode = lanCode, dubType = dubType)
                                val dubLinks = dubData?.get("links")?.jsonArray.orEmpty()
                                for (dl in dubLinks) {
                                    val encDubLink = dl.jsonObject["link"]?.jsonPrimitive?.contentOrNull ?: continue
                                    val decDubUrl = CryptoJsAes.decrypt(encDubLink, cryptoPassphrase)
                                    val fullDubUrl = if (decDubUrl.startsWith("http")) decDubUrl else "$vidstuckBaseUrl$decDubUrl"
                                    val taggedDubUrl = if (fullDubUrl.contains("#")) fullDubUrl else "$fullDubUrl#dash.mpd"

                                    emitResolvedSource(
                                        sourceId = "freekz_cent_dub_${lanCode}_${fullDubUrl.hashCode().toString(16)}",
                                        serverName = "Freekz - Centaurus [$lanName Dub]",
                                        url = taggedDubUrl,
                                        quality = "1080p",
                                        isHls = false,
                                        audioTracks = listOf(CoreAudioTrackDescriptor(lanName, lanCode, 2, "aac")),
                                        v2AudioTracks = listOf(EupAudioTrackDescriptor(lanName, lanCode, false, "aac", 2)),
                                        streamHeaders = streamHeaders,
                                        introOffsetMs = introOffsetMs,
                                        subtitles = collectedV2Subs,
                                        onStreamFound = onStreamFound,
                                        onV2StreamFound = onV2StreamFound
                                    )
                                }
                            } catch (_: Throwable) {}
                        }
                    }
                }
            } catch (ce: CancellationException) {
                throw ce
            } catch (t: Throwable) {
                safeLog("FreekzPlugin", "Centaurus error: ${t.message}", t)
            }
        }

        // 5. Andromeda (HD 1080p Smooth Playback)
        launch {
            try {
                onStatus("Andromeda", "Connecting to Andromeda HD cluster...")
                val sData = fetchServerData("andromeda", tmdbId, mediaType, isTv, season, episode, effectiveTitle, effectiveYear, effectiveDate, effectiveImdb, streamHeaders)
                if (sData != null) {
                    val links = sData["links"]?.jsonArray.orEmpty()
                    for (l in links) {
                        val encLink = l.jsonObject["link"]?.jsonPrimitive?.contentOrNull ?: continue
                        try {
                            val decUrl = CryptoJsAes.decrypt(encLink, cryptoPassphrase)
                            val fullUrl = if (decUrl.startsWith("http")) decUrl else "$vidstuckBaseUrl$decUrl"
                            val taggedUrl = if (fullUrl.contains("#")) fullUrl else "$fullUrl#dash.mpd"

                            emitResolvedSource(
                                sourceId = "freekz_andro_${fullUrl.hashCode().toString(16)}",
                                serverName = "Freekz - Andromeda [HD 1080p]",
                                url = taggedUrl,
                                quality = "1080p",
                                isHls = false,
                                audioTracks = listOf(CoreAudioTrackDescriptor("English", "en", 2, "aac")),
                                v2AudioTracks = listOf(EupAudioTrackDescriptor("English", "en", true, "aac", 2)),
                                streamHeaders = streamHeaders,
                                introOffsetMs = introOffsetMs,
                                subtitles = collectedV2Subs,
                                onStreamFound = onStreamFound,
                                onV2StreamFound = onV2StreamFound
                            )
                        } catch (e: Throwable) {
                            safeLog("FreekzPlugin", "Andromeda decrypt error: ${e.message}", e)
                        }
                    }
                }
            } catch (ce: CancellationException) {
                throw ce
            } catch (t: Throwable) {
                safeLog("FreekzPlugin", "Andromeda error: ${t.message}", t)
            }
        }
    }

    private suspend fun fetchServerData(
        serverName: String,
        tmdbId: String,
        mediaType: String,
        isTv: Boolean,
        season: Int,
        episode: Int,
        title: String,
        year: String,
        date: String,
        imdbId: String?,
        streamHeaders: Map<String, String>,
        dubCode: String? = null,
        dubType: String? = null
    ): JsonObject? {
        val tokenBody = buildJsonObject {
            put(paramTmdbId, tmdbId)
            put(paramMediaType, mediaType)
            put(paramServer, serverName)
            if (isTv) {
                put(paramSeason, season)
                put(paramEpisode, episode)
            }
        }.toString()

        val tReq = Request.Builder()
            .url("$vidstuckBaseUrl/backend/fuckoffniggawtaf")
            .headers(Headers.Builder().apply { streamHeaders.forEach { (k, v) -> add(k, v) } }.build())
            .post(tokenBody.toRequestBody("application/json".toMediaType()))
            .build()

        val tokenResp = http.meta.newCall(tReq).execute().use { resp ->
            if (resp.isSuccessful) resp.body?.string().orEmpty() else ""
        }
        if (tokenResp.isBlank() || !tokenResp.startsWith("{")) return null

        val tJson = json.parseToJsonElement(tokenResp).jsonObject
        val ts = tJson["ts"]?.jsonPrimitive?.longOrNull ?: System.currentTimeMillis()
        val token = tJson["token"]?.jsonPrimitive?.contentOrNull ?: ""

        val sUrl = HttpUrl.Builder()
            .scheme("https")
            .host("vidstuck.xyz")
            .addPathSegments("backend/servers/$serverName")
            .addQueryParameter(paramTmdbId, tmdbId)
            .addQueryParameter(paramServer, serverName)
            .addQueryParameter(paramMediaType, mediaType)
            .addQueryParameter(paramTs, ts.toString())
            .addQueryParameter(paramToken, token)
            .addQueryParameter(paramTitle, title)
            .addQueryParameter(paramYear, year)
            .addQueryParameter(paramDate, date)
            .apply {
                if (isTv) {
                    addQueryParameter(paramSeason, season.toString())
                    addQueryParameter(paramEpisode, episode.toString())
                }
                if (!imdbId.isNullOrBlank()) {
                    addQueryParameter(paramImdb, imdbId)
                }
                if (!dubCode.isNullOrBlank() && !dubType.isNullOrBlank()) {
                    addQueryParameter("dubCode", dubCode)
                    addQueryParameter("dubType", dubType)
                }
            }.build()

        val sReq = Request.Builder().url(sUrl)
            .headers(Headers.Builder().apply { streamHeaders.forEach { (k, v) -> add(k, v) } }.build())
            .build()

        return http.stream.newCall(sReq).execute().use { resp ->
            if (resp.isSuccessful) {
                val body = resp.body?.string().orEmpty()
                if (body.startsWith("{")) json.parseToJsonElement(body).jsonObject else null
            } else null
        }
    }

    private suspend fun emitResolvedSource(
        sourceId: String,
        serverName: String,
        url: String,
        quality: String,
        isHls: Boolean,
        audioTracks: List<CoreAudioTrackDescriptor>,
        v2AudioTracks: List<EupAudioTrackDescriptor>,
        streamHeaders: Map<String, String>,
        introOffsetMs: Long,
        subtitles: List<SubtitleDescriptor>,
        onStreamFound: suspend (CoreStreamSource) -> Unit,
        onV2StreamFound: suspend (EupStreamSource) -> Unit
    ) {
        // Legacy UniversalPlugin source
        val coreSource = CoreStreamSource(
            url = url,
            serverName = serverName,
            resolutionLabel = quality,
            quality = quality,
            isM3u8 = isHls,
            audioTracks = audioTracks,
            releaseType = if (serverName.contains("Original")) AudioReleaseType.ORIGINAL else AudioReleaseType.DUB,
            headers = streamHeaders
        )
        onStreamFound(coreSource)

        // EUP v2 StreamSource
        val v2Source = EupStreamSource(
            id = sourceId,
            serverId = serverName.filter { it.isLetterOrDigit() }.lowercase(),
            serverLabel = serverName,
            url = url,
            kind = if (isHls) StreamKind.HLS else StreamKind.DASH,
            headers = HeaderPolicy(sticky = streamHeaders),
            video = VideoInfo(height = quality.filter { it.isDigit() }.toIntOrNull() ?: 1080),
            audioTracks = v2AudioTracks,
            subtitles = subtitles,
            introOffsetMs = introOffsetMs
        )
        onV2StreamFound(v2Source)
    }
}
