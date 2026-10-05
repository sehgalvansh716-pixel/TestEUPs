package com.euthopiar.core.provider

import com.euthopiar.core.model.*
import com.euthopiar.core.network.DohDns
import com.euthopiar.core.util.TmdbBridge
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import okhttp3.OkHttpClient
import okhttp3.Request
import java.net.URLEncoder
import java.util.Base64
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * High-speed native streaming provider plugin for Movy (https://www.movy.sx/).
 *
 * Architecture & Features:
 * - Direct TMDB Catalog & Universal Multi-Search: Indexes movies and TV series directly via TMDB IDs.
 * - Multi-Server Video Backend: Reverse-engineered https://api.wecollege.net API.
 * - Dynamic Session Seed Negotiation: Obtains session seed per mediaId with automatic TTL caching.
 * - Custom PRNG Stream Cipher Decryption: Decrypts base64-encoded server payloads using a 61-element
 *   sparse state array, FNV-1a seed hashing, MurmurHash3 fmix32, and bitwise rotation.
 * - Multi-Server Mirror Support:
 *     - Primary CDNs: Miami, Boise, Seattle, Denver, Atlanta, Phoenix, Portland, Austin, Dallas, Tampa, Orlando
 *     - Regional & Dubbed Mirrors: Delhi (Hindi), Munich (German), Berlin (German), Paris (French), Cancun (Spanish)
 * - Progressive Stream Emission: Streams 2160p (4K UHD), 1080p (FHD), 720p (HD), 480p (SD), and Master HLS playlists.
 * - Multilingual Subtitles: Integrated Movy embedded subtitles + Granite + Natsuki fallbacks.
 * - Fast Direct Downloads: Pre-configured download descriptors with authenticated headers.
 * - High-Resolution Clear Logos: TMDB Images API with Metahub fallback.
 */
class MovyPlugin(
    private val client: OkHttpClient = DohDns.createOkHttpClient(
        connectTimeoutSeconds = 15,
        readTimeoutSeconds = 25
    )
) : UniversalPlugin {

    override val name: String = "Movy"
    override val mainUrl: String = "https://www.movy.sx"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE, MediaType.TV_SERIES)

    private val json = Json { ignoreUnknownKeys = true }
    private val tmdbApiKey = "1865f43a0549ca50d341dd9ab8b29f49"

    private val defaultHeaders = mapOf(
        "Origin" to "https://www.movy.sx",
        "Referer" to "https://www.movy.sx/",
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Ranges" to "bytes"
    )

    private val mirrorClient = client.newBuilder()
        .connectTimeout(6, TimeUnit.SECONDS)
        .readTimeout(8, TimeUnit.SECONDS)
        .callTimeout(10, TimeUnit.SECONDS)
        .build()

    // Caches
    private val logoCache = ConcurrentHashMap<String, String>()
    private val metadataCache = ConcurrentHashMap<String, MediaMetadata>()
    private val streamCache = ConcurrentHashMap<String, Pair<Long, StreamResult>>()
    private val seedCache = ConcurrentHashMap<Long, CachedSeed>()

    private data class CachedSeed(val seed: String, val expiresAt: Long)

    private data class MediaMetadata(
        val title: String,
        val year: Int?,
        val imdbId: String?,
        val totalSeasons: Int
    )

    private data class ServerMirror(
        val id: String,
        val name: String,
        val url: String,
        val releaseType: AudioReleaseType = AudioReleaseType.ORIGINAL,
        val audioTracks: List<AudioTrackDescriptor> = emptyList()
    )

    private val primaryMirrors = listOf(
        ServerMirror("miami", "Movy - Miami CDN", "https://api.wecollege.net/miami/sources"),
        ServerMirror("boise", "Movy - Boise CDN", "https://api.wecollege.net/boise/sources"),
        ServerMirror("vegas", "Movy - Vegas CDN", "https://api.wecollege.net/vegas/sources"),
        ServerMirror("phoenix", "Movy - Phoenix CDN", "https://api.wecollege.net/phoenix/sources"),
        ServerMirror("atlanta", "Movy - Atlanta CDN", "https://api.wecollege.net/atlanta/sources"),
        ServerMirror("portland", "Movy - Portland CDN", "https://api.wecollege.net/portland/sources"),
        ServerMirror("dallas", "Movy - Dallas CDN", "https://api.wecollege.net/dallas/sources"),
        ServerMirror("tampa", "Movy - Tampa CDN", "https://api.wecollege.net/tampa/sources"),
        ServerMirror("orlando", "Movy - Orlando CDN", "https://api.wecollege.net/orlando/sources"),
        ServerMirror("austin", "Movy - Austin CDN", "https://api.wecollege.net/austin/sources"),
        ServerMirror("seattle", "Movy - Seattle CDN", "https://api.wecollege.net/seattle/sources"),
        ServerMirror("denver", "Movy - Denver CDN", "https://api.wecollege.net/denver/sources")
    )

    private val regionalMirrors = listOf(
        ServerMirror(
            id = "delhi",
            name = "Movy - Delhi (Hindi)",
            url = "https://api.wecollege.net/delhi/sources",
            releaseType = AudioReleaseType.DUB,
            audioTracks = listOf(AudioTrackDescriptor(languageName = "Hindi", isoCode = "hi"))
        ),
        ServerMirror(
            id = "paris",
            name = "Movy - Paris (French)",
            url = "https://api.wecollege.net/paris/sources",
            releaseType = AudioReleaseType.DUB,
            audioTracks = listOf(AudioTrackDescriptor(languageName = "French", isoCode = "fr"))
        ),
        ServerMirror(
            id = "munich",
            name = "Movy - Munich (German)",
            url = "https://api.wecollege.net/munich/sources",
            releaseType = AudioReleaseType.DUB,
            audioTracks = listOf(AudioTrackDescriptor(languageName = "German", isoCode = "de"))
        ),
        ServerMirror(
            id = "berlin",
            name = "Movy - Berlin (German)",
            url = "https://api.wecollege.net/berlin/sources",
            releaseType = AudioReleaseType.DUB,
            audioTracks = listOf(AudioTrackDescriptor(languageName = "German", isoCode = "de"))
        ),
        ServerMirror(
            id = "cancun",
            name = "Movy - Cancun (Spanish)",
            url = "https://api.wecollege.net/cancun/sources",
            releaseType = AudioReleaseType.DUB,
            audioTracks = listOf(AudioTrackDescriptor(languageName = "Spanish", isoCode = "es"))
        )
    )

    // ==========================================
    // 1. Curated Home Catalog
    // ==========================================

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        coroutineScope {
            val categories = listOf(
                "Trending This Week" to "https://api.themoviedb.org/3/trending/all/week?api_key=$tmdbApiKey",
                "Now Playing in Theatres" to "https://api.themoviedb.org/3/movie/now_playing?api_key=$tmdbApiKey",
                "Popular Movies" to "https://api.themoviedb.org/3/movie/popular?api_key=$tmdbApiKey",
                "Popular TV Shows" to "https://api.themoviedb.org/3/tv/popular?api_key=$tmdbApiKey",
                "Top Rated Movies" to "https://api.themoviedb.org/3/movie/top_rated?api_key=$tmdbApiKey",
                "Top Rated TV Series" to "https://api.themoviedb.org/3/tv/top_rated?api_key=$tmdbApiKey",
                "Action Blockbusters" to "https://api.themoviedb.org/3/discover/movie?api_key=$tmdbApiKey&with_genres=28&sort_by=popularity.desc",
                "Sci-Fi & Fantasy Hits" to "https://api.themoviedb.org/3/discover/movie?api_key=$tmdbApiKey&with_genres=878&sort_by=popularity.desc",
                "Crime & Mystery Series" to "https://api.themoviedb.org/3/discover/tv?api_key=$tmdbApiKey&with_genres=80,9648&sort_by=popularity.desc",
                "Animation & Anime" to "https://api.themoviedb.org/3/discover/tv?api_key=$tmdbApiKey&with_genres=16&sort_by=popularity.desc",
                "Comedy Specials & Hits" to "https://api.themoviedb.org/3/discover/movie?api_key=$tmdbApiKey&with_genres=35&sort_by=popularity.desc"
            )

            val deferredRows = categories.map { (title, url) ->
                async { fetchCatalogRow(title, url) }
            }

            val rows = deferredRows.awaitAll().filterNotNull()
            if (rows.isNotEmpty()) {
                val firstRow = rows.first()
                val candidateItemsWithLogos = firstRow.items.take(10).map { item ->
                    async {
                        val logo = resolveLogo(item)
                        if (logo != null) item.copy(logoUrl = logo) else item
                    }
                }.awaitAll() + firstRow.items.drop(10)
                listOf(firstRow.copy(items = candidateItemsWithLogos)) + rows.drop(1)
            } else {
                rows
            }
        }
    }

    private fun fetchCatalogRow(title: String, url: String): CatalogRow? {
        return try {
            val req = Request.Builder()
                .url(url)
                .header("User-Agent", defaultHeaders["User-Agent"]!!)
                .build()
            client.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return null
                val body = resp.body?.string() ?: return null
                val root = json.parseToJsonElement(body).jsonObject
                val resultsArr = root["results"]?.jsonArray ?: return null

                val items = mutableListOf<MediaItem>()
                for (elem in resultsArr) {
                    val obj = elem.jsonObject
                    val id = obj["id"]?.jsonPrimitive?.contentOrNull ?: continue
                    val mediaTypeStr = obj["media_type"]?.jsonPrimitive?.contentOrNull
                    val isTv = mediaTypeStr == "tv" || obj.containsKey("first_air_date") || obj.containsKey("name")
                    val itemTitle = obj["title"]?.jsonPrimitive?.contentOrNull
                        ?: obj["name"]?.jsonPrimitive?.contentOrNull
                        ?: "Unknown"

                    val posterPath = obj["poster_path"]?.jsonPrimitive?.contentOrNull
                    val posterUrl = posterPath?.let { "https://image.tmdb.org/t/p/w500$it" }
                    val backdropPath = obj["backdrop_path"]?.jsonPrimitive?.contentOrNull
                    val backdropUrl = backdropPath?.let { "https://image.tmdb.org/t/p/w1280$it" } ?: posterUrl

                    val dateStr = obj["release_date"]?.jsonPrimitive?.contentOrNull
                        ?: obj["first_air_date"]?.jsonPrimitive?.contentOrNull
                    val year = dateStr?.take(4)?.toIntOrNull()
                    val vote = obj["vote_average"]?.jsonPrimitive?.doubleOrNull

                    items.add(
                        MediaItem(
                            id = id,
                            title = itemTitle,
                            url = "https://www.movy.sx/${if (isTv) "tv" else "movie"}/$id",
                            posterUrl = posterUrl,
                            backdropUrl = backdropUrl,
                            type = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE,
                            year = year,
                            rating = vote?.let { String.format(Locale.US, "%.1f", it) },
                            quality = "1080p",
                            provider = name
                        )
                    )
                }

                if (items.isNotEmpty()) CatalogRow(title = title, items = items) else null
            }
        } catch (_: Exception) {
            null
        }
    }

    // ==========================================
    // 2. Universal Search
    // ==========================================

    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        val encodedQuery = URLEncoder.encode(query, "UTF-8")
        val results = mutableListOf<MediaItem>()
        val seenIds = mutableSetOf<String>()

        try {
            val url = "https://api.themoviedb.org/3/search/multi?api_key=$tmdbApiKey&query=$encodedQuery"
            val req = Request.Builder()
                .url(url)
                .header("User-Agent", defaultHeaders["User-Agent"]!!)
                .build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    val body = resp.body?.string() ?: ""
                    val root = json.parseToJsonElement(body).jsonObject
                    val resultsArr = root["results"]?.jsonArray
                    if (resultsArr != null) {
                        for (elem in resultsArr) {
                            val obj = elem.jsonObject
                            val mediaTypeStr = obj["media_type"]?.jsonPrimitive?.contentOrNull ?: continue
                            if (mediaTypeStr != "movie" && mediaTypeStr != "tv") continue
                            val id = obj["id"]?.jsonPrimitive?.contentOrNull ?: continue
                            if (!seenIds.add(id)) continue

                            val title = obj["title"]?.jsonPrimitive?.contentOrNull
                                ?: obj["name"]?.jsonPrimitive?.contentOrNull
                                ?: "Unknown"
                            val type = if (mediaTypeStr == "tv") MediaType.TV_SERIES else MediaType.MOVIE
                            val posterPath = obj["poster_path"]?.jsonPrimitive?.contentOrNull
                            val posterUrl = posterPath?.let { "https://image.tmdb.org/t/p/w500$it" }
                            val backdropPath = obj["backdrop_path"]?.jsonPrimitive?.contentOrNull
                            val backdropUrl = backdropPath?.let { "https://image.tmdb.org/t/p/w1280$it" } ?: posterUrl

                            val dateStr = obj["release_date"]?.jsonPrimitive?.contentOrNull
                                ?: obj["first_air_date"]?.jsonPrimitive?.contentOrNull
                            val year = dateStr?.take(4)?.toIntOrNull()
                            val vote = obj["vote_average"]?.jsonPrimitive?.doubleOrNull

                            results.add(
                                MediaItem(
                                    id = id,
                                    title = title,
                                    url = "https://www.movy.sx/$mediaTypeStr/$id",
                                    posterUrl = posterUrl,
                                    backdropUrl = backdropUrl,
                                    type = type,
                                    year = year,
                                    rating = vote?.let { String.format(Locale.US, "%.1f", it) },
                                    quality = "1080p",
                                    provider = name
                                )
                            )
                        }
                    }
                }
            }
        } catch (_: Exception) {}

        results
    }

    // ==========================================
    // 3. Media Details & Episode Manifest
    // ==========================================

    override suspend fun getDetails(mediaItem: MediaItem): MediaDetail = withContext(Dispatchers.IO) {
        val isTv = mediaItem.type == MediaType.TV_SERIES
        val tmdbId = mediaItem.id
        val enriched = TmdbBridge.fetchEnrichedDetails(client, tmdbId, isTv, mediaItem.title, tmdbApiKey)

        val title = enriched?.title ?: mediaItem.title
        val posterUrl = enriched?.posterUrl ?: mediaItem.posterUrl
        val backdropUrl = enriched?.backdropUrl ?: mediaItem.backdropUrl
        val year = enriched?.year ?: mediaItem.year
        val synopsis = enriched?.synopsis ?: ""
        val genres = enriched?.genres ?: emptyList()
        val cast = enriched?.cast ?: emptyList()
        val rating = enriched?.rating ?: mediaItem.rating
        val rottenTomatoes = enriched?.rottenTomatoesRating
        val contentRating = enriched?.contentRating
        val duration = enriched?.duration
        val trailerUrl = enriched?.trailerUrl
        val directors = enriched?.directors ?: emptyList()
        val recommendations = enriched?.recommendations ?: emptyList()
        val logoUrl = enriched?.logoUrl ?: resolveLogo(mediaItem)
        val imdbId = enriched?.imdbId

        val episodes = mutableListOf<EpisodeItem>()
        if (isTv) {
            val seasonNumbers = enriched?.seasonNumbers ?: listOf(1)
            val tmdbSeasonsData = TmdbBridge.fetchTmdbSeasons(client, tmdbId, seasonNumbers, tmdbApiKey)
            for (sNum in seasonNumbers) {
                val epMap = tmdbSeasonsData[sNum]
                if (!epMap.isNullOrEmpty()) {
                    for ((epNum, epDetail) in epMap.toSortedMap()) {
                        episodes.add(
                            EpisodeItem(
                                id = "$tmdbId-s${sNum}e$epNum",
                                title = epDetail.name.ifBlank { "Season $sNum - Episode $epNum" },
                                seasonNumber = sNum,
                                episodeNumber = epNum,
                                data = "$tmdbId:$sNum:$epNum",
                                thumbnail = epDetail.stillUrl ?: backdropUrl,
                                description = epDetail.overview,
                                duration = epDetail.duration
                            )
                        )
                    }
                }
            }
            if (episodes.isEmpty()) {
                episodes.add(
                    EpisodeItem(
                        id = "$tmdbId-s1e1",
                        title = "$title - Episode 1",
                        seasonNumber = 1,
                        episodeNumber = 1,
                        data = "$tmdbId:1:1",
                        thumbnail = backdropUrl
                    )
                )
            }
        } else {
            episodes.add(
                EpisodeItem(
                    id = tmdbId,
                    title = title,
                    seasonNumber = 1,
                    episodeNumber = 1,
                    data = tmdbId,
                    thumbnail = backdropUrl,
                    description = synopsis,
                    duration = duration
                )
            )
        }

        // Cache metadata for rapid subsequent stream resolution
        val maxSeason = enriched?.seasonNumbers?.maxOrNull() ?: 1
        metadataCache[tmdbId] = MediaMetadata(
            title = title,
            year = year,
            imdbId = imdbId,
            totalSeasons = maxSeason
        )

        val typeStr = if (isTv) "tv" else "movie"
        MediaDetail(
            id = tmdbId,
            title = title,
            url = "https://www.movy.sx/$typeStr/$tmdbId",
            posterUrl = posterUrl,
            backdropUrl = backdropUrl,
            type = mediaItem.type,
            year = year,
            synopsis = synopsis,
            genres = genres,
            duration = duration,
            episodes = episodes,
            rating = rating,
            rottenTomatoesRating = rottenTomatoes,
            contentRating = contentRating,
            provider = name,
            cast = cast,
            directors = directors,
            recommendations = recommendations,
            trailerUrl = trailerUrl,
            logoUrl = logoUrl,
            imdbId = imdbId
        )
    }

    // ==========================================
    // 4. Stream Resolution & PRNG Decryption Flow
    // ==========================================

    private fun parseEpisodeData(episodeData: String): Triple<String, Int?, Int?> {
        val clean = episodeData.trim()
            .removePrefix("https://www.movy.sx/")
            .removePrefix("http://www.movy.sx/")
            .removePrefix("https://movy.sx/")
            .removePrefix("http://movy.sx/")
            .removePrefix("/")
            .removePrefix("movie/")
            .removePrefix("tv/")

        return if (clean.contains(":")) {
            val parts = clean.split(":")
            val id = parts[0].filter { it.isDigit() }
            val s = parts.getOrNull(1)?.toIntOrNull()
            val e = parts.getOrNull(2)?.toIntOrNull()
            Triple(id, s, e)
        } else if (clean.contains("/")) {
            val parts = clean.split("/")
            val id = parts[0].filter { it.isDigit() }
            val s = parts.getOrNull(1)?.toIntOrNull()
            val e = parts.getOrNull(2)?.toIntOrNull()
            Triple(id, s, e)
        } else {
            val id = clean.filter { it.isDigit() }
            Triple(id, null, null)
        }
    }

    private suspend fun resolveMetadata(tmdbId: String, isTv: Boolean): MediaMetadata = withContext(Dispatchers.IO) {
        metadataCache[tmdbId]?.let { return@withContext it }

        val typeStr = if (isTv) "tv" else "movie"
        try {
            val url = "https://api.themoviedb.org/3/$typeStr/$tmdbId?api_key=$tmdbApiKey&append_to_response=external_ids"
            val req = Request.Builder().url(url).header("User-Agent", defaultHeaders["User-Agent"]!!).build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    val body = resp.body?.string() ?: ""
                    val root = json.parseToJsonElement(body).jsonObject
                    val title = root["title"]?.jsonPrimitive?.contentOrNull
                        ?: root["name"]?.jsonPrimitive?.contentOrNull
                        ?: "Media"
                    val dateStr = root["release_date"]?.jsonPrimitive?.contentOrNull
                        ?: root["first_air_date"]?.jsonPrimitive?.contentOrNull
                    val year = dateStr?.take(4)?.toIntOrNull()
                    val externalIds = root["external_ids"]?.jsonObject
                    val imdbId = externalIds?.get("imdb_id")?.jsonPrimitive?.contentOrNull
                        ?: root["imdb_id"]?.jsonPrimitive?.contentOrNull
                    val totalSeasons = root["number_of_seasons"]?.jsonPrimitive?.intOrNull
                        ?: root["seasons"]?.jsonArray?.size ?: 1

                    val meta = MediaMetadata(title, year, imdbId, totalSeasons)
                    metadataCache[tmdbId] = meta
                    return@withContext meta
                }
            }
        } catch (_: Exception) {}

        val fallback = MediaMetadata("Media", null, null, 1)
        metadataCache[tmdbId] = fallback
        fallback
    }

    private suspend fun fetchSessionSeed(mediaId: Long): String? = withContext(Dispatchers.IO) {
        val now = System.currentTimeMillis()
        val cached = seedCache[mediaId]
        if (cached != null && cached.expiresAt > now + 5000L) {
            return@withContext cached.seed
        }

        try {
            val url = "https://api.wecollege.net/seed?mediaId=$mediaId"
            val req = Request.Builder()
                .url(url)
                .header("Origin", defaultHeaders["Origin"]!!)
                .header("Referer", defaultHeaders["Referer"]!!)
                .header("User-Agent", defaultHeaders["User-Agent"]!!)
                .build()

            client.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return@withContext null
                val body = resp.body?.string() ?: return@withContext null
                val root = json.parseToJsonElement(body).jsonObject
                val seed = root["seed"]?.jsonPrimitive?.contentOrNull ?: return@withContext null
                val ttlMs = root["ttlMs"]?.jsonPrimitive?.longOrNull ?: 30000L
                seedCache[mediaId] = CachedSeed(seed, now + ttlMs)
                return@withContext seed
            }
        } catch (_: Exception) {
            null
        }
    }

    override fun getStreamFlow(episodeData: String): Flow<StreamEmission> = channelFlow {
        val (tmdbIdStr, season, episode) = parseEpisodeData(episodeData)
        if (tmdbIdStr.isBlank()) return@channelFlow
        val tmdbId = tmdbIdStr.toLongOrNull() ?: return@channelFlow
        val isTv = season != null && episode != null

        send(StreamEmission.StatusUpdate(name, "Resolving metadata for TMDB $tmdbId..."))
        val meta = resolveMetadata(tmdbIdStr, isTv)

        send(StreamEmission.StatusUpdate(name, "Requesting Movy session seed..."))
        val seed = fetchSessionSeed(tmdbId)
        if (seed.isNullOrBlank()) {
            send(StreamEmission.StatusUpdate(name, "Failed to negotiate session seed"))
            return@channelFlow
        }

        send(StreamEmission.StatusUpdate(name, "Connecting to Movy high-speed mirrors..."))

        val emittedStreamUrls = ConcurrentHashMap.newKeySet<String>()
        val emittedSubUrls = ConcurrentHashMap.newKeySet<String>()
        val emittedSubLangs = ConcurrentHashMap.newKeySet<String>()

        // 1. Fetch Subtitles from Granite concurrently
        launch {
            try {
                val graniteUrl = if (isTv) {
                    "https://sub.vdrk.site/v1/tv/$tmdbId/$season/$episode"
                } else {
                    "https://sub.vdrk.site/v1/movie/$tmdbId"
                }
                val req = Request.Builder().url(graniteUrl).header("User-Agent", defaultHeaders["User-Agent"]!!).build()
                client.newCall(req).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val body = resp.body?.string() ?: ""
                        val arr = json.parseToJsonElement(body).jsonArray
                        for (elem in arr) {
                            val obj = elem.jsonObject
                            val fileUrl = obj["file"]?.jsonPrimitive?.contentOrNull ?: continue
                            val rawLabel = obj["label"]?.jsonPrimitive?.contentOrNull ?: "English"
                            val isHi = rawLabel.contains(Regex("""\b(hi\d*|sdh)\b""", RegexOption.IGNORE_CASE))
                            val trackNum = Regex("""\d+$""").find(rawLabel)?.value
                            if (trackNum != null && (trackNum.toIntOrNull() ?: 1) > 1) {
                                continue
                            }
                            val cleanName = rawLabel.replace(Regex("""\s*(hi\d*|sdh)\b""", RegexOption.IGNORE_CASE), "")
                                .replace(Regex("""\d+$"""), "")
                                .trim()
                            val displayLabel = if (isHi) "$cleanName [CC]" else cleanName
                            val langKey = cleanName.lowercase().trim()
                            val variantKey = if (isHi) "$langKey [cc]" else langKey

                            if (emittedSubLangs.add(variantKey) && emittedSubUrls.add(fileUrl)) {
                                send(StreamEmission.SubtitleFound(SubtitleTrack(url = fileUrl, language = displayLabel)))
                            }
                        }
                    }
                }
            } catch (_: Exception) {}
        }

        // 2. Fetch Subtitles from Natsuki concurrently (if IMDb ID present)
        launch {
            try {
                val imdb = meta.imdbId
                if (!imdb.isNullOrBlank() && imdb.startsWith("tt")) {
                    val natsukiUrl = if (isTv) {
                        "https://natsuki.hls.lol/subs?imdbId=$imdb&season=$season&episode=$episode"
                    } else {
                        "https://natsuki.hls.lol/subs?imdbId=$imdb"
                    }
                    val req = Request.Builder()
                        .url(natsukiUrl)
                        .header("Origin", defaultHeaders["Origin"]!!)
                        .header("Referer", defaultHeaders["Referer"]!!)
                        .header("User-Agent", defaultHeaders["User-Agent"]!!)
                        .build()
                    client.newCall(req).execute().use { resp ->
                        if (resp.isSuccessful) {
                            val body = resp.body?.string() ?: ""
                            val root = json.parseToJsonElement(body).jsonObject
                            val subsArr = root["subtitles"]?.jsonArray
                            subsArr?.forEach { sElem ->
                                val sObj = sElem.jsonObject
                                val subUrl = sObj["url"]?.jsonPrimitive?.contentOrNull ?: return@forEach
                                val lang = sObj["language"]?.jsonPrimitive?.contentOrNull
                                    ?: sObj["langCode"]?.jsonPrimitive?.contentOrNull
                                    ?: "English"
                                val isHi = sObj["hearingImpaired"]?.jsonPrimitive?.booleanOrNull == true
                                val cleanLang = lang.trim()
                                val displayLabel = if (isHi) "$cleanLang [CC]" else cleanLang
                                val langKey = cleanLang.lowercase()
                                val variantKey = if (isHi) "$langKey [cc]" else langKey

                                if (emittedSubLangs.add(variantKey) && emittedSubUrls.add(subUrl)) {
                                    send(StreamEmission.SubtitleFound(SubtitleTrack(url = subUrl, language = displayLabel)))
                                }
                            }
                        }
                    }
                }
            } catch (_: Exception) {}
        }

        // 3. Query Mirror Servers Concurrently
        val allMirrors = primaryMirrors + regionalMirrors

        allMirrors.forEach { mirror ->
            launch {
                try {
                    val sources = queryMirror(mirror, seed, tmdbId, isTv, season, episode, meta)
                    sources.first.forEach { stream ->
                        if (emittedStreamUrls.add(stream.url)) {
                            send(StreamEmission.SourceFound(stream))
                        }
                    }
                    sources.second.forEach { sub ->
                        val isCc = sub.language.contains("[CC]", ignoreCase = true)
                        val base = sub.language.replace(Regex("""\s*\[CC\]""", RegexOption.IGNORE_CASE), "").trim().lowercase()
                        val variantKey = if (isCc) "$base [cc]" else base
                        if (emittedSubLangs.add(variantKey) && emittedSubUrls.add(sub.url)) {
                            send(StreamEmission.SubtitleFound(sub))
                        }
                    }
                } catch (_: Exception) {}
            }
        }
    }

    private suspend fun queryMirror(
        mirror: ServerMirror,
        seed: String,
        tmdbId: Long,
        isTv: Boolean,
        season: Int?,
        episode: Int?,
        meta: MediaMetadata
    ): Pair<List<StreamSource>, List<SubtitleTrack>> = withContext(Dispatchers.IO) {
        val streams = mutableListOf<StreamSource>()
        val subtitles = mutableListOf<SubtitleTrack>()

        val params = StringBuilder()
        params.append("title=").append(URLEncoder.encode(meta.title, "UTF-8"))
        params.append("&mediaType=").append(if (isTv) "tv" else "movie")
        meta.year?.let { params.append("&year=").append(it) }
        params.append("&tmdbId=").append(tmdbId)
        meta.imdbId?.takeIf { it.isNotBlank() }?.let { params.append("&imdbId=").append(it) }
        if (isTv && season != null && episode != null) {
            params.append("&seasonId=").append(season)
            params.append("&episodeId=").append(episode)
            params.append("&totalSeasons=").append(meta.totalSeasons)
        }
        if (mirror.id == "munich") {
            params.append("&language=german")
        }
        params.append("&enc=2")
        params.append("&seed=").append(URLEncoder.encode(seed, "UTF-8"))

        val url = "${mirror.url}?$params"
        val req = Request.Builder()
            .url(url)
            .header("Origin", defaultHeaders["Origin"]!!)
            .header("Referer", defaultHeaders["Referer"]!!)
            .header("User-Agent", defaultHeaders["User-Agent"]!!)
            .build()

        var encB64: String? = null
        try {
            mirrorClient.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    encB64 = resp.body?.string()?.trim()
                }
            }
        } catch (_: Exception) {
            return@withContext Pair(emptyList(), emptyList())
        }

        if (encB64.isNullOrBlank()) return@withContext Pair(emptyList(), emptyList())

        // If response is already an unencrypted error JSON string, skip
        if (encB64!!.startsWith("{") || encB64!!.startsWith("[")) {
            return@withContext Pair(emptyList(), emptyList())
        }

        val decryptedJson = try {
            MovyCipher.decrypt(encB64!!, seed, tmdbId)
        } catch (_: Exception) {
            return@withContext Pair(emptyList(), emptyList())
        }

        try {
            val root = json.parseToJsonElement(decryptedJson).jsonObject

            // Master playlist
            val playlistUrl = root["playlist"]?.jsonPrimitive?.contentOrNull
            if (!playlistUrl.isNullOrBlank()) {
                streams.add(
                    StreamSource(
                        url = playlistUrl,
                        serverName = mirror.name,
                        resolutionLabel = "Auto (Master)",
                        quality = "${mirror.name} - Auto",
                        isM3u8 = true,
                        audioTracks = mirror.audioTracks,
                        releaseType = mirror.releaseType,
                        headers = defaultHeaders
                    )
                )
            }

            // Quality sources
            val sourcesArr = root["sources"]?.jsonArray
            sourcesArr?.forEach { sElem ->
                val sObj = sElem.jsonObject
                val sUrl = sObj["url"]?.jsonPrimitive?.contentOrNull ?: return@forEach
                val rawQuality = sObj["quality"]?.jsonPrimitive?.contentOrNull ?: "Auto"
                val resLabel = when {
                    rawQuality.contains("2160", ignoreCase = true) || rawQuality.contains("4k", ignoreCase = true) -> "4K (2160p)"
                    rawQuality.contains("1080", ignoreCase = true) -> "1080p"
                    rawQuality.contains("720", ignoreCase = true) -> "720p"
                    rawQuality.contains("480", ignoreCase = true) -> "480p"
                    rawQuality.contains("360", ignoreCase = true) -> "360p"
                    rawQuality.equals("auto", ignoreCase = true) -> "Auto"
                    rawQuality.equals("voe", ignoreCase = true) -> "Auto (VOE)"
                    else -> rawQuality
                }

                streams.add(
                    StreamSource(
                        url = sUrl,
                        serverName = mirror.name,
                        resolutionLabel = resLabel,
                        quality = "${mirror.name} - $resLabel",
                        isM3u8 = sUrl.contains(".m3u8") || sUrl.contains("type=m3u8") || sUrl.contains("/vd/") || sUrl.contains("/vdb/") || sUrl.contains("/r6/") || sUrl.contains("/config-") || sUrl.contains("/master"),
                        audioTracks = mirror.audioTracks,
                        releaseType = mirror.releaseType,
                        headers = defaultHeaders
                    )
                )
            }

            // Embedded Subtitles
            val subsArr = root["subtitles"]?.jsonArray
            subsArr?.forEach { subElem ->
                val subObj = subElem.jsonObject
                val subUrl = subObj["url"]?.jsonPrimitive?.contentOrNull ?: return@forEach
                val rawLang = subObj["language"]?.jsonPrimitive?.contentOrNull
                    ?: subObj["lang"]?.jsonPrimitive?.contentOrNull
                    ?: "English"
                val displayLang = formatSubtitleLanguage(rawLang)
                subtitles.add(SubtitleTrack(url = subUrl, language = displayLang))
            }
        } catch (_: Exception) {}

        Pair(streams, subtitles)
    }

    private fun formatSubtitleLanguage(raw: String): String {
        val trimmed = raw.trim()
        val isSdh = trimmed.contains(Regex("""\b(sdh|cc)\b""", RegexOption.IGNORE_CASE))
        val baseCode = trimmed.replace(Regex("""\s*\((.*?)\)"""), "")
            .replace(Regex("""\s*\[(.*?)\]"""), "")
            .trim()
            .lowercase(Locale.ROOT)

        val languageName = when (baseCode) {
            "eng", "en" -> "English"
            "spa", "es" -> "Spanish"
            "fre", "fra", "fr" -> "French"
            "ger", "deu", "de" -> "German"
            "hin", "hi" -> "Hindi"
            "ita", "it" -> "Italian"
            "por", "pt" -> "Portuguese"
            "rus", "ru" -> "Russian"
            "jpn", "ja" -> "Japanese"
            "kor", "ko" -> "Korean"
            "chi", "zho", "zh" -> "Chinese"
            "ara", "ar" -> "Arabic"
            "bul", "bg" -> "Bulgarian"
            "cat", "ca" -> "Catalan"
            "cze", "cs" -> "Czech"
            "dan", "da" -> "Danish"
            "dut", "nld", "nl" -> "Dutch"
            "fin", "fi" -> "Finnish"
            "gre", "ell", "el" -> "Greek"
            "heb", "he" -> "Hebrew"
            "hun", "hu" -> "Hungarian"
            "ind", "id" -> "Indonesian"
            "nor", "no" -> "Norwegian"
            "pol", "pl" -> "Polish"
            "rum", "ron", "ro" -> "Romanian"
            "slo", "slk", "sk" -> "Slovak"
            "slv", "sl" -> "Slovenian"
            "srp", "sr" -> "Serbian"
            "swe", "sv" -> "Swedish"
            "tha", "th" -> "Thai"
            "tur", "tr" -> "Turkish"
            "ukr", "uk" -> "Ukrainian"
            "vie", "vi" -> "Vietnamese"
            else -> baseCode.replaceFirstChar { if (it.isLowerCase()) it.titlecase(Locale.ROOT) else it.toString() }
        }

        return if (isSdh) "$languageName [SDH]" else languageName
    }

    override suspend fun getStreamLinks(episodeData: String): StreamResult = withContext(Dispatchers.IO) {
        val (tmdbId, season, episode) = parseEpisodeData(episodeData)
        if (tmdbId.isBlank()) return@withContext StreamResult(emptyList(), emptyList())

        val cacheKey = "$tmdbId:$season:$episode"
        val cached = streamCache[cacheKey]
        val now = System.currentTimeMillis()
        if (cached != null && (now - cached.first < 120_000L) && cached.second.streams.isNotEmpty()) {
            return@withContext cached.second
        }

        val streamSources = mutableListOf<StreamSource>()
        val subtitleTracks = mutableListOf<SubtitleTrack>()

        getStreamFlow(episodeData).collect { emission ->
            when (emission) {
                is StreamEmission.SourceFound -> streamSources.add(emission.source)
                is StreamEmission.SubtitleFound -> if (subtitleTracks.none { it.url == emission.track.url }) subtitleTracks.add(emission.track)
                is StreamEmission.StatusUpdate -> {}
            }
        }

        val sortedStreams = streamSources.sortedWith(
            compareBy<StreamSource> {
                when {
                    it.resolutionLabel.contains("2160") || it.resolutionLabel.contains("4k", ignoreCase = true) -> 0
                    it.resolutionLabel.contains("1080") -> 1
                    it.resolutionLabel.contains("720") -> 2
                    it.resolutionLabel.contains("480") -> 3
                    it.resolutionLabel.contains("Auto", ignoreCase = true) -> 4
                    else -> 5
                }
            }.thenBy { it.serverName }
        )

        val sortedSubs = subtitleTracks.sortedWith(
            compareByDescending<SubtitleTrack> { it.language.contains("english", ignoreCase = true) }
                .thenBy { it.language }
        ).distinctBy {
            val isCc = it.language.contains("[CC]", ignoreCase = true)
            val base = it.language.replace(Regex("""\s*\[CC\]""", RegexOption.IGNORE_CASE), "").trim().lowercase()
            if (isCc) "$base [cc]" else base
        }

        val finalResult = StreamResult(streams = sortedStreams, subtitles = sortedSubs)
        if (finalResult.streams.isNotEmpty()) {
            streamCache[cacheKey] = now to finalResult
        }
        finalResult
    }

    override suspend fun getDownloadLinks(episodeData: String): List<DownloadOption> = withContext(Dispatchers.IO) {
        var streamResult = try {
            getStreamLinks(episodeData)
        } catch (_: Exception) {
            StreamResult(emptyList(), emptyList())
        }
        if (streamResult.streams.isEmpty()) {
            delay(500L)
            streamResult = try {
                getStreamLinks(episodeData)
            } catch (_: Exception) {
                StreamResult(emptyList(), emptyList())
            }
        }
        streamResult.streams.map { stream ->
            val sizeEstimate = when {
                stream.resolutionLabel.contains("2160") || stream.resolutionLabel.contains("4k", ignoreCase = true) -> "~6.8 GB"
                stream.resolutionLabel.contains("1080") -> "~2.4 GB"
                stream.resolutionLabel.contains("720") -> "~1.2 GB"
                stream.resolutionLabel.contains("480") -> "~650 MB"
                else -> "~1.8 GB"
            }
            DownloadOption(
                title = "${stream.quality} - Movy Direct Download",
                quality = stream.quality,
                size = sizeEstimate,
                url = stream.url,
                source = stream.serverName,
                provider = name,
                headers = stream.headers
            )
        }
    }

    override suspend fun resolveLogo(mediaItem: MediaItem): String? = withContext(Dispatchers.IO) {
        val tmdbId = mediaItem.id
        if (tmdbId.isBlank()) return@withContext null

        logoCache[tmdbId]?.let { return@withContext it }

        val isTv = mediaItem.type == MediaType.TV_SERIES
        val logo = TmdbBridge.resolveLogo(client, tmdbId, isTv, apiKey = tmdbApiKey)
        if (logo != null) {
            logoCache[tmdbId] = logo
            return@withContext logo
        }

        null
    }

    // ==========================================
    // 5. Movy Stream Cipher Decryptor
    // ==========================================

    object MovyCipher {
        private val D = intArrayOf(
            0x428a2f98.toInt(), 0x71374491.toInt(), 0xb5c0fbcf.toInt(), 0xe9b5dba5.toInt(),
            0x3956c25b, 0x59f111f1, 0x923f82a4.toInt(), 0xab1c5ed5.toInt(),
            0xd807aa98.toInt(), 0x12835b01, 0x243185be, 0x550c7dc3,
            0x72be5d74, 0x80deb1fe.toInt(), 0x9bdc06a7.toInt(), 0xc19bf174.toInt()
        )
        private val MAGIC_PREFIX = byteArrayOf(109, 118, 109, 49) // "mvm1"

        private fun o(e: Int): Int {
            var x = e xor (e ushr 16)
            x *= 0x85ebca6b.toInt()
            x = x xor (x ushr 13)
            x *= 0xc2b2ae35.toInt()
            return x xor (x ushr 16)
        }

        private fun u(e: Int, a: Int): Int {
            val shift = a and 31
            return if (shift == 0) e else (e shl shift) or (e ushr (32 - shift))
        }

        fun decrypt(encB64: String, seedStr: String, mediaId: Long): String {
            val normalized = encB64.trim().replace('-', '+').replace('_', '/')
            val padLen = (4 - (normalized.length % 4)) % 4
            val padded = normalized + "=".repeat(padLen)
            val raw = Base64.getDecoder().decode(padded)
            val t = raw.size

            val sValues = IntArray(61)
            val sPresent = BooleanArray(61)
            var h = 0x811c9dc5.toInt()
            for (i in 0 until seedStr.length) {
                h = (h xor seedStr[i].code) * 0x1000193
            }

            val mediaIdLow32 = (mediaId and 0xFFFFFFFFL).toInt()
            var n = o(o(h) xor o(mediaIdLow32 xor 0x9e3779b9.toInt()))

            for (e in 0 until 8) {
                if (((e * (e + 1)) and 1) == 0) {
                    val a = Integer.remainderUnsigned(n, 61)
                    n = u(n + 0x9e3779b9.toInt(), 7 + (7 and e))
                    sValues[a] = n xor o(n)
                    sPresent[a] = true
                    n = o(n + a)
                } else {
                    sValues[e] = D[15 and e]
                    sPresent[e] = true
                }
            }

            var acc = o(0xa5a5a5a5.toInt() xor n)

            val keystream = ByteArray(t)
            var lCounter = 0
            var e = 0
            while (e < t) {
                val dVal = Integer.remainderUnsigned(acc, 61)
                val isPresent = sPresent[dVal]
                val iFlag = if (isPresent) -1 else 0
                val rVal = if (isPresent) sValues[dVal] else 0
                val cVal = 0x9e3779b9.toInt() * (lCounter + 1)
                lCounter++
                val tXor = acc
                val sXor = rVal xor cVal
                val bVal = (tXor xor sXor) or (tXor and sXor and iFlag)
                val rot1 = u(bVal + acc, 31 and dVal)
                val rot2 = u(acc, 31 and (dVal * 7))
                acc = o((rot1 xor rot2) + 0x9e3779b9.toInt())
                sValues[dVal] = acc
                sPresent[dVal] = true
                val kw = acc

                keystream[e++] = (kw and 0xFF).toByte()
                if (e < t) keystream[e++] = ((kw ushr 8) and 0xFF).toByte()
                if (e < t) keystream[e++] = ((kw ushr 16) and 0xFF).toByte()
                if (e < t) keystream[e++] = ((kw ushr 24) and 0xFF).toByte()
            }

            for (i in 0 until t) {
                raw[i] = (raw[i].toInt() xor keystream[i].toInt()).toByte()
            }

            for (i in 0 until 4) {
                if (raw[i] != MAGIC_PREFIX[i]) {
                    throw IllegalArgumentException("Decrypt header mismatch")
                }
            }

            return String(raw, 4, t - 4, Charsets.UTF_8)
        }
    }

    override suspend fun fetchCast(mediaId: String, imdbId: String?, type: MediaType): List<CastMember> = emptyList()
}
