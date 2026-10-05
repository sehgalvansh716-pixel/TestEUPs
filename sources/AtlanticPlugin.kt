package com.euthopiar.core.provider

import com.euthopiar.core.dsl.*
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
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.net.URI
import java.net.URLEncoder
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import javax.crypto.Cipher
import javax.crypto.Mac
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * Native Atlantic streaming provider plugin (https://atlantic.st/).
 *
 * Implements the [UniversalPlugin] contract:
 * - Curated & Trending Catalogs: TMDB v3 API feeds across movies and TV series
 * - Universal Cross-Search: Multi-search across movies & TV series
 * - Media Details & Episodes: Complete metadata, real episode titles, still thumbnails, and cast
 * - Stream Extraction:
 *     - Aphrodite: Native Atlantic CDN (cdn.hls.lol) with dynamic key derivation & HMAC-SHA256 session handshake
 *     - Helios: AES-256-GCM decrypted stream engine across all native server mirrors (Moscow, Novo, Omsk)
 *     - Master Playlist Resolution: Extracts 1080p FHD, 720p HD, 480p SD tiers and multi-audio tracks
 * - Multilingual Subtitles: Granite (sub.vdrk.site) + Natsuki (natsuki.hls.lol)
 * - Direct Downloads: Native stream mirrors with authenticated playback headers
 * - Clear Logos: TMDB images API with Metahub fallback
 */
class AtlanticPlugin(
    private val client: OkHttpClient = DohDns.createOkHttpClient(
        connectTimeoutSeconds = 15,
        readTimeoutSeconds = 25
    )
) : UniversalPlugin {

    override val name: String = "Atlantic"
    override val mainUrl: String = "https://atlantic.st"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE, MediaType.TV_SERIES)

    private val json = Json { ignoreUnknownKeys = true }

    private val tmdbApiKey = "1865f43a0549ca50d341dd9ab8b29f49"

    private val defaultHeaders = mapOf(
        "Referer" to "https://atlantic.st/",
        "Origin" to "https://atlantic.st",
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Ranges" to "bytes"
    )

    // Helios AES-256-GCM decryption key (updated live key)
    private val heliosKeyHex = "117c358bcfcaf8fe2cfca57c9d2238a300e1c4de2efb83a5012ba84d8a31f1dd"

    // Moscow AES-256-GCM decryption key (high-speed proxy cluster)
    private val moscowKeyHex = "55060a042823f51a94c296894a58a0db15f3baef807155140983083fa799ef47"

    // Fallback Aphrodite master key (extracted from production bundle)
    private val defaultAphroditeKey = hexToBytes("c12a152cee1630cf8c6f1041c06aa20b7ed6c5e280bce24fc44db23ae3785ddd")

    @Volatile
    private var cachedAphroditeKey: ByteArray? = defaultAphroditeKey

    // Aphrodite session cache: sid, skey, exp (timestamp in seconds)
    private data class AphroditeSession(val sid: String, val skey: ByteArray, val exp: Long)
    @Volatile
    private var activeSession: AphroditeSession? = null

    private val secureRandom = SecureRandom()
    private val logoCache = ConcurrentHashMap<String, String>()

    private fun hexToBytes(hex: String): ByteArray {
        val len = hex.length
        val data = ByteArray(len / 2)
        var i = 0
        while (i < len) {
            data[i / 2] = ((Character.digit(hex[i], 16) shl 4) + Character.digit(hex[i + 1], 16)).toByte()
            i += 2
        }
        return data
    }

    private fun bytesToHex(bytes: ByteArray): String {
        val sb = StringBuilder(bytes.size * 2)
        for (b in bytes) {
            sb.append(String.format("%02x", b))
        }
        return sb.toString()
    }

    private fun hmacSha256(key: ByteArray, data: String): ByteArray {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(key, "HmacSHA256"))
        return mac.doFinal(data.toByteArray(Charsets.UTF_8))
    }

    private fun newRequestBuilder(url: String): Request.Builder {
        val builder = Request.Builder().url(url)
        defaultHeaders.forEach { (k, v) -> builder.header(k, v) }
        return builder
    }

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
                async {
                    fetchCatalogRow(title, url)
                }
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
        try {
            val req = Request.Builder()
                .url(url)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
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
                            url = "https://atlantic.st/${if (isTv) "tv" else "movie"}/$id",
                            posterUrl = posterUrl,
                            backdropUrl = backdropUrl,
                            type = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE,
                            year = year,
                            rating = vote?.let { String.format("%.1f", it) },
                            quality = "1080p",
                            provider = name
                        )
                    )
                }

                return if (items.isNotEmpty()) CatalogRow(title = title, items = items) else null
            }
        } catch (_: Exception) {
            return null
        }
    }

    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        val encodedQuery = URLEncoder.encode(query, "UTF-8")
        val results = mutableListOf<MediaItem>()
        val seenIds = mutableSetOf<String>()

        try {
            val url = "https://api.themoviedb.org/3/search/multi?api_key=$tmdbApiKey&query=$encodedQuery"
            val req = Request.Builder()
                .url(url)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
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
                                    url = "https://atlantic.st/$mediaTypeStr/$id",
                                    posterUrl = posterUrl,
                                    backdropUrl = backdropUrl,
                                    type = type,
                                    year = year,
                                    rating = vote?.let { String.format("%.1f", it) },
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
                                id = TmdbBridge.sanitizeSlug("$tmdbId-s${sNum}e$epNum"),
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
                        id = TmdbBridge.sanitizeSlug("$tmdbId-s1e1"),
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
                    id = TmdbBridge.sanitizeSlug(tmdbId),
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

        val typeStr = if (isTv) "tv" else "movie"
        MediaDetail(
            id = TmdbBridge.sanitizeSlug(tmdbId),
            title = title,
            url = "https://atlantic.st/$typeStr/$tmdbId",
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

    private val streamCache = ConcurrentHashMap<String, Pair<Long, StreamResult>>()

    private fun parseEpisodeData(episodeData: String): Triple<String, Int?, Int?> {
        val clean = episodeData.trim()
            .removePrefix("https://atlantic.st/")
            .removePrefix("http://atlantic.st/")
            .removePrefix("/")
            .removePrefix("movie/")
            .removePrefix("tv/")

        return if (clean.contains(":")) {
            val parts = clean.split(":")
            val id = parts[0].filter { it.isDigit() }
            val s = parts.getOrNull(1)?.toIntOrNull()
            val e = parts.getOrNull(2)?.toIntOrNull()
            Triple(id, s, e)
        } else {
            val id = clean.filter { it.isDigit() }
            Triple(id, null, null)
        }
    }

    override fun getStreamFlow(episodeData: String): Flow<StreamEmission> = channelFlow {
        val (tmdbId, season, episode) = parseEpisodeData(episodeData)
        if (tmdbId.isBlank()) return@channelFlow
        val isTv = season != null && episode != null

        send(StreamEmission.StatusUpdate("Atlantic", "Connecting to Atlantic CDN & Helios mirrors..."))

        val emittedStreamKeys = ConcurrentHashMap.newKeySet<String>()
        val emittedSubUrls = ConcurrentHashMap.newKeySet<String>()
        val emittedSubLangs = ConcurrentHashMap.newKeySet<String>()

        // 1a. Fetch Subtitles from Granite concurrently
        launch {
            try {
                val graniteUrl = if (isTv) {
                    "https://sub.vdrk.site/v1/tv/$tmdbId/$season/$episode"
                } else {
                    "https://sub.vdrk.site/v1/movie/$tmdbId"
                }
                val req = Request.Builder().url(graniteUrl).header("User-Agent", "Mozilla/5.0").build()
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
                            // Skip redundant community duplicate variants (e.g. English2, English3) to keep subtitle list clean and uncluttered
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

        // 1b. Fetch Subtitles from Natsuki concurrently
        launch {
            try {
                val imdb = resolveImdbId(tmdbId, isTv)
                if (!imdb.isNullOrBlank() && imdb.startsWith("tt")) {
                    val natsukiUrl = if (isTv) {
                        "https://natsuki.hls.lol/subs?imdbId=$imdb&season=$season&episode=$episode"
                    } else {
                        "https://natsuki.hls.lol/subs?imdbId=$imdb"
                    }
                    val req = newRequestBuilder(natsukiUrl).build()
                    client.newCall(req).execute().use { resp ->
                        if (resp.isSuccessful) {
                            val body = resp.body?.string() ?: ""
                            val root = json.parseToJsonElement(body).jsonObject
                            val subsArr = root["subtitles"]?.jsonArray
                            subsArr?.forEach { sElem ->
                                val sObj = sElem.jsonObject
                                val subUrl = sObj["url"]?.jsonPrimitive?.contentOrNull ?: return@forEach
                                val rawLang = sObj["language"]?.jsonPrimitive?.contentOrNull
                                    ?: sObj["langCode"]?.jsonPrimitive?.contentOrNull
                                    ?: "English"
                                val isHi = sObj["hearingImpaired"]?.jsonPrimitive?.booleanOrNull == true
                                val cleanLang = rawLang.trim()
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

        // 2. Fetch Aphrodite Native Stream (Primary High-Speed CDN at cdn.hls.lol)
        launch {
            try {
                val aphroditeStreams = fetchAphroditeStreams(tmdbId, isTv, season, episode)
                aphroditeStreams.forEach { stream ->
                    val key = "${stream.serverName}:${stream.resolutionLabel}:${stream.url}"
                    if (emittedStreamKeys.add(key)) {
                        send(StreamEmission.SourceFound(stream))
                    }
                }
            } catch (_: Exception) {}
        }

        // 3. Fetch Moscow Native High-Speed Proxy Engine (transcode.cfd proxy cluster)
        launch {
            try {
                val moscowStreams = fetchMoscowStreams(tmdbId, isTv, season, episode)
                moscowStreams.forEach { stream ->
                    val key = "${stream.serverName}:${stream.resolutionLabel}:${stream.url}"
                    if (emittedStreamKeys.add(key)) {
                        send(StreamEmission.SourceFound(stream))
                    }
                }
            } catch (_: Exception) {}
        }

        // 4. Fetch Helios Multi-Quality Server Cluster (Direct 1080p, 720p, 480p & Master HLS)
        launch {
            try {
                val heliosStreams = fetchHeliosStreams(tmdbId, isTv, season, episode)
                heliosStreams.forEach { stream ->
                    val key = "${stream.serverName}:${stream.resolutionLabel}:${stream.url}"
                    if (emittedStreamKeys.add(key)) {
                        send(StreamEmission.SourceFound(stream))
                    }
                }
            } catch (_: Exception) {}
        }
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
                    it.resolutionLabel.contains("1080") -> 0
                    it.resolutionLabel.contains("720") -> 1
                    it.resolutionLabel.contains("480") -> 2
                    it.resolutionLabel.contains("Auto", ignoreCase = true) -> 3
                    else -> 4
                }
            }.thenBy { it.serverName }
        )

        val sortedSubs = subtitleTracks
            .distinctBy {
                val isCc = it.language.contains("[CC]", ignoreCase = true)
                val base = it.language.replace(Regex("""\s*\[CC\]""", RegexOption.IGNORE_CASE), "").trim().lowercase()
                if (isCc) "$base [cc]" else base
            }
            .sortedWith(
                compareByDescending<SubtitleTrack> { it.language.contains("english", ignoreCase = true) }
                    .thenBy { it.language }
            )

        val finalResult = StreamResult(streams = sortedStreams, subtitles = sortedSubs)
        if (finalResult.streams.isNotEmpty()) {
            streamCache[cacheKey] = now to finalResult
        }
        finalResult
    }

    private suspend fun fetchAphroditeStreams(
        tmdbId: String,
        isTv: Boolean,
        season: Int?,
        episode: Int?
    ): List<StreamSource> = withContext(Dispatchers.IO) {
        val path = if (isTv && season != null && episode != null) {
            "/content/tv/$tmdbId/$season/$episode"
        } else {
            "/content/movie/$tmdbId"
        }

        // 1. Preferred: run Atlantic's live signing bundle (handles Cloudflare Turnstile gate)
        var streamUrl: String? = try {
            val chunk = discoverSigningChunkPath()
            val raw = if (chunk != null) AtlanticWebSigner.fetchContent(chunk, path) else null
            raw?.let {
                val root = json.parseToJsonElement(it).jsonObject
                if (root["found"]?.jsonPrimitive?.booleanOrNull == false) null
                else root["hls"]?.jsonPrimitive?.contentOrNull?.takeIf { u -> u.isNotBlank() }
                    ?: root["url"]?.jsonPrimitive?.contentOrNull
            }
        } catch (_: Throwable) { null }

        // 2. Fallback: native handshake with cached/extracted key if live signer fails
        if (streamUrl.isNullOrBlank()) {
            try {
                streamUrl = requestAphroditeContent(path)
                if (streamUrl == null) {
                    activeSession = null
                    streamUrl = requestAphroditeContent(path)
                }
            } catch (_: Throwable) {}
        }

        // 3. Fallback: direct high-speed GET
        if (streamUrl.isNullOrBlank()) {
            try {
                val req = Request.Builder()
                    .url("https://cdn.hls.lol$path")
                    .header("Referer", "https://atlantic.st/")
                    .header("Origin", "https://atlantic.st")
                    .header("User-Agent", defaultHeaders["User-Agent"] ?: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                    .header("Accept", "application/json, text/plain, */*")
                    .build()
                client.newCall(req).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val body = resp.body?.string()
                        if (!body.isNullOrBlank()) {
                            val root = json.parseToJsonElement(body).jsonObject
                            if (root["found"]?.jsonPrimitive?.booleanOrNull != false) {
                                streamUrl = root["url"]?.jsonPrimitive?.contentOrNull
                                    ?: root["hls"]?.jsonPrimitive?.contentOrNull
                            }
                        }
                    }
                }
            } catch (_: Throwable) {}
        }

        val resolvedUrl = streamUrl?.takeIf { it.startsWith("http") && !it.contains("totallyacdn.org", ignoreCase = true) }
        val isHoneypot = resolvedUrl == null || isHoneypotStream(resolvedUrl)

        if (resolvedUrl != null && !isHoneypot) {
            val isLive = withTimeoutOrNull(3000L) {
                isStreamReachable(resolvedUrl, defaultHeaders)
            } ?: false

            if (isLive) {
                val results = mutableListOf<StreamSource>()
                results.add(
                    StreamSource(
                        url = resolvedUrl,
                        serverName = "Aphrodite (1080p)",
                        resolutionLabel = "1080p FHD",
                        quality = "Atlantic Aphrodite (1080p FHD HLS)",
                        isM3u8 = true,
                        releaseType = AudioReleaseType.ORIGINAL,
                        headers = defaultHeaders
                    )
                )

                // Try inspecting master playlist for additional quality variants (1080p, 720p, 480p)
                try {
                    val variants = resolveMasterPlaylistVariants(resolvedUrl, "Atlantic Aphrodite", defaultHeaders)
                    variants.forEach { v ->
                        if (!results.any { it.url == v.url }) {
                            results.add(v)
                        }
                    }
                } catch (_: Throwable) {}

                return@withContext results
            }
        }

        // Direct Aphrodite stream was absent or returned the 8-segment Cloudflare VPN honeypot video.
        // Fall back to the verified high-speed master stream mapped directly to Aphrodite.
        fetchAphroditeFallbackStreams(tmdbId, isTv, season, episode)
    }

    private suspend fun fetchAphroditeFallbackStreams(
        tmdbId: String,
        isTv: Boolean,
        season: Int?,
        episode: Int?
    ): List<StreamSource> = withContext(Dispatchers.IO) {
        val results = mutableListOf<StreamSource>()
        try {
            val heliosUrl = if (isTv && season != null && episode != null) {
                "https://stream.hls.lol/helios?tmdbId=$tmdbId&type=tv&seasonId=$season&episodeId=$episode"
            } else {
                "https://stream.hls.lol/helios?tmdbId=$tmdbId&type=movie"
            }

            val req = newRequestBuilder(heliosUrl).build()
            client.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return@withContext emptyList()
                val body = resp.body?.string() ?: return@withContext emptyList()
                val root = json.parseToJsonElement(body).jsonObject
                val sources = root["sources"]?.jsonObject ?: return@withContext emptyList()

                val masterEntry = sources["Moscow"]?.jsonObject ?: sources.values.firstOrNull()?.jsonObject
                val encUrl = masterEntry?.get("url")?.jsonPrimitive?.contentOrNull ?: return@withContext emptyList()
                val masterDecrypted = decryptHeliosUrl(encUrl) ?: return@withContext emptyList()
                if (!masterDecrypted.startsWith("http")) return@withContext emptyList()
                if (masterDecrypted.contains("totallyacdn.org", ignoreCase = true)) return@withContext emptyList()

                // Extract direct quality variants if ?q= exists
                val qParam = masterDecrypted.substringAfter("?q=", "").substringBefore("&")
                if (qParam.isNotBlank()) {
                    try {
                        val decodedBytes = try {
                            java.util.Base64.getUrlDecoder().decode(qParam)
                        } catch (_: Exception) {
                            val padded = qParam + "=".repeat((4 - qParam.length % 4) % 4)
                            java.util.Base64.getDecoder().decode(padded)
                        }
                        val decodedJsonStr = String(decodedBytes, Charsets.UTF_8)
                        val qRoot = json.parseToJsonElement(decodedJsonStr).jsonObject
                        val customHeadersObj = qRoot["h"]?.jsonObject
                        val streamHeaders = mutableMapOf<String, String>()
                        customHeadersObj?.forEach { (k, v) ->
                            v.jsonPrimitive.contentOrNull?.let { streamHeaders[k] = it }
                        }
                        val aphHeaders = mapOf(
                            "Referer" to (streamHeaders["referer"] ?: "https://www.movy.sx/"),
                            "Origin" to (streamHeaders["origin"] ?: "https://www.movy.sx"),
                            "User-Agent" to defaultHeaders["User-Agent"]!!
                        )

                        val variantsArr = qRoot["v"]?.jsonArray
                        if (variantsArr != null) {
                            for (vElem in variantsArr) {
                                val vObj = vElem.jsonObject
                                val vUrl = vObj["u"]?.jsonPrimitive?.contentOrNull ?: continue
                                if (vUrl.contains("totallyacdn.org", ignoreCase = true)) continue
                                val vQuality = vObj["q"]?.jsonPrimitive?.contentOrNull ?: "1080p"
                                val qualityLabel = when {
                                    vQuality.contains("1080") -> "1080p FHD"
                                    vQuality.contains("720") -> "720p HD"
                                    vQuality.contains("480") -> "480p SD"
                                    else -> vQuality
                                }
                                val aphVariantUrl = if (vUrl.contains("?")) "$vUrl&server=aphrodite" else "$vUrl?server=aphrodite"
                                results.add(
                                    StreamSource(
                                        url = aphVariantUrl,
                                        serverName = "Aphrodite ($qualityLabel)",
                                        resolutionLabel = qualityLabel,
                                        quality = "Atlantic Aphrodite ($qualityLabel HLS)",
                                        isM3u8 = true,
                                        releaseType = AudioReleaseType.ORIGINAL,
                                        headers = aphHeaders
                                    )
                                )
                            }
                        }
                    } catch (_: Exception) {}
                }

                val separator = if (masterDecrypted.contains("?")) "&" else "?"
                val aphroditeMasterUrl = "$masterDecrypted${separator}server=aphrodite"
                val aphMasterHeaders = mapOf(
                    "Referer" to "https://stream.hls.lol/",
                    "Origin" to "https://stream.hls.lol",
                    "User-Agent" to defaultHeaders["User-Agent"]!!
                )

                results.add(
                    StreamSource(
                        url = aphroditeMasterUrl,
                        serverName = "Aphrodite (Auto)",
                        resolutionLabel = "Auto",
                        quality = "Atlantic Aphrodite (Auto HLS)",
                        isM3u8 = true,
                        releaseType = AudioReleaseType.ORIGINAL,
                        headers = aphMasterHeaders
                    )
                )

                try {
                    val variants = resolveMasterPlaylistVariants(masterDecrypted, "Atlantic Aphrodite", aphMasterHeaders)
                    variants.forEach { v ->
                        val vSep = if (v.url.contains("?")) "&" else "?"
                        val vUrl = "${v.url}${vSep}server=aphrodite"
                        results.add(
                            v.copy(
                                url = vUrl,
                                serverName = "Aphrodite (${v.resolutionLabel})",
                                quality = "Atlantic Aphrodite (${v.resolutionLabel} HLS)"
                            )
                        )
                    }
                } catch (_: Exception) {}
            }
        } catch (_: Exception) {}
        results
    }

    private suspend fun fetchMoscowStreams(
        tmdbId: String,
        isTv: Boolean,
        season: Int?,
        episode: Int?
    ): List<StreamSource> = withContext(Dispatchers.IO) {
        val results = mutableListOf<StreamSource>()
        try {
            val moscowUrl = if (isTv && season != null && episode != null) {
                "https://stream.hls.lol/moscow?tmdbId=$tmdbId&type=tv&seasonId=$season&episodeId=$episode"
            } else {
                "https://stream.hls.lol/moscow?tmdbId=$tmdbId&type=movie"
            }
            val req = newRequestBuilder(moscowUrl).build()
            client.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return@withContext emptyList()
                val body = resp.body?.string() ?: return@withContext emptyList()
                val root = json.parseToJsonElement(body).jsonObject
                val sources = root["sources"]?.jsonObject ?: return@withContext emptyList()
                for ((_, serverVal) in sources) {
                    val sObj = serverVal.jsonObject
                    val encUrl = sObj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                    val decryptedUrl = decryptMoscowUrl(encUrl) ?: continue
                    if (!decryptedUrl.startsWith("http")) continue
                    if (decryptedUrl.contains("totallyacdn.org", ignoreCase = true)) continue

                    val isLive = withTimeoutOrNull(3000L) {
                        isStreamReachable(decryptedUrl, defaultHeaders)
                    } ?: false

                    if (isLive) {
                        val moscowSource = StreamSource(
                            url = decryptedUrl,
                            serverName = "Moscow (1080p)",
                            resolutionLabel = "1080p FHD",
                            quality = "Atlantic Moscow (1080p FHD HLS)",
                            isM3u8 = true,
                            releaseType = AudioReleaseType.ORIGINAL,
                            headers = defaultHeaders
                        )
                        results.add(moscowSource)

                        val variants = resolveMasterPlaylistVariants(decryptedUrl, "Atlantic Moscow", defaultHeaders)
                        for (v in variants) {
                            if (!results.any { it.url == v.url }) {
                                results.add(v)
                            }
                        }
                    }
                }
            }
        } catch (_: Exception) {}
        results
    }

    private fun isStreamReachable(url: String, headers: Map<String, String>): Boolean {
        if (url.contains("totallyacdn.org", ignoreCase = true)) return false
        return try {
            val req = Request.Builder().url(url)
            headers.forEach { (k, v) -> req.header(k, v) }
            client.newCall(req.build()).execute().use { resp ->
                resp.isSuccessful
            }
        } catch (_: Throwable) {
            false
        }
    }

    private fun isHoneypotStream(masterUrl: String): Boolean {
        if (masterUrl.contains("totallyacdn.org", ignoreCase = true)) return true
        return try {
            val req = Request.Builder()
                .url(masterUrl)
                .header("Referer", "https://atlantic.st/")
                .header("Origin", "https://atlantic.st")
                .header("User-Agent", defaultHeaders["User-Agent"] ?: "Mozilla/5.0")
                .build()
            client.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return true
                val playlistText = resp.body?.string() ?: return true
                if (!playlistText.startsWith("#EXTM3U")) return true

                val baseUri = try { URI(masterUrl) } catch (_: Exception) { null }
                val subLine = playlistText.lines().firstOrNull { it.trim().isNotEmpty() && !it.trim().startsWith("#") }?.trim()
                    ?: return true
                val subUrl = baseUri?.resolve(subLine)?.toString() ?: subLine

                val subReq = Request.Builder()
                    .url(subUrl)
                    .header("Referer", "https://atlantic.st/")
                    .header("Origin", "https://atlantic.st")
                    .header("User-Agent", defaultHeaders["User-Agent"] ?: "Mozilla/5.0")
                    .build()
                client.newCall(subReq).execute().use { subResp ->
                    if (!subResp.isSuccessful) return true
                    val subText = subResp.body?.string() ?: return true
                    val extinfCount = subText.lines().count { it.trim().startsWith("#EXTINF:") }
                    extinfCount in 1..12
                }
            }
        } catch (_: Throwable) {
            true
        }
    }

    private fun requestAphroditeContent(path: String): String? {
        val session = getOrCreateAphroditeSession() ?: return null

        try {
            val reqTs = System.currentTimeMillis() / 1000
            val reqNonceBytes = ByteArray(8)
            secureRandom.nextBytes(reqNonceBytes)
            val reqNonce = bytesToHex(reqNonceBytes)

            val tagMsg = "v2:${session.sid}:$path:$reqTs:$reqNonce"
            val reqTag = bytesToHex(hmacSha256(session.skey, tagMsg))

            val contentReq = Request.Builder()
                .url("https://cdn.hls.lol$path")
                .header("X-Az-Id", session.sid)
                .header("X-Az-Ts", reqTs.toString())
                .header("X-Az-Nonce", reqNonce)
                .header("X-Az-Tag", reqTag)
                .header("Referer", "https://atlantic.st/")
                .header("Origin", "https://atlantic.st")
                .header("User-Agent", defaultHeaders["User-Agent"]!!)
                .build()

            client.newCall(contentReq).execute().use { resp ->
                if (!resp.isSuccessful) {
                    if (resp.code == 401 || resp.code == 403) {
                        activeSession = null
                    }
                    return null
                }
                val body = resp.body?.string() ?: return null
                val root = json.parseToJsonElement(body).jsonObject

                if (root["renew"]?.jsonPrimitive?.booleanOrNull == true) {
                    activeSession = null
                    return null
                }

                return root["url"]?.jsonPrimitive?.contentOrNull
                    ?: root["hls"]?.jsonPrimitive?.contentOrNull
            }
        } catch (_: Exception) {
            return null
        }
    }

    private fun getOrCreateAphroditeSession(): AphroditeSession? {
        val now = System.currentTimeMillis() / 1000
        val current = activeSession
        if (current != null && current.exp > now + 30) {
            return current
        }

        synchronized(this) {
            val locked = activeSession
            if (locked != null && locked.exp > now + 30) {
                return locked
            }

            // Attempt handshake with cached key
            var key = cachedAphroditeKey ?: defaultAphroditeKey
            var session = performHandshake(key)

            if (session == null) {
                // Key may have been rotated; dynamically extract master key from bundle
                val extractedKey = extractMasterKeyFromBundle()
                if (extractedKey != null) {
                    cachedAphroditeKey = extractedKey
                    session = performHandshake(extractedKey)
                }
            }

            if (session != null) {
                activeSession = session
            }
            return session
        }
    }

    private fun performHandshake(masterKey: ByteArray): AphroditeSession? {
        try {
            val ts = System.currentTimeMillis() / 1000
            val nonceBytes = ByteArray(8)
            secureRandom.nextBytes(nonceBytes)
            val nonce = bytesToHex(nonceBytes)

            val indexMsg = "v2:a:$ts:$nonce"
            val indexSig = bytesToHex(hmacSha256(masterKey, indexMsg))

            val indexJson = """{"c":"a","ts":$ts,"n":"$nonce","s":"$indexSig"}"""
            val indexReq = Request.Builder()
                .url("https://cdn.hls.lol/content/index")
                .header("Content-Type", "application/json")
                .header("Referer", "https://atlantic.st/")
                .header("Origin", "https://atlantic.st")
                .header("User-Agent", defaultHeaders["User-Agent"]!!)
                .post(indexJson.toRequestBody("application/json".toMediaType()))
                .build()

            client.newCall(indexReq).execute().use { resp ->
                if (!resp.isSuccessful) return null
                val body = resp.body?.string() ?: return null
                val d = json.parseToJsonElement(body).jsonObject["d"]?.jsonPrimitive?.contentOrNull ?: return null

                // Derive AES key: hmacSha256(masterKey, "aphrodite-seal-v2|$ts|$nonce")
                val deriveMsg = "aphrodite-seal-v2|$ts|$nonce"
                val aesKey = hmacSha256(masterKey, deriveMsg)

                val raw = hexToBytes(d)
                if (raw.size < 28) return null
                val iv = raw.copyOfRange(0, 12)
                val ctAndTag = raw.copyOfRange(12, raw.size)

                val cipher = Cipher.getInstance("AES/GCM/NoPadding")
                cipher.init(Cipher.DECRYPT_MODE, SecretKeySpec(aesKey, "AES"), GCMParameterSpec(128, iv))
                val sessionPlain = String(cipher.doFinal(ctAndTag), Charsets.UTF_8)
                val sessionObj = json.parseToJsonElement(sessionPlain).jsonObject

                val sid = sessionObj["sid"]?.jsonPrimitive?.contentOrNull ?: return null
                val skeyHex = sessionObj["skey"]?.jsonPrimitive?.contentOrNull ?: return null
                val exp = sessionObj["exp"]?.jsonPrimitive?.longOrNull ?: (ts + 3600)

                return AphroditeSession(sid = sid, skey = hexToBytes(skeyHex), exp = exp)
            }
        } catch (_: Exception) {
            return null
        }
    }

    @Volatile private var cachedChunkPath: Pair<Long, String>? = null

    /** Finds the live signing chunk (e.g. /assets/chunk-xxxx.js) imported by the index bundle. */
    private fun discoverSigningChunkPath(): String? {
        val c = cachedChunkPath
        if (c != null && System.currentTimeMillis() - c.first < 10 * 60_000L) return c.second
        return try {
            val ua = defaultHeaders["User-Agent"]!!
            val html = client.newCall(Request.Builder().url("https://atlantic.st/").header("User-Agent", ua).build())
                .execute().use { if (it.isSuccessful) it.body?.string() else null } ?: return null
            val direct = Regex("""/assets/chunk-[A-Za-z0-9_-]+\.js""").find(html)?.value
            val found = direct ?: run {
                val idx = Regex("""/assets/index-[A-Za-z0-9_-]+\.js""").find(html)?.value ?: return null
                val js = client.newCall(Request.Builder().url("https://atlantic.st$idx").header("User-Agent", ua).build())
                    .execute().use { if (it.isSuccessful) it.body?.string() else null } ?: return null
                Regex("""chunk-[A-Za-z0-9_-]+\.js""").find(js)?.value?.let { "/assets/$it" }
            }
            if (found != null) cachedChunkPath = System.currentTimeMillis() to found
            found
        } catch (_: Exception) { null }
    }

    /**
     * Dynamically extracts the Aphrodite master key from the live Atlantic production bundle.
     */
    private fun extractMasterKeyFromBundle(): ByteArray? {
        try {
            val htmlReq = Request.Builder()
                .url("https://atlantic.st/")
                .header("User-Agent", defaultHeaders["User-Agent"]!!)
                .build()
            val chunkPath = client.newCall(htmlReq).execute().use { htmlResp ->
                if (!htmlResp.isSuccessful) return null
                val html = htmlResp.body?.string() ?: return null
                val match = Regex("""/assets/chunk-[a-zA-Z0-9_-]+\.js""").find(html) ?: return null
                match.value
            }

            val chunkUrl = "https://atlantic.st$chunkPath"
            val chunkReq = Request.Builder()
                .url(chunkUrl)
                .header("User-Agent", defaultHeaders["User-Agent"]!!)
                .build()

            client.newCall(chunkReq).execute().use { chunkResp ->
                if (!chunkResp.isSuccessful) return null
                val chunkCode = chunkResp.body?.string() ?: return null

                val bMatch = Regex("""const\s+b\s*=\s*new\s+Uint8Array\(\[([\s\S]*?)\]\)""").find(chunkCode) ?: return null
                val uMatch = Regex("""U\s*=\s*new\s+Uint8Array\(\[([\s\S]*?)\]\)""").find(chunkCode) ?: return null

                val bVals = parseArithmeticIntArray(bMatch.groupValues[1])
                val uVals = parseArithmeticIntArray(uMatch.groupValues[1])
                if (bVals.size < 64 || uVals.size < 32) return null

                val p = 32
                val t = 1
                val x = 2
                val pArr = ByteArray(p)
                for (m in 0 until p) {
                    pArr[m] = (bVals[t + m * x] xor uVals[m]).toByte()
                }

                val prefix = "aphrodite.a.v2".toByteArray(Charsets.UTF_8)
                val combined = prefix + pArr
                val md = MessageDigest.getInstance("SHA-256")
                return md.digest(combined)
            }
        } catch (_: Exception) {
            return null
        }
    }

    private fun parseHexOrDec(s: String): Int {
        val trimmed = s.trim()
        var sign = 1
        var numStr = trimmed
        if (numStr.startsWith("-")) {
            sign = -1
            numStr = numStr.substring(1).trim()
        } else if (numStr.startsWith("+")) {
            numStr = numStr.substring(1).trim()
        }
        return if (numStr.startsWith("0x", ignoreCase = true)) {
            sign * numStr.substring(2).toInt(16)
        } else {
            sign * numStr.toInt(10)
        }
    }

    private fun parseArithmeticIntArray(text: String): List<Int> {
        val items = text.split(",")
        return items.mapNotNull { item ->
            try {
                var sum = 0
                val sanitized = item.replace("+-", "-").trim()
                val termRegex = Regex("""([+-]?[0-9a-fA-FxX]+)(?:\*([+-]?[0-9a-fA-FxX]+))?""")
                termRegex.findAll(sanitized).forEach { m ->
                    val f1 = parseHexOrDec(m.groupValues[1])
                    val term = if (m.groupValues[2].isNotEmpty()) {
                        f1 * parseHexOrDec(m.groupValues[2])
                    } else {
                        f1
                    }
                    sum += term
                }
                sum
            } catch (_: Exception) {
                null
            }
        }
    }

    private suspend fun fetchHeliosStreams(
        tmdbId: String,
        isTv: Boolean,
        season: Int?,
        episode: Int?
    ): List<StreamSource> = withContext(Dispatchers.IO) {
        val results = mutableListOf<StreamSource>()
        try {
            val heliosUrl = if (isTv && season != null && episode != null) {
                "https://stream.hls.lol/helios?tmdbId=$tmdbId&type=tv&seasonId=$season&episodeId=$episode"
            } else {
                "https://stream.hls.lol/helios?tmdbId=$tmdbId&type=movie"
            }

            val req = newRequestBuilder(heliosUrl).build()
            client.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return@withContext emptyList()
                val body = resp.body?.string() ?: return@withContext emptyList()
                val root = json.parseToJsonElement(body).jsonObject
                val sources = root["sources"]?.jsonObject ?: return@withContext emptyList()

                for ((serverName, serverVal) in sources) {
                    val sObj = serverVal.jsonObject
                    val encUrl = sObj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                    val decryptedUrl = decryptHeliosUrl(encUrl) ?: continue
                    if (!decryptedUrl.startsWith("http")) continue
                    if (decryptedUrl.contains("totallyacdn.org", ignoreCase = true)) continue

                    // 1. Parse individual direct quality variants from "q" query parameter
                    val qParam = decryptedUrl.substringAfter("?q=", "").substringBefore("&")
                    if (qParam.isNotBlank()) {
                        try {
                            val decodedBytes = try {
                                java.util.Base64.getUrlDecoder().decode(qParam)
                            } catch (_: Exception) {
                                val padded = qParam + "=".repeat((4 - qParam.length % 4) % 4)
                                java.util.Base64.getDecoder().decode(padded)
                            }
                            val decodedJsonStr = String(decodedBytes, Charsets.UTF_8)
                            val qRoot = json.parseToJsonElement(decodedJsonStr).jsonObject
                            val customHeadersObj = qRoot["h"]?.jsonObject
                            val streamHeaders = mutableMapOf<String, String>()
                            customHeadersObj?.forEach { (k, v) ->
                                v.jsonPrimitive.contentOrNull?.let { streamHeaders[k] = it }
                            }
                            val reqHeaders = mapOf(
                                "Referer" to (streamHeaders["referer"] ?: "https://www.movy.sx/"),
                                "Origin" to (streamHeaders["origin"] ?: "https://www.movy.sx"),
                                "User-Agent" to defaultHeaders["User-Agent"]!!
                            )

                            val variantsArr = qRoot["v"]?.jsonArray
                            if (variantsArr != null) {
                                for (vElem in variantsArr) {
                                    val vObj = vElem.jsonObject
                                    val vUrl = vObj["u"]?.jsonPrimitive?.contentOrNull ?: continue
                                    if (vUrl.contains("totallyacdn.org", ignoreCase = true)) continue
                                    val vQuality = vObj["q"]?.jsonPrimitive?.contentOrNull ?: "1080p"
                                    val qualityLabel = when {
                                        vQuality.contains("1080") -> "1080p FHD"
                                        vQuality.contains("720") -> "720p HD"
                                        vQuality.contains("480") -> "480p SD"
                                        else -> vQuality
                                    }

                                    val isLive = withTimeoutOrNull(3000L) {
                                        isStreamReachable(vUrl, reqHeaders)
                                    } ?: false

                                    if (isLive) {
                                        results.add(
                                            StreamSource(
                                                url = vUrl,
                                                serverName = "Helios ($qualityLabel)",
                                                resolutionLabel = qualityLabel,
                                                quality = "Atlantic Helios ($qualityLabel HLS)",
                                                isM3u8 = true,
                                                releaseType = AudioReleaseType.ORIGINAL,
                                                headers = reqHeaders
                                            )
                                        )
                                    }
                                }
                            }
                        } catch (_: Exception) {}
                    }

                    // 2. Also emit Master stream
                    val heliosMasterHeaders = mapOf(
                        "Referer" to "https://stream.hls.lol/",
                        "Origin" to "https://stream.hls.lol",
                        "User-Agent" to defaultHeaders["User-Agent"]!!
                    )
                    val isMasterLive = withTimeoutOrNull(3000L) {
                        isStreamReachable(decryptedUrl, heliosMasterHeaders)
                    } ?: false

                    if (isMasterLive) {
                        results.add(
                            StreamSource(
                                url = decryptedUrl,
                                serverName = "Helios ($serverName Auto)",
                                resolutionLabel = "Auto",
                                quality = "Atlantic Helios ($serverName Auto HLS)",
                                isM3u8 = true,
                                releaseType = AudioReleaseType.ORIGINAL,
                                headers = heliosMasterHeaders
                            )
                        )

                        // 3. Resolve master playlist variants if any
                        try {
                            val variants = resolveMasterPlaylistVariants(decryptedUrl, "Atlantic Helios $serverName", heliosMasterHeaders)
                            for (v in variants) {
                                if (!results.any { it.url == v.url }) {
                                    results.add(v)
                                }
                            }
                        } catch (_: Exception) {}
                    }
                }
            }
        } catch (_: Exception) {}
        results
    }

    private fun resolveMasterPlaylistVariants(
        masterUrl: String,
        serverPrefix: String,
        customHeaders: Map<String, String> = defaultHeaders
    ): List<StreamSource> {
        val variants = mutableListOf<StreamSource>()
        try {
            val req = Request.Builder().url(masterUrl)
            customHeaders.forEach { (k, v) -> req.header(k, v) }
            client.newCall(req.build()).execute().use { resp ->
                if (!resp.isSuccessful) return emptyList()
                val playlistText = resp.body?.string() ?: return emptyList()
                if (!playlistText.contains("#EXTM3U") || !playlistText.contains("#EXT-X-STREAM-INF")) {
                    return emptyList()
                }

                val baseUri = try { URI(masterUrl) } catch (_: Exception) { null }
                val lines = playlistText.lines()

                // Check for multi-audio tracks in manifest
                val audioTracks = mutableListOf<String>()
                for (line in lines) {
                    val trimmed = line.trim()
                    if (trimmed.startsWith("#EXT-X-MEDIA:") && trimmed.contains("TYPE=AUDIO")) {
                        val nameMatch = Regex("""NAME="([^"]+)"""").find(trimmed)?.groupValues?.get(1)
                        if (!nameMatch.isNullOrBlank()) {
                            audioTracks.add(nameMatch)
                        }
                    }
                }

                // Extract stream quality variants
                val seenRes = mutableSetOf<String>()
                for (i in lines.indices) {
                    val line = lines[i].trim()
                    if (line.startsWith("#EXT-X-STREAM-INF:")) {
                        val resMatch = Regex("""RESOLUTION=(\d+x\d+)""").find(line)?.groupValues?.get(1)
                        val width = resMatch?.substringBefore("x")?.toIntOrNull()
                        val height = resMatch?.substringAfter("x")?.toIntOrNull()

                        for (j in (i + 1) until lines.size) {
                            val subLine = lines[j].trim()
                            if (subLine.isNotEmpty() && !subLine.startsWith("#")) {
                                val resolvedUrl = baseUri?.resolve(subLine)?.toString() ?: subLine

                                val qLabel = when {
                                    (width != null && width >= 1900) || (height != null && height >= 1080) -> "1080p FHD"
                                    (width != null && width >= 1200) || (height != null && height >= 700) -> "720p HD"
                                    (width != null && width >= 600) || (height != null && height >= 450) -> "480p SD"
                                    height != null -> "${height}p"
                                    else -> "Direct"
                                }

                                if (seenRes.add(qLabel)) {
                                    val audioBadge = if (audioTracks.isNotEmpty()) " [Multi-Audio]" else ""
                                    val audioDescs = audioTracks.map {
                                        AudioTrackDescriptor(
                                            languageName = it,
                                            isoCode = it.take(2).lowercase()
                                        )
                                    }
                                    val releaseType = if (audioTracks.isNotEmpty()) AudioReleaseType.DUAL_AUDIO else AudioReleaseType.ORIGINAL
                                    variants.add(
                                        StreamSource(
                                            url = resolvedUrl,
                                            serverName = "$serverPrefix ($qLabel)",
                                            resolutionLabel = qLabel,
                                            quality = "$serverPrefix ($qLabel)$audioBadge",
                                            isM3u8 = true,
                                            releaseType = releaseType,
                                            audioTracks = audioDescs,
                                            headers = customHeaders
                                        )
                                    )
                                }
                                break
                            }
                        }
                    }
                }
            }
        } catch (_: Exception) {}
        return variants
    }

    private fun decryptHeliosUrl(encUrl: String): String? {
        return try {
            val rawHex = when {
                encUrl.startsWith("hl_") -> encUrl.removePrefix("hl_")
                encUrl.startsWith("ns_") -> encUrl.removePrefix("ns_")
                encUrl.startsWith("http") -> return encUrl
                else -> encUrl
            }
            val rawBytes = hexToBytes(rawHex)
            if (rawBytes.size < 28) return null

            val iv = rawBytes.copyOfRange(0, 12)
            val ctAndTag = rawBytes.copyOfRange(12, rawBytes.size)

            val key = hexToBytes(heliosKeyHex)
            val cipher = Cipher.getInstance("AES/GCM/NoPadding")
            val keySpec = SecretKeySpec(key, "AES")
            val gcmSpec = GCMParameterSpec(128, iv)
            cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec)

            val decryptedBytes = cipher.doFinal(ctAndTag)
            String(decryptedBytes, Charsets.UTF_8)
        } catch (e: Exception) {
            null
        }
    }

    private fun decryptMoscowUrl(encUrl: String): String? {
        return try {
            val rawHex = when {
                encUrl.startsWith("ms_") -> encUrl.removePrefix("ms_")
                encUrl.startsWith("http") -> return encUrl
                else -> encUrl
            }
            val rawBytes = hexToBytes(rawHex)
            if (rawBytes.size < 28) return null

            val iv = rawBytes.copyOfRange(0, 12)
            val ctAndTag = rawBytes.copyOfRange(12, rawBytes.size)

            val key = hexToBytes(moscowKeyHex)
            val cipher = Cipher.getInstance("AES/GCM/NoPadding")
            val keySpec = SecretKeySpec(key, "AES")
            val gcmSpec = GCMParameterSpec(128, iv)
            cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec)

            val decryptedBytes = cipher.doFinal(ctAndTag)
            String(decryptedBytes, Charsets.UTF_8)
        } catch (e: Exception) {
            null
        }
    }

    private fun resolveImdbId(tmdbId: String, isTv: Boolean): String? {
        return try {
            val typeStr = if (isTv) "tv" else "movie"
            val url = "https://api.themoviedb.org/3/$typeStr/$tmdbId/external_ids?api_key=$tmdbApiKey"
            val req = Request.Builder()
                .url(url)
                .header("User-Agent", "Mozilla/5.0")
                .build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    val body = resp.body?.string() ?: ""
                    val root = json.parseToJsonElement(body).jsonObject
                    root["imdb_id"]?.jsonPrimitive?.contentOrNull
                } else null
            }
        } catch (e: Exception) {
            null
        }
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
                stream.quality.contains("1080p", ignoreCase = true) -> "~2.4 GB"
                stream.quality.contains("720p", ignoreCase = true) -> "~1.2 GB"
                stream.quality.contains("480p", ignoreCase = true) -> "~650 MB"
                else -> "~1.8 GB"
            }
            DownloadOption(
                title = "${stream.quality} - Atlantic Fast Download",
                quality = stream.quality,
                size = sizeEstimate,
                url = stream.url,
                source = "Atlantic (${stream.quality})",
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

    override suspend fun fetchCast(mediaId: String, imdbId: String?, type: MediaType): List<CastMember> = emptyList()
}
