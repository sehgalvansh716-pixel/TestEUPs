package com.euthopiar.core.provider

import com.euthopiar.core.model.*
import com.euthopiar.core.network.DohDns
import com.euthopiar.core.util.TmdbBridge
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.serialization.json.*
import okhttp3.OkHttpClient
import okhttp3.Request
import java.net.URI
import java.net.URLEncoder
import java.util.concurrent.ConcurrentHashMap
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * Official Cinejoy Universal Provider Plugin (https://cinejoy.pk/).
 *
 * Re-architected from scratch following the proven Aether and 4KHDHub production model:
 * - Curated Cinejoy & TMDB trending catalog feeds with studio clear logos and 16:9 backdrops
 * - Universal search across movies and TV series
 * - Seamless season & episode breakdown with canonical titles and stills
 * - Multi-Server Progressive Live Streaming Engine:
 *     1. Nebula: Native Cinejoy Wasm gateway 1080p FHD & 720p HD master HLS streams
 *     2. Lisbon: Native Cinejoy edge mirror with real-time pre-flight verification
 *     3. Helios: Ultra high-speed multi-quality engine (1080p FHD, 720p HD, 480p SD, Auto) via AES-GCM
 *     4. Aphrodite: High-speed edge CDN with Atlantic origin playback
 *     5. Meridian & Link: High-speed direct HLS backup mirrors
 * - Multilingual Subtitle Engine:
 *     - Wing Subtitles (subs.wing.st)
 *     - Granite API (sub.vdrk.site) 60+ languages
 *     - Natsuki API (natsuki.hls.lol) 250+ languages
 * - High-Speed Direct Downloads for all resolution tiers (1080p, 720p, 480p)
 */
class CinejoyPlugin(
    private val client: OkHttpClient = DohDns.createOkHttpClient(
        connectTimeoutSeconds = 15,
        readTimeoutSeconds = 25
    )
) : UniversalPlugin {

    override val name: String = "Cinejoy"
    override val mainUrl: String = "https://cinejoy.pk"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE, MediaType.TV_SERIES)

    @Volatile
    protected var host: HostApi? = null

    override fun init(host: HostApi) {
        this.host = host
    }

    private val json = Json { ignoreUnknownKeys = true; isLenient = true }

    private val tmdbApiKey = "1865f43a0549ca50d341dd9ab8b29f49"

    private val defaultHeaders: Map<String, String>
        get() = mapOf(
            "Referer" to "https://cinejoy.pk/",
            "Origin" to "https://cinejoy.pk",
            "User-Agent" to (host?.defaultUserAgent ?: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
            "Accept-Ranges" to "bytes"
        )

    private val heliosKeyHex = "117c358bcfcaf8fe2cfca57c9d2238a300e1c4de2efb83a5012ba84d8a31f1dd"

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

    private fun newRequestBuilder(url: String, customHeaders: Map<String, String>? = null): Request.Builder {
        val builder = Request.Builder().url(url)
        defaultHeaders.forEach { (k, v) -> builder.header(k, v) }
        customHeaders?.forEach { (k, v) -> builder.header(k, v) }
        return builder
    }

    init {
        CoroutineScope(Dispatchers.IO).launch {
            try {
                CinejoyWasmEngine.prewarm(client)
            } catch (_: Throwable) {}
        }
    }

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        coroutineScope {
            val categories = listOf(
                "Trending On Cinejoy" to "https://api.themoviedb.org/3/trending/all/week?api_key=$tmdbApiKey",
                "Now Playing in Theatres" to "https://api.themoviedb.org/3/movie/now_playing?api_key=$tmdbApiKey",
                "Popular Movies" to "https://api.themoviedb.org/3/movie/popular?api_key=$tmdbApiKey",
                "Top Rated TV Shows" to "https://api.themoviedb.org/3/tv/top_rated?api_key=$tmdbApiKey",
                "Cinejoy Featured Series" to "https://api.themoviedb.org/3/tv/popular?api_key=$tmdbApiKey",
                "Critically Acclaimed Cinema" to "https://api.themoviedb.org/3/movie/top_rated?api_key=$tmdbApiKey",
                "Action & Sci-Fi Blockbusters" to "https://api.themoviedb.org/3/discover/movie?api_key=$tmdbApiKey&with_genres=28,878&sort_by=popularity.desc",
                "Crime & Mystery Hits" to "https://api.themoviedb.org/3/discover/tv?api_key=$tmdbApiKey&with_genres=80,9648&sort_by=popularity.desc",
                "Animation & Family Hits" to "https://api.themoviedb.org/3/discover/tv?api_key=$tmdbApiKey&with_genres=16&sort_by=popularity.desc",
                "Comedy Specials & Hits" to "https://api.themoviedb.org/3/discover/movie?api_key=$tmdbApiKey&with_genres=35&sort_by=popularity.desc"
            )

            val deferredRows = categories.map { (title, url) ->
                async { fetchCatalogRow(title, url) }
            }
            val rows = deferredRows.awaitAll().filterNotNull()

            // Concurrently enrich clear logos for hero/top row items
            if (rows.isNotEmpty()) {
                val firstRow = rows.first()
                val candidateItemsWithLogos = firstRow.items.take(10).map { item ->
                    async {
                        val logo = item.logoUrl ?: resolveLogo(item)
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
            val req = Request.Builder().url(url).build()
            val resp = client.newCall(req).execute()
            if (!resp.isSuccessful) return null
            val body = resp.body?.string() ?: return null
            val root = json.parseToJsonElement(body).jsonObject
            val results = root["results"]?.jsonArray ?: return null

            val items = mutableListOf<MediaItem>()
            for (elem in results) {
                val obj = elem.jsonObject
                val id = obj["id"]?.jsonPrimitive?.contentOrNull ?: continue
                val isTv = obj.containsKey("first_air_date") || obj.containsKey("name")
                val itemTitle = obj["title"]?.jsonPrimitive?.contentOrNull
                    ?: obj["name"]?.jsonPrimitive?.contentOrNull
                    ?: continue

                val posterPath = obj["poster_path"]?.jsonPrimitive?.contentOrNull
                val poster = if (posterPath != null) "https://image.tmdb.org/t/p/w500$posterPath" else null

                val backdropPath = obj["backdrop_path"]?.jsonPrimitive?.contentOrNull
                val backdrop = if (backdropPath != null) "https://image.tmdb.org/t/p/w1280$backdropPath" else poster

                val dateStr = obj["release_date"]?.jsonPrimitive?.contentOrNull
                    ?: obj["first_air_date"]?.jsonPrimitive?.contentOrNull
                val year = dateStr?.take(4)?.toIntOrNull()

                val voteAvg = obj["vote_average"]?.jsonPrimitive?.doubleOrNull
                val rating = if (voteAvg != null && voteAvg > 0.0) String.format("%.1f", voteAvg) else null

                items.add(
                    MediaItem(
                        id = id,
                        title = itemTitle,
                        url = "https://cinejoy.pk/${if (isTv) "tv" else "movie"}/$id",
                        posterUrl = poster,
                        backdropUrl = backdrop,
                        type = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE,
                        year = year,
                        rating = rating,
                        provider = name
                    )
                )
            }
            return if (items.isNotEmpty()) CatalogRow(title, items) else null
        } catch (_: Exception) {
            return null
        }
    }

    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        if (query.isBlank()) return@withContext emptyList()
        val results = mutableListOf<MediaItem>()
        try {
            val encodedQuery = URLEncoder.encode(query, "UTF-8")
            val url = "https://api.themoviedb.org/3/search/multi?api_key=$tmdbApiKey&query=$encodedQuery"
            val req = Request.Builder().url(url).build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    val body = resp.body?.string() ?: ""
                    val root = json.parseToJsonElement(body).jsonObject
                    val arr = root["results"]?.jsonArray ?: return@use
                    for (elem in arr) {
                        val obj = elem.jsonObject
                        val mediaTypeStr = obj["media_type"]?.jsonPrimitive?.contentOrNull
                        val isMovie = mediaTypeStr == "movie"
                        val isTv = mediaTypeStr == "tv"
                        if (!isMovie && !isTv) continue

                        val id = obj["id"]?.jsonPrimitive?.contentOrNull ?: continue
                        val itemTitle = obj["title"]?.jsonPrimitive?.contentOrNull
                            ?: obj["name"]?.jsonPrimitive?.contentOrNull
                            ?: continue

                        val posterPath = obj["poster_path"]?.jsonPrimitive?.contentOrNull
                        val poster = if (posterPath != null) "https://image.tmdb.org/t/p/w500$posterPath" else null

                        val backdropPath = obj["backdrop_path"]?.jsonPrimitive?.contentOrNull
                        val backdrop = if (backdropPath != null) "https://image.tmdb.org/t/p/w1280$backdropPath" else poster

                        val dateStr = obj["release_date"]?.jsonPrimitive?.contentOrNull
                            ?: obj["first_air_date"]?.jsonPrimitive?.contentOrNull
                        val year = dateStr?.take(4)?.toIntOrNull()

                        val voteAvg = obj["vote_average"]?.jsonPrimitive?.doubleOrNull
                        val rating = if (voteAvg != null && voteAvg > 0.0) String.format("%.1f", voteAvg) else null

                        results.add(
                            MediaItem(
                                id = id,
                                title = itemTitle,
                                url = "https://cinejoy.pk/${if (isTv) "tv" else "movie"}/$id",
                                posterUrl = poster,
                                backdropUrl = backdrop,
                                type = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE,
                                year = year,
                                rating = rating,
                                provider = name
                            )
                        )
                    }
                }
            }
        } catch (_: Exception) {}
        results
    }

    override suspend fun getDetails(mediaItem: MediaItem): MediaDetail = withContext(Dispatchers.IO) {
        val isTv = mediaItem.type == MediaType.TV_SERIES
        val rawId = mediaItem.id.ifBlank { mediaItem.url }
        val tmdbId = Regex("""\b(\d+)\b""").find(rawId)?.value ?: rawId

        val enriched = TmdbBridge.fetchEnrichedDetails(client, tmdbId, isTv, mediaItem.title, tmdbApiKey)

        val title = enriched?.title?.ifBlank { mediaItem.title } ?: mediaItem.title
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
            url = "https://cinejoy.pk/$typeStr/$tmdbId",
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

    override fun getStreamFlow(episodeData: String): Flow<StreamEmission> = channelFlow {
        val tmdbId: String
        val season: Int?
        val episode: Int?
        val isTv: Boolean

        if (episodeData.contains(":")) {
            val parts = episodeData.split(":")
            tmdbId = parts[0]
            season = parts.getOrNull(1)?.toIntOrNull()
            episode = parts.getOrNull(2)?.toIntOrNull()
            isTv = true
        } else {
            tmdbId = episodeData
            season = null
            episode = null
            isTv = false
        }

        send(StreamEmission.StatusUpdate(name, "Searching Cinejoy multi-server streams and CDNs ($tmdbId)..."))

        val emittedStreamKeys = ConcurrentHashMap.newKeySet<String>()
        val emittedSubUrls = ConcurrentHashMap.newKeySet<String>()
        val emittedSubLangs = ConcurrentHashMap.newKeySet<String>()

        // 0. Subtitles Job (Wing Subtitles + Granite API + Natsuki API)
        launch {
            // A. Native Wing Subtitles
            try {
                val wingSubUrl = "https://subs.wing.st/subtitles?type=${if (isTv) "tv" else "movie"}&tmdb=$tmdbId"
                val req = newRequestBuilder(wingSubUrl).build()
                client.newCall(req).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val body = resp.body?.string() ?: ""
                        val root = json.parseToJsonElement(body).jsonObject
                        val subArr = root["subtitles"]?.jsonArray
                        subArr?.forEach { sElem ->
                            val sObj = sElem.jsonObject
                            val subUrl = sObj["url"]?.jsonPrimitive?.contentOrNull ?: return@forEach
                            val lang = sObj["language"]?.jsonPrimitive?.contentOrNull ?: "English"
                            val cleanLang = lang.trim()
                            val langKey = cleanLang.lowercase()
                            if (emittedSubLangs.add(langKey) && emittedSubUrls.add(subUrl)) {
                                send(StreamEmission.SubtitleFound(SubtitleTrack(url = subUrl, language = cleanLang)))
                            }
                        }
                    }
                }
            } catch (_: Exception) {}

            // B. Granite API (vdrk.site)
            try {
                val graniteUrl = if (isTv && season != null && episode != null) {
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
                            if (trackNum != null && (trackNum.toIntOrNull() ?: 1) > 1) continue
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

            // C. Natsuki Subtitles API
            val imdb = resolveImdbId(tmdbId, isTv)
            if (imdb != null && imdb.startsWith("tt")) {
                try {
                    val natsukiUrl = if (isTv && season != null && episode != null) {
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
                } catch (_: Exception) {}
            }
        }

        // 1. Cinejoy Native Nebula Engine (1080p FHD Direct Master & Adaptive Variants via Wasm)
        launch {
            try {
                val nebulaJson = withTimeoutOrNull(10000L) {
                    CinejoyWasmEngine.requestStream(
                        client = client,
                        server = "Nebula",
                        type = if (isTv) "tv" else "movie",
                        tmdbId = tmdbId,
                        season = season,
                        episode = episode
                    )
                }
                if (nebulaJson != null) {
                    val root = json.parseToJsonElement(nebulaJson).jsonObject
                    val streamArr = root["data"]?.jsonObject?.get("stream")?.jsonArray
                    if (streamArr != null) {
                        for (streamElem in streamArr) {
                            val sObj = streamElem.jsonObject
                            val playlistUrl = sObj["playlist"]?.jsonPrimitive?.contentOrNull ?: continue
                            if (!playlistUrl.startsWith("http")) continue

                            // A. Emit Master source
                            val nebulaMaster = StreamSource(
                                url = playlistUrl,
                                serverName = "Nebula (Auto)",
                                resolutionLabel = "Auto",
                                quality = "Cinejoy Nebula (Auto HLS)",
                                isM3u8 = true,
                                releaseType = AudioReleaseType.ORIGINAL,
                                headers = defaultHeaders
                            )
                            val masterKey = "${nebulaMaster.serverName}:${nebulaMaster.url}"
                            if (emittedStreamKeys.add(masterKey)) {
                                send(StreamEmission.SourceFound(nebulaMaster))
                            }

                            // B. Resolve child variants (1080p FHD, 720p HD)
                            val variants = resolveMasterPlaylistVariants(playlistUrl, "Nebula", defaultHeaders)
                            for (v in variants) {
                                val vKey = "${v.serverName}:${v.url}"
                                if (emittedStreamKeys.add(vKey)) {
                                    send(StreamEmission.SourceFound(v))
                                }
                            }
                        }
                    }
                }
            } catch (_: Throwable) {}
        }

        // 2. Cinejoy Native Lisbon Engine (With live pre-flight verification to prevent 502 stalls)
        launch {
            try {
                val lisbonJson = withTimeoutOrNull(10000L) {
                    CinejoyWasmEngine.requestStream(
                        client = client,
                        server = "Lisbon",
                        type = if (isTv) "tv" else "movie",
                        tmdbId = tmdbId,
                        season = season,
                        episode = episode
                    )
                }
                if (lisbonJson != null) {
                    val root = json.parseToJsonElement(lisbonJson).jsonObject
                    val streamArr = root["data"]?.jsonObject?.get("stream")?.jsonArray
                    if (streamArr != null) {
                        for (streamElem in streamArr) {
                            val sObj = streamElem.jsonObject
                            val rawUrl = sObj["playlist"]?.jsonPrimitive?.contentOrNull ?: continue
                            if (!rawUrl.startsWith("http")) continue

                            // Perform non-blocking liveness check
                            val isLive = withTimeoutOrNull(3000L) {
                                isStreamReachable(rawUrl, defaultHeaders)
                            } ?: false

                            if (isLive) {
                                val lisbonSource = StreamSource(
                                    url = rawUrl,
                                    serverName = "Lisbon (1080p)",
                                    resolutionLabel = "1080p FHD",
                                    quality = "Cinejoy Lisbon (1080p FHD HLS)",
                                    isM3u8 = true,
                                    releaseType = AudioReleaseType.ORIGINAL,
                                    headers = defaultHeaders
                                )
                                val lisbonKey = "${lisbonSource.serverName}:${lisbonSource.url}"
                                if (emittedStreamKeys.add(lisbonKey)) {
                                    send(StreamEmission.SourceFound(lisbonSource))
                                }
                            }
                        }
                    }
                }
            } catch (_: Throwable) {}
        }

        // 3. Cinejoy Helios Multi-Quality High-Throughput Engine (1080p FHD, 720p HD, 480p SD, Auto)
        launch {
            try {
                val heliosUrl = if (isTv && season != null && episode != null) {
                    "https://stream.hls.lol/helios?tmdbId=$tmdbId&type=tv&seasonId=$season&episodeId=$episode"
                } else {
                    "https://stream.hls.lol/helios?tmdbId=$tmdbId&type=movie"
                }

                val req = Request.Builder()
                    .url(heliosUrl)
                    .header("Referer", "https://cinejoy.pk/")
                    .header("Origin", "https://cinejoy.pk")
                    .header("User-Agent", defaultHeaders["User-Agent"]!!)
                    .build()

                client.newCall(req).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val body = resp.body?.string() ?: ""
                        val root = json.parseToJsonElement(body).jsonObject
                        val sources = root["sources"]?.jsonObject
                        if (sources != null) {
                            for ((serverName, serverVal) in sources) {
                                val sObj = serverVal.jsonObject
                                val encUrl = sObj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                                val decryptedUrl = decryptHeliosUrl(encUrl) ?: continue
                                if (!decryptedUrl.startsWith("http")) continue

                                // Parse individual direct quality variants from "q" query parameter
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
                                                val vQuality = vObj["q"]?.jsonPrimitive?.contentOrNull ?: "1080p"
                                                val qualityLabel = when {
                                                    vQuality.contains("1080") -> "1080p FHD"
                                                    vQuality.contains("720") -> "720p HD"
                                                    vQuality.contains("480") -> "480p SD"
                                                    else -> vQuality
                                                }
                                                val src = StreamSource(
                                                    url = vUrl,
                                                    serverName = "Helios ($qualityLabel)",
                                                    resolutionLabel = qualityLabel,
                                                    quality = "Cinejoy Helios ($qualityLabel HLS)",
                                                    isM3u8 = true,
                                                    releaseType = AudioReleaseType.ORIGINAL,
                                                    headers = reqHeaders
                                                )
                                                val streamKey = "${src.serverName}:${src.resolutionLabel}:${src.url}"
                                                if (emittedStreamKeys.add(streamKey)) send(StreamEmission.SourceFound(src))
                                            }
                                        }
                                    } catch (_: Exception) {}
                                }

                                // Also emit Helios Master playlist
                                val heliosMasterHeaders = mapOf(
                                    "Referer" to "https://stream.hls.lol/",
                                    "Origin" to "https://stream.hls.lol",
                                    "User-Agent" to defaultHeaders["User-Agent"]!!
                                )
                                val masterSrc = StreamSource(
                                    url = decryptedUrl,
                                    serverName = "Helios ($serverName Auto)",
                                    resolutionLabel = "Auto",
                                    quality = "Cinejoy Helios ($serverName Auto HLS)",
                                    isM3u8 = true,
                                    releaseType = AudioReleaseType.ORIGINAL,
                                    headers = heliosMasterHeaders
                                )
                                val masterKey = "${masterSrc.serverName}:${masterSrc.resolutionLabel}:${masterSrc.url}"
                                if (emittedStreamKeys.add(masterKey)) send(StreamEmission.SourceFound(masterSrc))
                            }
                        }
                    }
                }
            } catch (_: Throwable) {}
        }

        // 4. Cinejoy Aphrodite CDN Engine (High-speed edge CDN with Atlantic origin playback)
        launch {
            try {
                val aphPath = if (isTv && season != null && episode != null) {
                    "/content/tv/$tmdbId/$season/$episode"
                } else {
                    "/content/movie/$tmdbId"
                }

                val req = Request.Builder()
                    .url("https://cdn.hls.lol$aphPath")
                    .header("Referer", "https://atlantic.st/")
                    .header("Origin", "https://atlantic.st")
                    .header("User-Agent", defaultHeaders["User-Agent"]!!)
                    .header("Accept", "application/json, text/plain, */*")
                    .build()

                client.newCall(req).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val body = resp.body?.string()
                        if (!body.isNullOrBlank()) {
                            val root = json.parseToJsonElement(body).jsonObject
                            val streamUrl = root["url"]?.jsonPrimitive?.contentOrNull
                                ?: root["hls"]?.jsonPrimitive?.contentOrNull
                            if (streamUrl != null && streamUrl.startsWith("http")) {
                                val aphHeaders = mapOf(
                                    "Referer" to "https://atlantic.st/",
                                    "Origin" to "https://atlantic.st",
                                    "User-Agent" to defaultHeaders["User-Agent"]!!
                                )
                                val src = StreamSource(
                                    url = streamUrl,
                                    serverName = "Aphrodite (Direct 1080p)",
                                    resolutionLabel = "1080p FHD",
                                    quality = "Cinejoy Aphrodite CDN (1080p FHD HLS)",
                                    isM3u8 = true,
                                    releaseType = AudioReleaseType.ORIGINAL,
                                    headers = aphHeaders
                                )
                                val aphKey = "${src.serverName}:${src.resolutionLabel}:${src.url}"
                                if (emittedStreamKeys.add(aphKey)) {
                                    send(StreamEmission.SourceFound(src))
                                }
                            }
                        }
                    }
                }
            } catch (_: Throwable) {}
        }

        // 5. Cinejoy Meridian & Link Engine (High-speed 1080p HLS mirrors)
        launch {
            try {
                val meridianUrl = if (isTv && season != null && episode != null) {
                    "https://meridian.aether.cx/show/$tmdbId/$season/$episode"
                } else {
                    "https://meridian.aether.cx/movie/$tmdbId"
                }
                val req = Request.Builder().url(meridianUrl).header("User-Agent", defaultHeaders["User-Agent"]!!).build()
                client.newCall(req).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val body = resp.body?.string() ?: ""
                        val root = json.parseToJsonElement(body).jsonObject
                        val streamUrl = root["url"]?.jsonPrimitive?.contentOrNull
                        if (!streamUrl.isNullOrBlank() && streamUrl.startsWith("http")) {
                            val streamHost = try { URI(streamUrl).host } catch (_: Exception) { null } ?: "aether.cx"
                            val playbackHeaders = mapOf(
                                "Referer" to "https://$streamHost/",
                                "Origin" to "https://$streamHost",
                                "User-Agent" to defaultHeaders["User-Agent"]!!,
                                "Accept-Ranges" to "bytes"
                            )
                            val src = StreamSource(
                                url = streamUrl,
                                serverName = "Meridian (1080p)",
                                resolutionLabel = "1080p FHD",
                                quality = "Cinejoy Meridian (1080p FHD HLS)",
                                isM3u8 = true,
                                releaseType = AudioReleaseType.ORIGINAL,
                                headers = playbackHeaders
                            )
                            val meridianKey = "${src.serverName}:${src.resolutionLabel}:${src.url}"
                            if (emittedStreamKeys.add(meridianKey)) send(StreamEmission.SourceFound(src))
                        }
                    }
                }
            } catch (_: Throwable) {}
        }
    }

    override suspend fun getStreamLinks(episodeData: String): StreamResult = withContext(Dispatchers.IO) {
        val streamSources = mutableListOf<StreamSource>()
        val subtitleTracks = mutableListOf<SubtitleTrack>()
        getStreamFlow(episodeData).collect { emission ->
            when (emission) {
                is StreamEmission.SourceFound -> streamSources.add(emission.source)
                is StreamEmission.SubtitleFound -> if (subtitleTracks.none { it.url == emission.track.url }) subtitleTracks.add(emission.track)
                is StreamEmission.StatusUpdate -> {}
            }
        }
        val sortedSubs = subtitleTracks.sortedWith(
            compareByDescending<SubtitleTrack> { it.language.contains("english", ignoreCase = true) }
                .thenBy { it.language }
        ).distinctBy {
            val isCc = it.language.contains("[CC]", ignoreCase = true)
            val base = it.language.replace(Regex("""\s*\[CC\]""", RegexOption.IGNORE_CASE), "").trim().lowercase()
            if (isCc) "$base [cc]" else base
        }
        val distinctStreams = streamSources.distinctBy { "${it.serverName}:${it.resolutionLabel}:${it.url}" }
        StreamResult(streams = distinctStreams, subtitles = sortedSubs)
    }

    override suspend fun getDownloadLinks(episodeData: String): List<DownloadOption> = withContext(Dispatchers.IO) {
        val tmdbId: String
        val season: Int?
        val episode: Int?
        val isTv: Boolean

        if (episodeData.contains(":")) {
            val parts = episodeData.split(":")
            tmdbId = parts[0]
            season = parts.getOrNull(1)?.toIntOrNull()
            episode = parts.getOrNull(2)?.toIntOrNull()
            isTv = true
        } else {
            tmdbId = episodeData
            season = null
            episode = null
            isTv = false
        }

        val options = mutableListOf<DownloadOption>()

        // 1. Helios Direct Quality Download Mirrors (1080p, 720p, 480p)
        try {
            val heliosUrl = if (isTv && season != null && episode != null) {
                "https://stream.hls.lol/helios?tmdbId=$tmdbId&type=tv&seasonId=$season&episodeId=$episode"
            } else {
                "https://stream.hls.lol/helios?tmdbId=$tmdbId&type=movie"
            }
            val req = Request.Builder().url(heliosUrl).header("User-Agent", defaultHeaders["User-Agent"]!!).build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    val body = resp.body?.string() ?: ""
                    val root = json.parseToJsonElement(body).jsonObject
                    val sources = root["sources"]?.jsonObject
                    if (sources != null) {
                        val masterVal = sources["Moscow"]?.jsonObject ?: sources.values.firstOrNull()?.jsonObject
                        val encUrl = masterVal?.get("url")?.jsonPrimitive?.contentOrNull
                        if (encUrl != null) {
                            val dec = decryptHeliosUrl(encUrl)
                            if (dec != null && dec.startsWith("http")) {
                                val qParam = dec.substringAfter("?q=", "").substringBefore("&")
                                if (qParam.isNotBlank()) {
                                    val decodedBytes = try {
                                        java.util.Base64.getUrlDecoder().decode(qParam)
                                    } catch (_: Exception) {
                                        val padded = qParam + "=".repeat((4 - qParam.length % 4) % 4)
                                        java.util.Base64.getDecoder().decode(padded)
                                    }
                                    val qRoot = json.parseToJsonElement(String(decodedBytes, Charsets.UTF_8)).jsonObject
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
                                            val vQuality = vObj["q"]?.jsonPrimitive?.contentOrNull ?: "1080p"
                                            val (qualityLabel, estimatedSize) = when {
                                                vQuality.contains("1080") -> "1080p FHD" to "2.4 GB"
                                                vQuality.contains("720") -> "720p HD" to "1.2 GB"
                                                vQuality.contains("480") -> "480p SD" to "650 MB"
                                                else -> vQuality to "1.0 GB"
                                            }
                                            options.add(
                                                DownloadOption(
                                                    title = "Cinejoy Direct ($qualityLabel)",
                                                    quality = qualityLabel,
                                                    size = estimatedSize,
                                                    url = vUrl,
                                                    source = "Cinejoy Helios CDN",
                                                    provider = name,
                                                    headers = reqHeaders
                                                )
                                            )
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (_: Exception) {}

        options.distinctBy { it.url }
    }

    override suspend fun resolveLogo(mediaItem: MediaItem): String? = withContext(Dispatchers.IO) {
        val tmdbId = Regex("""\b(\d+)\b""").find(mediaItem.id)?.value ?: mediaItem.id
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

    override suspend fun fetchCast(mediaId: String, imdbId: String?, type: MediaType): List<CastMember> = withContext(Dispatchers.IO) {
        val cleanTmdbId = Regex("""\b(\d+)\b""").find(mediaId)?.value ?: mediaId
        if (cleanTmdbId.isBlank()) return@withContext emptyList()
        val isTv = type == MediaType.TV_SERIES
        val enriched = TmdbBridge.fetchEnrichedDetails(client, cleanTmdbId, isTv, name, tmdbApiKey)
        enriched?.cast ?: emptyList()
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
                if (!playlistText.startsWith("#EXTM3U")) return emptyList()

                val baseUri = try { URI(masterUrl) } catch (_: Exception) { null }
                val lines = playlistText.lines()
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
                                    variants.add(
                                        StreamSource(
                                            url = resolvedUrl,
                                            serverName = "$serverPrefix ($qLabel)",
                                            resolutionLabel = qLabel,
                                            quality = "Cinejoy $serverPrefix ($qLabel)",
                                            isM3u8 = true,
                                            releaseType = AudioReleaseType.ORIGINAL,
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

    private fun isStreamReachable(url: String, headers: Map<String, String>): Boolean {
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

    private fun resolveImdbId(tmdbId: String, isTv: Boolean): String? {
        return try {
            val typeStr = if (isTv) "tv" else "movie"
            val url = "https://api.themoviedb.org/3/$typeStr/$tmdbId/external_ids?api_key=$tmdbApiKey"
            val req = Request.Builder().url(url).build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    val body = resp.body?.string() ?: ""
                    json.parseToJsonElement(body).jsonObject["imdb_id"]?.jsonPrimitive?.contentOrNull
                } else null
            }
        } catch (_: Exception) {
            null
        }
    }
}
