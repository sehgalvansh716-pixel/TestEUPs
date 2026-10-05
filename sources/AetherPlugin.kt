package com.euthopiar.core.provider

import com.euthopiar.core.model.*
import com.euthopiar.core.network.DohDns
import com.euthopiar.core.util.TmdbBridge
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
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
 * Official Native Aether Streaming Provider Plugin (https://aether.ist/).
 *
 * Fully integrated with TMDB Metadata & Clear Logos, AniSkip, and OpenSubtitles bridges:
 * - Curated Aether / TMDB catalog feeds and global multi-search
 * - High-resolution 16:9 backdrops, 2:3 posters, and transparent PNG logos
 * - Full cast with character roles and headshot pictures, directors, and recommendations
 * - TV season and episode breakdown with 16:9 stills, canonical titles, synopses, and durations
 * - Multi-Server Live Streaming Engine:
 *     1. Helios: Direct 1080p FHD, 720p HD, and 480p SD streams extracted from AES-GCM payload with authentic headers
 *     2. Moscow: Native high-speed proxy cluster (stream.hls.lol/moscow)
 *     3. Meridian: High-speed 1080p HLS stream (meridian.aether.cx) with Neuronix CDN playback
 *     4. Lula: Multi-audio HLS master playlist (lul.aether.cx) with Cloudflare Worker edge playback
 *     5. Link: Multi-quality HLS stream (link.aether.cx)
 *     6. Aphrodite: High-speed edge CDN (cdn.hls.lol) with Atlantic origin playback
 * - Multilingual Subtitle Engine:
 *     - Granite API (sub.vdrk.site) 60+ languages
 *     - Natsuki API (natsuki.hls.lol) 250+ languages
 *     - Embedded & manifest subtitles
 * - High-Speed Direct Downloads with exact authenticated headers for all quality tiers (1080p, 720p, 480p)
 */
class AetherPlugin(
    private val client: OkHttpClient = DohDns.createOkHttpClient(
        connectTimeoutSeconds = 15,
        readTimeoutSeconds = 25
    )
) : UniversalPlugin {

    override val name: String = "Aether"
    override val mainUrl: String = "https://aether.ist"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE, MediaType.TV_SERIES)

    private val json = Json { ignoreUnknownKeys = true; isLenient = true }

    private val tmdbApiKey = "1865f43a0549ca50d341dd9ab8b29f49"

    private val defaultHeaders = mapOf(
        "Referer" to "https://aether.ist/",
        "Origin" to "https://aether.ist",
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Ranges" to "bytes"
    )

    private val heliosKeyHex = "117c358bcfcaf8fe2cfca57c9d2238a300e1c4de2efb83a5012ba84d8a31f1dd"
    private val moscowKeyHex = "55060a042823f51a94c296894a58a0db15f3baef807155140983083fa799ef47"

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

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        coroutineScope {
            val categories = listOf(
                "Trending On Aether" to "https://api.themoviedb.org/3/trending/all/week?api_key=$tmdbApiKey",
                "Now Playing in Theatres" to "https://api.themoviedb.org/3/movie/now_playing?api_key=$tmdbApiKey",
                "Popular Movies" to "https://api.themoviedb.org/3/movie/popular?api_key=$tmdbApiKey",
                "Top Rated TV Shows" to "https://api.themoviedb.org/3/tv/top_rated?api_key=$tmdbApiKey",
                "Aether Featured Series" to "https://api.themoviedb.org/3/tv/popular?api_key=$tmdbApiKey",
                "Critically Acclaimed Films" to "https://api.themoviedb.org/3/movie/top_rated?api_key=$tmdbApiKey",
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
        for (attempt in 1..2) {
            try {
                val req = Request.Builder()
                    .url(url)
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                    .build()
                client.newCall(req).execute().use { resp ->
                    if (!resp.isSuccessful) return@use null

                    val body = resp.body?.string() ?: return@use null
                    val root = json.parseToJsonElement(body).jsonObject
                    val resultsArr = root["results"]?.jsonArray ?: return@use null

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
                                url = "https://aether.ist/${if (isTv) "tv" else "movie"}/$id",
                                posterUrl = posterUrl,
                                backdropUrl = backdropUrl,
                                type = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE,
                                year = year,
                                rating = vote?.let { String.format(java.util.Locale.US, "%.1f", it) },
                                quality = "1080p",
                                provider = name
                            )
                        )
                    }

                    if (items.isNotEmpty()) return CatalogRow(title = title, items = items)
                }
            } catch (_: Exception) {}
        }
        return null
    }

    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        val encodedQuery = URLEncoder.encode(query, "UTF-8")
        val results = mutableListOf<MediaItem>()
        val seenIds = mutableSetOf<String>()

        val searchUrls = listOf(
            "https://api.themoviedb.org/3/search/multi?api_key=$tmdbApiKey&query=$encodedQuery",
            "https://api.themoviedb.org/3/search/movie?api_key=$tmdbApiKey&query=$encodedQuery"
        )

        for (url in searchUrls) {
            if (results.isNotEmpty()) break
            for (attempt in 1..2) {
                try {
                    val req = Request.Builder()
                        .url(url)
                        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                        .build()
                    client.newCall(req).execute().use { resp ->
                        if (resp.isSuccessful) {
                            val body = resp.body?.string() ?: ""
                            val root = json.parseToJsonElement(body).jsonObject
                            val resultsArr = root["results"]?.jsonArray
                            if (resultsArr != null) {
                                for (elem in resultsArr) {
                                    val obj = elem.jsonObject
                                    val mediaTypeStr = obj["media_type"]?.jsonPrimitive?.contentOrNull ?: if (url.contains("/movie?")) "movie" else "tv"
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
                                            url = "https://aether.ist/$mediaTypeStr/$id",
                                            posterUrl = posterUrl,
                                            backdropUrl = backdropUrl,
                                            type = type,
                                            year = year,
                                            rating = vote?.let { String.format(java.util.Locale.US, "%.1f", it) },
                                            quality = "1080p",
                                            provider = name
                                        )
                                    )
                                }
                            }
                        }
                    }
                    if (results.isNotEmpty()) break
                } catch (_: Exception) {}
            }
        }

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
            url = "https://aether.ist/$typeStr/$tmdbId",
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

        send(StreamEmission.StatusUpdate("Aether", "Querying Aether CDN, mirrors & third-party host extractors..."))

        val emittedStreamKeys = ConcurrentHashMap.newKeySet<String>()
        val emittedSubUrls = ConcurrentHashMap.newKeySet<String>()
        val emittedSubLangs = ConcurrentHashMap.newKeySet<String>()

        // 0. Subtitle Job (Granite & Natsuki)
        launch {
            // Granite API (vdrk.site)
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

            // Natsuki Subtitles
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

        // 1. Helios Multi-Quality Engine (Fastest, High Throughput, 1080p / 720p / 480p)
        launch {
            try {
                val heliosUrl = if (isTv && season != null && episode != null) {
                    "https://stream.hls.lol/helios?tmdbId=$tmdbId&type=tv&seasonId=$season&episodeId=$episode"
                } else {
                    "https://stream.hls.lol/helios?tmdbId=$tmdbId&type=movie"
                }

                val req = newRequestBuilder(heliosUrl).build()
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
                                                    val src = StreamSource(
                                                        url = vUrl,
                                                        serverName = "Helios ($qualityLabel)",
                                                        resolutionLabel = qualityLabel,
                                                        quality = "Aether Helios ($qualityLabel HLS)",
                                                        isM3u8 = true,
                                                        releaseType = AudioReleaseType.ORIGINAL,
                                                        headers = reqHeaders
                                                    )
                                                    val streamKey = "${src.serverName}:${src.resolutionLabel}:${src.url}"
                                                    if (emittedStreamKeys.add(streamKey)) send(StreamEmission.SourceFound(src))
                                                }
                                            }
                                        }
                                    } catch (_: Exception) {}
                                }

                                // 2. Also emit Master playlist
                                val heliosMasterHeaders = mapOf(
                                    "Referer" to "https://stream.hls.lol/",
                                    "Origin" to "https://stream.hls.lol",
                                    "User-Agent" to defaultHeaders["User-Agent"]!!
                                )
                                val isMasterLive = withTimeoutOrNull(3000L) {
                                    isStreamReachable(decryptedUrl, heliosMasterHeaders)
                                } ?: false

                                if (isMasterLive) {
                                    val masterSrc = StreamSource(
                                        url = decryptedUrl,
                                        serverName = "Helios ($serverName Auto)",
                                        resolutionLabel = "Auto",
                                        quality = "Aether Helios ($serverName Auto HLS)",
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
                }
            } catch (_: Exception) {}
        }

        // 2. Moscow Native High-Speed Proxy Engine (transcode.cfd cluster)
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

        // 3. Aphrodite CDN (High-speed multi-edge & multi-quality mirrors)
        launch {
            try {
                val aphroditeStreams = fetchAphroditeStreams(tmdbId, isTv, season, episode)
                aphroditeStreams.forEach { aphroditeSource ->
                    val aphKey = "${aphroditeSource.serverName}:${aphroditeSource.resolutionLabel}:${aphroditeSource.url}"
                    if (emittedStreamKeys.add(aphKey)) {
                        send(StreamEmission.SourceFound(aphroditeSource))
                    }
                }
            } catch (_: Throwable) {}
        }

        // 4. Meridian Engine
        launch {
            try {
                val meridianUrl = if (isTv && season != null && episode != null) {
                    "https://meridian.aether.cx/show/$tmdbId/$season/$episode"
                } else {
                    "https://meridian.aether.cx/movie/$tmdbId"
                }
                val req = newRequestBuilder(meridianUrl).build()
                client.newCall(req).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val body = resp.body?.string() ?: ""
                        val root = json.parseToJsonElement(body).jsonObject
                        val streamUrl = root["url"]?.jsonPrimitive?.contentOrNull
                        if (!streamUrl.isNullOrBlank() && streamUrl.startsWith("http") && !streamUrl.contains("totallyacdn.org", ignoreCase = true)) {
                            val streamHost = URI(streamUrl).host
                            val playbackHeaders = mapOf(
                                "Referer" to "https://$streamHost/",
                                "Origin" to "https://$streamHost",
                                "User-Agent" to defaultHeaders["User-Agent"]!!,
                                "Accept-Ranges" to "bytes"
                            )

                            val isLive = withTimeoutOrNull(3000L) {
                                isStreamReachable(streamUrl, playbackHeaders)
                            } ?: false

                            if (isLive) {
                                val src = StreamSource(
                                    url = streamUrl,
                                    serverName = "Meridian (1080p)",
                                    resolutionLabel = "1080p FHD",
                                    quality = "Aether Meridian (1080p FHD HLS)",
                                    isM3u8 = true,
                                    releaseType = AudioReleaseType.ORIGINAL,
                                    headers = playbackHeaders
                                )
                                val meridianKey = "${src.serverName}:${src.resolutionLabel}:${src.url}"
                                if (emittedStreamKeys.add(meridianKey)) send(StreamEmission.SourceFound(src))

                                val variants = resolveMasterPlaylistVariants(streamUrl, "Aether Meridian", playbackHeaders)
                                variants.forEach { v ->
                                    val vKey = "${v.serverName}:${v.resolutionLabel}:${v.url}"
                                    if (emittedStreamKeys.add(vKey)) send(StreamEmission.SourceFound(v))
                                }
                            }
                        }
                        root["subtitles"]?.jsonArray?.forEach { subElem ->
                            val subObj = subElem.jsonObject
                            val subUrl = subObj["url"]?.jsonPrimitive?.contentOrNull ?: return@forEach
                            val lang = subObj["language"]?.jsonPrimitive?.contentOrNull ?: "English"
                            val isHi = lang.contains("[CC]", ignoreCase = true)
                            val cleanLang = lang.replace(Regex("""\s*\[CC\]""", RegexOption.IGNORE_CASE), "").trim()
                            val variantKey = if (isHi) "${cleanLang.lowercase()} [cc]" else cleanLang.lowercase()
                            if (emittedSubLangs.add(variantKey) && emittedSubUrls.add(subUrl)) {
                                send(StreamEmission.SubtitleFound(SubtitleTrack(url = subUrl, language = lang)))
                            }
                        }
                    }
                }
            } catch (_: Exception) {}
        }

        // 5. Lula Multi-Audio Engine
        launch {
            try {
                val lulUrl = if (isTv && season != null && episode != null) {
                    "https://lul.aether.cx/tv/$tmdbId/$season/$episode"
                } else {
                    "https://lul.aether.cx/movie/$tmdbId"
                }
                val req = newRequestBuilder(lulUrl).build()
                client.newCall(req).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val body = resp.body?.string() ?: ""
                        val root = json.parseToJsonElement(body).jsonObject
                        val streamUrl = root["stream"]?.jsonPrimitive?.contentOrNull
                            ?: root["stream_url"]?.jsonPrimitive?.contentOrNull
                        if (!streamUrl.isNullOrBlank() && streamUrl.startsWith("http") && !streamUrl.contains("totallyacdn.org", ignoreCase = true)) {
                            val lulHeaders = mapOf(
                                "Referer" to "https://aether.ist/",
                                "Origin" to "https://aether.ist",
                                "User-Agent" to defaultHeaders["User-Agent"]!!
                            )

                            val isLive = withTimeoutOrNull(3000L) {
                                isStreamReachable(streamUrl, lulHeaders)
                            } ?: false

                            if (isLive) {
                                val (audioBadge, manifestSubs) = inspectLulManifest(streamUrl)
                                val badge = if (audioBadge.isNotBlank()) {
                                    "Aether Lula ($audioBadge Auto/1080p HLS)"
                                } else {
                                    "Aether Lula (Auto/1080p HLS)"
                                }
                                val relType = if (audioBadge.isNotBlank()) AudioReleaseType.DUAL_AUDIO else AudioReleaseType.ORIGINAL
                                val src = StreamSource(
                                    url = streamUrl,
                                    serverName = "Lula (Auto)",
                                    resolutionLabel = "Auto",
                                    quality = badge,
                                    isM3u8 = true,
                                    releaseType = relType,
                                    headers = lulHeaders
                                )
                                val lulKey = "${src.serverName}:${src.resolutionLabel}:${src.url}"
                                if (emittedStreamKeys.add(lulKey)) send(StreamEmission.SourceFound(src))

                                // Extract individual variants from Lula master playlist
                                val variants = resolveMasterPlaylistVariants(streamUrl, "Aether Lula", lulHeaders)
                                variants.forEach { v ->
                                    val vSrc = v.copy(serverName = "Lula (${v.resolutionLabel})")
                                    val vKey = "${vSrc.serverName}:${vSrc.resolutionLabel}:${vSrc.url}"
                                    if (emittedStreamKeys.add(vKey)) send(StreamEmission.SourceFound(vSrc))
                                }

                                for ((subUrl, subLang) in manifestSubs) {
                                    val isHi = subLang.contains("[CC]", ignoreCase = true)
                                    val cleanLang = subLang.replace(Regex("""\s*\[CC\]""", RegexOption.IGNORE_CASE), "").trim()
                                    val variantKey = if (isHi) "${cleanLang.lowercase()} [cc]" else cleanLang.lowercase()
                                    if (emittedSubLangs.add(variantKey) && emittedSubUrls.add(subUrl)) {
                                        send(StreamEmission.SubtitleFound(SubtitleTrack(url = subUrl, language = subLang)))
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (_: Exception) {}
        }

        // 6. Link Multi-Quality Engine
        launch {
            try {
                val linkUrl = if (isTv && season != null && episode != null) {
                    "https://link.aether.cx/tv/$tmdbId/$season/$episode"
                } else {
                    "https://link.aether.cx/movie/$tmdbId"
                }
                val req = newRequestBuilder(linkUrl).build()
                client.newCall(req).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val body = resp.body?.string() ?: ""
                        val root = json.parseToJsonElement(body).jsonObject
                        val streamUrl = root["stream"]?.jsonPrimitive?.contentOrNull
                            ?: root["stream_url"]?.jsonPrimitive?.contentOrNull
                        if (!streamUrl.isNullOrBlank() && streamUrl.startsWith("http") && !streamUrl.contains("totallyacdn.org", ignoreCase = true)) {
                            val linkHeaders = mapOf(
                                "Referer" to "https://link.aether.cx/",
                                "Origin" to "https://link.aether.cx",
                                "User-Agent" to defaultHeaders["User-Agent"]!!
                            )

                            val isLive = withTimeoutOrNull(3000L) {
                                isStreamReachable(streamUrl, linkHeaders)
                            } ?: false

                            if (isLive) {
                                val src = StreamSource(
                                    url = streamUrl,
                                    serverName = "Link (Auto)",
                                    resolutionLabel = "Auto",
                                    quality = "Aether Link (Auto HLS)",
                                    isM3u8 = true,
                                    releaseType = AudioReleaseType.ORIGINAL,
                                    headers = linkHeaders
                                )
                                val linkKey = "${src.serverName}:${src.resolutionLabel}:${src.url}"
                                if (emittedStreamKeys.add(linkKey)) send(StreamEmission.SourceFound(src))

                                val variants = resolveMasterPlaylistVariants(streamUrl, "Aether Link", linkHeaders)
                                variants.forEach { v ->
                                    val vSrc = v.copy(serverName = "Link (${v.resolutionLabel})")
                                    val vKey = "${vSrc.serverName}:${vSrc.resolutionLabel}:${vSrc.url}"
                                    if (emittedStreamKeys.add(vKey)) send(StreamEmission.SourceFound(vSrc))
                                }
                            }
                        }
                    }
                }
            } catch (_: Exception) {}
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
        val sortedStreams = distinctStreams.sortedWith(
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
        StreamResult(streams = sortedStreams, subtitles = sortedSubs)
    }

    private fun inspectLulManifest(manifestUrl: String): Pair<String, List<Pair<String, String>>> {
        val audioLangs = mutableSetOf<String>()
        val subsList = mutableListOf<Pair<String, String>>()
        try {
            val req = newRequestBuilder(manifestUrl).build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    val body = resp.body?.string() ?: ""
                    val lines = body.lines()
                    for (line in lines) {
                        val trimmed = line.trim()
                        if (trimmed.startsWith("#EXT-X-MEDIA:TYPE=AUDIO")) {
                            val langMatch = Regex("LANGUAGE=\"([^\"]+)\"").find(trimmed)
                            langMatch?.let { audioLangs.add(it.groupValues[1].uppercase()) }
                        } else if (trimmed.startsWith("#EXT-X-MEDIA:TYPE=SUBTITLES")) {
                            val langMatch = Regex("NAME=\"([^\"]+)\"").find(trimmed)
                            val uriMatch = Regex("URI=\"([^\"]+)\"").find(trimmed)
                            if (langMatch != null && uriMatch != null) {
                                subsList.add(uriMatch.groupValues[1] to langMatch.groupValues[1])
                            }
                        }
                    }
                }
            }
        } catch (_: Exception) {}

        val audioBadge = if (audioLangs.size > 1) {
            "Multi-Audio ${audioLangs.take(3).joinToString("/")} "
        } else ""

        return audioBadge to subsList
    }

    private suspend fun fetchAphroditeStreams(
        tmdbId: String,
        isTv: Boolean,
        season: Int?,
        episode: Int?
    ): List<StreamSource> = withContext(Dispatchers.IO) {
        val results = mutableListOf<StreamSource>()
        val path = if (isTv && season != null && episode != null) {
            "/content/tv/$tmdbId/$season/$episode"
        } else {
            "/content/movie/$tmdbId"
        }

        var streamUrl: String? = null

        // 1. Direct Aphrodite CDN GET with fast non-blocking timeout
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

        val resolvedUrl = streamUrl?.takeIf { it.startsWith("http") && !it.contains("totallyacdn.org", ignoreCase = true) }
        val isHoneypot = resolvedUrl == null || isHoneypotStream(resolvedUrl)

        if (resolvedUrl != null && !isHoneypot) {
            val aphroditeDirectHeaders = mapOf(
                "Referer" to "https://atlantic.st/",
                "Origin" to "https://atlantic.st",
                "User-Agent" to defaultHeaders["User-Agent"]!!
            )
            val isLive = withTimeoutOrNull(3000L) {
                isStreamReachable(resolvedUrl, aphroditeDirectHeaders)
            } ?: false

            if (isLive) {
                results.add(
                    StreamSource(
                        url = resolvedUrl,
                        serverName = "Aphrodite (1080p)",
                        resolutionLabel = "1080p FHD",
                        quality = "Aether Aphrodite (1080p FHD HLS)",
                        isM3u8 = true,
                        releaseType = AudioReleaseType.ORIGINAL,
                        headers = aphroditeDirectHeaders
                    )
                )

                try {
                    val variants = resolveMasterPlaylistVariants(resolvedUrl, "Aether Aphrodite", aphroditeDirectHeaders)
                    variants.forEach { v ->
                        if (!results.any { it.url == v.url }) {
                            results.add(v)
                        }
                    }
                } catch (_: Throwable) {}

                return@withContext results
            }
        }

        // 2. High-Speed Multi-Quality Aphrodite Mirrors (Fallback when direct returns honeypot or is blocked)
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
                val sources = root["sources"]?.jsonObject
                if (sources != null) {
                    val masterEntry = sources["Moscow"]?.jsonObject ?: sources.values.firstOrNull()?.jsonObject
                    val encUrl = masterEntry?.get("url")?.jsonPrimitive?.contentOrNull
                    if (encUrl != null) {
                        val masterDecrypted = decryptHeliosUrl(encUrl)
                        if (masterDecrypted != null && masterDecrypted.startsWith("http") && !masterDecrypted.contains("totallyacdn.org", ignoreCase = true)) {
                            val qParam = masterDecrypted.substringAfter("?q=", "").substringBefore("&")
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
                                val aphroditeHeaders = mapOf(
                                    "Referer" to (streamHeaders["referer"] ?: "https://www.movy.sx/"),
                                    "Origin" to (streamHeaders["origin"] ?: "https://www.movy.sx"),
                                    "User-Agent" to defaultHeaders["User-Agent"]!!,
                                    "Accept-Ranges" to "bytes"
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
                                                quality = "Aether Aphrodite ($qualityLabel HLS)",
                                                isM3u8 = true,
                                                releaseType = AudioReleaseType.ORIGINAL,
                                                headers = aphroditeHeaders
                                            )
                                        )
                                    }
                                }

                                val aphMasterUrl = if (masterDecrypted.contains("?")) "$masterDecrypted&server=aphrodite" else "$masterDecrypted?server=aphrodite"
                                val aphMasterHeaders = mapOf(
                                    "Referer" to "https://stream.hls.lol/",
                                    "Origin" to "https://stream.hls.lol",
                                    "User-Agent" to defaultHeaders["User-Agent"]!!
                                )
                                results.add(
                                    StreamSource(
                                        url = aphMasterUrl,
                                        serverName = "Aphrodite (Auto)",
                                        resolutionLabel = "Auto",
                                        quality = "Aether Aphrodite (Auto HLS)",
                                        isM3u8 = true,
                                        releaseType = AudioReleaseType.ORIGINAL,
                                        headers = aphMasterHeaders
                                    )
                                )
                            }
                        }
                    }
                }
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
                            quality = "Aether Moscow (1080p FHD HLS)",
                            isM3u8 = true,
                            releaseType = AudioReleaseType.ORIGINAL,
                            headers = defaultHeaders
                        )
                        results.add(moscowSource)

                        val variants = resolveMasterPlaylistVariants(decryptedUrl, "Aether Moscow", defaultHeaders)
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
                                            quality = "$serverPrefix ($qLabel)",
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

        // 1. Helios Direct Quality Mirrors (1080p, 720p, 480p)
        try {
            val heliosUrl = if (isTv && season != null && episode != null) {
                "https://stream.hls.lol/helios?tmdbId=$tmdbId&type=tv&seasonId=$season&episodeId=$episode"
            } else {
                "https://stream.hls.lol/helios?tmdbId=$tmdbId&type=movie"
            }
            val req = newRequestBuilder(heliosUrl).build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    val body = resp.body?.string() ?: ""
                    val root = json.parseToJsonElement(body).jsonObject
                    val sources = root["sources"]?.jsonObject
                    if (sources != null) {
                        val moscowEnc = sources["Moscow"]?.jsonObject?.get("url")?.jsonPrimitive?.contentOrNull
                            ?: sources.values.firstOrNull()?.jsonObject?.get("url")?.jsonPrimitive?.contentOrNull
                        if (moscowEnc != null) {
                            val dec = decryptHeliosUrl(moscowEnc)
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
                                            val qLabel = when {
                                                vQuality.contains("1080") -> "1080p FHD"
                                                vQuality.contains("720") -> "720p HD"
                                                vQuality.contains("480") -> "480p SD"
                                                else -> vQuality
                                            }
                                            val sizeEstimate = when {
                                                qLabel.contains("1080") -> "~2.2 GB"
                                                qLabel.contains("720") -> "~1.2 GB"
                                                qLabel.contains("480") -> "~650 MB"
                                                else -> "~1.5 GB"
                                            }
                                            options.add(
                                                DownloadOption(
                                                    title = "$qLabel - Aether Helios",
                                                    quality = qLabel,
                                                    size = sizeEstimate,
                                                    url = vUrl,
                                                    source = "Aether Helios CDN",
                                                    provider = name,
                                                    headers = reqHeaders
                                                )
                                            )
                                            val aphUrl = if (vUrl.contains("?")) "$vUrl&server=aphrodite" else "$vUrl?server=aphrodite"
                                            options.add(
                                                DownloadOption(
                                                    title = "$qLabel - Aether Aphrodite",
                                                    quality = qLabel,
                                                    size = sizeEstimate,
                                                    url = aphUrl,
                                                    source = "Aether Aphrodite CDN",
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

        // 2. Add Meridian high-speed HLS option
        try {
            val meridianUrl = if (isTv && season != null && episode != null) {
                "https://meridian.aether.cx/show/$tmdbId/$season/$episode"
            } else {
                "https://meridian.aether.cx/movie/$tmdbId"
            }
            val req = newRequestBuilder(meridianUrl).build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    val body = resp.body?.string() ?: ""
                    val root = json.parseToJsonElement(body).jsonObject
                    val streamUrl = root["url"]?.jsonPrimitive?.contentOrNull
                    if (!streamUrl.isNullOrBlank() && streamUrl.startsWith("http")) {
                        val streamHost = URI(streamUrl).host
                        val meridianHeaders = mapOf(
                            "Referer" to "https://$streamHost/",
                            "Origin" to "https://$streamHost",
                            "User-Agent" to defaultHeaders["User-Agent"]!!
                        )
                        options.add(
                            DownloadOption(
                                title = "1080p Adaptive - Aether Meridian",
                                quality = "1080p FHD",
                                size = "~2.4 GB",
                                url = streamUrl,
                                source = "Aether Meridian CDN",
                                provider = name,
                                headers = meridianHeaders
                            )
                        )
                    }
                }
            }
        } catch (_: Exception) {}

        // 3. Add Link high-speed HLS option
        try {
            val linkUrl = if (isTv && season != null && episode != null) {
                "https://link.aether.cx/tv/$tmdbId/$season/$episode"
            } else {
                "https://link.aether.cx/movie/$tmdbId"
            }
            val req = newRequestBuilder(linkUrl).build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    val body = resp.body?.string() ?: ""
                    val root = json.parseToJsonElement(body).jsonObject
                    val streamUrl = root["stream"]?.jsonPrimitive?.contentOrNull
                        ?: root["stream_url"]?.jsonPrimitive?.contentOrNull
                    if (!streamUrl.isNullOrBlank() && streamUrl.startsWith("http")) {
                        val linkHeaders = mapOf(
                            "Referer" to "https://link.aether.cx/",
                            "Origin" to "https://link.aether.cx",
                            "User-Agent" to defaultHeaders["User-Agent"]!!
                        )
                        options.add(
                            DownloadOption(
                                title = "1080p Adaptive - Aether Link",
                                quality = "1080p FHD",
                                size = "~2.2 GB",
                                url = streamUrl,
                                source = "Aether Link CDN",
                                provider = name,
                                headers = linkHeaders
                            )
                        )
                    }
                }
            }
        } catch (_: Exception) {}

        // 4. Fallback to stream links if options empty
        if (options.isEmpty()) {
            val streamResult = try {
                getStreamLinks(episodeData)
            } catch (_: Exception) {
                StreamResult(emptyList(), emptyList())
            }
            options.addAll(
                streamResult.streams.map { stream ->
                    DownloadOption(
                        title = "${stream.quality} - Aether Direct",
                        quality = stream.resolutionLabel.ifBlank { "1080p FHD" },
                        size = "~2.0 GB",
                        url = stream.url,
                        source = "Aether (${stream.serverName})",
                        provider = name,
                        headers = stream.headers
                    )
                }
            )
        }

        options.distinctBy { "${it.title}:${it.url}" }.sortedByDescending {
            when {
                it.quality.contains("1080") -> 1080
                it.quality.contains("720") -> 720
                it.quality.contains("480") -> 480
                it.quality.contains("Auto") -> 100
                else -> 50
            }
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

    override suspend fun fetchCast(mediaId: String, imdbId: String?, type: MediaType): List<CastMember> = withContext(Dispatchers.IO) {
        val cleanTmdbId = Regex("""\b(\d+)\b""").find(mediaId)?.value ?: mediaId
        if (cleanTmdbId.isBlank()) return@withContext emptyList()
        val isTv = type == MediaType.TV_SERIES
        val enriched = TmdbBridge.fetchEnrichedDetails(client, cleanTmdbId, isTv, name, tmdbApiKey)
        enriched?.cast ?: emptyList()
    }
}
