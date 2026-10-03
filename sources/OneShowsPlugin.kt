package com.euthopiar.core.provider

import com.euthopiar.core.dsl.*
import com.euthopiar.core.extractor.ExtractorRegistry
import com.euthopiar.core.model.*
import com.euthopiar.core.network.DohDns
import com.euthopiar.core.util.TmdbBridge
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import okhttp3.OkHttpClient
import okhttp3.Request
import java.net.URLEncoder
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * Official 1Shows streaming provider plugin (https://www.1shows.bz/).
 *
 * Fully reverse-engineered from 1Shows architecture:
 * - Native 1Shows Next.js REST API for trending movies, trending TV, and query search
 * - Direct details & seasons schema from https://www.1shows.bz/api/
 * - Complete TMDB metadata synchronization for authentic 16:9 episode thumbnails and titles
 * - Clear title logo resolution via TMDB images API with Metahub fallback
 * - Comprehensive multi-language subtitle aggregation via https://core.vidzee.wtf/subs/
 * - Vidzee dynamic multi-language HLS audio tracks (Hindi, English, etc.) & servers (Dcloud, TCloud, IPcloud)
 *   decrypted natively via [OneShowsWasmEngine]
 * - Viduki / MakimaDL high-speed direct CDN streams & downloads (4K, 1080p FHD, 720p, 5.1/DTS-HD audio)
 *   decrypted natively via [OneShowsWasmEngine]
 * - Zero cross-contamination: 100% native 1Shows / Vidzee / Viduki infrastructure
 */
class OneShowsPlugin(
    private val client: OkHttpClient = DohDns.createOkHttpClient(
        connectTimeoutSeconds = 15,
        readTimeoutSeconds = 25
    )
) : UniversalPlugin {

    override val name: String = "1Shows"
    override val mainUrl: String = "https://www.1shows.bz"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE, MediaType.TV_SERIES)

    private val json = Json { ignoreUnknownKeys = true }

    private val defaultHeaders = mapOf(
        "Referer" to "https://www.1shows.bz/",
        "Origin" to "https://www.1shows.bz",
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Ranges" to "bytes"
    )

    private val vidukiHeaders = mapOf(
        "Referer" to "https://www.1shows.bz/",
        "Origin" to "https://www.1shows.bz",
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Ranges" to "bytes"
    )

    private val vidzeeHeaders = mapOf(
        "Referer" to "https://player.vidzee.wtf/",
        "Origin" to "https://player.vidzee.wtf",
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Ranges" to "bytes"
    )

    private val logoCache = ConcurrentHashMap<String, String>()

    private fun withByteRanges(headers: Map<String, String>): Map<String, String> =
        if (headers.containsKey("Accept-Ranges")) headers else headers + ("Accept-Ranges" to "bytes")

    private fun newRequestBuilder(url: String, headers: Map<String, String> = defaultHeaders): Request.Builder {
        val builder = Request.Builder().url(url)
        headers.forEach { (k, v) -> builder.header(k, v) }
        return builder
    }

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        coroutineScope {
            val categories = listOf(
                "Trending Movies Today" to "https://www.1shows.bz/api/trending/movie/day",
                "Trending TV Shows Today" to "https://www.1shows.bz/api/trending/tv/day",
                "Popular Movies This Week" to "https://www.1shows.bz/api/trending/movie/week",
                "Popular TV Shows This Week" to "https://www.1shows.bz/api/trending/tv/week",
                "Now Playing in Theatres" to "https://api.themoviedb.org/3/movie/now_playing?api_key=1865f43a0549ca50d341dd9ab8b29f49",
                "Top Rated Movies" to "https://api.themoviedb.org/3/movie/top_rated?api_key=1865f43a0549ca50d341dd9ab8b29f49",
                "Top Rated TV Series" to "https://api.themoviedb.org/3/tv/top_rated?api_key=1865f43a0549ca50d341dd9ab8b29f49",
                "Action Blockbusters" to "https://api.themoviedb.org/3/discover/movie?api_key=1865f43a0549ca50d341dd9ab8b29f49&with_genres=28&sort_by=popularity.desc",
                "Sci-Fi & Fantasy Hits" to "https://api.themoviedb.org/3/discover/movie?api_key=1865f43a0549ca50d341dd9ab8b29f49&with_genres=878&sort_by=popularity.desc",
                "Crime & Mystery Series" to "https://api.themoviedb.org/3/discover/tv?api_key=1865f43a0549ca50d341dd9ab8b29f49&with_genres=80,9648&sort_by=popularity.desc",
                "Animation & Anime" to "https://api.themoviedb.org/3/discover/tv?api_key=1865f43a0549ca50d341dd9ab8b29f49&with_genres=16&sort_by=popularity.desc"
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
            val req = newRequestBuilder(url).build()
            val resp = client.newCall(req).execute()
            if (!resp.isSuccessful) return null

            val body = resp.body?.string() ?: return null
            val root = json.parseToJsonElement(body).jsonObject
            val resultsArr = root["results"]?.jsonArray ?: return null

            val items = mutableListOf<MediaItem>()
            for (elem in resultsArr) {
                val obj = elem.jsonObject
                val id = obj["id"]?.jsonPrimitive?.contentOrNull ?: continue
                val isTv = url.contains("/tv") || obj.containsKey("first_air_date") || obj.containsKey("name")
                val itemTitle = obj["title"]?.jsonPrimitive?.contentOrNull
                    ?: obj["name"]?.jsonPrimitive?.contentOrNull
                    ?: "Unknown"

                val posterPath = obj["poster_path"]?.jsonPrimitive?.contentOrNull
                val posterUrl = posterPath?.let {
                    if (it.startsWith("http")) it else "https://image.tmdb.org/t/p/w500$it"
                }
                val backdropPath = obj["backdrop_path"]?.jsonPrimitive?.contentOrNull
                val backdropUrl = backdropPath?.let {
                    if (it.startsWith("http")) it else "https://image.tmdb.org/t/p/w1280$it"
                } ?: posterUrl

                val dateStr = obj["release_date"]?.jsonPrimitive?.contentOrNull
                    ?: obj["first_air_date"]?.jsonPrimitive?.contentOrNull
                val year = dateStr?.take(4)?.toIntOrNull()
                val vote = obj["vote_average"]?.jsonPrimitive?.doubleOrNull

                items.add(
                    MediaItem(
                        id = id,
                        title = itemTitle,
                        url = "https://www.1shows.bz/${if (isTv) "tv" else "movie"}/$id",
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
        } catch (_: Exception) {
            return null
        }
    }

    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        val encodedQuery = URLEncoder.encode(query, "UTF-8")
        val results = mutableListOf<MediaItem>()
        val seenIds = mutableSetOf<String>()

        try {
            val url = "https://www.1shows.bz/api/search/query?query=$encodedQuery"
            val req = newRequestBuilder(url).build()
            val resp = client.newCall(req).execute()
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
                        val posterUrl = posterPath?.let {
                            if (it.startsWith("http")) it else "https://image.tmdb.org/t/p/w500$it"
                        }
                        val backdropPath = obj["backdrop_path"]?.jsonPrimitive?.contentOrNull
                        val backdropUrl = backdropPath?.let {
                            if (it.startsWith("http")) it else "https://image.tmdb.org/t/p/w1280$it"
                        } ?: posterUrl

                        val dateStr = obj["release_date"]?.jsonPrimitive?.contentOrNull
                            ?: obj["first_air_date"]?.jsonPrimitive?.contentOrNull
                        val year = dateStr?.take(4)?.toIntOrNull()
                        val vote = obj["vote_average"]?.jsonPrimitive?.doubleOrNull

                        results.add(
                            MediaItem(
                                id = id,
                                title = title,
                                url = "https://www.1shows.bz/$mediaTypeStr/$id",
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
        } catch (_: Exception) {}

        results
    }

    override suspend fun getDetails(mediaItem: MediaItem): MediaDetail = withContext(Dispatchers.IO) {
        val isTv = mediaItem.type == MediaType.TV_SERIES
        val typeStr = if (isTv) "tv" else "movie"
        val tmdbId = mediaItem.id

        var siteTitle: String? = null
        var siteSynopsis: String? = null
        var sitePoster: String? = null
        var siteBackdrop: String? = null
        var siteYear: Int? = null
        var siteRating: String? = null
        var siteImdbId: String? = null
        var siteGenres = listOf<String>()
        var validSeasons = listOf<Int>()

        // 1. Fetch details from 1Shows API (Provider-first)
        try {
            val url = "https://www.1shows.bz/api/$typeStr/$tmdbId"
            val req = newRequestBuilder(url).build()
            val resp = client.newCall(req).execute()
            if (resp.isSuccessful) {
                val body = resp.body?.string() ?: ""
                val root = json.parseToJsonElement(body).jsonObject

                siteTitle = root["title"]?.jsonPrimitive?.contentOrNull
                    ?: root["name"]?.jsonPrimitive?.contentOrNull
                siteSynopsis = root["overview"]?.jsonPrimitive?.contentOrNull

                val posterPath = root["poster_path"]?.jsonPrimitive?.contentOrNull
                if (posterPath != null) {
                    sitePoster = if (posterPath.startsWith("http")) posterPath else "https://image.tmdb.org/t/p/w500$posterPath"
                }

                val backdropPath = root["backdrop_path"]?.jsonPrimitive?.contentOrNull
                if (backdropPath != null) {
                    siteBackdrop = if (backdropPath.startsWith("http")) backdropPath else "https://image.tmdb.org/t/p/w1280$backdropPath"
                }

                val dateStr = root["release_date"]?.jsonPrimitive?.contentOrNull
                    ?: root["first_air_date"]?.jsonPrimitive?.contentOrNull
                siteYear = dateStr?.take(4)?.toIntOrNull()

                val vote = root["vote_average"]?.jsonPrimitive?.doubleOrNull
                if (vote != null) {
                    siteRating = String.format("%.1f", vote)
                }

                siteImdbId = root["imdb_id"]?.jsonPrimitive?.contentOrNull

                val gList = mutableListOf<String>()
                root["genres"]?.jsonArray?.forEach { gElem ->
                    val gName = gElem.jsonObject["name"]?.jsonPrimitive?.contentOrNull
                    if (gName != null) gList.add(gName)
                }
                siteGenres = gList

                if (isTv) {
                    val seasonsArr = root["seasons"]?.jsonArray
                    if (seasonsArr != null) {
                        validSeasons = seasonsArr.mapNotNull { sElem ->
                            val sObj = sElem.jsonObject
                            val sNum = sObj["season_number"]?.jsonPrimitive?.intOrNull ?: return@mapNotNull null
                            if (sNum <= 0) return@mapNotNull null
                            sNum
                        }
                    }
                }
            }
        } catch (_: Exception) {}

        // 2. TMDB Enrichment Bridge
        val enriched = TmdbBridge.fetchEnrichedDetails(client, tmdbId, isTv, siteTitle ?: mediaItem.title)

        val title = siteTitle ?: enriched?.title ?: mediaItem.title
        val posterUrl = enriched?.posterUrl ?: sitePoster ?: mediaItem.posterUrl
        val backdropUrl = enriched?.backdropUrl ?: siteBackdrop ?: mediaItem.backdropUrl ?: posterUrl
        val year = enriched?.year ?: siteYear ?: mediaItem.year
        val synopsis = siteSynopsis ?: enriched?.synopsis ?: ""
        val genres = siteGenres.ifEmpty { enriched?.genres ?: emptyList() }
        val duration = enriched?.duration
        val rating = siteRating ?: enriched?.rating ?: mediaItem.rating
        val rottenTomatoes = enriched?.rottenTomatoesRating
        val contentRating = enriched?.contentRating
        val imdbId = siteImdbId ?: enriched?.imdbId
        val directors = enriched?.directors ?: emptyList()
        val recommendations = enriched?.recommendations ?: emptyList()
        val trailerUrl = enriched?.trailerUrl
        val logoUrl = enriched?.logoUrl ?: resolveLogo(mediaItem) ?: if (!imdbId.isNullOrBlank() && imdbId.startsWith("tt")) {
            "https://images.metahub.space/logo/medium/$imdbId/img.png"
        } else null

        val cast = enriched?.cast ?: emptyList()

        val episodes = mutableListOf<EpisodeItem>()
        if (isTv) {
            val seasonNumbers = enriched?.seasonNumbers?.ifEmpty { null } ?: validSeasons.ifEmpty { listOf(1) }
            val tmdbSeasonsData = TmdbBridge.fetchTmdbSeasons(client, tmdbId, seasonNumbers)

            for (s in seasonNumbers) {
                val epMap = tmdbSeasonsData[s]
                if (!epMap.isNullOrEmpty()) {
                    for ((e, epDetail) in epMap.toSortedMap()) {
                        val epName = epDetail.name.ifBlank { "Episode $e" }
                        val epAudioType = when {
                            epName.contains("hindi", ignoreCase = true) && epName.contains("eng", ignoreCase = true) -> AudioReleaseType.DUAL_AUDIO
                            epName.contains("hindi", ignoreCase = true) || epName.contains("dub", ignoreCase = true) -> AudioReleaseType.DUB
                            else -> AudioReleaseType.SUB
                        }
                        val epLangs = when (epAudioType) {
                            AudioReleaseType.DUAL_AUDIO -> listOf("hi", "en")
                            AudioReleaseType.DUB -> listOf("hi")
                            else -> listOf("en")
                        }
                        episodes.add(
                            EpisodeItem(
                                id = TmdbBridge.sanitizeSlug("$tmdbId-s${s}e$e"),
                                title = epName,
                                seasonNumber = s,
                                episodeNumber = e,
                                data = "$tmdbId:$s:$e",
                                thumbnail = epDetail.stillUrl ?: backdropUrl,
                                description = epDetail.overview,
                                duration = epDetail.duration ?: duration ?: "45 min",
                                audioType = epAudioType,
                                availableLanguages = epLangs
                            )
                        )
                    }
                } else {
                    for (e in 1..10) {
                        episodes.add(
                            EpisodeItem(
                                id = TmdbBridge.sanitizeSlug("$tmdbId-s${s}e$e"),
                                title = "Episode $e",
                                seasonNumber = s,
                                episodeNumber = e,
                                data = "$tmdbId:$s:$e",
                                thumbnail = backdropUrl,
                                description = "Season $s • Episode $e",
                                duration = duration ?: "45 min",
                                audioType = AudioReleaseType.SUB,
                                availableLanguages = listOf("en")
                            )
                        )
                    }
                }
            }

            if (episodes.isEmpty()) {
                episodes.add(
                    EpisodeItem(
                        id = TmdbBridge.sanitizeSlug("$tmdbId-s1e1"),
                        title = "Episode 1",
                        seasonNumber = 1,
                        episodeNumber = 1,
                        data = "$tmdbId:1:1",
                        thumbnail = backdropUrl,
                        description = synopsis,
                        duration = duration ?: "45 min"
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

        MediaDetail(
            id = TmdbBridge.sanitizeSlug(tmdbId),
            title = title,
            url = "https://www.1shows.bz/$typeStr/$tmdbId",
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
            imdbId = imdbId,
            availableAudioTypes = setOf(AudioReleaseType.SUB, AudioReleaseType.DUB, AudioReleaseType.DUAL_AUDIO)
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

        send(StreamEmission.StatusUpdate("1Shows", "Searching all 1Shows premium, embedded, direct, and multi-language sources..."))

        val emittedStreamUrls = ConcurrentHashMap.newKeySet<String>()
        val emittedSubUrls = ConcurrentHashMap.newKeySet<String>()
        val emittedSubLangs = ConcurrentHashMap.newKeySet<String>()

        // 1. Fetch Subtitles from core.vidzee.wtf
        launch {
            try {
                val subsUrl = if (isTv && season != null && episode != null) {
                    "https://core.vidzee.wtf/subs/tv/$tmdbId/$season/$episode"
                } else {
                    "https://core.vidzee.wtf/subs/movie/$tmdbId"
                }

                val req = newRequestBuilder(subsUrl, vidzeeHeaders).build()
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

        // 2. Fetch Vidzee Multi-Audio & Server Streams
        launch {
            try {
                val streams = fetchVidzeeStreams(tmdbId, season, episode, isTv)
                for (vStream in streams) {
                    val displayQuality = if (!vStream.language.isNullOrBlank() && !vStream.serverLabel.contains(vStream.language, ignoreCase = true)) {
                        "${vStream.serverLabel} [${vStream.language}]"
                    } else {
                        vStream.serverLabel
                    }
                    val isHindi = vStream.language?.contains("hindi", ignoreCase = true) == true
                    val audioType = if (isHindi) AudioReleaseType.DUB else AudioReleaseType.SUB
                    val audioDescs = listOf(
                        AudioTrackDescriptor(
                            languageName = vStream.language ?: "Default",
                            isoCode = if (isHindi) "hi" else "en"
                        )
                    )
                    val source = StreamSource(
                        url = vStream.url,
                        serverName = vStream.serverLabel,
                        resolutionLabel = "Auto",
                        quality = displayQuality,
                        isM3u8 = true,
                        releaseType = audioType,
                        audioTracks = audioDescs,
                        headers = mapOf(
                            "Referer" to "https://player.vidzee.wtf/",
                            "Origin" to "https://player.vidzee.wtf",
                            "User-Agent" to defaultHeaders["User-Agent"]!!
                        )
                    )
                    if (emittedStreamUrls.add(source.url)) {
                        send(StreamEmission.SourceFound(source))
                    }
                }
            } catch (_: Exception) {}
        }

        // 3. Fetch and decrypt Viduki / MakimaDL direct video sources via [OneShowsWasmEngine]
        launch {
            try {
                val tokenReq = newRequestBuilder("https://api.viduki.net/download-token", vidukiHeaders).build()
                val token = client.newCall(tokenReq).execute().use { tokenResp ->
                    if (tokenResp.isSuccessful) {
                        val tokenBody = tokenResp.body?.string() ?: ""
                        json.parseToJsonElement(tokenBody).jsonObject["token"]?.jsonPrimitive?.contentOrNull
                    } else null
                }

                if (!token.isNullOrBlank()) {
                    val dlUrl = if (isTv && season != null && episode != null) {
                        "https://api.viduki.net/download/tv/$tmdbId/$season/$episode"
                    } else {
                        "https://api.viduki.net/download/movie/$tmdbId"
                    }

                    val payloadReq = Request.Builder()
                        .url(dlUrl)
                        .header("Referer", "https://www.1shows.bz/")
                        .header("Origin", "https://www.1shows.bz")
                        .header("User-Agent", defaultHeaders["User-Agent"]!!)
                        .header("x-download-token", token)
                        .build()

                    client.newCall(payloadReq).execute().use { payloadResp ->
                        if (payloadResp.isSuccessful) {
                            val payloadBody = payloadResp.body?.string() ?: ""
                            val payloadObj = json.parseToJsonElement(payloadBody).jsonObject

                            val iv = payloadObj["iv"]?.jsonPrimitive?.contentOrNull
                            val tag = payloadObj["tag"]?.jsonPrimitive?.contentOrNull
                            val ct = payloadObj["ct"]?.jsonPrimitive?.contentOrNull

                            if (iv != null && tag != null && ct != null) {
                                val decryptedJson = OneShowsWasmEngine.decryptPayload(client, token, iv, tag, ct)
                                if (decryptedJson != null) {
                                    val decObj = json.parseToJsonElement(decryptedJson).jsonObject
                                    val sourcesArr = decObj["sources"]?.jsonArray
                                    if (sourcesArr != null) {
                                        for (sElem in sourcesArr) {
                                            val sObj = sElem.jsonObject
                                            val sourceUrl = sObj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                                            val label = sObj["label"]?.jsonPrimitive?.contentOrNull ?: "Direct"

                                            if (!isPlayableDirectVideo(sourceUrl)) continue

                                            val sizeRegex = Regex("""\(([\d.]+)\s*(?:GB|Gb)\)""")
                                            val sizeGb = sizeRegex.find(label)?.groupValues?.get(1)?.toDoubleOrNull()
                                            if (sizeGb != null && sizeGb > 20.0) {
                                                continue
                                            }

                                            val qualityLabel = when {
                                                label.contains("2160p", ignoreCase = true) || label.contains("4K", ignoreCase = true) -> "4K 2160p"
                                                label.contains("1080p", ignoreCase = true) -> "1080p FHD"
                                                label.contains("720p", ignoreCase = true) -> "720p HD"
                                                label.contains("480p", ignoreCase = true) -> "480p SD"
                                                else -> "Direct Video"
                                            }

                                            val audioReleaseType = when {
                                                label.contains("dual", ignoreCase = true) || (label.contains("hindi", ignoreCase = true) && label.contains("eng", ignoreCase = true)) ->
                                                    AudioReleaseType.DUAL_AUDIO
                                                label.contains("hindi", ignoreCase = true) || label.contains("dub", ignoreCase = true) ->
                                                    AudioReleaseType.DUB
                                                label.contains("sub", ignoreCase = true) ->
                                                    AudioReleaseType.SUB
                                                else ->
                                                    AudioReleaseType.ORIGINAL
                                            }

                                            val availableLangs = when (audioReleaseType) {
                                                AudioReleaseType.DUAL_AUDIO -> listOf("hi", "en")
                                                AudioReleaseType.DUB -> listOf("hi")
                                                AudioReleaseType.SUB -> listOf("en")
                                                else -> listOf("en")
                                            }

                                            val audioDescs = availableLangs.map {
                                                AudioTrackDescriptor(
                                                    languageName = if (it == "hi") "Hindi" else "English",
                                                    isoCode = it
                                                )
                                            }

                                            val audioBadge = when (audioReleaseType) {
                                                AudioReleaseType.DUAL_AUDIO -> "Dual Audio (Hindi-Eng)"
                                                AudioReleaseType.DUB -> "Hindi Dubbed"
                                                AudioReleaseType.SUB -> "English Sub"
                                                AudioReleaseType.ORIGINAL -> if (label.contains("5.1")) "5.1 Surround" else null
                                            }

                                            val streamQuality = if (audioBadge != null) {
                                                "$qualityLabel ($audioBadge)"
                                            } else {
                                                "$qualityLabel - Direct CDN"
                                            }

                                            val source = StreamSource(
                                                url = sourceUrl,
                                                serverName = "Viduki Direct",
                                                resolutionLabel = qualityLabel,
                                                quality = streamQuality,
                                                isM3u8 = sourceUrl.contains(".m3u8"),
                                                releaseType = audioReleaseType,
                                                audioTracks = audioDescs,
                                                headers = mapOf(
                                                    "Referer" to "https://www.1shows.bz/",
                                                    "Origin" to "https://www.1shows.bz",
                                                    "User-Agent" to defaultHeaders["User-Agent"]!!
                                                )
                                            )
                                            if (emittedStreamUrls.add(source.url)) {
                                                send(StreamEmission.SourceFound(source))
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (_: Exception) {}
        }

        // 4. Fetch Multi-Language Sources from https://multilang-api.viduki.net
        launch {
            try {
                val multiBase = "https://multilang-api.viduki.net"
                val nonce = OneShowsWasmEngine.getVidukiSessionNonce(client, multiBase)
                if (!nonce.isNullOrBlank()) {
                    val path = if (isTv && season != null && episode != null) {
                        "/tv/$tmdbId/$season/$episode"
                    } else {
                        "/movie/$tmdbId"
                    }
                    val random = java.security.SecureRandom()
                    val cnBytes = ByteArray(16).also { random.nextBytes(it) }
                    val reqIdBytes = ByteArray(16).also { random.nextBytes(it) }
                    val clientNonceHex = cnBytes.joinToString("") { "%02x".format(it) }
                    val reqIdHex = reqIdBytes.joinToString("") { "%02x".format(it) }

                    val req = Request.Builder()
                        .url("$multiBase$path")
                        .header("Referer", "https://www.viduki.net/")
                        .header("Origin", "https://www.viduki.net")
                        .header("User-Agent", defaultHeaders["User-Agent"]!!)
                        .header("X-Nonce", nonce)
                        .header("X-Client-Nonce", clientNonceHex)
                        .header("X-Request-Id", reqIdHex)
                        .header("Accept", "application/json")
                        .build()

                    client.newCall(req).execute().use { resp ->
                        if (resp.isSuccessful) {
                            val body = resp.body?.string() ?: ""
                            val root = json.parseToJsonElement(body).jsonObject
                            val streamsArr = root["data"]?.jsonObject?.get("streams")?.jsonArray
                            if (streamsArr != null) {
                                for (elem in streamsArr) {
                                    val obj = elem.jsonObject
                                    val sUrl = obj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                                    val lang = obj["language"]?.jsonPrimitive?.contentOrNull ?: "Multi-Audio"
                                    val streamHeaders = mutableMapOf(
                                        "User-Agent" to defaultHeaders["User-Agent"]!!
                                    )
                                    obj["headers"]?.jsonObject?.forEach { (k, v) ->
                                        v.jsonPrimitive.contentOrNull?.let { streamHeaders[k] = it }
                                    }
                                    if (!streamHeaders.containsKey("Referer")) {
                                        streamHeaders["Referer"] = "https://slast430did.com"
                                    }

                                    val isDub = !lang.equals("English", ignoreCase = true)
                                    val audioReleaseType = if (isDub) AudioReleaseType.DUB else AudioReleaseType.SUB
                                    val isoCode = getIsoLanguage(lang)
                                    val audioDesc = AudioTrackDescriptor(
                                        languageName = lang,
                                        isoCode = isoCode
                                    )

                                    val source = StreamSource(
                                        url = sUrl,
                                        serverName = "Viduki Multi-Lang [$lang]",
                                        resolutionLabel = "Auto (HLS)",
                                        quality = "Viduki Multi-Lang [$lang]",
                                        isM3u8 = sUrl.contains(".m3u8") || obj["type"]?.jsonPrimitive?.contentOrNull == "hls",
                                        releaseType = audioReleaseType,
                                        audioTracks = listOf(audioDesc),
                                        headers = streamHeaders
                                    )
                                    if (emittedStreamUrls.add(source.url)) {
                                        send(StreamEmission.SourceFound(source))
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (_: Exception) {}
        }

        // 5. Fetch Premium Embeds from https://api.viduki.net/premium_embeds/
        launch {
            try {
                val premBase = "https://api.viduki.net"
                val nonce = OneShowsWasmEngine.getVidukiSessionNonce(client, premBase)
                if (!nonce.isNullOrBlank()) {
                    val path = if (isTv && season != null && episode != null) {
                        "/premium_embeds/tv/$tmdbId/$season/$episode"
                    } else {
                        "/premium_embeds/movie/$tmdbId"
                    }
                    val random = java.security.SecureRandom()
                    val cnBytes = ByteArray(16).also { random.nextBytes(it) }
                    val reqIdBytes = ByteArray(16).also { random.nextBytes(it) }
                    val clientNonceHex = cnBytes.joinToString("") { "%02x".format(it) }
                    val reqIdHex = reqIdBytes.joinToString("") { "%02x".format(it) }

                    val req = Request.Builder()
                        .url("$premBase$path")
                        .header("Referer", "https://www.viduki.net/")
                        .header("Origin", "https://www.viduki.net")
                        .header("User-Agent", defaultHeaders["User-Agent"]!!)
                        .header("X-Nonce", nonce)
                        .header("X-Client-Nonce", clientNonceHex)
                        .header("X-Request-Id", reqIdHex)
                        .header("Accept", "application/json")
                        .build()

                    client.newCall(req).execute().use { resp ->
                        if (resp.isSuccessful) {
                            val body = resp.body?.string() ?: ""
                            val root = json.parseToJsonElement(body).jsonObject
                            val linksArr = root["data"]?.jsonObject?.get("links")?.jsonArray
                            if (linksArr != null) {
                                for (elem in linksArr) {
                                    val obj = elem.jsonObject
                                    val host = obj["host"]?.jsonPrimitive?.contentOrNull ?: "Premium"
                                    val hostUrl = obj["url"]?.jsonPrimitive?.contentOrNull ?: continue

                                    // Extract via ExtractorRegistry (e.g. FilemoonExtractor)
                                    val extractedSources: List<StreamSource> = try {
                                        ExtractorRegistry.resolveUrl(hostUrl, "https://www.1shows.bz/")
                                    } catch (_: Exception) {
                                        emptyList()
                                    }
                                    for (ext in extractedSources) {
                                        if (emittedStreamUrls.add(ext.url)) {
                                            send(StreamEmission.SourceFound(ext))
                                        }
                                    }

                                    // Also emit host embed mirror if it is a verified direct media link
                                    if (isPlayableDirectVideo(hostUrl)) {
                                        val hostDisplay = host.replaceFirstChar { if (it.isLowerCase()) it.titlecase() else it.toString() }
                                        val embedSource = StreamSource(
                                            url = hostUrl,
                                            serverName = "Premium Embed [$hostDisplay]",
                                            resolutionLabel = "FHD Direct",
                                            quality = "Premium Embed [$hostDisplay]",
                                            isM3u8 = hostUrl.contains(".m3u8"),
                                            releaseType = AudioReleaseType.SUB,
                                            headers = mapOf(
                                                "Referer" to "https://www.1shows.bz/",
                                                "Origin" to "https://www.1shows.bz",
                                                "User-Agent" to defaultHeaders["User-Agent"]!!
                                            )
                                        )
                                        if (emittedStreamUrls.add(embedSource.url)) {
                                            send(StreamEmission.SourceFound(embedSource))
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (_: Exception) {}
        }

        // 6. Fetch Viduki Main Dynamic Server Pool (Leon, Jill, Ada, Claire, Rebecca, Rose, Sherry, Hunk, Ethan, Chris, Wesker, Grace, Ashley)
        launch {
            try {
                val mainBase = "https://api.viduki.net"
                val srvReq = Request.Builder()
                    .url("$mainBase/main/servers")
                    .header("Referer", "https://www.viduki.net/")
                    .header("Origin", "https://www.viduki.net")
                    .header("User-Agent", defaultHeaders["User-Agent"]!!)
                    .build()
                val srvBody = client.newCall(srvReq).execute().use { it.body?.string() ?: "" }
                if (srvBody.isNotBlank()) {
                    val srvArr = json.parseToJsonElement(srvBody).jsonArray
                    val nonce = OneShowsWasmEngine.getVidukiSessionNonce(client, mainBase)
                    if (!nonce.isNullOrBlank()) {
                        coroutineScope {
                            for (elem in srvArr) {
                                val srvObj = elem.jsonObject
                                val srvName = srvObj["name"]?.jsonPrimitive?.contentOrNull ?: continue
                                val srvLang = srvObj["language"]?.jsonPrimitive?.contentOrNull ?: "ENGLISH"

                                launch {
                                    try {
                                        val path = if (isTv && season != null && episode != null) {
                                            "/main/tv/$tmdbId/$season/$episode?srv=$srvName"
                                        } else {
                                            "/main/movie/$tmdbId?srv=$srvName"
                                        }

                                        val decryptedJson = OneShowsWasmEngine.decryptVidukiMainStream(
                                            client = client,
                                            baseUrl = mainBase,
                                            nonce = nonce,
                                            pathAndQuery = path
                                        )

                                        if (!decryptedJson.isNullOrBlank()) {
                                            val dObj = json.parseToJsonElement(decryptedJson).jsonObject
                                            val streamObj = dObj["stream"]?.jsonObject
                                                ?: dObj["data"]?.jsonObject?.get("stream")?.jsonObject
                                            val streamUrl = streamObj?.get("url")?.jsonPrimitive?.contentOrNull
                                            if (!streamUrl.isNullOrBlank()) {
                                                val isHindi = srvLang.equals("HINDI", ignoreCase = true)
                                                val isVietnam = srvLang.equals("VIETNAM", ignoreCase = true)
                                                val audioReleaseType = when {
                                                    isHindi || isVietnam -> AudioReleaseType.DUB
                                                    else -> AudioReleaseType.ORIGINAL
                                                }
                                                val isoCode = when {
                                                    isHindi -> "hi"
                                                    isVietnam -> "vi"
                                                    else -> "en"
                                                }
                                                val langDisplayName = when {
                                                    isHindi -> "Hindi Dub"
                                                    isVietnam -> "Vietnamese Dub"
                                                    else -> "English"
                                                }
                                                val audioDesc = AudioTrackDescriptor(
                                                    languageName = langDisplayName,
                                                    isoCode = isoCode
                                                )

                                                val qualityLabel = when {
                                                    streamUrl.contains(".m3u8") -> "Auto (HLS)"
                                                    streamUrl.contains("h265") -> "1080p H.265"
                                                    streamUrl.contains(".mp4") || streamUrl.contains(".mkv") -> "1080p FHD Direct"
                                                    else -> "FHD Stream"
                                                }

                                                val displayTitle = if (audioReleaseType == AudioReleaseType.DUB) {
                                                    "Viduki Main [$srvName] ($langDisplayName)"
                                                } else {
                                                    "Viduki Main [$srvName]"
                                                }

                                                val source = StreamSource(
                                                    url = streamUrl,
                                                    serverName = "Viduki Main [$srvName]",
                                                    resolutionLabel = qualityLabel,
                                                    quality = displayTitle,
                                                    isM3u8 = streamUrl.contains(".m3u8"),
                                                    releaseType = audioReleaseType,
                                                    audioTracks = listOf(audioDesc),
                                                    headers = mapOf(
                                                        "Referer" to "https://www.viduki.net/",
                                                        "Origin" to "https://www.viduki.net",
                                                        "User-Agent" to defaultHeaders["User-Agent"]!!
                                                    )
                                                )
                                                if (emittedStreamUrls.add(source.url)) {
                                                    send(StreamEmission.SourceFound(source))
                                                }
                                            }
                                        }
                                    } catch (_: Exception) {}
                                }
                            }
                        }
                    }
                }
            } catch (_: Exception) {}
        }

        // 7. Fetch Vidrock High-Speed CDN Streams (Nova, Atlas, Luna, Orion) natively decrypted via AES-256-GCM
        launch {
            try {
                val vidrockApiUrl = if (isTv && season != null && episode != null) {
                    "https://vidrock.net/api/tv/$tmdbId/$season/$episode"
                } else {
                    "https://vidrock.net/api/movie/$tmdbId"
                }
                val vReq = Request.Builder()
                    .url(vidrockApiUrl)
                    .header("Referer", "https://vidrock.to/")
                    .header("Origin", "https://vidrock.to")
                    .header("User-Agent", defaultHeaders["User-Agent"]!!)
                    .build()
                client.newCall(vReq).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val body = resp.body?.string() ?: ""
                        val root = json.parseToJsonElement(body).jsonObject
                        for ((srvName, srvElem) in root) {
                            val encUrl = srvElem.jsonObject["url"]?.jsonPrimitive?.contentOrNull ?: continue
                            val decryptedUrl = OneShowsWasmEngine.decryptVidrockPayload(encUrl)
                            if (!decryptedUrl.isNullOrBlank() && decryptedUrl.startsWith("http") && (decryptedUrl.contains(".m3u8") || decryptedUrl.contains(".mp4"))) {
                                val source = StreamSource(
                                    url = decryptedUrl,
                                    serverName = "Vidrock [$srvName]",
                                    resolutionLabel = "1080p HLS",
                                    quality = "Vidrock [$srvName]",
                                    isM3u8 = decryptedUrl.contains(".m3u8"),
                                    releaseType = AudioReleaseType.SUB,
                                    headers = mapOf(
                                        "Referer" to "https://vidrock.to/",
                                        "Origin" to "https://vidrock.to",
                                        "User-Agent" to defaultHeaders["User-Agent"]!!
                                    )
                                )
                                if (emittedStreamUrls.add(source.url)) {
                                    send(StreamEmission.SourceFound(source))
                                }
                            }
                        }
                    }
                }
            } catch (_: Exception) {}
        }

        // 8. Fetch Vidy High-Speed Streams & Multi-Language Audio natively decrypted via PRNG stream cipher
        launch {
            try {
                val seedReq = Request.Builder()
                    .url("https://api.wecollege.net/seed?mediaId=$tmdbId")
                    .header("Referer", "https://www.vidy.st/")
                    .header("Origin", "https://www.vidy.st")
                    .header("User-Agent", defaultHeaders["User-Agent"]!!)
                    .build()
                val seed = client.newCall(seedReq).execute().use { seedResp ->
                    if (seedResp.isSuccessful) {
                        val seedBody = seedResp.body?.string() ?: ""
                        json.parseToJsonElement(seedBody).jsonObject["seed"]?.jsonPrimitive?.contentOrNull
                    } else null
                }

                if (!seed.isNullOrBlank()) {
                    val vidyServers = listOf(
                        "atlanta" to "Atlanta",
                        "seattle" to "Seattle",
                        "denver" to "Denver",
                        "phoenix" to "Phoenix",
                        "portland" to "Portland",
                        "austin" to "Austin",
                        "dallas" to "Dallas",
                        "paris" to "Paris (French Dub)",
                        "delhi" to "Delhi (Hindi Dub)",
                        "cancun" to "Cancun (Spanish Dub)"
                    )

                    coroutineScope {
                        for ((srvKey, srvLabel) in vidyServers) {
                            launch {
                                try {
                                    val qMediaType = if (isTv) "tv" else "movie"
                                    val qParams = StringBuilder("mediaType=$qMediaType&tmdbId=$tmdbId&enc=2&seed=$seed")
                                    if (isTv && season != null && episode != null) {
                                        qParams.append("&seasonId=$season&episodeId=$episode")
                                    }
                                    val qUrl = "https://api.wecollege.net/$srvKey/sources?$qParams"
                                    val req = Request.Builder()
                                        .url(qUrl)
                                        .header("Referer", "https://www.vidy.st/")
                                        .header("Origin", "https://www.vidy.st")
                                        .header("User-Agent", defaultHeaders["User-Agent"]!!)
                                        .build()

                                    client.newCall(req).execute().use { resp ->
                                        if (resp.isSuccessful) {
                                            val body = resp.body?.string() ?: ""
                                            val decryptedJson = OneShowsWasmEngine.decryptVidyPayload(body, seed, tmdbId)
                                            if (!decryptedJson.isNullOrBlank()) {
                                                val root = json.parseToJsonElement(decryptedJson).jsonObject

                                                // Parse subtitles
                                                val subsArr = root["subtitles"]?.jsonArray
                                                if (subsArr != null) {
                                                    for (subElem in subsArr) {
                                                        val subObj = subElem.jsonObject
                                                        val subUrl = subObj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                                                        val subLang = subObj["language"]?.jsonPrimitive?.contentOrNull
                                                            ?: subObj["lang"]?.jsonPrimitive?.contentOrNull
                                                            ?: "English"
                                                        val isHi = subLang.contains("[CC]", ignoreCase = true)
                                                        val cleanLang = subLang.replace(Regex("""\s*\[CC\]""", RegexOption.IGNORE_CASE), "").trim()
                                                        val variantKey = if (isHi) "${cleanLang.lowercase()} [cc]" else cleanLang.lowercase()
                                                        if (emittedSubLangs.add(variantKey) && emittedSubUrls.add(subUrl)) {
                                                            send(StreamEmission.SubtitleFound(SubtitleTrack(url = subUrl, language = subLang)))
                                                        }
                                                    }
                                                }

                                                // Parse sources
                                                val sourcesArr = root["sources"]?.jsonArray
                                                if (sourcesArr != null) {
                                                    val isDub = srvKey == "delhi" || srvKey == "cancun" || srvKey == "paris"
                                                    val audioType = if (isDub) AudioReleaseType.DUB else AudioReleaseType.SUB
                                                    val langIso = when (srvKey) {
                                                        "delhi" -> "hi"
                                                        "cancun" -> "es"
                                                        "paris" -> "fr"
                                                        else -> "en"
                                                    }
                                                    val langName = when (srvKey) {
                                                        "delhi" -> "Hindi"
                                                        "cancun" -> "Spanish"
                                                        "paris" -> "French"
                                                        else -> "English"
                                                    }
                                                    val audioDesc = AudioTrackDescriptor(
                                                        languageName = langName,
                                                        isoCode = langIso
                                                    )

                                                    for (sElem in sourcesArr) {
                                                        val sObj = sElem.jsonObject
                                                        val sUrl = sObj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                                                        val sQuality = sObj["quality"]?.jsonPrimitive?.contentOrNull ?: "Auto"
                                                        val isM3u8 = sUrl.contains(".m3u8") || sObj["type"]?.jsonPrimitive?.contentOrNull == "hls"

                                                        val source = StreamSource(
                                                            url = sUrl,
                                                            serverName = "Vidy [$srvLabel]",
                                                            resolutionLabel = sQuality,
                                                            quality = "Vidy [$srvLabel] ($sQuality)",
                                                            isM3u8 = isM3u8,
                                                            releaseType = audioType,
                                                            audioTracks = listOf(audioDesc),
                                                            headers = mapOf(
                                                                "Referer" to "https://www.vidy.st/",
                                                                "Origin" to "https://www.vidy.st",
                                                                "User-Agent" to defaultHeaders["User-Agent"]!!
                                                            )
                                                        )
                                                        if (emittedStreamUrls.add(source.url)) {
                                                            send(StreamEmission.SourceFound(source))
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                } catch (_: Exception) {}
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
        StreamResult(streams = streamSources, subtitles = sortedSubs)
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

        val directCdnOptions = mutableListOf<DownloadOption>()
        val vidzeeOptions = mutableListOf<DownloadOption>()

        coroutineScope {
            // 1. Fetch direct CDN downloads from MakimaDL (111477.xyz, direct MKV/MP4 files)
            val cdnJob = async {
                try {
                    val tokenReq = newRequestBuilder("https://api.viduki.net/download-token", vidukiHeaders).build()
                    val token = client.newCall(tokenReq).execute().use { tokenResp ->
                        if (tokenResp.isSuccessful) {
                            val tokenBody = tokenResp.body?.string() ?: ""
                            json.parseToJsonElement(tokenBody).jsonObject["token"]?.jsonPrimitive?.contentOrNull
                        } else null
                    }

                    if (!token.isNullOrBlank()) {
                        val dlUrl = if (isTv && season != null && episode != null) {
                            "https://api.viduki.net/download/tv/$tmdbId/$season/$episode"
                        } else {
                            "https://api.viduki.net/download/movie/$tmdbId"
                        }

                        val payloadReq = Request.Builder()
                            .url(dlUrl)
                            .header("Referer", "https://www.1shows.bz/")
                            .header("Origin", "https://www.1shows.bz")
                            .header("User-Agent", defaultHeaders["User-Agent"]!!)
                            .header("x-download-token", token)
                            .build()

                        client.newCall(payloadReq).execute().use { payloadResp ->
                            if (payloadResp.isSuccessful) {
                                val payloadBody = payloadResp.body?.string() ?: ""
                                val payloadObj = json.parseToJsonElement(payloadBody).jsonObject

                                val iv = payloadObj["iv"]?.jsonPrimitive?.contentOrNull
                                val tag = payloadObj["tag"]?.jsonPrimitive?.contentOrNull
                                val ct = payloadObj["ct"]?.jsonPrimitive?.contentOrNull

                                if (iv != null && tag != null && ct != null) {
                                    val decryptedJson = OneShowsWasmEngine.decryptPayload(client, token, iv, tag, ct)
                                    if (decryptedJson != null) {
                                        val decObj = json.parseToJsonElement(decryptedJson).jsonObject
                                        val sourcesArr = decObj["sources"]?.jsonArray
                                        if (sourcesArr != null) {
                                            for (sElem in sourcesArr) {
                                                val sObj = sElem.jsonObject
                                                val sourceUrl = sObj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                                                val label = sObj["label"]?.jsonPrimitive?.contentOrNull ?: "Direct Download"

                                                // Strictly validate direct playable media: reject lockers and landing pages
                                                if (!isPlayableDirectVideo(sourceUrl)) continue

                                                val sizeRegex = Regex("""\(([\d.]+\s*(?:GB|MB|Gb|mb))\)""")
                                                val sizeMatch = sizeRegex.find(label)
                                                val size = sizeMatch?.groupValues?.get(1) ?: "~1.5 GB"

                                                val sizeGbRegex = Regex("""\(([\d.]+)\s*(?:GB|Gb)\)""")
                                                val sizeGb = sizeGbRegex.find(label)?.groupValues?.get(1)?.toDoubleOrNull()
                                                if (sizeGb != null && sizeGb > 15.0) {
                                                    continue
                                                }

                                                val quality = when {
                                                    label.contains("2160p", ignoreCase = true) || label.contains("4K", ignoreCase = true) -> "4K 2160p"
                                                    label.contains("1080p", ignoreCase = true) -> "1080p FHD"
                                                    label.contains("720p", ignoreCase = true) -> "720p HD"
                                                    label.contains("480p", ignoreCase = true) -> "480p SD"
                                                    else -> "Direct Video"
                                                }

                                                synchronized(directCdnOptions) {
                                                    directCdnOptions.add(
                                                        DownloadOption(
                                                            title = label,
                                                            quality = quality,
                                                            size = size,
                                                            url = sourceUrl,
                                                            source = "1Shows CDN",
                                                            provider = name,
                                                            headers = mapOf(
                                                                "Referer" to "https://www.1shows.bz/",
                                                                "Origin" to "https://www.1shows.bz",
                                                                "User-Agent" to defaultHeaders["User-Agent"]!!
                                                            )
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
            }

            // 2. Fetch authentic Vidzee HLS streams (multi-language & native high-speed servers)
            val vidzeeJob = async {
                val streams = fetchVidzeeStreams(tmdbId, season, episode, isTv)
                for (vStream in streams) {
                    val quality = when {
                        vStream.serverLabel.contains("2160", ignoreCase = true) || vStream.serverLabel.contains("4K", ignoreCase = true) -> "4K 2160p"
                        vStream.serverLabel.contains("1080", ignoreCase = true) || vStream.url.contains("1080") -> "1080p FHD"
                        vStream.serverLabel.contains("720", ignoreCase = true) || vStream.url.contains("720") -> "720p HD"
                        vStream.serverLabel.contains("480", ignoreCase = true) || vStream.url.contains("480") -> "480p SD"
                        else -> "1080p FHD"
                    }

                    val estimatedSize = when (quality) {
                        "4K 2160p" -> "~4.0 GB"
                        "1080p FHD" -> "~1.5 GB"
                        "720p HD" -> "~950 MB"
                        "480p SD" -> "~500 MB"
                        else -> "~1.2 GB"
                    }

                    val displayTitle = if (!vStream.language.isNullOrBlank() && !vStream.serverLabel.contains(vStream.language, ignoreCase = true)) {
                        "${vStream.serverLabel} [${vStream.language}]"
                    } else {
                        vStream.serverLabel
                    }

                    synchronized(vidzeeOptions) {
                        vidzeeOptions.add(
                            DownloadOption(
                                title = displayTitle,
                                quality = quality,
                                size = estimatedSize,
                                url = vStream.url,
                                source = "1Shows Stream",
                                provider = name,
                                headers = mapOf(
                                    "Referer" to "https://player.vidzee.wtf/",
                                    "Origin" to "https://player.vidzee.wtf",
                                    "User-Agent" to defaultHeaders["User-Agent"]!!
                                )
                            )
                        )
                    }
                }
            }

            cdnJob.await()
            vidzeeJob.await()
        }

        val combined = mutableListOf<DownloadOption>()
        combined.addAll(directCdnOptions)
        combined.addAll(vidzeeOptions)
        combined
    }

    private data class VidzeeStream(
        val url: String,
        val serverLabel: String,
        val language: String?,
        val isM3u8: Boolean = true
    )

    private fun getIsoLanguage(lang: String): String {
        return when (lang.lowercase()) {
            "hindi" -> "hi"
            "english" -> "en"
            "spanish", "español" -> "es"
            "french", "français" -> "fr"
            "german", "deutsch" -> "de"
            "japanese", "nihongo" -> "ja"
            "korean" -> "ko"
            "chinese" -> "zh"
            "italian", "italiano" -> "it"
            "portuguese" -> "pt"
            "russian" -> "ru"
            "vietnam", "vietnamese" -> "vi"
            "indonesian" -> "id"
            "tamil" -> "ta"
            "telugu" -> "te"
            "arabic" -> "ar"
            else -> "en"
        }
    }

    private fun isPlayableDirectVideo(url: String): Boolean {
        val clean = url.trim()
        if (clean.isBlank()) return false
        val lower = clean.lowercase()

        // Filter out HTML lockers, web hosters, landing pages, URL shorteners, and redirectors
        val isWebLocker = lower.contains("kmhd.me") ||
                lower.contains("hubcloud") ||
                lower.contains("greenmotors") ||
                lower.contains("fastxyz") ||
                lower.contains("goodstream") ||
                lower.contains("moondl") ||
                lower.contains("takefile") ||
                lower.contains("filecrypt") ||
                lower.contains("droplink") ||
                lower.contains("shrinkme") ||
                lower.contains("gdtot") ||
                lower.contains("drive.google.com") ||
                lower.contains("mega.nz") ||
                lower.contains("youtube.com") ||
                lower.contains("youtu.be") ||
                lower.contains("/cloud/") ||
                lower.contains("/drive/") ||
                lower.contains("/play?") ||
                lower.contains("/file/") ||
                lower.contains("/folder/")

        if (isWebLocker) return false

        // Check for direct video extension before query string or known CDN video storage
        val pathWithoutQuery = lower.substringBefore("?").substringBefore("#")
        val hasVideoExt = pathWithoutQuery.endsWith(".mkv") ||
                pathWithoutQuery.endsWith(".mp4") ||
                pathWithoutQuery.endsWith(".webm") ||
                pathWithoutQuery.endsWith(".m3u8") ||
                pathWithoutQuery.endsWith(".ts")

        val isDirectCdn = lower.contains("111477.xyz") && !lower.contains("/play")

        return hasVideoExt || isDirectCdn
    }

    private suspend fun fetchVidzeeStreams(
        tmdbId: String,
        season: Int?,
        episode: Int?,
        isTv: Boolean
    ): List<VidzeeStream> = withContext(Dispatchers.IO) {
        val discoveredServers = mutableListOf<Pair<String, String>>()

        // Probe dynamic languages
        try {
            val langUrl = if (isTv && season != null && episode != null) {
                "https://core.vidzee.wtf/streams/languages/tv/$tmdbId/$season/$episode"
            } else {
                "https://core.vidzee.wtf/streams/languages/movie/$tmdbId"
            }
            val req = newRequestBuilder(langUrl, vidzeeHeaders).build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    val body = resp.body?.string() ?: ""
                    val root = json.parseToJsonElement(body).jsonObject
                    val langs = root["languages"]?.jsonArray
                    langs?.forEach { lElem ->
                        val langName = lElem.jsonPrimitive.contentOrNull ?: return@forEach
                        discoveredServers.add("v4:$langName" to "Vidzee $langName (Multi-Audio HLS)")
                    }
                }
            }
        } catch (_: Exception) {}

        // Add standard native servers
        discoveredServers.add("dcloud" to "Dcloud Premium (1080p HLS)")
        discoveredServers.add("tik" to "TCloud Fast (1080p HLS)")
        discoveredServers.add("ipcloud" to "IPcloud (HLS)")
        discoveredServers.add("v6:Hindi" to "Vidzee Hindi v3 (HLS)")
        discoveredServers.add("v6:English" to "Vidzee English (Multi-Audio HLS)")

        val results = mutableListOf<VidzeeStream>()
        coroutineScope {
            val serverJobs = discoveredServers.map { (serverId, serverLabel) ->
                async {
                    try {
                        val streamUrl = if (isTv && season != null && episode != null) {
                            "https://core.vidzee.wtf/streams/tv/$tmdbId/$season/$episode?s=$serverId&e=1"
                        } else {
                            "https://core.vidzee.wtf/streams/movie/$tmdbId?s=$serverId&e=1"
                        }

                        val req = Request.Builder()
                            .url(streamUrl)
                            .header("Referer", "https://player.vidzee.wtf/")
                            .header("Origin", "https://player.vidzee.wtf")
                            .header("User-Agent", defaultHeaders["User-Agent"]!!)
                            .build()

                        client.newCall(req).execute().use { resp ->
                            if (resp.isSuccessful) {
                                val body = resp.body?.string() ?: ""
                                val root = json.parseToJsonElement(body).jsonObject
                                val c = root["c"]?.jsonPrimitive?.contentOrNull
                                if (!c.isNullOrBlank()) {
                                    val decryptedJson = OneShowsWasmEngine.decryptVidzeePayload(client, c)
                                    if (decryptedJson != null) {
                                        val decObj = json.parseToJsonElement(decryptedJson).jsonObject
                                        val finalUrl = decObj["url"]?.jsonPrimitive?.contentOrNull
                                        val lang = decObj["language"]?.jsonPrimitive?.contentOrNull
                                        if (!finalUrl.isNullOrBlank() && finalUrl.startsWith("http")) {
                                            synchronized(results) {
                                                results.add(
                                                    VidzeeStream(
                                                        url = finalUrl,
                                                        serverLabel = serverLabel,
                                                        language = lang,
                                                        isM3u8 = true
                                                    )
                                                )
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } catch (_: Exception) {}
                }
            }
            serverJobs.awaitAll()
        }
        results
    }

    override suspend fun resolveLogo(mediaItem: MediaItem): String? = withContext(Dispatchers.IO) {
        val tmdbId = mediaItem.id
        if (tmdbId.isBlank()) return@withContext null

        logoCache[tmdbId]?.let { return@withContext it }

        val itemLogo = mediaItem.logoUrl
        if (!itemLogo.isNullOrBlank()) {
            logoCache[tmdbId] = itemLogo
            return@withContext itemLogo
        }

        val isTv = mediaItem.type == MediaType.TV_SERIES
        val logo = TmdbBridge.resolveLogo(client, tmdbId, isTv)
        if (logo != null) {
            logoCache[tmdbId] = logo
            return@withContext logo
        }

        null
    }

    override suspend fun fetchCast(mediaId: String, imdbId: String?, type: MediaType): List<CastMember> = emptyList()
}
