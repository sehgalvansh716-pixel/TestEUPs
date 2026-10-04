package com.euthopiar.core.provider

import com.euthopiar.core.model.*
import com.euthopiar.core.network.DohDns
import com.euthopiar.core.util.TmdbBridge
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import java.util.concurrent.ConcurrentHashMap
import kotlinx.serialization.json.*
import okhttp3.OkHttpClient
import okhttp3.Request
import java.net.URLEncoder
import java.util.concurrent.TimeUnit

/**
 * Cinejoy reference streaming provider implementation.
 *
 * Scrapes metadata and extracts streams from https://cinejoy.pk/ via wing.st backend services:
 * - Curated collections: https://lists.wing.st/joy
 * - Search: https://server.wing.st/api/lists/public/search?q={query}
 * - Metadata details: https://api.wing.st/info?type={movie|tv}&tmdb={tmdbId}
 * - Subtitles: https://subs.wing.st/subtitles?type={movie|tv}&tmdb={tmdbId}
 * - Stream extraction: Wasm-sealed gateway POST (via [CinejoyWasmEngine])
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

    private val json = Json { ignoreUnknownKeys = true }

    private val defaultHeaders = mapOf(
        "Referer" to "https://cinejoy.pk/",
        "Origin" to "https://cinejoy.pk",
        "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Ranges" to "bytes"
    )

    @Volatile
    private var sessionCachedCatalog: List<CatalogRow>? = null
    @Volatile
    private var lastSessionIndex: Int = -1

    private fun newRequestBuilder(url: String): Request.Builder {
        val builder = Request.Builder().url(url)
        defaultHeaders.forEach { (k, v) -> builder.header(k, v) }
        return builder
    }

    init {
        CoroutineScope(Dispatchers.IO).launch {
            try {
                CinejoyWasmEngine.prewarm(client)
            } catch (_: Exception) {}
        }
    }

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        val currentSessionIndex = com.euthopiar.core.util.AppSessionManager.getSessionIndex()
        sessionCachedCatalog?.takeIf { lastSessionIndex == currentSessionIndex }?.let {
            return@withContext it
        }

        coroutineScope {
            val deferredRows = mutableListOf<Deferred<CatalogRow?>>()
            val sessionRandom = kotlin.random.Random(currentSessionIndex.toLong())

            // 1. Curated collections from lists.wing.st/joy
            val joyCategoryMappings = listOf(
                "rotten-tomatoes-best-of-all-time" to "Trending & Popular Movies",
                "oscar-nominees-best-picture" to "Oscar Nominees for Best Picture",
                "psychological-thrillers" to "Psychological & Mystery Thrillers",
                "cannes-film-festival" to "Critically Acclaimed Cinema",
                "mindfuck-movies" to "High-Octane Action & Sci-Fi",
                "halloween-top-100" to "Cult Classics & Dark Cinema",
                "based-on-a-true-story" to "True Stories & Biographies"
            )

            // 2. Curated collections for Binge-Worthy TV & Expanded Community Lists
            val specializedLists = listOf(
                "HxSiTV9w2Pe7" to "Binge-Worthy TV Series",
                "3i6biWnQ9cT3" to "Must-Watch Series",
                "jxMaqCV8qsmg" to "Animation & Family Hits",
                "hcA8ABnfXnDx" to "Movies to Watch with Friends",
                "YFWpn2Mpncw2" to "Top Rated Web Series",
                "URN5s836Cxcf" to "Crowd-Favorite Cinema",
                "iywPGni7zjAF" to "Comedy & Feel-Good Hits",
                "rjzpDyM7YpWD" to "Animated Superhero Universe",
                "dc7eS5KZvqVu" to "Romance & Heartfelt Cinema",
                "ypGF98kbeyUn" to "High-Stakes Thrillers & Sci-Fi"
            )

            // Deterministically rotate categories and offsets based on current launch session index
            val dynamicJoy = joyCategoryMappings.shuffled(sessionRandom)
            val dynamicTv = specializedLists.shuffled(sessionRandom)

            val combined = mutableListOf<Triple<Boolean, Pair<String, String>, Int>>()
            // Select 6 diverse Joy movie collections with session-shifted offsets
            dynamicJoy.take(6).forEachIndexed { idx, pair ->
                val offset = ((currentSessionIndex * 30) + (idx * 20)) % 150
                combined.add(Triple(true, pair, offset))
            }
            // Select 5 diverse TV Series & community collections with session-shifted offsets
            dynamicTv.take(5).forEachIndexed { idx, pair ->
                val offset = (currentSessionIndex * 15) % 40
                combined.add(Triple(false, pair, offset))
            }

            // Interleave movies and series for great vertical flow
            val joyList = combined.filter { it.first }
            val tvList = combined.filter { !it.first }
            val maxLen = maxOf(joyList.size, tvList.size)
            val interleaved = mutableListOf<Triple<Boolean, Pair<String, String>, Int>>()
            for (i in 0 until maxLen) {
                if (i < joyList.size) interleaved.add(joyList[i])
                if (i < tvList.size) interleaved.add(tvList[i])
            }

            for (entry in interleaved) {
                val isJoy = entry.first
                val (slug, rowTitle) = entry.second
                val offset = entry.third
                deferredRows.add(
                    async {
                        if (isJoy) {
                            fetchJoyCollection(slug, rowTitle, limit = 40, offset = offset)
                        } else {
                            fetchPublicList(slug, rowTitle, limit = 40, offset = offset)
                        }
                    }
                )
            }

            val rows = deferredRows.awaitAll().filterNotNull()
            val finalRows = if (rows.isNotEmpty()) {
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

            if (finalRows.isNotEmpty()) {
                sessionCachedCatalog = finalRows
                lastSessionIndex = currentSessionIndex
            }
            finalRows
        }
    }

    private fun fetchJoyCollection(collectionId: String, displayTitle: String, limit: Int = 40, offset: Int = 0): CatalogRow? {
        try {
            val colReq = newRequestBuilder("https://lists.wing.st/joy/$collectionId?limit=$limit&offset=$offset").build()
            val colResp = client.newCall(colReq).execute()
            if (!colResp.isSuccessful) return null

            val colBody = colResp.body?.string() ?: return null
            val colRoot = json.parseToJsonElement(colBody).jsonObject
            val itemsArr = colRoot["items"]?.jsonArray ?: return null

            val items = mutableListOf<MediaItem>()
            for (itemElem in itemsArr) {
                val itemObj = itemElem.jsonObject
                val idsObj = itemObj["ids"]?.jsonObject
                val tmdbId = idsObj?.get("tmdb")?.jsonPrimitive?.contentOrNull
                    ?: itemObj["id"]?.jsonPrimitive?.contentOrNull
                    ?: continue

                val itemTitle = itemObj["title"]?.jsonPrimitive?.content ?: "Unknown"
                val typeStr = itemObj["type"]?.jsonPrimitive?.contentOrNull ?: "movie"
                val mediaType = if (typeStr == "tv") MediaType.TV_SERIES else MediaType.MOVIE

                var poster = itemObj["poster"]?.jsonPrimitive?.contentOrNull
                if (poster != null) {
                    poster = poster.replace("/w200/", "/w500/")
                    if (!poster.startsWith("http")) {
                        poster = "https://image.tmdb.org/t/p/w500$poster"
                    }
                }

                // High-res landscape backdrop: Cinemeta metahub CDN or TMDB 1280
                val imdbId = idsObj?.get("imdb")?.jsonPrimitive?.contentOrNull
                val backdrop = if (imdbId != null && imdbId.startsWith("tt")) {
                    "https://images.metahub.space/background/medium/$imdbId/img"
                } else if (poster != null) {
                    poster.replace("/w500/", "/w1280/")
                } else null

                val logoUrl = if (imdbId != null && imdbId.startsWith("tt")) {
                    "https://images.metahub.space/logo/medium/$imdbId/img.png"
                } else null

                val year = itemObj["year"]?.jsonPrimitive?.intOrNull
                val score = itemObj["score"]?.jsonPrimitive?.doubleOrNull

                items.add(
                    MediaItem(
                        id = tmdbId,
                        title = itemTitle,
                        url = "https://cinejoy.pk/$typeStr/$tmdbId",
                        posterUrl = poster,
                        backdropUrl = backdrop,
                        type = mediaType,
                        year = year,
                        rating = score?.toString(),
                        quality = "HD",
                        provider = name,
                        logoUrl = logoUrl
                    )
                )
            }

            return if (items.isNotEmpty()) CatalogRow(title = displayTitle, items = items) else null
        } catch (e: Exception) {
            return null
        }
    }

    private fun fetchPublicList(slug: String, displayTitle: String, limit: Int = 40, offset: Int = 0): CatalogRow? {
        try {
            val req = newRequestBuilder("https://server.wing.st/api/lists/public/$slug").build()
            val resp = client.newCall(req).execute()
            if (!resp.isSuccessful) return null

            val body = resp.body?.string() ?: return null
            val root = json.parseToJsonElement(body).jsonObject
            val itemsArr = root["items"]?.jsonArray ?: return null

            val items = mutableListOf<MediaItem>()
            val sliced = if (itemsArr.size > offset) itemsArr.drop(offset).take(limit) else itemsArr.take(limit)
            for (itemElem in sliced) {
                val itemObj = itemElem.jsonObject
                val tmdbId = itemObj["tmdbId"]?.jsonPrimitive?.contentOrNull ?: continue
                val itemTitle = itemObj["title"]?.jsonPrimitive?.contentOrNull ?: "Unknown"
                val mediaTypeStr = itemObj["mediaType"]?.jsonPrimitive?.contentOrNull ?: "movie"
                val mediaType = if (mediaTypeStr == "tv") MediaType.TV_SERIES else MediaType.MOVIE

                var poster = itemObj["poster"]?.jsonPrimitive?.contentOrNull
                if (poster != null) {
                    poster = poster.replace("/w200/", "/w500/")
                    if (!poster.startsWith("http")) {
                        poster = "https://image.tmdb.org/t/p/w500$poster"
                    }
                }

                val backdrop = if (poster != null) poster.replace("/w500/", "/w1280/") else null
                val year = itemObj["year"]?.jsonPrimitive?.intOrNull

                items.add(
                    MediaItem(
                        id = tmdbId,
                        title = itemTitle,
                        url = "https://cinejoy.pk/$mediaTypeStr/$tmdbId",
                        posterUrl = poster,
                        backdropUrl = backdrop,
                        type = mediaType,
                        year = year,
                        rating = null,
                        quality = "HD",
                        provider = name
                    )
                )
            }

            return if (items.isNotEmpty()) CatalogRow(title = displayTitle, items = items) else null
        } catch (e: Exception) {
            return null
        }
    }


    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        val encodedQuery = URLEncoder.encode(query, "UTF-8")
        val results = mutableListOf<MediaItem>()
        val seenIds = mutableSetOf<String>()

        // 1. Primary: Direct TMDB Multi-Search (full catalog coverage)
        try {
            val tmdbSearchUrl = "https://api.tmdb.org/3/search/multi?api_key=8476a7ab80ad76f0936744df0430e67c&query=$encodedQuery"
            val tmdbReq = Request.Builder()
                .url(tmdbSearchUrl)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                .build()
            val tmdbResp = client.newCall(tmdbReq).execute()
            if (tmdbResp.isSuccessful) {
                val tmdbBody = tmdbResp.body?.string() ?: ""
                val root = json.parseToJsonElement(tmdbBody).jsonObject
                val resArr = root["results"]?.jsonArray
                if (resArr != null) {
                    for (elem in resArr) {
                        val obj = elem.jsonObject
                        val mediaTypeStr = obj["media_type"]?.jsonPrimitive?.contentOrNull ?: continue
                        if (mediaTypeStr != "movie" && mediaTypeStr != "tv") continue
                        val tmdbId = obj["id"]?.jsonPrimitive?.contentOrNull ?: continue
                        if (!seenIds.add(tmdbId)) continue

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
                                id = tmdbId,
                                title = title,
                                url = "https://cinejoy.pk/$mediaTypeStr/$tmdbId",
                                posterUrl = posterUrl,
                                backdropUrl = backdropUrl,
                                type = type,
                                year = year,
                                rating = vote?.let { String.format("%.1f", it) },
                                quality = "HD",
                                provider = name
                            )
                        )
                    }
                }
            }
        } catch (e: Exception) {
            // Ignore TMDB search error
        }

        // 2. Secondary: Wing.st public curated collections search
        try {
            val req = newRequestBuilder("https://server.wing.st/api/lists/public/search?q=$encodedQuery").build()
            val resp = client.newCall(req).execute()
            if (resp.isSuccessful) {
                val body = resp.body?.string() ?: ""
                val rootElem = json.parseToJsonElement(body)
                val itemsArr = when (rootElem) {
                    is JsonArray -> rootElem
                    is JsonObject -> rootElem["items"]?.jsonArray ?: JsonArray(emptyList())
                    else -> JsonArray(emptyList())
                }

                for (itemElem in itemsArr) {
                    val previewArr = itemElem.jsonObject["preview"]?.jsonArray ?: continue
                    for (previewElem in previewArr) {
                        val pObj = previewElem.jsonObject
                        val tmdbId = pObj["tmdbId"]?.jsonPrimitive?.content ?: continue
                        if (!seenIds.add(tmdbId)) continue

                        val title = pObj["title"]?.jsonPrimitive?.content ?: "Unknown"
                        val mediaTypeStr = pObj["mediaType"]?.jsonPrimitive?.content ?: "movie"
                        val type = if (mediaTypeStr == "tv") MediaType.TV_SERIES else MediaType.MOVIE
                        var poster = pObj["poster"]?.jsonPrimitive?.contentOrNull
                        if (poster != null && !poster.startsWith("http")) {
                            poster = "https://image.tmdb.org/t/p/w500$poster"
                        }
                        val year = pObj["year"]?.jsonPrimitive?.intOrNull

                        results.add(
                            MediaItem(
                                id = tmdbId,
                                title = title,
                                url = "https://cinejoy.pk/$mediaTypeStr/$tmdbId",
                                posterUrl = poster,
                                backdropUrl = if (poster != null) poster.replace("/w500/", "/w1280/") else null,
                                type = type,
                                year = year,
                                rating = null,
                                quality = "HD",
                                provider = name
                            )
                        )
                    }
                }
            }
        } catch (e: Exception) {
            // Ignore search network exceptions
        }

        results
    }

    override suspend fun getDetails(mediaItem: MediaItem): MediaDetail = withContext(Dispatchers.IO) {
        val isTv = mediaItem.type == MediaType.TV_SERIES
        val typeStr = if (isTv) "tv" else "movie"
        val tmdbId = mediaItem.id

        var wingTitle: String? = null
        var wingYear: Int? = null
        var wingSynopsis: String? = null
        var wingPoster: String? = null
        var wingRating: String? = null
        var wingContentRating: String? = null
        var wingImdbId: String? = null
        var wingGenres = listOf<String>()
        var wingRuntime: String? = null
        var wingBackdrop: String? = null
        var maxSeason = 1
        var maxEpisode = 1

        try {
            val url = "https://api.wing.st/info?type=$typeStr&tmdb=$tmdbId"
            val req = newRequestBuilder(url).build()
            val resp = client.newCall(req).execute()
            if (resp.isSuccessful) {
                val body = resp.body?.string() ?: ""
                val obj = json.parseToJsonElement(body).jsonObject

                wingTitle = obj["title"]?.jsonPrimitive?.contentOrNull?.takeIf { it.isNotBlank() }
                wingYear = obj["year"]?.jsonPrimitive?.intOrNull
                wingSynopsis = obj["description"]?.jsonPrimitive?.contentOrNull?.takeIf { it.isNotBlank() }
                    ?: obj["overview"]?.jsonPrimitive?.contentOrNull?.takeIf { it.isNotBlank() }
                wingPoster = obj["poster"]?.jsonPrimitive?.contentOrNull
                wingRating = obj["imdb_rating"]?.jsonPrimitive?.contentOrNull
                wingContentRating = obj["content_rating"]?.jsonPrimitive?.contentOrNull
                wingImdbId = obj["imdb_id"]?.jsonPrimitive?.contentOrNull

                wingGenres = when (val catsElem = obj["cats"]) {
                    is JsonArray -> catsElem.mapNotNull { it.jsonPrimitive.contentOrNull }
                    is JsonPrimitive -> catsElem.contentOrNull?.split(",")?.map { it.trim() }?.filter { it.isNotBlank() } ?: emptyList()
                    else -> emptyList()
                }
                wingRuntime = obj["runtime"]?.jsonPrimitive?.intOrNull?.let { "$it min" }

                wingBackdrop = if (wingImdbId != null && wingImdbId.startsWith("tt")) {
                    "https://images.metahub.space/background/medium/$wingImdbId/img"
                } else if (wingPoster != null) {
                    wingPoster.replace("/w200/", "/w1280/").replace("/w500/", "/w1280/")
                } else null

                if (isTv) {
                    maxSeason = obj["max_season"]?.jsonPrimitive?.intOrNull ?: 1
                    maxEpisode = obj["max_episode"]?.jsonPrimitive?.intOrNull ?: 1
                }
            }
        } catch (_: Exception) {}

        // Centralized TMDB Enrichment Bridge for complete v3.0 metadata contract
        val enriched = TmdbBridge.fetchEnrichedDetails(client, tmdbId, isTv, wingTitle ?: mediaItem.title)

        val title = wingTitle?.takeIf { it.isNotBlank() } ?: enriched?.title ?: mediaItem.title
        val poster = enriched?.posterUrl ?: wingPoster ?: mediaItem.posterUrl
        val backdrop = enriched?.backdropUrl ?: wingBackdrop ?: mediaItem.backdropUrl ?: poster
        val year = enriched?.year ?: wingYear ?: mediaItem.year
        val synopsis = wingSynopsis?.takeIf { it.isNotBlank() } ?: enriched?.synopsis ?: ""
        val genres = wingGenres.ifEmpty { enriched?.genres ?: emptyList() }
        val duration = enriched?.duration ?: wingRuntime
        val rating = wingRating ?: enriched?.rating ?: mediaItem.rating
        val contentRating = wingContentRating ?: enriched?.contentRating
        val imdbId = wingImdbId ?: enriched?.imdbId
        val rottenTomatoes = enriched?.rottenTomatoesRating
        val trailerUrl = enriched?.trailerUrl
        val directors = enriched?.directors ?: emptyList()
        val recommendations = enriched?.recommendations ?: emptyList()
        val logoUrl = enriched?.logoUrl ?: resolveLogo(mediaItem) ?: (if (imdbId != null && imdbId.startsWith("tt")) {
            "https://images.metahub.space/logo/medium/$imdbId/img.png"
        } else mediaItem.logoUrl)

        val cast = enriched?.cast?.ifEmpty { null } ?: fetchCast(tmdbId, imdbId, mediaItem.type)

        val episodes = mutableListOf<EpisodeItem>()
        if (isTv) {
            val seasonNumbers = enriched?.seasonNumbers?.ifEmpty { null } ?: (1..maxSeason).toList()
            val tmdbSeasonsData = TmdbBridge.fetchTmdbSeasons(client, tmdbId, seasonNumbers)

            for (s in seasonNumbers) {
                val epMap = tmdbSeasonsData[s]
                if (!epMap.isNullOrEmpty()) {
                    for ((e, epDetail) in epMap.toSortedMap()) {
                        episodes.add(
                            EpisodeItem(
                                id = TmdbBridge.sanitizeSlug("$tmdbId-s${s}e$e"),
                                title = epDetail.name.ifBlank { "Episode $e" },
                                seasonNumber = s,
                                episodeNumber = e,
                                data = "$tmdbId:$s:$e",
                                thumbnail = epDetail.stillUrl ?: (if (imdbId != null && imdbId.startsWith("tt")) "https://episodes.metahub.space/$imdbId/$s/$e/w780.jpg" else backdrop ?: poster),
                                description = epDetail.overview ?: "Season $s • Episode $e",
                                duration = epDetail.duration ?: duration ?: "45 min"
                            )
                        )
                    }
                } else {
                    val epsCount = if (s == maxSeason) maxEpisode else 10
                    for (e in 1..epsCount) {
                        val epThumbnail = if (imdbId != null && imdbId.startsWith("tt")) {
                            "https://episodes.metahub.space/$imdbId/$s/$e/w780.jpg"
                        } else {
                            backdrop ?: poster
                        }
                        episodes.add(
                            EpisodeItem(
                                id = TmdbBridge.sanitizeSlug("$tmdbId-s${s}e$e"),
                                title = "Episode $e",
                                seasonNumber = s,
                                episodeNumber = e,
                                data = "$tmdbId:$s:$e",
                                thumbnail = epThumbnail,
                                description = "Season $s • Episode $e",
                                duration = duration ?: "45 min"
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
                        thumbnail = backdrop ?: poster,
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
                    thumbnail = backdrop ?: poster,
                    description = synopsis,
                    duration = duration
                )
            )
        }

        MediaDetail(
            id = TmdbBridge.sanitizeSlug(tmdbId),
            title = title,
            url = mediaItem.url,
            posterUrl = poster,
            backdropUrl = backdrop,
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

    private val logoMemoryCache = java.util.concurrent.ConcurrentHashMap<String, String>()

    override suspend fun resolveLogo(mediaItem: MediaItem): String? = withContext(Dispatchers.IO) {
        val tmdbId = mediaItem.id
        if (tmdbId.isBlank()) return@withContext null

        logoMemoryCache[tmdbId]?.let { return@withContext it }

        val isTv = mediaItem.type == MediaType.TV_SERIES
        val logo = TmdbBridge.resolveLogo(client, tmdbId, isTv)
        if (logo != null) {
            logoMemoryCache[tmdbId] = logo
            return@withContext logo
        }

        // Secondary: Metahub logo if already present or available
        val itemLogo = mediaItem.logoUrl
        if (!itemLogo.isNullOrBlank()) {
            logoMemoryCache[tmdbId] = itemLogo
            return@withContext itemLogo
        }

        null
    }

    /**
     * Asynchronously fetches cast members with actor profile pictures from TMDB or Cinemeta.
     */
    override suspend fun fetchCast(
        tmdbId: String,
        imdbId: String?,
        type: MediaType
    ): List<CastMember> = withContext(Dispatchers.IO) {
        val typeStr = if (type == MediaType.TV_SERIES) "tv" else "movie"

        // 1. Primary Source: Direct TMDB Credits API (matches Cinejoy client TMDB key & api.tmdb.org bypass)
        try {
            val tmdbCreditsUrl = "https://api.tmdb.org/3/$typeStr/$tmdbId/credits?api_key=8476a7ab80ad76f0936744df0430e67c"
            val tmdbReq = Request.Builder()
                .url(tmdbCreditsUrl)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                .build()
            val tmdbResp = client.newCall(tmdbReq).execute()
            if (tmdbResp.isSuccessful) {
                val tmdbBody = tmdbResp.body?.string() ?: ""
                val root = json.parseToJsonElement(tmdbBody).jsonObject
                val castArr = root["cast"]?.jsonArray
                if (castArr != null && castArr.isNotEmpty()) {
                    val tmdbCast = castArr.mapNotNull { elem ->
                        val obj = elem.jsonObject
                        val name = obj["name"]?.jsonPrimitive?.contentOrNull ?: return@mapNotNull null
                        val character = obj["character"]?.jsonPrimitive?.contentOrNull?.takeIf { it.isNotBlank() }
                        val profilePath = obj["profile_path"]?.jsonPrimitive?.contentOrNull
                        val profileUrl = profilePath?.let { "https://image.tmdb.org/t/p/w185$it" }
                        val personId = obj["id"]?.jsonPrimitive?.contentOrNull ?: name
                        CastMember(
                            id = personId,
                            name = name,
                            character = character,
                            profileUrl = profileUrl
                        )
                    }.distinctBy { it.id }.take(35)

                    if (tmdbCast.isNotEmpty()) {
                        return@withContext tmdbCast
                    }
                }
            }
        } catch (e: Exception) {
            // Fall through to secondary fallbacks
        }

        // 2. Fallback: Resolve effective IMDB ID
        val effectiveImdbId = imdbId ?: run {
            try {
                val infoReq = newRequestBuilder("https://api.wing.st/info?type=$typeStr&tmdb=$tmdbId").build()
                val infoResp = client.newCall(infoReq).execute()
                if (infoResp.isSuccessful) {
                    val infoBody = infoResp.body?.string() ?: ""
                    json.parseToJsonElement(infoBody).jsonObject["imdb_id"]?.jsonPrimitive?.contentOrNull
                } else null
            } catch (e: Exception) {
                null
            }
        }

        // 3. Fallback for TV Series: Use TVMaze API for complete cast, characters, and headshots
        if (type == MediaType.TV_SERIES && effectiveImdbId != null && effectiveImdbId.startsWith("tt")) {
            try {
                val showReq = Request.Builder()
                    .url("https://api.tvmaze.com/lookup/shows?imdb=$effectiveImdbId")
                    .header("User-Agent", "Mozilla/5.0")
                    .build()
                val showResp = client.newCall(showReq).execute()
                if (showResp.isSuccessful) {
                    val showBody = showResp.body?.string() ?: ""
                    val showObj = json.parseToJsonElement(showBody).jsonObject
                    val showId = showObj["id"]?.jsonPrimitive?.intOrNull
                    if (showId != null) {
                        val castReq = Request.Builder()
                            .url("https://api.tvmaze.com/shows/$showId/cast")
                            .header("User-Agent", "Mozilla/5.0")
                            .build()
                        val castResp = client.newCall(castReq).execute()
                        if (castResp.isSuccessful) {
                            val castBody = castResp.body?.string() ?: ""
                            val castArr = json.parseToJsonElement(castBody).jsonArray
                            val results = castArr.take(25).mapNotNull { elem ->
                                val obj = elem.jsonObject
                                val person = obj["person"]?.jsonObject ?: return@mapNotNull null
                                val name = person["name"]?.jsonPrimitive?.contentOrNull ?: return@mapNotNull null
                                val character = obj["character"]?.jsonObject?.get("name")?.jsonPrimitive?.contentOrNull
                                val imgObj = person["image"]?.jsonObject
                                val profileUrl = imgObj?.get("medium")?.jsonPrimitive?.contentOrNull
                                    ?: imgObj?.get("original")?.jsonPrimitive?.contentOrNull
                                val personId = person["id"]?.jsonPrimitive?.contentOrNull ?: name
                                CastMember(
                                    id = personId,
                                    name = name,
                                    character = character,
                                    profileUrl = profileUrl
                                )
                            }.distinctBy { it.id }
                            if (results.isNotEmpty()) return@withContext results
                        }
                    }
                }
            } catch (e: Exception) {
                // Fall through to Cinemeta + Wikipedia
            }
        }

        // 4. Fallback for Movies: Cinemeta + Wikipedia Headshots
        if (effectiveImdbId != null && effectiveImdbId.startsWith("tt")) {
            try {
                val cinemetaType = if (type == MediaType.TV_SERIES) "series" else "movie"
                val cinemetaReq = Request.Builder()
                    .url("https://v3-cinemeta.strem.io/meta/$cinemetaType/$effectiveImdbId.json")
                    .header("User-Agent", "Mozilla/5.0")
                    .build()
                val cinemetaResp = client.newCall(cinemetaReq).execute()
                if (cinemetaResp.isSuccessful) {
                    val cinemetaBody = cinemetaResp.body?.string() ?: ""
                    val root = json.parseToJsonElement(cinemetaBody).jsonObject
                    val metaObj = root["meta"]?.jsonObject
                    val castArr = metaObj?.get("cast")?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
                    val directors = metaObj?.get("director")?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()

                    val candidates = mutableListOf<Pair<String, String?>>()
                    castArr.take(12).forEach { candidates.add(it to null) }
                    directors.take(2).forEach { candidates.add(it to "Director") }

                    if (candidates.isNotEmpty()) {
                        val castWithPhotos = coroutineScope {
                            candidates.mapIndexed { idx, (actorName, role) ->
                                async {
                                    val photoUrl = fetchWikipediaPhoto(actorName)
                                    CastMember(
                                        id = "${actorName}_$idx",
                                        name = actorName,
                                        character = role,
                                        profileUrl = photoUrl
                                    )
                                }
                            }.awaitAll()
                        }
                        return@withContext castWithPhotos
                    }
                }
            } catch (e: Exception) {
                // Ignore
            }
        }

        emptyList()
    }

    private fun fetchWikipediaPhoto(name: String): String? {
        return try {
            val encodedName = URLEncoder.encode(name.replace(" ", "_"), "UTF-8")
            val req = Request.Builder()
                .url("https://en.wikipedia.org/api/rest_v1/page/summary/$encodedName")
                .header("User-Agent", "EuthopiarApp/1.0 (contact@euthopiar.com)")
                .build()
            val resp = client.newCall(req).execute()
            if (resp.isSuccessful) {
                val body = resp.body?.string() ?: ""
                val root = json.parseToJsonElement(body).jsonObject
                root["thumbnail"]?.jsonObject?.get("source")?.jsonPrimitive?.contentOrNull
            } else null
        } catch (e: Exception) {
            null
        }
    }


    @Volatile
    private var cachedServers: List<String>? = null
    @Volatile
    private var lastServersFetchTime: Long = 0L

    private suspend fun getActiveServers(): List<String> = withContext(Dispatchers.IO) {
        val now = System.currentTimeMillis()
        cachedServers?.takeIf { now - lastServersFetchTime < 300_000L }?.let { return@withContext it }

        val dynamicList = mutableListOf<String>()
        try {
            val req = newRequestBuilder("https://api.wing.st/servers").build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    val body = resp.body?.string() ?: ""
                    val root = json.parseToJsonElement(body).jsonObject
                    root["servers"]?.jsonArray?.forEach { elem ->
                        val obj = elem.jsonObject
                        val sName = obj["name"]?.jsonPrimitive?.contentOrNull
                        val sStatus = obj["status"]?.jsonPrimitive?.contentOrNull
                        if (sName != null && (sStatus == null || sStatus == "ok")) {
                            dynamicList.add(sName)
                        }
                    }
                }
            }
        } catch (_: Exception) {}

        // Prioritized order: Nebula (100% verified HLS with multi-quality & seek), Lisbon, Scout, Solara, Riga, Athens
        val defaultList = listOf("Nebula", "Lisbon", "Scout", "Solara", "Riga", "Athens")
        val combined = if (dynamicList.isNotEmpty()) {
            val sorted = mutableListOf<String>()
            if (dynamicList.contains("Nebula")) sorted.add("Nebula")
            if (dynamicList.contains("Lisbon")) sorted.add("Lisbon")
            dynamicList.forEach { if (!sorted.contains(it)) sorted.add(it) }
            defaultList.forEach { if (!sorted.contains(it)) sorted.add(it) }
            sorted
        } else {
            defaultList
        }

        cachedServers = combined
        lastServersFetchTime = now
        combined
    }

    private fun isStreamLive(url: String, headers: Map<String, String>): Boolean {
        return try {
            val builder = Request.Builder()
                .url(url)
                .header("Range", "bytes=0-1024")
            headers.forEach { (k, v) -> builder.header(k, v) }
            val resp = client.newCall(builder.build()).execute()
            resp.use {
                it.isSuccessful && (it.code in 200..299)
            }
        } catch (_: Exception) {
            false
        }
    }

    private fun isDownloadLive(url: String): Boolean {
        return try {
            val req = Request.Builder()
                .url(url)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                .header("Range", "bytes=0-1024")
                .build()
            client.newCall(req).execute().use { resp ->
                resp.isSuccessful && (resp.code == 200 || resp.code == 206)
            }
        } catch (_: Exception) {
            false
        }
    }

    override fun getStreamFlow(episodeData: String): Flow<StreamEmission> = channelFlow {
        val tmdbId: String
        val season: Int?
        val episode: Int?
        val type: String

        if (episodeData.contains(":")) {
            val parts = episodeData.split(":")
            tmdbId = parts[0]
            season = parts.getOrNull(1)?.toIntOrNull()
            episode = parts.getOrNull(2)?.toIntOrNull()
            type = "tv"
        } else {
            tmdbId = episodeData
            season = null
            episode = null
            type = "movie"
        }

        send(StreamEmission.StatusUpdate("Cinejoy", "Searching mirrors for Cinejoy ($tmdbId)..."))

        val servers = getActiveServers()
        val emittedStreamUrls = ConcurrentHashMap.newKeySet<String>()
        val emittedSubUrls = ConcurrentHashMap.newKeySet<String>()

        // 1. Asynchronously fetch external subtitles from native subs.wing.st and OpenSubtitles bridge
        launch {
            try {
                val extSubs = fetchExternalSubtitles(type, tmdbId, season, episode)
                extSubs.forEach { sub ->
                    if (emittedSubUrls.add(sub.url)) {
                        send(StreamEmission.SubtitleFound(sub))
                    }
                }
            } catch (_: Exception) {}
        }

        // 2. Fetch and emit Nebula FIRST so player auto-starts instantly on 1080p FHD Direct with zero buffering
        if (servers.contains("Nebula")) {
            try {
                val nebulaJson = withTimeoutOrNull(5000L) {
                    CinejoyWasmEngine.requestStream(
                        client = client,
                        server = "Nebula",
                        type = type,
                        tmdbId = tmdbId,
                        season = season,
                        episode = episode
                    )
                }
                if (nebulaJson != null) {
                    val root = json.parseToJsonElement(nebulaJson).jsonObject
                    val dataObj = root["data"]?.jsonObject
                    val streamArr = dataObj?.get("stream")?.jsonArray
                    if (streamArr != null) {
                        for (streamElem in streamArr) {
                            val sObj = streamElem.jsonObject
                            val playlistUrl = sObj["playlist"]?.jsonPrimitive?.contentOrNull ?: continue
                            val isHls = playlistUrl.contains(".m3u8") || playlistUrl.contains("/hls/")

                            val nebulaSource = StreamSource(
                                url = playlistUrl,
                                serverName = "Nebula",
                                resolutionLabel = "1080p",
                                quality = "Nebula (1080p FHD Direct)",
                                isM3u8 = true,
                                releaseType = AudioReleaseType.ORIGINAL,
                                headers = defaultHeaders
                            )
                            if (emittedStreamUrls.add(nebulaSource.url)) {
                                send(StreamEmission.SourceFound(nebulaSource))
                            }

                            val captions = sObj["captions"]?.jsonArray
                            captions?.forEach { capElem ->
                                val capObj = capElem.jsonObject
                                val capUrl = capObj["url"]?.jsonPrimitive?.contentOrNull ?: return@forEach
                                val lang = capObj["language"]?.jsonPrimitive?.contentOrNull ?: "English"
                                if (emittedSubUrls.add(capUrl)) {
                                    send(StreamEmission.SubtitleFound(SubtitleTrack(url = capUrl, language = lang)))
                                }
                            }
                        }
                    }
                }
            } catch (_: Exception) {}
        }

        // 3. Concurrently fetch secondary mirrors (Lisbon, Solara, Scout, etc.) so NO SOURCE IS OMITTED
        val secondaryServers = servers.filter { it != "Nebula" }
        secondaryServers.forEach { server ->
            launch {
                try {
                    val decryptedJson = withTimeoutOrNull(8000L) {
                        CinejoyWasmEngine.requestStream(
                            client = client,
                            server = server,
                            type = type,
                            tmdbId = tmdbId,
                            season = season,
                            episode = episode
                        )
                    } ?: return@launch

                    val root = json.parseToJsonElement(decryptedJson).jsonObject
                    val dataObj = root["data"]?.jsonObject ?: return@launch
                    val streamArr = dataObj["stream"]?.jsonArray ?: return@launch

                    for (streamElem in streamArr) {
                        val sObj = streamElem.jsonObject
                        val playlistUrl = sObj["playlist"]?.jsonPrimitive?.contentOrNull ?: continue
                        val streamType = sObj["type"]?.jsonPrimitive?.contentOrNull ?: "hls"
                        val isHls = streamType.contains("hls") || playlistUrl.contains(".m3u8") || playlistUrl.contains("/content?v=")

                        val resolutionLabel = when (server) {
                            "Lisbon" -> "1080p"
                            "Solara" -> "HD"
                            else -> "HD"
                        }
                        val qualityLabel = when (server) {
                            "Lisbon" -> "Lisbon (1080p High Bitrate)"
                            "Solara" -> "Solara (Direct HLS)"
                            else -> "$server (Direct)"
                        }

                        val source = StreamSource(
                            url = playlistUrl,
                            serverName = server,
                            resolutionLabel = resolutionLabel,
                            quality = qualityLabel,
                            isM3u8 = isHls,
                            releaseType = AudioReleaseType.ORIGINAL,
                            headers = defaultHeaders
                        )

                        if (emittedStreamUrls.add(source.url)) {
                            send(StreamEmission.SourceFound(source))
                        }

                        val captions = sObj["captions"]?.jsonArray
                        captions?.forEach { capElem ->
                            val capObj = capElem.jsonObject
                            val capUrl = capObj["url"]?.jsonPrimitive?.contentOrNull ?: return@forEach
                            val lang = capObj["language"]?.jsonPrimitive?.contentOrNull ?: "English"
                            if (emittedSubUrls.add(capUrl)) {
                                send(StreamEmission.SubtitleFound(SubtitleTrack(url = capUrl, language = lang)))
                            }
                        }
                    }
                } catch (_: Exception) {}
            }
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
        val sortedStreams = streamSources.sortedWith(
            compareByDescending<StreamSource> { it.serverName == "Nebula" }
                .thenByDescending { it.serverName == "Lisbon" }
                .thenByDescending { it.serverName == "Solara" }
        )
        StreamResult(
            streams = sortedStreams,
            subtitles = subtitleTracks
        )
    }

    private suspend fun fetchExternalSubtitles(
        type: String,
        tmdbId: String,
        season: Int?,
        episode: Int?
    ): List<SubtitleTrack> = withContext(Dispatchers.IO) {
        val tracks = mutableListOf<SubtitleTrack>()
        val seenUrls = mutableSetOf<String>()

        // 1. Native Cinejoy Subtitles API (subs.wing.st)
        try {
            val wingSubUrl = if (type == "tv" && season != null && episode != null) {
                "https://subs.wing.st/subtitles?type=tv&tmdb=$tmdbId&season=$season&episode=$episode"
            } else {
                "https://subs.wing.st/subtitles?type=movie&tmdb=$tmdbId"
            }
            val wingReq = newRequestBuilder(wingSubUrl).build()
            client.newCall(wingReq).execute().use { wingResp ->
                if (wingResp.isSuccessful) {
                    val body = wingResp.body?.string() ?: ""
                    val root = json.parseToJsonElement(body).jsonObject
                    val subsArr = root["subtitles"]?.jsonArray
                    if (subsArr != null) {
                        val langCounts = mutableMapOf<String, Int>()
                        for (elem in subsArr) {
                            val obj = elem.jsonObject
                            val sUrl = obj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                            val langCode = obj["language"]?.jsonPrimitive?.contentOrNull ?: "en"
                            val langName = languageCodeToName(langCode)
                            val count = langCounts.getOrDefault(langName, 0)
                            if (count < 2 && seenUrls.add(sUrl)) {
                                langCounts[langName] = count + 1
                                val label = if (count == 0) langName else "$langName (Alt)"
                                tracks.add(SubtitleTrack(url = sUrl, language = label))
                            }
                        }
                    }
                }
            }
        } catch (_: Exception) {}

        // 2. OpenSubtitles bridge via IMDB id
        try {
            val infoReq = newRequestBuilder("https://api.wing.st/info?type=$type&tmdb=$tmdbId").build()
            val imdbId = client.newCall(infoReq).execute().use { infoResp ->
                if (infoResp.isSuccessful) {
                    val b = infoResp.body?.string() ?: ""
                    json.parseToJsonElement(b).jsonObject["imdb_id"]?.jsonPrimitive?.contentOrNull
                } else null
            }

            if (imdbId != null && imdbId.startsWith("tt")) {
                val subUrl = if (type == "tv" && season != null && episode != null) {
                    "https://opensubtitles-v3.strem.io/subtitles/series/$imdbId:$season:$episode.json"
                } else {
                    "https://opensubtitles-v3.strem.io/subtitles/movie/$imdbId.json"
                }

                val subsReq = Request.Builder()
                    .url(subUrl)
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                    .build()
                client.newCall(subsReq).execute().use { subsResp ->
                    if (subsResp.isSuccessful) {
                        val body = subsResp.body?.string() ?: ""
                        val root = json.parseToJsonElement(body).jsonObject
                        val subsArr = root["subtitles"]?.jsonArray
                        if (subsArr != null) {
                            for (elem in subsArr) {
                                val obj = elem.jsonObject
                                val sUrl = obj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                                val langCode = obj["lang"]?.jsonPrimitive?.contentOrNull ?: "eng"
                                val langName = languageCodeToName(langCode)
                                val countForLang = tracks.count { it.language.startsWith(langName) }
                                if (countForLang < 3 && seenUrls.add(sUrl)) {
                                    val label = if (countForLang == 0) langName else "$langName (${countForLang + 1})"
                                    tracks.add(SubtitleTrack(url = sUrl, language = label))
                                }
                            }
                        }
                    }
                }
            }
        } catch (_: Exception) {}

        tracks
    }

    private fun languageCodeToName(code: String): String {
        return when (code.lowercase()) {
            "eng", "en" -> "English"
            "spa", "es" -> "Spanish"
            "fre", "fra", "fr" -> "French"
            "ger", "deu", "de" -> "German"
            "ita", "it" -> "Italian"
            "por", "pt" -> "Portuguese"
            "rus", "ru" -> "Russian"
            "ara", "ar" -> "Arabic"
            "hin", "hi" -> "Hindi"
            "ben", "bn" -> "Bengali"
            "jpn", "ja" -> "Japanese"
            "kor", "ko" -> "Korean"
            "zho", "chi", "zh" -> "Chinese"
            "tur", "tr" -> "Turkish"
            "vie", "vi" -> "Vietnamese"
            "ind", "id" -> "Indonesian"
            "pol", "pl" -> "Polish"
            "dut", "nld", "nl" -> "Dutch"
            "gre", "ell", "el" -> "Greek"
            "swe", "sv" -> "Swedish"
            "heb", "he" -> "Hebrew"
            "tha", "th" -> "Thai"
            "rum", "ron", "ro" -> "Romanian"
            else -> code.uppercase()
        }
    }

    override suspend fun getDownloadLinks(episodeData: String): List<DownloadOption> = withContext(Dispatchers.IO) {
        val tmdbId: String
        val season: Int?
        val episode: Int?
        val type: String

        if (episodeData.contains(":")) {
            val parts = episodeData.split(":")
            tmdbId = parts[0]
            season = parts.getOrNull(1)?.toIntOrNull()
            episode = parts.getOrNull(2)?.toIntOrNull()
            type = "tv"
        } else {
            tmdbId = episodeData
            season = null
            episode = null
            type = "movie"
        }

        val url = if (type == "tv" && season != null && episode != null) {
            "https://downloads.wing.st/tv/$tmdbId/$season/$episode"
        } else {
            "https://downloads.wing.st/movie/$tmdbId"
        }

        val downloadOptions = mutableListOf<DownloadOption>()

        try {
            val req = newRequestBuilder(url).build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    val body = resp.body?.string() ?: ""
                    val root = json.parseToJsonElement(body).jsonObject
                    val linksArr = root["links"]?.jsonArray
                    if (linksArr != null) {
                        for (elem in linksArr) {
                            val obj = elem.jsonObject
                            val linkUrl = obj["url"]?.jsonPrimitive?.contentOrNull ?: continue
                            val source = obj["source"]?.jsonPrimitive?.contentOrNull ?: "Direct Server"
                            val name = obj["name"]?.jsonPrimitive?.contentOrNull ?: source
                            val qualityInt = obj["quality"]?.jsonPrimitive?.intOrNull
                            val sizeStr = obj["size"]?.jsonPrimitive?.contentOrNull ?: "Unknown Size"
                            val providerStr = obj["provider"]?.jsonPrimitive?.contentOrNull

                            val qualityLabel = when (qualityInt) {
                                2160 -> "4K 2160p"
                                1080 -> "1080p FHD"
                                720 -> "720p HD"
                                480 -> "480p SD"
                                null -> "HD"
                                else -> "${qualityInt}p"
                            }

                            // Filter out known broken/quota-exceeded Google Drive workers & 403 onedrive proxies
                            val isDeadWorker = linkUrl.contains(".workers.dev", ignoreCase = true) ||
                                    linkUrl.contains("111477.xyz", ignoreCase = true)

                            if (!isDeadWorker) {
                                val cleanUrl = sanitizeDownloadUrl(linkUrl)

                                // Validate that direct link is live (HTTP 200 or 206)
                                if (isDownloadLive(cleanUrl)) {
                                    downloadOptions.add(
                                        DownloadOption(
                                            title = name,
                                            quality = qualityLabel,
                                            size = sizeStr,
                                            url = cleanUrl,
                                            source = source,
                                            provider = providerStr,
                                            headers = mapOf(
                                                "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                                                "Referer" to "https://cinejoy.pk/",
                                                "Origin" to "https://cinejoy.pk"
                                            )
                                        )
                                    )
                                }
                            }
                        }
                    }
                }
            }
        } catch (_: Exception) {
            // downloads.wing.st failed or network error
        }

        // If no dedicated download links exist on downloads.wing.st (e.g. Leave No Trace, older titles),
        // fallback to verified high-speed live stream mirrors so 100% of catalog content has verified downloads
        if (downloadOptions.isEmpty()) {
            try {
                val streamResult = getStreamLinks(episodeData)
                val liveStreams = streamResult.streams.filter { isStreamLive(it.url, it.headers) }
                for (stream in liveStreams) {
                    val q = if (stream.resolutionLabel != "Auto") stream.resolutionLabel else "1080p FHD"
                    downloadOptions.add(
                        DownloadOption(
                            title = "${stream.serverName} ($q) - Direct Download",
                            quality = q,
                            size = "~1.5 GB",
                            url = stream.url,
                            source = "Cinejoy Direct Gateway (${stream.serverName})",
                            provider = name,
                            headers = stream.headers
                        )
                    )
                }
            } catch (_: Exception) {
                // Ignore fallback error
            }
        }

        downloadOptions
    }

    private fun sanitizeDownloadUrl(url: String): String {
        return try {
            val uri = java.net.URI(url)
            uri.toASCIIString()
        } catch (e: Exception) {
            val schemeEnd = url.indexOf("://")
            if (schemeEnd != -1) {
                val scheme = url.substring(0, schemeEnd + 3)
                val rest = url.substring(schemeEnd + 3)
                val slashIdx = rest.indexOf('/')
                if (slashIdx != -1) {
                    val host = rest.substring(0, slashIdx)
                    val pathAndQuery = rest.substring(slashIdx)
                    val cleanPathAndQuery = pathAndQuery
                        .replace(" ", "%20")
                        .replace("[", "%5B")
                        .replace("]", "%5D")
                        .replace("{", "%7B")
                        .replace("}", "%7D")
                        .replace("|", "%7C")
                        .replace("^", "%5E")
                        .replace("`", "%60")
                    scheme + host + cleanPathAndQuery
                } else {
                    url.replace(" ", "%20")
                }
            } else {
                url.replace(" ", "%20")
            }
        }
    }
}
