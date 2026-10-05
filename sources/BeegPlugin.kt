package com.euthopiar.core.provider

import com.euthopiar.core.model.*
import com.euthopiar.core.network.DohDns
import org.json.JSONArray
import org.json.JSONObject
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import java.util.Locale

/**
 * Beeg Universal Plugin Provider.
 *
 * Direct integration with Beeg (https://beeg.com) and the Externulls content delivery network
 * (store.externulls.com / thumbs.externulls.com / video.beeg.com).
 *
 * Uses built-in DNS-over-HTTPS (DoH) via [DohDns] to bypass ISP DNS tampering and provide
 * uninterrupted streaming of high-definition uncut scenes.
 */
class BeegPlugin(
    private val client: OkHttpClient = DohDns.createOkHttpClient(
        connectTimeoutSeconds = 15,
        readTimeoutSeconds = 25
    )
) : UniversalPlugin {

    constructor() : this(DohDns.createOkHttpClient())

    override val name: String = "Beeg"
    override val mainUrl: String = "https://beeg.com"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE)
    override val isSearchGlobalOnly: Boolean get() = false

    private val defaultUserAgent =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    private val baseHeaders = mapOf(
        "User-Agent" to defaultUserAgent,
        "Referer" to "$mainUrl/",
        "Origin" to mainUrl,
        "Accept" to "application/json, text/plain, */*"
    )

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        val rows = mutableListOf<CatalogRow>()

        coroutineScope {
            val trendingDeferred = async { fetchTagItems(slug = "index", limit = 20, offset = 0) }
            val popularDeferred = async { fetchTagItems(slug = "index", limit = 20, offset = 20) }
            val recommendedDeferred = async { fetchTagItems(slug = "index", limit = 20, offset = 40) }

            val trending = trendingDeferred.await()
            if (trending.isNotEmpty()) {
                rows.add(CatalogRow(title = "Featured & Trending", items = trending))
            }

            val popular = popularDeferred.await()
            if (popular.isNotEmpty()) {
                rows.add(CatalogRow(title = "Popular Full Length", items = popular))
            }

            val recommended = recommendedDeferred.await()
            if (recommended.isNotEmpty()) {
                rows.add(CatalogRow(title = "Recommended Full Scenes", items = recommended))
            }
        }

        rows
    }

    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        if (query.isBlank()) return@withContext emptyList()
        val trimmed = query.trim().lowercase(Locale.ROOT)

        // Fetch multiple pages and filter by search query keywords
        val pool = mutableListOf<MediaItem>()
        coroutineScope {
            val page1 = async { fetchTagItems(slug = "index", limit = 20, offset = 0) }
            val page2 = async { fetchTagItems(slug = "index", limit = 20, offset = 20) }
            val page3 = async { fetchTagItems(slug = "index", limit = 20, offset = 40) }

            pool.addAll(page1.await())
            pool.addAll(page2.await())
            pool.addAll(page3.await())
        }

        val filtered = pool.filter { item ->
            item.title.lowercase(Locale.ROOT).contains(trimmed)
        }

        // If specific keyword filtering returned items, use them; otherwise return the pool as related matches
        if (filtered.isNotEmpty()) filtered else pool.take(16)
    }

    override suspend fun getDetails(mediaItem: MediaItem): MediaDetail = withContext(Dispatchers.IO) {
        val cleanId = mediaItem.id.trimStart('0')
        val fileUrl = "https://store.externulls.com/facts/file/$cleanId"

        var title = mediaItem.title
        var duration = mediaItem.rating ?: "25:00"
        var streamUrl: String? = null
        var synopsis = "High-definition full length scene from Beeg."

        try {
            val req = Request.Builder()
                .url(fileUrl)
                .apply { baseHeaders.forEach { (k, v) -> header(k, v) } }
                .build()

            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) {
                    val body = resp.body?.string()
                    if (!body.isNullOrBlank()) {
                        val node = JSONObject(body)
                        val fileNode = node.optJSONObject("file")
                        val dataArray = fileNode?.optJSONArray("data")
                        if (dataArray != null) {
                            for (i in 0 until dataArray.length()) {
                                val entry = dataArray.optJSONObject(i) ?: continue
                                if (entry.optString("cd_column") == "sf_name") {
                                    val t = entry.optString("cd_value")
                                    if (t.isNotBlank()) title = t
                                }
                                if (entry.optString("cd_column") == "sf_story") {
                                    val s = entry.optString("cd_value")
                                    if (s.isNotBlank()) synopsis = s
                                }
                            }
                        }

                        val durSec = fileNode?.optInt("fl_duration", 0) ?: 0
                        if (durSec > 0) {
                            duration = formatDuration(durSec)
                        }
                    }
                }
            }
        } catch (_: Exception) {}

        // Fetch authenticated master adaptive HLS playlist from Beeg play_url API
        try {
            val playReq = Request.Builder()
                .url("https://store.externulls.com/video/play_url/$cleanId")
                .apply { baseHeaders.forEach { (k, v) -> header(k, v) } }
                .build()

            client.newCall(playReq).execute().use { resp ->
                if (resp.isSuccessful) {
                    val path = resp.body?.string()?.trim()
                    if (!path.isNullOrBlank()) {
                        streamUrl = if (path.startsWith("http")) path else "https://video.beeg.com/$path"
                    }
                }
            }
        } catch (_: Exception) {}

        val finalStreamUrl = if (!streamUrl.isNullOrBlank()) streamUrl!! else "https://video.beeg.com/$cleanId"

        val episode = EpisodeItem(
            id = cleanId,
            title = "Auto (Adaptive Master 1080p)",
            seasonNumber = 1,
            episodeNumber = 1,
            data = finalStreamUrl
        )

        MediaDetail(
            id = cleanId,
            title = title,
            url = "$mainUrl/$cleanId",
            posterUrl = mediaItem.posterUrl,
            backdropUrl = mediaItem.backdropUrl ?: mediaItem.posterUrl,
            type = MediaType.MOVIE,
            year = 2026,
            synopsis = synopsis,
            genres = listOf("Full HD", "Beeg Exclusive"),
            rating = duration,
            duration = duration,
            provider = name,
            episodes = listOf(episode)
        )
    }

    override suspend fun getStreamLinks(streamData: String): StreamResult = withContext(Dispatchers.IO) {
        val streams = mutableListOf<StreamSource>()
        var finalUrl = streamData

        if (!finalUrl.startsWith("http") || finalUrl.contains("/video/play_url/") || finalUrl.matches(Regex(".*/\\d+$"))) {
            val fileId = if (finalUrl.contains("/")) finalUrl.substringAfterLast("/") else finalUrl
            val cleanId = fileId.trimStart('0')
            try {
                val playReq = Request.Builder()
                    .url("https://store.externulls.com/video/play_url/$cleanId")
                    .apply { baseHeaders.forEach { (k, v) -> header(k, v) } }
                    .build()
                client.newCall(playReq).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val path = resp.body?.string()?.trim()
                        if (!path.isNullOrBlank()) {
                            finalUrl = if (path.startsWith("http")) path else "https://video.beeg.com/$path"
                        }
                    }
                }
            } catch (_: Exception) {}
        } else if (!finalUrl.contains("video.beeg.com") && finalUrl.contains(".mp4.m3u8")) {
            finalUrl = if (finalUrl.startsWith("/")) "https://video.beeg.com$finalUrl" else "https://video.beeg.com/$finalUrl"
        }

        val headers = mapOf(
            "User-Agent" to defaultUserAgent,
            "Referer" to "$mainUrl/",
            "Origin" to mainUrl
        )

        streams.add(
            StreamSource(
                url = finalUrl,
                serverName = "Beeg Master CDN (Adaptive HLS 1080p)",
                quality = "1080p Full HD",
                isM3u8 = true,
                headers = headers
            )
        )

        StreamResult(streams = streams)
    }

    private fun fetchTagItems(slug: String = "index", limit: Int = 20, offset: Int = 0): List<MediaItem> {
        val url = "https://store.externulls.com/facts/tag?slug=$slug&limit=$limit&offset=$offset"
        val items = mutableListOf<MediaItem>()

        try {
            val req = Request.Builder()
                .url(url)
                .apply { baseHeaders.forEach { (k, v) -> header(k, v) } }
                .build()

            client.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return emptyList()
                val body = resp.body?.string() ?: return emptyList()
                val arrayNode = JSONArray(body)

                for (i in 0 until arrayNode.length()) {
                    val card = arrayNode.optJSONObject(i) ?: continue
                    val fileNode = card.optJSONObject("file") ?: continue
                    val fileId = fileNode.optString("id")
                    if (fileId.isBlank()) continue

                    var title = "Full Scene #$fileId"
                    val dataArray = fileNode.optJSONArray("data")
                    if (dataArray != null) {
                        for (j in 0 until dataArray.length()) {
                            val entry = dataArray.optJSONObject(j) ?: continue
                            if (entry.optString("cd_column") == "sf_name") {
                                val t = entry.optString("cd_value")
                                if (t.isNotBlank()) {
                                    title = t
                                    break
                                }
                            }
                        }
                    }

                    val durSec = fileNode.optInt("fl_duration", 0)
                    val durStr = if (durSec > 0) formatDuration(durSec) else "25:00"

                    val height = fileNode.optInt("fl_height", 1080)
                    val quality = "${height}p HD"

                    // Calculate thumbnail URL from first thumbnail offset
                    val fcFacts = card.optJSONArray("fc_facts")
                    var thumbOffset = 0
                    if (fcFacts != null && fcFacts.length() > 0) {
                        val thumbsArray = fcFacts.optJSONObject(0)?.optJSONArray("fc_thumbs")
                        if (thumbsArray != null && thumbsArray.length() > 0) {
                            thumbOffset = thumbsArray.optInt(0, 0)
                        }
                    }

                    val posterUrl = "https://thumbs.externulls.com/videos/$fileId/$thumbOffset.webp?w=480"

                    // Extract model / tag names if available
                    val tagsList = mutableListOf<String>()
                    val tagsArray = card.optJSONArray("tags")
                    if (tagsArray != null) {
                        for (k in 0 until tagsArray.length()) {
                            val tNode = tagsArray.optJSONObject(k) ?: continue
                            val tgName = tNode.optString("tg_name")
                            if (tgName.isNotBlank()) tagsList.add(tgName)
                        }
                    }

                    items.add(
                        MediaItem(
                            id = fileId,
                            title = title,
                            url = "$mainUrl/$fileId",
                            posterUrl = posterUrl,
                            backdropUrl = posterUrl,
                            type = MediaType.MOVIE,
                            year = 2026,
                            quality = quality,
                            rating = durStr,
                            provider = name
                        )
                    )
                }
            }
        } catch (_: Exception) {}

        return items
    }

    private fun formatDuration(seconds: Int): String {
        val m = seconds / 60
        val s = seconds % 60
        return String.format(Locale.US, "%d:%02d", m, s)
    }
}
