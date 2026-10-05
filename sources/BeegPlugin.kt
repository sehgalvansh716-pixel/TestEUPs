package com.euthopiar.core.provider

import com.euthopiar.core.model.*
import com.euthopiar.core.network.DohDns
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
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

    override val name: String = "Beeg"
    override val mainUrl: String = "https://beeg.com"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE)
    override val isSearchGlobalOnly: Boolean get() = false

    private val mapper = jacksonObjectMapper()

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
                        val node = mapper.readTree(body)
                        val fileNode = node.path("file")
                        val dataArray = fileNode.path("data")
                        if (dataArray.isArray) {
                            for (entry in dataArray) {
                                if (entry.path("cd_column").asText() == "sf_name") {
                                    val t = entry.path("cd_value").asText()
                                    if (t.isNotBlank()) title = t
                                }
                                if (entry.path("cd_column").asText() == "sf_story") {
                                    val s = entry.path("cd_value").asText()
                                    if (s.isNotBlank()) synopsis = s
                                }
                            }
                        }

                        val durSec = fileNode.path("fl_duration").asInt(0)
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
                val arrayNode = mapper.readTree(body)
                if (!arrayNode.isArray) return emptyList()

                for (card in arrayNode) {
                    val fileNode = card.path("file")
                    val fileId = fileNode.path("id").asText()
                    if (fileId.isBlank()) continue

                    var title = "Full Scene #$fileId"
                    val dataArray = fileNode.path("data")
                    if (dataArray.isArray) {
                        for (entry in dataArray) {
                            if (entry.path("cd_column").asText() == "sf_name") {
                                val t = entry.path("cd_value").asText()
                                if (t.isNotBlank()) {
                                    title = t
                                    break
                                }
                            }
                        }
                    }

                    val durSec = fileNode.path("fl_duration").asInt(0)
                    val durStr = if (durSec > 0) formatDuration(durSec) else "25:00"

                    val height = fileNode.path("fl_height").asInt(1080)
                    val quality = "${height}p HD"

                    // Calculate thumbnail URL from first thumbnail offset
                    val fcFacts = card.path("fc_facts")
                    var thumbOffset = 0
                    if (fcFacts.isArray && fcFacts.size() > 0) {
                        val thumbsArray = fcFacts.get(0).path("fc_thumbs")
                        if (thumbsArray.isArray && thumbsArray.size() > 0) {
                            thumbOffset = thumbsArray.get(0).asInt(0)
                        }
                    }

                    val posterUrl = "https://thumbs.externulls.com/videos/$fileId/$thumbOffset.webp?w=480"

                    // Extract model / tag names if available
                    val tagsList = mutableListOf<String>()
                    val tagsArray = card.path("tags")
                    if (tagsArray.isArray) {
                        for (tNode in tagsArray) {
                            val tgName = tNode.path("tg_name").asText()
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
