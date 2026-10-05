package com.euthopiar.core.provider

import com.euthopiar.core.model.*
import com.euthopiar.core.network.DohDns
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import java.net.URLEncoder
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

/**
 * Faphouse Universal Plugin Provider.
 *
 * Scrapes 16:9 short-form and full-length scene metadata, categories, creators,
 * and high-bitrate multi-resolution streams (4K / 1080p / 720p MP4) from https://faphouse.com.
 *
 * Designed to operate within the isolated Ephemeral Incognito Mode without persisting
 * watch history, tracking, or ad redirection.
 */
class FaphousePlugin(
    private val client: OkHttpClient = DohDns.createOkHttpClient(
        connectTimeoutSeconds = 15,
        readTimeoutSeconds = 20
    )
) : UniversalPlugin {

    override val name: String = "Faphouse"
    override val mainUrl: String = "https://faphouse.com"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE)
    override val isSearchGlobalOnly: Boolean get() = false

    private val defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    private val headers = mapOf(
        "User-Agent" to defaultUserAgent,
        "Referer" to "https://faphouse.com/",
        "Accept-Language" to "en-US,en;q=0.9"
    )

    private val thumbBlockSplitter = Pattern.compile("""data-el="Thumb"""")
    private val urlPattern = Pattern.compile("""href="(/videos/[^"#]+)""")
    private val idPattern = Pattern.compile("""data-id="(\d+)"""")
    private val imgPattern = Pattern.compile("""class="t-i"[^>]+(?:src="([^"]+)"|srcset="([^"]+)")""")
    private val qualityDurationPattern = Pattern.compile("""class="t-vi">\s*([A-Za-z0-9]+)?\s*<span>([^<]+)</span>""")
    private val titlePattern = Pattern.compile("""class="t-tv"[^>]*>([^<]+)</a>""")
    private val altPattern = Pattern.compile("""alt="([^"]+)"""")
    private val creatorAvatarPattern = Pattern.compile("""class="t-ta"[^>]*><img[^>]+src="([^"]+)"""")
    private val creatorNamePattern = Pattern.compile("""class="t-ti-s[^"]*"[^>]*>([^<]+)</a>""")
    private val creatorUrlPattern = Pattern.compile("""class="t-ta"[^>]+href="(/[^"]+)"""")

    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        val html = fetchHtml(mainUrl) ?: return@withContext emptyList()
        val allItems = parseVideoCards(html)

        if (allItems.isEmpty()) return@withContext emptyList()

        val rows = mutableListOf<CatalogRow>()

        // 1. Featured / Top Crowned Items
        val featured = allItems.take(15)
        if (featured.isNotEmpty()) {
            rows.add(CatalogRow(title = "Trending & Crowned", items = featured))
        }

        // 2. 4K Ultra High Definition
        val fourKItems = allItems.filter { it.quality.equals("4K", ignoreCase = true) }
        if (fourKItems.isNotEmpty()) {
            rows.add(CatalogRow(title = "4K Ultra HD", items = fourKItems.take(20)))
        }

        // 3. Recommended Feed
        val remaining = allItems.drop(15)
        if (remaining.isNotEmpty()) {
            rows.add(CatalogRow(title = "Latest Recommendations", items = remaining))
        }

        rows
    }

    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        val encoded = URLEncoder.encode(query.trim(), "UTF-8")
        val searchUrl = "$mainUrl/search/videos?q=$encoded&sort=popular"
        val html = fetchHtml(searchUrl) ?: return@withContext emptyList()
        parseVideoCards(html)
    }

    override suspend fun getDetails(mediaItem: MediaItem): MediaDetail = withContext(Dispatchers.IO) {
        val targetUrl = when {
            mediaItem.url.startsWith("http") -> mediaItem.url
            mediaItem.id.startsWith("http") -> mediaItem.id
            mediaItem.id.startsWith("/") -> "$mainUrl${mediaItem.id}"
            else -> "$mainUrl/videos/${mediaItem.id}"
        }

        val html = fetchHtml(targetUrl) ?: return@withContext createFallbackDetail(mediaItem)

        // 1. Title
        val titleMatcher = Pattern.compile("""<h1[^>]*>(.*?)</h1>""", Pattern.DOTALL).matcher(html)
        val title = if (titleMatcher.find()) {
            titleMatcher.group(1)?.replace(Regex("<[^>]+>"), "")?.trim() ?: mediaItem.title
        } else {
            mediaItem.title
        }

        // 2. Direct Video Stream URL
        var streamUrl: String? = null
        val sourceMatcher = Pattern.compile("""<video[^>]+id="video-trailer"[^>]*>.*?<source[^>]+src="([^"]+)"""", Pattern.DOTALL).matcher(html)
        if (sourceMatcher.find()) {
            streamUrl = sourceMatcher.group(1)?.replace("&amp;", "&")
        }
        if (streamUrl.isNullOrBlank()) {
            val fallbackMatcher = Pattern.compile("""data-fallback="([^"]+)"""").matcher(html)
            if (fallbackMatcher.find()) {
                streamUrl = fallbackMatcher.group(1)?.replace("&amp;", "&")
            }
        }
        if (streamUrl.isNullOrBlank()) {
            val directCdnMatcher = Pattern.compile("""https?://video-nss\.fhcdngroup\.online/[^\s"'<>]+""").matcher(html)
            if (directCdnMatcher.find()) {
                streamUrl = directCdnMatcher.group(0)?.replace("&amp;", "&")
            }
        }

        // 3. Duration
        val durMatcher = Pattern.compile(""""duration":\s*(\d+)""").matcher(html)
        val durationSeconds = if (durMatcher.find()) durMatcher.group(1)?.toIntOrNull() ?: 0 else 0
        val durationFormatted = if (durationSeconds > 0) {
            val mins = durationSeconds / 60
            val secs = durationSeconds % 60
            String.format("%02d:%02d", mins, secs)
        } else {
            mediaItem.rating ?: "15:00"
        }

        // 4. Categories / Tags
        val tagList = mutableListOf<String>()
        val tagMatcher = Pattern.compile("""href="/c/([^/"]+)/videos"""").matcher(html)
        while (tagMatcher.find()) {
            val rawTag = tagMatcher.group(1) ?: continue
            val tag = rawTag.replace("-", " ")
                .split(" ")
                .joinToString(" ") { it.replaceFirstChar { c -> c.uppercase() } }
            if (tag.isNotBlank() && tag !in tagList) {
                tagList.add(tag)
            }
        }

        // 5. Creator / Studio
        var creatorName: String? = null
        var creatorSlug: String? = null
        var creatorType: String = "models"
        var creatorAvatar: String? = null

        val crMatcher = Pattern.compile(""""studioName":\s*"([^"]+)"""").matcher(html)
        if (crMatcher.find()) {
            creatorName = crMatcher.group(1)
        }
        val crUrlMatcher = Pattern.compile(""""studioUrl":\s*"(/[^"]+)"""").matcher(html)
        if (crUrlMatcher.find()) {
            val fullCrUrl = crUrlMatcher.group(1) ?: ""
            val parts = fullCrUrl.trim().removePrefix("/").split("/")
            if (parts.size >= 2) {
                creatorType = parts[0]
                creatorSlug = parts[1]
            }
        }
        val crAvMatcher = Pattern.compile("""class="[^"]*(?:avatar|creator|author|studio)[^"]*"[^>]*src="([^"]+)"""").matcher(html)
        if (crAvMatcher.find()) {
            creatorAvatar = crAvMatcher.group(1)
        }

        val packedCreator = if (!creatorSlug.isNullOrBlank()) {
            "${creatorAvatar ?: ""}|$creatorSlug|${creatorName ?: creatorSlug}|$creatorType"
        } else {
            mediaItem.logoUrl
        }

        // 6. Cast
        val castList = mutableListOf<CastMember>()
        val pornstarMatcher = Pattern.compile(""""pornstarNames":\s*\[(.*?)\]""", Pattern.DOTALL).matcher(html)
        if (pornstarMatcher.find()) {
            val namesBlock = pornstarMatcher.group(1) ?: ""
            val nameRegex = Pattern.compile(""""([^"]+)"""")
            val nm = nameRegex.matcher(namesBlock)
            var idx = 1
            while (nm.find()) {
                val n = nm.group(1)?.trim() ?: ""
                if (n.isNotBlank()) {
                    castList.add(CastMember(id = "cast_$idx", name = n))
                    idx++
                }
            }
        }

        // 7. Episode Item for Playback
        val episodes = if (!streamUrl.isNullOrBlank()) {
            listOf(
                EpisodeItem(
                    id = mediaItem.id,
                    title = title,
                    seasonNumber = 1,
                    episodeNumber = 1,
                    data = streamUrl,
                    thumbnail = mediaItem.posterUrl,
                    duration = durationFormatted
                )
            )
        } else emptyList()

        MediaDetail(
            id = mediaItem.id,
            title = title,
            url = targetUrl,
            posterUrl = mediaItem.posterUrl,
            backdropUrl = mediaItem.backdropUrl ?: mediaItem.posterUrl,
            type = MediaType.MOVIE,
            year = 2026,
            synopsis = if (creatorName != null) "Produced by $creatorName. High-definition scene." else "High-definition scene.",
            genres = tagList.take(12),
            duration = durationFormatted,
            episodes = episodes,
            rating = mediaItem.rating,
            contentRating = "18+",
            provider = name,
            cast = castList,
            logoUrl = packedCreator
        )
    }

    override suspend fun getStreamLinks(episodeData: String): StreamResult = withContext(Dispatchers.IO) {
        val streamHeaders = mapOf(
            "User-Agent" to defaultUserAgent,
            "Referer" to "https://faphouse.com/"
        )

        // If episodeData is already a direct stream URL
        if (episodeData.startsWith("http://") || episodeData.startsWith("https://")) {
            val isAv1 = episodeData.contains(".av1.")
            val is4k = episodeData.contains("2160")
            val res = if (is4k) "4K" else "1080p"
            return@withContext StreamResult(
                streams = listOf(
                    StreamSource(
                        url = episodeData,
                        serverName = "FapHouse HighSpeed CDN",
                        resolutionLabel = if (isAv1) "$res (AV1)" else res,
                        quality = res,
                        isM3u8 = episodeData.contains(".m3u8"),
                        headers = streamHeaders
                    )
                )
            )
        }

        // Otherwise resolve details
        val detail = getDetails(MediaItem(id = episodeData, title = "", url = "", posterUrl = null, type = MediaType.MOVIE))
        val resolvedUrl = detail.episodes.firstOrNull()?.data
        if (!resolvedUrl.isNullOrBlank()) {
            val is4k = resolvedUrl.contains("2160")
            val res = if (is4k) "4K" else "1080p"
            StreamResult(
                streams = listOf(
                    StreamSource(
                        url = resolvedUrl,
                        serverName = "FapHouse HighSpeed CDN",
                        resolutionLabel = res,
                        quality = res,
                        isM3u8 = resolvedUrl.contains(".m3u8"),
                        headers = streamHeaders
                    )
                )
            )
        } else {
            StreamResult(emptyList())
        }
    }

    /**
     * Fetches videos belonging to a specific category slug (e.g. "desi", "amateur", "18-year-old").
     */
    suspend fun getCategoryVideos(slug: String, sort: String = "popular", page: Int = 1): List<MediaItem> = withContext(Dispatchers.IO) {
        val cleanSlug = slug.trim().removePrefix("/c/").removeSuffix("/videos").removePrefix("/")
        val pageQuery = if (page > 1) "&page=$page" else ""
        val url = "$mainUrl/c/$cleanSlug/videos?sort=$sort$pageQuery"
        val html = fetchHtml(url) ?: return@withContext emptyList()
        parseVideoCards(html)
    }

    /**
     * Fetches videos from a creator / model channel or studio.
     */
    suspend fun getCreatorVideos(slug: String, type: String = "models", page: Int = 1): List<MediaItem> = withContext(Dispatchers.IO) {
        val cleanSlug = slug.trim().removePrefix("/models/").removePrefix("/studios/").removePrefix("/pornstars/").removePrefix("/")
        val pageQuery = if (page > 1) "?page=$page" else ""
        val url = "$mainUrl/$type/$cleanSlug$pageQuery"
        val html = fetchHtml(url) ?: return@withContext emptyList()
        parseVideoCards(html)
    }

    /**
     * Extracts recommendations / related scenes embedded on a video details page.
     */
    suspend fun getRelatedVideos(videoUrlOrId: String): List<MediaItem> = withContext(Dispatchers.IO) {
        val targetUrl = when {
            videoUrlOrId.startsWith("http") -> videoUrlOrId
            videoUrlOrId.startsWith("/") -> "$mainUrl$videoUrlOrId"
            else -> "$mainUrl/videos/$videoUrlOrId"
        }
        val html = fetchHtml(targetUrl) ?: return@withContext emptyList()
        parseVideoCards(html)
    }

    /**
     * Parses all video cards from a page's HTML body.
     */
    private fun parseVideoCards(html: String): List<MediaItem> {
        val items = mutableListOf<MediaItem>()
        val blocks = thumbBlockSplitter.split(html)

        for (i in 1 until blocks.size) {
            val block = if (blocks[i].length > 2500) blocks[i].substring(0, 2500) else blocks[i]

            // Video URL
            val urlM = urlPattern.matcher(block)
            if (!urlM.find()) continue
            val relativeUrl = urlM.group(1) ?: continue

            // ID
            val idM = idPattern.matcher(block)
            val vidId = if (idM.find()) idM.group(1) ?: relativeUrl.substringAfterLast("-") else relativeUrl.substringAfterLast("-")

            // Thumbnail Image
            var posterUrl: String? = null
            val imgM = imgPattern.matcher(block)
            if (imgM.find()) {
                val src = imgM.group(1)
                val srcset = imgM.group(2)
                posterUrl = when {
                    !srcset.isNullOrBlank() -> {
                        srcset.split(",").lastOrNull()?.trim()?.split(" ")?.firstOrNull() ?: src
                    }
                    !src.isNullOrBlank() -> src
                    else -> null
                }
            }

            // Quality & Duration
            var quality = "HD"
            var duration: String? = null
            val qdM = qualityDurationPattern.matcher(block)
            if (qdM.find()) {
                val q = qdM.group(1)
                val d = qdM.group(2)
                if (!q.isNullOrBlank()) quality = q.trim()
                if (!d.isNullOrBlank()) duration = d.trim()
            }

            // Title
            var title = ""
            val titleM = titlePattern.matcher(block)
            if (titleM.find()) {
                title = titleM.group(1)?.trim() ?: ""
            } else {
                val altM = altPattern.matcher(block)
                if (altM.find()) {
                    title = altM.group(1)?.trim() ?: ""
                }
            }
            if (title.isBlank()) continue

            // Creator Info
            var creatorAvatar: String? = null
            val avM = creatorAvatarPattern.matcher(block)
            if (avM.find()) {
                creatorAvatar = avM.group(1)
            }

            var creatorSlug: String? = null
            var creatorType: String = "models"
            val urlCrM = creatorUrlPattern.matcher(block)
            if (urlCrM.find()) {
                val fullCrUrl = urlCrM.group(1) ?: ""
                val parts = fullCrUrl.trim().removePrefix("/").split("/")
                if (parts.size >= 2) {
                    creatorType = parts[0]
                    creatorSlug = parts[1]
                } else if (parts.size == 1) {
                    creatorSlug = parts[0]
                }
            }

            var creatorName: String? = null
            val nameCrM = creatorNamePattern.matcher(block)
            if (nameCrM.find()) {
                creatorName = nameCrM.group(1)?.trim()
            }

            val packedCreator = if (!creatorSlug.isNullOrBlank()) {
                "${creatorAvatar ?: ""}|$creatorSlug|${creatorName ?: creatorSlug}|$creatorType"
            } else {
                creatorAvatar
            }

            val fullUrl = if (relativeUrl.startsWith("http")) relativeUrl else "$mainUrl$relativeUrl"

            items.add(
                MediaItem(
                    id = vidId,
                    title = title,
                    url = fullUrl,
                    posterUrl = posterUrl,
                    backdropUrl = posterUrl,
                    type = MediaType.MOVIE,
                    quality = quality,
                    rating = duration,
                    provider = name,
                    logoUrl = packedCreator
                )
            )
        }
        return items
    }

    private fun createFallbackDetail(item: MediaItem): MediaDetail {
        return MediaDetail(
            id = item.id,
            title = item.title,
            url = item.url,
            posterUrl = item.posterUrl,
            backdropUrl = item.backdropUrl ?: item.posterUrl,
            type = MediaType.MOVIE,
            year = 2026,
            synopsis = "HD Video scene from Faphouse.",
            genres = listOf("HD", "Exclusive"),
            duration = item.rating ?: "15:00",
            episodes = emptyList(),
            rating = item.rating,
            provider = name,
            logoUrl = item.logoUrl
        )
    }

    private fun fetchHtml(url: String): String? {
        return try {
            val req = Request.Builder()
                .url(url)
                .apply { headers.forEach { (k, v) -> header(k, v) } }
                .build()
            client.newCall(req).execute().use { resp ->
                if (resp.isSuccessful) resp.body?.string() else null
            }
        } catch (e: Exception) {
            null
        }
    }
}
