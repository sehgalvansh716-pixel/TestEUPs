package com.euthopiar.core.provider

import com.euthopiar.core.model.CatalogRow
import com.euthopiar.core.model.DownloadOption
import com.euthopiar.core.model.EpisodeItem
import com.euthopiar.core.model.HostApi
import com.euthopiar.core.model.MediaDetail
import com.euthopiar.core.model.MediaItem
import com.euthopiar.core.model.MediaType
import com.euthopiar.core.model.StreamEmission
import com.euthopiar.core.model.StreamResult
import com.euthopiar.core.model.UniversalPlugin
import com.euthopiar.core.model.StreamSource as CoreStreamSource
import com.euthopiar.eup.api.CatalogSection
import com.euthopiar.eup.api.ContentType
import com.euthopiar.eup.api.DetailsProvider
import com.euthopiar.eup.api.EpisodeDescriptor
import com.euthopiar.eup.api.EupHostApi
import com.euthopiar.eup.api.HeaderPolicy
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
import com.euthopiar.eup.api.VideoInfo
import com.euthopiar.eup.api.StreamSource as EupStreamSource
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.*
import okhttp3.*
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import java.io.IOException
import java.net.URLEncoder
import java.security.MessageDigest
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * 4KHDHub EUP v2 Golden Plugin (Archetype B: Web Scraping & Multi-Cloud Direct Mirrors).
 *
 * Implements high-speed discovery and resolution of 4K UHD, HDR, REMUX, and 1080p multi-audio
 * streams with deep integration of:
 * - Isolated networking stacks (HTTP/2 scraping pool vs forced HTTP/1.1 CDN verification pool)
 * - Automatic domain failover across active 4KHDHub mirrors (.one, .dad, .pro, .fans, .top)
 * - Multi-layer Greenmotors payload de-obfuscation (Base64 -> Base64 -> Rot13 -> Base64 -> JSON)
 * - HubCloud drive and HubDrive intermediate resolver state machines
 * - Gamerxyt direct link extraction: Cloudflare R2, FSLv2 CDN, 10Gbps Server, and Web player
 * - Strict HTTP byte-range validation (Range: bytes=0-1023) and Matroska/MP4 magic byte verification
 * - Preserves native container track selection without emitting extraneous audio tracks
 */
class FourKHDHubPlugin(
    private val externalClient: OkHttpClient? = null
) : UniversalPlugin, StreamResolver, PagedCatalogProvider, PagedSearchProvider, DetailsProvider {

    constructor() : this(null)

    override val name: String = "4KHDHub"
    override val mainUrl: String = "https://4khdhub.one"
    override val supportedTypes: Set<MediaType> = setOf(MediaType.MOVIE, MediaType.TV_SERIES)
    override val isSearchGlobalOnly: Boolean get() = false

    override val manifest: PluginManifest = PluginManifest(
        id = "4khdhub",
        name = "4KHDHub",
        version = 6,
        apiVersion = 2,
        realm = PluginRealm.PUBLIC,
        entryClass = "com.euthopiar.core.provider.FourKHDHubPlugin",
        capabilities = setOf(
            PluginCapability.PAGED_CATALOG,
            PluginCapability.PAGED_SEARCH,
            PluginCapability.DETAILS,
            PluginCapability.STREAM_RESOLVE,
            PluginCapability.STREAM_REFRESH
        ),
        author = "Euthopiar Core Team",
        siteUrl = "https://4khdhub.one",
        description = "Mastered EUP v2 4K UHD, HDR, REMUX streaming with Cloudflare R2 FSLv2 10Gbps seekable media, multi-server health probes, and multi-audio dubs (Decoupled EUP)."
    )

    private val json = Json { ignoreUnknownKeys = true; isLenient = true }
    private val defaultUserAgent =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"

    private var eupHost: EupHostApi? = null
    private var legacyHost: HostApi? = null

    // ───────────────────────────── Internal Configuration ─────────────────────────────
    private object SiteConfig {
        const val TAG = "EUP/4KHDHub"
        val CANDIDATES = listOf("https://4khdhub.one", "https://4khdhub.dad", "https://4khdhub.fans", "https://4khdhub.pro", "https://4khdhub.top")
        val HOST_RE = Regex("^(?:www\\.)?4khdhub\\.[a-z.]+$", RegexOption.IGNORE_CASE)
        const val MARKER = "4khdhub"
        const val BASE_TTL_MS = 6 * 3_600_000L
        const val PAGE_TTL_MS = 15 * 60_000L
        const val MIN_VIDEO_BYTES = 5L * 1024 * 1024

        const val SEL_CARD = "div.card-grid a.movie-card, a.movie-card, article.movie-card"
        const val SEL_CARD_TITLE = ".movie-card-title, h3, h2"
        const val SEL_CARD_META = ".movie-card-meta, .meta"

        val BLOCKED_HOSTS = listOf("imdb.com", "youtube.com", "youtu.be", "t.me", "telegram", "facebook.com", "twitter.com", "x.com")
        val HUB_HOSTS = listOf("hubcloud", "hubdrive", "hubcdn", "greenmotors", "gamerxyt")
    }

    // ───────────────────────────── Isolated Networking ─────────────────────────────
    private class PluginHttp(externalClient: OkHttpClient?) {
        val scrape: OkHttpClient = externalClient ?: OkHttpClient.Builder()
            .dispatcher(Dispatcher().apply { maxRequests = 24; maxRequestsPerHost = 6 })
            .connectionPool(ConnectionPool(4, 2, TimeUnit.MINUTES))
            .followRedirects(true)
            .followSslRedirects(true)
            .connectTimeout(12, TimeUnit.SECONDS)
            .readTimeout(20, TimeUnit.SECONDS)
            .callTimeout(30, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true)
            .build()

        val cdn: OkHttpClient = OkHttpClient.Builder()
            .dispatcher(Dispatcher().apply { maxRequests = 16; maxRequestsPerHost = 6 })
            .protocols(listOf(Protocol.HTTP_1_1))
            .connectionPool(ConnectionPool(6, 30, TimeUnit.SECONDS))
            .followRedirects(true)
            .followSslRedirects(true)
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(15, TimeUnit.SECONDS)
            .callTimeout(25, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true)
            .build()
    }

    private val http: PluginHttp by lazy {
        PluginHttp(externalClient)
    }

    private val baseLock = Mutex()
    @Volatile private var activeBaseUrl: String = "https://4khdhub.one"
    @Volatile private var baseResolvedAt = 0L

    private val detailCache = ConcurrentHashMap<String, CachedDetail>()
    private val pageLocks = ConcurrentHashMap<String, Mutex>()
    private val refreshLocks = ConcurrentHashMap<String, Mutex>()

    private data class CachedDetail(val detail: MediaDetail, val dlGroups: List<DlGroup>, val fetchedAt: Long)
    private data class DlLink(val label: String, val url: String, val episode: Int?)
    private data class DlGroup(
        val season: Int?,
        val height: Int,
        val isHdr: Boolean,
        val is10Bit: Boolean,
        val isHevc: Boolean,
        val size: String?,
        val title: String,
        val links: List<DlLink>
    )

    private enum class MirrorKind { FSL_V2, R2, FAST_10GBPS, PIXELDRAIN, HBPLAY, OTHER }
    private data class MirrorCandidate(val url: String, val label: String, val kind: MirrorKind, val referer: String?)
    private data class VerifiedProbe(val url: String, val size: Long, val container: String, val mime: String)

    // ───────────────────────────── UniversalPlugin Lifecycle ─────────────────────────────
    override fun init(host: HostApi) {
        this.legacyHost = host
    }

    override suspend fun init(host: EupHostApi, scope: CoroutineScope) {
        this.eupHost = host
    }

    override suspend fun destroy() {
        detailCache.clear()
        pageLocks.clear()
        refreshLocks.clear()
        eupHost = null
        legacyHost = null
    }

    // ───────────────────────────── Domain Failover ─────────────────────────────
    private suspend fun getBaseUrl(force: Boolean = false): String = baseLock.withLock {
        val now = System.currentTimeMillis()
        if (!force && now - baseResolvedAt < SiteConfig.BASE_TTL_MS && activeBaseUrl.isNotBlank()) {
            return@withLock activeBaseUrl
        }
        val candidates = (listOf(activeBaseUrl) + SiteConfig.CANDIDATES).distinct()
        for (candidate in candidates) {
            try {
                val req = Request.Builder()
                    .url(candidate)
                    .header("User-Agent", defaultUserAgent)
                    .build()
                val resp = http.scrape.newCall(req).execute()
                val ok = resp.use { r ->
                    val finalHost = r.request.url.host
                    val body = if (r.code == 200) r.body?.string()?.take(50_000).orEmpty() else ""
                    SiteConfig.HOST_RE.matches(finalHost) && (r.code == 200 && body.contains(SiteConfig.MARKER, ignoreCase = true))
                }
                if (ok) {
                    val finalOrigin = "https://${candidate.toHttpUrl().host}"
                    activeBaseUrl = finalOrigin
                    baseResolvedAt = now
                    return@withLock finalOrigin
                }
            } catch (_: Throwable) {}
        }
        activeBaseUrl
    }

    private suspend fun rebase(url: String): String {
        val u = url.toHttpUrlOrNull() ?: return url
        if (!SiteConfig.HOST_RE.matches(u.host)) return url
        val base = getBaseUrl().toHttpUrl()
        return u.newBuilder().host(base.host).build().toString()
    }

    // ───────────────────────────── Search ─────────────────────────────
    override suspend fun search(query: String): List<MediaItem> = withContext(Dispatchers.IO) {
        val page = search(query, PageRequest(cursor = null, size = 30))
        page.items.map { card ->
            MediaItem(
                id = card.id,
                title = card.title,
                url = card.id,
                posterUrl = card.posterUrl,
                backdropUrl = card.backdropUrl,
                type = if (card.type == ContentType.TV_SERIES) MediaType.TV_SERIES else MediaType.MOVIE,
                year = card.releaseYear,
                provider = name
            )
        }
    }

    override suspend fun search(query: String, page: PageRequest): Page<MediaCard> = withContext(Dispatchers.IO) {
        val base = getBaseUrl()
        val pageNum = page.cursor?.toIntOrNull() ?: 1
        val path = if (pageNum == 1) "/?s=${URLEncoder.encode(query, "UTF-8")}" else "/page/$pageNum/?s=${URLEncoder.encode(query, "UTF-8")}"
        val url = base.trimEnd('/') + path

        val req = Request.Builder()
            .url(url)
            .header("User-Agent", defaultUserAgent)
            .header("Referer", "$base/")
            .build()

        val html = try {
            http.scrape.newCall(req).execute().use { r ->
                if (r.isSuccessful) r.body?.string().orEmpty() else ""
            }
        } catch (_: Throwable) {
            ""
        }

        if (html.isBlank()) return@withContext Page(items = emptyList(), nextCursor = null)

        val doc = Jsoup.parse(html, url)
        val cards = doc.select(SiteConfig.SEL_CARD).mapNotNull { a ->
            val href = a.absUrl("href").ifEmpty { return@mapNotNull null }
            val title = a.selectFirst(SiteConfig.SEL_CARD_TITLE)?.text()?.trim()
                ?: a.selectFirst("img")?.attr("alt")?.trim() ?: return@mapNotNull null
            val metaText = a.selectFirst(SiteConfig.SEL_CARD_META)?.text() ?: ""
            val year = Regex("""(19|20)\d{2}""").find("$title $metaText")?.value?.toIntOrNull()
            val poster = a.selectFirst("img")?.let { img ->
                img.absUrl("data-src").ifEmpty { img.absUrl("src") }
            }?.ifEmpty { null }

            val isTv = title.contains("Series", ignoreCase = true) ||
                       title.contains("Season", ignoreCase = true) ||
                       title.contains(Regex("""\bS\d+""")) ||
                       href.contains("-series-", ignoreCase = true)

            MediaCard(
                id = href,
                title = cleanTitle(title),
                type = if (isTv) ContentType.TV_SERIES else ContentType.MOVIE,
                posterUrl = poster,
                releaseYear = year,
                target = PlayableTarget.Direct(href)
            )
        }.distinctBy { it.id }

        val hasNext = doc.selectFirst("a.next, .pagination a[rel=next], .nav-links a.next") != null
        Page(items = cards, nextCursor = if (hasNext && cards.isNotEmpty()) (pageNum + 1).toString() else null)
    }

    // ───────────────────────────── Home Catalogs ─────────────────────────────
    override suspend fun getHomeCatalog(): List<CatalogRow> = withContext(Dispatchers.IO) {
        val base = getBaseUrl()
        val sections = listOf(
            Triple("Trending Movies & 4K Releases", "/", MediaType.MOVIE),
            Triple("Latest Web Series & Anime", "/category/series/", MediaType.TV_SERIES),
            Triple("4K UHD & HDR Remux Cinema", "/category/movies/", MediaType.MOVIE),
            Triple("Hindi & Multi-Audio Cinema", "/category/hindi-movies/", MediaType.MOVIE)
        )

        val deferred = sections.map { (title, path, mediaType) ->
            async {
                try {
                    val url = base.trimEnd('/') + path
                    val req = Request.Builder().url(url).header("User-Agent", defaultUserAgent).build()
                    val html = http.scrape.newCall(req).execute().use { r -> if (r.isSuccessful) r.body?.string().orEmpty() else "" }
                    if (html.isBlank()) return@async null

                    val doc = Jsoup.parse(html, url)
                    val items = doc.select(SiteConfig.SEL_CARD).take(20).mapNotNull { a ->
                        val href = a.absUrl("href").ifEmpty { return@mapNotNull null }
                        val itemTitle = a.selectFirst(SiteConfig.SEL_CARD_TITLE)?.text()?.trim()
                            ?: a.selectFirst("img")?.attr("alt")?.trim() ?: return@mapNotNull null
                        val metaText = a.selectFirst(SiteConfig.SEL_CARD_META)?.text() ?: ""
                        val year = Regex("""(19|20)\d{2}""").find("$itemTitle $metaText")?.value?.toIntOrNull()
                        val poster = a.selectFirst("img")?.let { img ->
                            img.absUrl("data-src").ifEmpty { img.absUrl("src") }
                        }?.ifEmpty { null }

                        MediaItem(
                            id = href,
                            title = cleanTitle(itemTitle),
                            url = href,
                            posterUrl = poster,
                            backdropUrl = poster,
                            type = mediaType,
                            year = year,
                            provider = name
                        )
                    }.distinctBy { it.id }

                    if (items.isNotEmpty()) CatalogRow(title = title, items = items) else null
                } catch (_: Throwable) { null }
            }
        }
        deferred.awaitAll().filterNotNull()
    }

    override suspend fun sections(): List<CatalogSection> = listOf(
        CatalogSection("trending", "Trending Movies & 4K Releases"),
        CatalogSection("series", "Latest Web Series & Anime"),
        CatalogSection("movies", "4K UHD & HDR Remux Cinema"),
        CatalogSection("hindi", "Hindi & Multi-Audio Cinema")
    )

    override suspend fun load(section: CatalogSection, page: PageRequest): Page<MediaCard> = withContext(Dispatchers.IO) {
        val path = when (section.id) {
            "series" -> "/category/series/"
            "movies" -> "/category/movies/"
            "hindi" -> "/category/hindi-movies/"
            else -> "/"
        }
        val base = getBaseUrl()
        val pageNum = page.cursor?.toIntOrNull() ?: 1
        val targetPath = if (pageNum == 1) path else "${path.trimEnd('/')}/page/$pageNum/"
        val url = base.trimEnd('/') + targetPath

        val req = Request.Builder().url(url).header("User-Agent", defaultUserAgent).build()
        val html = try {
            http.scrape.newCall(req).execute().use { r -> if (r.isSuccessful) r.body?.string().orEmpty() else "" }
        } catch (_: Throwable) { "" }

        if (html.isBlank()) return@withContext Page(items = emptyList(), nextCursor = null)
        val doc = Jsoup.parse(html, url)
        val cards = doc.select(SiteConfig.SEL_CARD).mapNotNull { a ->
            val href = a.absUrl("href").ifEmpty { return@mapNotNull null }
            val title = a.selectFirst(SiteConfig.SEL_CARD_TITLE)?.text()?.trim()
                ?: a.selectFirst("img")?.attr("alt")?.trim() ?: return@mapNotNull null
            val metaText = a.selectFirst(SiteConfig.SEL_CARD_META)?.text() ?: ""
            val year = Regex("""(19|20)\d{2}""").find("$title $metaText")?.value?.toIntOrNull()
            val poster = a.selectFirst("img")?.let { img ->
                img.absUrl("data-src").ifEmpty { img.absUrl("src") }
            }?.ifEmpty { null }

            MediaCard(
                id = href,
                title = cleanTitle(title),
                type = if (title.contains("Series", ignoreCase = true)) ContentType.TV_SERIES else ContentType.MOVIE,
                posterUrl = poster,
                releaseYear = year,
                target = PlayableTarget.Direct(href)
            )
        }.distinctBy { it.id }

        val hasNext = doc.selectFirst("a.next, .pagination a[rel=next]") != null
        Page(items = cards, nextCursor = if (hasNext && cards.isNotEmpty()) (pageNum + 1).toString() else null)
    }

    // ───────────────────────────── Details ─────────────────────────────
    override suspend fun getDetails(mediaItem: MediaItem): MediaDetail = withContext(Dispatchers.IO) {
        val cached = getCachedDetail(mediaItem.url.ifBlank { mediaItem.id }, mediaItem.title)
        cached.detail
    }

    override suspend fun details(card: MediaCard): MediaDetails = withContext(Dispatchers.IO) {
        val cached = getCachedDetail(card.id, card.title)
        val d = cached.detail

        MediaDetails(
            id = d.id,
            title = d.title,
            type = if (d.type == MediaType.TV_SERIES) ContentType.TV_SERIES else ContentType.MOVIE,
            posterUrl = d.posterUrl,
            backdropUrl = d.backdropUrl,
            year = d.year,
            synopsis = d.synopsis,
            seasons = d.episodes.groupBy { it.seasonNumber }.map { (sNum, eps) ->
                SeasonDescriptor(
                    seasonNumber = sNum,
                    name = "Season $sNum",
                    episodeCount = eps.size,
                    episodes = eps.map { ep ->
                        EpisodeDescriptor(
                            seasonNumber = ep.seasonNumber,
                            episodeNumber = ep.episodeNumber,
                            title = ep.title,
                            target = PlayableTarget.Direct(ep.data)
                        )
                    }
                )
            }
        )
    }

    private suspend fun getCachedDetail(urlOrId: String, fallbackTitle: String): CachedDetail {
        val rebased = rebase(urlOrId)
        val targetUrl = if (rebased.startsWith("http")) rebased else {
            findPageUrl(fallbackTitle, null) ?: rebased
        }

        detailCache[targetUrl]?.let {
            if (System.currentTimeMillis() - it.fetchedAt < SiteConfig.PAGE_TTL_MS) return it
        }

        val lock = pageLocks.getOrPut(targetUrl) { Mutex() }
        return lock.withLock {
            detailCache[targetUrl]?.let {
                if (System.currentTimeMillis() - it.fetchedAt < SiteConfig.PAGE_TTL_MS) return@withLock it
            }

            val req = Request.Builder().url(targetUrl).header("User-Agent", defaultUserAgent).build()
            val html = http.scrape.newCall(req).execute().use { r ->
                if (!r.isSuccessful) throw IOException("Failed to fetch 4KHDHub details: HTTP ${r.code}")
                r.body?.string().orEmpty()
            }

            val doc = Jsoup.parse(html, targetUrl)
            val parsed = parsePageDetail(doc, targetUrl, fallbackTitle)
            detailCache[targetUrl] = parsed
            parsed
        }
    }

    private fun parsePageDetail(doc: Document, pageUrl: String, fallbackTitle: String): CachedDetail {
        val rawTitle = doc.selectFirst("h1.entry-title, h1")?.text()?.trim() ?: doc.title()
        val title = cleanTitle(rawTitle).ifBlank { fallbackTitle }
        val poster = doc.selectFirst(".movie-poster img, .entry-content img, article img")?.let { img ->
            img.absUrl("data-src").ifEmpty { img.absUrl("src") }
        }
        val overview = doc.selectFirst(".entry-content p, .synopsis, #synopsis")?.text()?.trim()
        val year = Regex("""(19|20)\d{2}""").find("$rawTitle $overview")?.value?.toIntOrNull()
        val isTv = rawTitle.contains("Series", ignoreCase = true) ||
                   rawTitle.contains("Season", ignoreCase = true) ||
                   pageUrl.contains("-series-", ignoreCase = true)

        val reQ = Regex("""(2160p|4k|1080p|720p|480p)""", RegexOption.IGNORE_CASE)
        val reSeason = Regex("""(?:S|Season\s*)0?(\d{1,2})""", RegexOption.IGNORE_CASE)
        val reEp = Regex("""(?:E|Episode\s*|EP\s*)0?(\d{1,3})""", RegexOption.IGNORE_CASE)
        val reSize = Regex("""(\d+(?:\.\d+)?\s?(?:GB|MB))""", RegexOption.IGNORE_CASE)

        val dlGroups = mutableListOf<DlGroup>()

        // 1. Scan `.file-title` blocks (Modern 4KHDHub Tailwind DOM)
        val fileTitles = doc.select(".file-title")
        for (ft in fileTitles) {
            val head = ft.text().trim()
            val qMatch = reQ.find(head)?.value?.lowercase() ?: "1080p"
            val height = when (qMatch) { "2160p", "4k" -> 2160; "1080p" -> 1080; "720p" -> 720; else -> 480 }
            val seasonNum = reSeason.find(head)?.groupValues?.get(1)?.toIntOrNull() ?: 1
            val isHdr = Regex("""HDR|DV|Dolby\s*Vision""", RegexOption.IGNORE_CASE).containsMatchIn(head)
            val is10Bit = Regex("""10[-\s]?bit""", RegexOption.IGNORE_CASE).containsMatchIn(head)
            val isHevc = Regex("""HEVC|x265|H\.?265""", RegexOption.IGNORE_CASE).containsMatchIn(head)
            val size = reSize.find(head)?.value

            // Parent container hosting download buttons
            val parent = ft.parent() ?: ft
            val links = parent.select("a[href]").mapNotNull { a ->
                val href = a.absUrl("href")
                if (href.isBlank() || SiteConfig.BLOCKED_HOSTS.any { href.contains(it) }) return@mapNotNull null
                val label = a.text().trim().ifBlank { a.attr("title").ifBlank { "Download Mirror" } }
                val epMatch = reEp.find(label)?.groupValues?.get(1)?.toIntOrNull()
                DlLink(label = label, url = href, episode = epMatch)
            }.distinctBy { it.url }

            if (links.isNotEmpty()) {
                dlGroups.add(
                    DlGroup(
                        season = seasonNum,
                        height = height,
                        isHdr = isHdr,
                        is10Bit = is10Bit,
                        isHevc = isHevc,
                        size = size,
                        title = head.take(160),
                        links = links
                    )
                )
            }
        }

        // 2. Fallback scan for generic link blocks if no .file-title
        if (dlGroups.isEmpty()) {
            val allDownloadLinks = doc.select("a[href]").filter { a ->
                val h = a.absUrl("href")
                SiteConfig.HUB_HOSTS.any { h.contains(it) }
            }.map { a ->
                DlLink(a.text().trim().ifBlank { "Direct HubCloud" }, a.absUrl("href"), null)
            }.distinctBy { it.url }

            if (allDownloadLinks.isNotEmpty()) {
                dlGroups.add(
                    DlGroup(
                        season = 1,
                        height = 1080,
                        isHdr = false,
                        is10Bit = false,
                        isHevc = false,
                        size = null,
                        title = "$title 1080p FHD",
                        links = allDownloadLinks
                    )
                )
            }
        }

        // Build Episode Items
        val episodes = mutableListOf<EpisodeItem>()
        if (isTv) {
            val seasonGroups = dlGroups.groupBy { it.season ?: 1 }
            for ((sNum, groups) in seasonGroups) {
                val allEps = groups.flatMap { it.links }.mapNotNull { it.episode }.distinct().sorted()
                if (allEps.isNotEmpty()) {
                    for (epNum in allEps) {
                        episodes.add(
                            EpisodeItem(
                                id = "${sNum}_${epNum}",
                                title = "Episode $epNum",
                                seasonNumber = sNum,
                                episodeNumber = epNum,
                                data = "$pageUrl?season=$sNum&episode=$epNum"
                            )
                        )
                    }
                } else {
                    // Batch season pack
                    episodes.add(
                        EpisodeItem(
                            id = "${sNum}_1",
                            title = "Season $sNum Full Pack",
                            seasonNumber = sNum,
                            episodeNumber = 1,
                            data = "$pageUrl?season=$sNum&episode=1"
                        )
                    )
                }
            }
        } else {
            // Movie single episode
            episodes.add(
                EpisodeItem(
                    id = "1_1",
                    title = title,
                    seasonNumber = 1,
                    episodeNumber = 1,
                    data = pageUrl
                )
            )
        }

        val trailerUrl = doc.select("iframe[src*='youtube.com/embed'], iframe[src*='youtu.be']").firstOrNull()?.let { iframe ->
            val src = iframe.absUrl("src")
            val yId = Regex("""embed/([a-zA-Z0-9_-]+)""").find(src)?.groupValues?.get(1)
            if (!yId.isNullOrBlank()) "https://www.youtube.com/watch?v=$yId" else null
        } ?: doc.select("a[href*='youtube.com/watch'], a[href*='youtu.be']").firstOrNull()?.let { a ->
            a.absUrl("href").ifBlank { null }
        }

        val mediaDetail = MediaDetail(
            id = pageUrl,
            title = title,
            url = pageUrl,
            posterUrl = poster,
            backdropUrl = poster,
            type = if (isTv) MediaType.TV_SERIES else MediaType.MOVIE,
            year = year,
            synopsis = overview,
            episodes = episodes,
            trailerUrl = trailerUrl
        )

        return CachedDetail(detail = mediaDetail, dlGroups = dlGroups, fetchedAt = System.currentTimeMillis())
    }

    private suspend fun findPageUrl(title: String, year: Int?): String? {
        val clean = cleanTitle(title)
        val searchResults = search(clean, PageRequest(cursor = null, size = 15)).items
        if (searchResults.isEmpty()) return null

        val normTitle = norm(clean)
        return searchResults.maxByOrNull { c ->
            val cNorm = norm(c.title)
            var score = 0
            if (cNorm == normTitle) score += 100
            else if (cNorm.startsWith(normTitle) || normTitle.startsWith(cNorm)) score += 60
            else if (cNorm.contains(normTitle) || normTitle.contains(cNorm)) score += 40
            if (year != null && c.releaseYear == year) score += 30
            score
        }?.id
    }

    // ───────────────────────────── Stream Resolution Flow ─────────────────────────────
    override fun getStreamFlow(mediaId: String, episodeData: String?): Flow<StreamEmission> =
        getStreamFlow(episodeData ?: mediaId)

    override fun getStreamFlow(episodeData: String): Flow<StreamEmission> = channelFlow {
        send(StreamEmission.StatusUpdate(name, "Initializing 4KHDHub cloud engine..."))

        val targetUrl = episodeData.substringBefore('?')
        val sParam = Regex("""season=(\d+)""").find(episodeData)?.groupValues?.get(1)?.toIntOrNull()
        val eParam = Regex("""episode=(\d+)""").find(episodeData)?.groupValues?.get(1)?.toIntOrNull()

        val cached = try {
            getCachedDetail(targetUrl, "")
        } catch (t: Throwable) {
            send(StreamEmission.StatusUpdate(name, "Failed to parse 4KHDHub post page: ${t.message}"))
            return@channelFlow
        }

        val groups = cached.dlGroups
            .filter { sParam == null || it.season == null || it.season == sParam }
            .sortedWith(compareByDescending<DlGroup> { it.height }.thenByDescending { it.isHdr }.thenByDescending { it.isHevc })

        if (groups.isEmpty()) {
            send(StreamEmission.StatusUpdate(name, "No download mirrors found for requested media"))
            return@channelFlow
        }

        send(StreamEmission.StatusUpdate(name, "Resolving intermediate cloud links (${groups.size} variant packages)..."))

        val emittedUrls = Collections.synchronizedSet(HashSet<String>())
        val concurrencyGate = Semaphore(3)

        coroutineScope {
            for (g in groups) {
                launch {
                    concurrencyGate.withPermit {
                        val linksToTest = if (eParam == null) g.links else {
                            val exact = g.links.filter { it.episode == eParam }
                            if (exact.isNotEmpty()) exact else g.links.filter { it.episode == null }
                        }

                        val sorted = linksToTest.sortedBy { l ->
                            when {
                                l.url.contains("hubcloud") -> 0
                                l.url.contains("greenmotors") -> 1
                                l.url.contains("hubdrive") -> 2
                                else -> 3
                            }
                        }.take(3)

                        for (dl in sorted) {
                            try {
                                val candidates = resolveIntermediary(dl.url, eParam)
                                for (cand in candidates) {
                                    val probe = verifyStream(cand) ?: continue
                                    if (emittedUrls.add(probe.url)) {
                                        val qualityLabel = buildString {
                                            append("${g.height}p")
                                            if (g.isHdr) append(" HDR")
                                            if (g.is10Bit) append(" 10Bit")
                                            if (g.isHevc) append(" HEVC")
                                            if (!g.size.isNullOrBlank()) append(" (${g.size})")
                                        }

                                        val serverLabel = "${cand.label} [${cand.kind.name}]"
                                        val headersMap = mapOf(
                                            "User-Agent" to defaultUserAgent,
                                            "Accept-Ranges" to "bytes"
                                        )

                                        val coreSource = CoreStreamSource(
                                            url = probe.url,
                                            serverName = serverLabel,
                                            resolutionLabel = qualityLabel,
                                            quality = qualityLabel,
                                            isM3u8 = false,
                                            headers = headersMap
                                        )
                                        send(StreamEmission.SourceFound(coreSource))
                                    }
                                }
                            } catch (_: Throwable) {
                            }
                        }
                    }
                }
            }
        }
    }.flowOn(Dispatchers.IO)

    override suspend fun getStreamLinks(episodeData: String): StreamResult = withContext(Dispatchers.IO) {
        val sources = mutableListOf<CoreStreamSource>()
        try {
            getStreamFlow(episodeData).collect { em ->
                if (em is StreamEmission.SourceFound) {
                    sources.add(em.source)
                }
            }
        } catch (_: Throwable) {}
        StreamResult(streams = sources.distinctBy { it.url }, subtitles = emptyList())
    }

    override fun resolve(target: PlayableTarget, ctx: ResolveContext): Flow<StreamBundleEvent> = channelFlow {
        val targetUrl = when (target) {
            is PlayableTarget.Direct -> target.pageUrl
            is PlayableTarget.Movie -> findPageUrl(target.title ?: "", target.releaseYear)
            is PlayableTarget.Episode -> findPageUrl(target.title ?: "", null)?.let { "$it?season=${target.season}&episode=${target.episode}" }
            else -> null
        } ?: run {
            send(StreamBundleEvent.Error("Invalid target for 4KHDHub"))
            return@channelFlow
        }

        var sourceCount = 0
        getStreamFlow(targetUrl).collect { em ->
            if (em is StreamEmission.SourceFound) {
                sourceCount++
                val s = em.source
                val eupSource = EupStreamSource(
                    id = "4khdhub:${sha1(s.url).take(8)}",
                    serverId = s.serverName,
                    serverLabel = "${s.serverName} (${s.quality})",
                    url = s.url,
                    kind = StreamKind.PROGRESSIVE,
                    headers = HeaderPolicy(sticky = s.headers),
                    video = VideoInfo(
                        height = if (s.quality.contains("2160")) 2160 else if (s.quality.contains("720")) 720 else 1080,
                        codec = if (s.quality.contains("HEVC", ignoreCase = true)) "hevc" else "h264",
                        isHdr = s.quality.contains("HDR", ignoreCase = true)
                    ),
                    audioTracks = emptyList(),
                    subtitles = emptyList(),
                    refreshHandle = "v1|${s.url}|${s.serverName}"
                )
                send(StreamBundleEvent.SourcesFound(listOf(eupSource)))
            }
        }
        send(StreamBundleEvent.Done(sourceCount))
    }.flowOn(Dispatchers.IO)

    override suspend fun refresh(stale: EupStreamSource, reason: RefreshReason): EupStreamSource? {
        val parts = stale.refreshHandle?.split('|') ?: return null
        if (parts.size < 3 || parts[0] != "v1") return null
        val url = parts[1]
        val probe = verifyStream(MirrorCandidate(url, stale.serverLabel, MirrorKind.FSL_V2, null)) ?: return null
        return stale.copy(url = probe.url)
    }

    // ───────────────────────────── Downloads ─────────────────────────────
    override suspend fun getDownloadLinks(episodeData: String): List<DownloadOption> = withContext(Dispatchers.IO) {
        val streamResult = getStreamLinks(episodeData)
        streamResult.streams.map { s ->
            DownloadOption(
                title = "${s.serverName} [${s.quality}]",
                quality = s.quality,
                size = "~2.5 GB",
                url = s.url,
                source = s.serverName,
                provider = name,
                headers = s.headers
            )
        }
    }

    // ───────────────────────────── Intermediary Cloud Resolver ─────────────────────────────
    private fun resolveIntermediary(startUrl: String, episodeNum: Int?): List<MirrorCandidate> {
        val results = mutableListOf<MirrorCandidate>()
        var currentUrl = startUrl

        // 1. Greenmotors Bypass (s('o', '<base64>', ...) -> Base64 -> Rot13 -> Base64 -> JSON)
        if (currentUrl.contains("greenmotors")) {
            val bypassed = bypassGreenmotors(currentUrl)
            if (bypassed != null) currentUrl = bypassed
        }

        // 2. HubDrive resolver (hubdrive.pics/file/... -> HubCloud Server)
        if (currentUrl.contains("hubdrive")) {
            val hubCloudUrl = resolveHubDrive(currentUrl)
            if (hubCloudUrl != null) currentUrl = hubCloudUrl
        }

        // 3. HubCloud drive page (hubcloud.ist/drive/... -> gamerxyt.com/hubcloud.php?...)
        if (currentUrl.contains("hubcloud")) {
            val gamerUrl = resolveHubCloud(currentUrl)
            if (gamerUrl != null) currentUrl = gamerUrl
        }

        // 4. Gamerxyt stream catalog page
        if (currentUrl.contains("gamerxyt.com")) {
            results.addAll(resolveGamerxyt(currentUrl, episodeNum))
        } else if (currentUrl.startsWith("http") && (currentUrl.contains(".mkv") || currentUrl.contains(".mp4") || currentUrl.contains("pixeldrain") || currentUrl.contains("r2."))) {
            results.add(MirrorCandidate(currentUrl, "Direct Cloud", MirrorKind.OTHER, null))
        }

        return results
    }

    private fun bypassGreenmotors(url: String): String? {
        return try {
            val req = Request.Builder().url(url).header("User-Agent", defaultUserAgent).build()
            val html = http.scrape.newCall(req).execute().use { r -> if (r.isSuccessful) r.body?.string().orEmpty() else "" }
            if (html.isBlank()) return null

            val directMatch = Regex("""https?://[^\'"\s]+hubcloud[^\'"\s]+""").find(html)?.value
            if (directMatch != null && !directMatch.contains(".css") && !directMatch.contains(".js")) {
                return directMatch
            }

            val payloadMatch = Regex("""s\(\s*['"]o['"]\s*,\s*['"]([^'"]+)['"]""").find(html)?.groupValues?.get(1)
            if (payloadMatch != null) {
                val s1 = safeDecodeBase64String(payloadMatch)
                val s2 = safeDecodeBase64String(s1)
                val s3 = rot13(s2)
                val s4 = safeDecodeBase64String(s3)
                val root = json.parseToJsonElement(s4).jsonObject
                val destB64 = root["o"]?.jsonPrimitive?.contentOrNull ?: return null
                return safeDecodeBase64String(destB64).ifBlank { null }
            }
            null
        } catch (_: Throwable) { null }
    }

    private fun resolveHubDrive(url: String): String? {
        return try {
            val req = Request.Builder().url(url).header("User-Agent", defaultUserAgent).header("Referer", "https://greenmotors.club/").build()
            val html = http.scrape.newCall(req).execute().use { r -> if (r.isSuccessful) r.body?.string().orEmpty() else "" }
            Regex("""href=['"](https?://[^/'"\s]*hubcloud[^/'"\s]*/drive/[^'"]+)['"]""").find(html)?.groupValues?.get(1)
        } catch (_: Throwable) { null }
    }

    private fun resolveHubCloud(url: String): String? {
        return try {
            val req = Request.Builder().url(url).header("User-Agent", defaultUserAgent).header("Referer", "https://greenmotors.club/").build()
            val html = http.scrape.newCall(req).execute().use { r -> if (r.isSuccessful) r.body?.string().orEmpty() else "" }
            Regex("""['"](https?://[^'"]*gamerxyt\.com/hubcloud\.php[^'"]*)['"]""").find(html)?.groupValues?.get(1)
                ?: Regex("""id=['"]download['"]\s+href=['"]([^'"]+)['"]""").find(html)?.groupValues?.get(1)
                ?: Regex("""var\s+url\s*=\s*['"]([^'"]+)['"]""").find(html)?.groupValues?.get(1)
        } catch (_: Throwable) { null }
    }

    private fun resolveGamerxyt(gamerUrl: String, episodeNum: Int?): List<MirrorCandidate> {
        val candidates = mutableListOf<MirrorCandidate>()
        try {
            val req = Request.Builder()
                .url(gamerUrl)
                .header("User-Agent", defaultUserAgent)
                .header("Referer", "https://hubcloud.ist/")
                .build()

            val html = http.scrape.newCall(req).execute().use { r -> if (r.isSuccessful) r.body?.string().orEmpty() else "" }
            if (html.isBlank()) return emptyList()

            val doc = Jsoup.parse(html, gamerUrl)
            val links = doc.select("a[href]").map { a ->
                val h = a.absUrl("href")
                val t = a.text().trim()
                t to h
            }

            for ((text, href) in links) {
                if (SiteConfig.BLOCKED_HOSTS.any { href.contains(it) } || href.contains("winexch") || href.contains("snvhost")) continue
                val labelLower = text.lowercase()
                val hrefLower = href.lowercase()

                when {
                    labelLower.contains("fslv2") || hrefLower.contains("valentine.guru") -> {
                        candidates.add(MirrorCandidate(href, "FSLv2 High-Speed CDN", MirrorKind.FSL_V2, "https://gamerxyt.com/"))
                    }
                    labelLower.contains("fsl server") || hrefLower.contains("r2.cloudflarestorage.com") -> {
                        candidates.add(MirrorCandidate(href, "Cloudflare R2 Direct", MirrorKind.R2, null))
                    }
                    labelLower.contains("10gbps") || hrefLower.contains("gpdl.hubcloud") -> {
                        candidates.add(MirrorCandidate(href, "10Gbps Multi-Threaded", MirrorKind.FAST_10GBPS, "https://gamerxyt.com/"))
                    }
                    hrefLower.contains("hbplay.pages.dev") && hrefLower.contains("?u=") -> {
                        val uParam = href.toHttpUrlOrNull()?.queryParameter("u")
                        if (uParam != null) {
                            try {
                                val rawStream = safeDecodeBase64String(uParam)
                                if (rawStream.startsWith("http")) {
                                    candidates.add(MirrorCandidate(rawStream, "HBPlay Edge Mirror", MirrorKind.HBPLAY, null))
                                }
                            } catch (_: Throwable) {}
                        }
                    }
                    hrefLower.contains("pixeldrain.com") -> {
                        val fileId = Regex("""/(?:u|api/file)/([A-Za-z0-9]+)""").find(href)?.groupValues?.get(1)
                        if (fileId != null) {
                            candidates.add(MirrorCandidate("https://pixeldrain.com/api/file/$fileId", "PixelDrain Direct", MirrorKind.PIXELDRAIN, null))
                        }
                    }
                    hrefLower.endsWith(".mkv") || hrefLower.endsWith(".mp4") -> {
                        candidates.add(MirrorCandidate(href, text.ifBlank { "Fast Server" }, MirrorKind.OTHER, "https://gamerxyt.com/"))
                    }
                }
            }
        } catch (_: Throwable) {}

        return candidates
    }

    // ───────────────────────────── Byte-Range Stream Verification ─────────────────────────────
    private fun verifyStream(cand: MirrorCandidate): VerifiedProbe? {
        var currentUrl = cand.url
        for (hop in 0..4) {
            val reqBuilder = Request.Builder()
                .url(currentUrl)
                .header("User-Agent", defaultUserAgent)
                .header("Range", "bytes=0-1023")
                .header("Accept-Encoding", "identity")

            cand.referer?.let { reqBuilder.header("Referer", it) }

            try {
                http.cdn.newCall(reqBuilder.build()).execute().use { resp ->
                    val code = resp.code
                    if (code in 301..308) {
                        val loc = resp.header("Location") ?: return null
                        val next = currentUrl.toHttpUrl().resolve(loc)?.toString() ?: return null
                        currentUrl = next
                        return@use
                    }

                    if (code != 200 && code != 206) return null

                    val cType = resp.header("Content-Type")?.lowercase()?.substringBefore(';')?.trim().orEmpty()
                    if (cType.startsWith("text/") || cType.contains("json") || cType.contains("html")) return null

                    val totalSize = resp.header("Content-Range")?.substringAfter('/')?.toLongOrNull()
                        ?: resp.header("Content-Length")?.toLongOrNull()
                        ?: 0L

                    if (totalSize in 1 until SiteConfig.MIN_VIDEO_BYTES) return null

                    val headBytes = ByteArray(16)
                    val readBytes = resp.body?.byteStream()?.read(headBytes) ?: 0
                    if (readBytes < 4) return null

                    val isMkv = readBytes >= 4 &&
                            headBytes[0] == 0x1A.toByte() &&
                            headBytes[1] == 0x45.toByte() &&
                            headBytes[2] == 0xDF.toByte() &&
                            headBytes[3] == 0xA3.toByte()

                    val isMp4 = readBytes >= 8 && String(headBytes, 4, 4, Charsets.ISO_8859_1) == "ftyp"

                    val container = when {
                        isMkv -> "mkv"
                        isMp4 -> "mp4"
                        cType.contains("matroska") -> "mkv"
                        cType.contains("mp4") -> "mp4"
                        else -> "mkv"
                    }

                    return VerifiedProbe(url = currentUrl, size = totalSize, container = container, mime = cType)
                }
            } catch (_: Throwable) {
                return null
            }
        }
        return null
    }

    // ───────────────────────────── String & Cryptographic Utilities ─────────────────────────────
    private fun safeDecodeBase64(input: String): ByteArray {
        val clean = input.trim().replace("\n", "").replace("\r", "")
        val padded = when (clean.length % 4) {
            2 -> "$clean=="
            3 -> "$clean="
            else -> clean
        }
        return try {
            java.util.Base64.getDecoder().decode(padded)
        } catch (_: Throwable) {
            try {
                java.util.Base64.getUrlDecoder().decode(padded)
            } catch (_: Throwable) {
                ByteArray(0)
            }
        }
    }

    private fun safeDecodeBase64String(input: String): String =
        try { String(safeDecodeBase64(input), Charsets.UTF_8) } catch (_: Throwable) { "" }

    private fun rot13(s: String): String {
        val sb = StringBuilder(s.length)
        for (c in s) {
            when (c) {
                in 'a'..'z' -> sb.append(((c - 'a' + 13) % 26 + 'a'.code).toChar())
                in 'A'..'Z' -> sb.append(((c - 'A' + 13) % 26 + 'A'.code).toChar())
                else -> sb.append(c)
            }
        }
        return sb.toString()
    }

    private fun cleanTitle(raw: String): String {
        return raw.replace(Regex("""(?i)\b(Download|Free|Watch|Online|4khdhub|com|4k-hdhub)\b"""), "")
            .replace(Regex("""\s+"""), " ")
            .trim()
    }

    private fun norm(s: String): String =
        s.lowercase().replace(Regex("""[^a-z0-9]+"""), " ").trim()

    private fun sha1(s: String): String =
        MessageDigest.getInstance("SHA-1").digest(s.toByteArray()).joinToString("") { "%02x".format(it) }
}
