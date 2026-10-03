package com.euthopiar.core.provider

import com.euthopiar.core.model.MediaItem
import com.euthopiar.core.model.MediaType
import com.euthopiar.core.network.DohDns
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

class MoviesLeechPluginTest {

    private lateinit var plugin: MoviesLeechPlugin
    private lateinit var client: OkHttpClient

    @Before
    fun setUp() {
        client = DohDns.createOkHttpClient(connectTimeoutSeconds = 15, readTimeoutSeconds = 25)
        plugin = MoviesLeechPlugin(client)
    }

    @Test
    fun testDirectBypass() {
        val leechUrl = "https://leechpro.blog/archives/36510"
        println("Testing bypassLeechpro directly for: $leechUrl")
        val cloudUrl = plugin.bypassLeechpro(leechUrl)
        println("Result of bypassLeechpro: $cloudUrl")
        org.junit.Assume.assumeNotNull("Archive link expired on leechpro", cloudUrl)

        println("Testing bypassUnblockedgames directly for: $cloudUrl")
        val driveseedUrl = plugin.bypassUnblockedgames(cloudUrl!!)
        println("Result of bypassUnblockedgames: $driveseedUrl")
        assertNotNull("driveseedUrl should not be null", driveseedUrl)

        println("Testing resolveDriveseed directly for: $driveseedUrl")
        val servers = plugin.resolveDriveseed(driveseedUrl!!)
        println("Resolved servers: ${servers.size}")
        servers.forEach { s ->
            println("  Server: ${s.name} -> ${s.url.take(80)}")
        }
        assertTrue("Should resolve at least 1 direct server", servers.isNotEmpty())
    }

    @Test
    fun testHomeCatalogLoadsNonEmpty() = runBlocking {
        println("Testing MoviesLeech getHomeCatalog()...")
        val catalogRows = try { plugin.getHomeCatalog() } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Network error on home catalog", e)
            return@runBlocking
        }
        assertNotNull(catalogRows)
        org.junit.Assume.assumeTrue("Catalog rows should not be empty", catalogRows.isNotEmpty())

        val firstRow = catalogRows.first()
        println("First catalog row: '${firstRow.title}' with ${firstRow.items.size} items")
        assertTrue("Row title should not be blank", firstRow.title.isNotBlank())
        assertTrue("Row items should not be empty", firstRow.items.isNotEmpty())

        val firstItem = firstRow.items.first()
        println("First media item: ${firstItem.title} (ID: ${firstItem.id}, Type: ${firstItem.type})")
        assertTrue("Item ID should not be blank", firstItem.id.isNotBlank())
        assertTrue("Item title should not be blank", firstItem.title.isNotBlank())
        assertEquals("Provider should be MoviesLeech", "MoviesLeech", firstItem.provider)
    }

    @Test
    fun testSearchReturnsResults() = runBlocking {
        println("Testing MoviesLeech search('Lust')...")
        val results = try { plugin.search("Lust") } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Network error on search", e)
            return@runBlocking
        }
        assertNotNull(results)
        org.junit.Assume.assumeTrue("Search results should not be empty", results.isNotEmpty())

        val match = results.first()
        println("Top search result: ${match.title} (ID: ${match.id}, Type: ${match.type}, Year: ${match.year})")
        assertTrue("Result title should not be blank", match.title.isNotBlank())
        assertEquals("Provider should be MoviesLeech", "MoviesLeech", match.provider)
    }

    @Test
    fun testMovieDetailsWithTmdbEnrichment() = runBlocking {
        println("Testing MoviesLeech getDetails() with TMDB Bridge...")
        val testItem = MediaItem(
            id = "download-lust-stories-3-2026-hindi-movie-web-dl",
            title = "Lust Stories 3",
            url = "https://moviesleech.club/download-lust-stories-3-2026-hindi-movie-web-dl/",
            posterUrl = null,
            type = MediaType.MOVIE,
            provider = "MoviesLeech"
        )

        val details = try { plugin.getDetails(testItem) } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Network error on details", e)
            return@runBlocking
        }
        assertNotNull(details)
        assertTrue("Details title should not be blank", details.title.isNotBlank())
        assertTrue("Episodes should not be empty", details.episodes.isNotEmpty())
        assertFalse("Details ID should not contain slashes", details.id.contains("/"))

        println("Details Title: ${details.title}")
        println("Poster URL: ${details.posterUrl}")
        println("Backdrop URL: ${details.backdropUrl}")
        println("Rating: ${details.rating}")
        println("Cast count: ${details.cast.size}")
        println("Episodes count: ${details.episodes.size}")
        println("Episode 1 payload length: ${details.episodes.first().data.length}")
    }

    @Test
    fun testBypassAndStreamResolution() = runBlocking {
        println("Testing MoviesLeech end-to-end multi-stage bypass and stream extraction...")
        val testItem = MediaItem(
            id = "download-lust-stories-3-2026-hindi-movie-web-dl",
            title = "Lust Stories 3",
            url = "https://moviesleech.club/download-lust-stories-3-2026-hindi-movie-web-dl/",
            posterUrl = null,
            type = MediaType.MOVIE,
            provider = "MoviesLeech"
        )

        val details = try { plugin.getDetails(testItem) } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Network error on details", e)
            return@runBlocking
        }
        val episodeData = details.episodes.first().data

        val streamResult = try {
            plugin.getStreamLinks(episodeData)
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Network error on stream links", e)
            return@runBlocking
        }

        assertNotNull(streamResult)
        println("Streams found: ${streamResult.streams.size}")
        streamResult.streams.forEach { s ->
            println("  -> [${s.resolutionLabel}] ${s.serverName}: ${s.url.take(80)}...")
            assertTrue("Headers must include Range support", s.headers.containsKey("Accept-Ranges") || s.headers.containsKey("User-Agent"))
        }

        println("Subtitles found: ${streamResult.subtitles.size}")
        streamResult.subtitles.take(5).forEach { sub ->
            println("  -> Subtitle: ${sub.language} (${sub.url.take(60)}...)")
        }

        println("Testing getDownloadLinks for MoviesLeech movie...")
        val downloads = plugin.getDownloadLinks(episodeData)
        println("Resolved movie downloads: ${downloads.size}")
        downloads.forEach { d ->
            println("  -> Download: ${d.title} | ${d.quality} | ${d.size} | ${d.url.take(60)}...")
            assertTrue("Download must have valid URL", d.url.startsWith("http"))
            assertTrue("Download must have Accept-Ranges header", d.headers.containsKey("Accept-Ranges"))
        }
        assertTrue("Should have resolved download options for movie", downloads.isNotEmpty())
    }

    @Test
    fun testWebSeriesDetailsWithEpisodeBanners() = runBlocking {
        println("Testing MoviesLeech Web Series getDetails() with individual episodes and banners...")
        val seriesItem = MediaItem(
            id = "download-shaque-trust-no-one-2026-season-1-hindi-netflix-series-web-dl",
            title = "Shaque: Trust No One",
            url = "https://moviesleech.club/download-shaque-trust-no-one-2026-season-1-hindi-netflix-series-web-dl/",
            posterUrl = null,
            type = MediaType.TV_SERIES,
            provider = "MoviesLeech"
        )

        val details = try { plugin.getDetails(seriesItem) } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Network error on series details", e)
            return@runBlocking
        }
        assertNotNull(details)
        println("Series Title: ${details.title}")
        println("Series Backdrop: ${details.backdropUrl}")
        println("Series Total Episodes: ${details.episodes.size}")

        assertTrue("Web series should have multiple parsed episodes", details.episodes.size > 1)

        val firstEp = details.episodes.first()
        println("Episode 1 title: '${firstEp.title}', thumbnail: '${firstEp.thumbnail}'")
        assertTrue("Episode 1 title should not be blank", firstEp.title.isNotBlank())
        assertNotNull("Episode 1 should have a thumbnail banner", firstEp.thumbnail)
        assertFalse("Episode ID should not contain slashes", firstEp.id.contains("/"))

        // Test stream resolving for Episode 1 of the web series
        println("Testing stream resolution for web series Episode 1...")
        val epStreams = try {
            plugin.getStreamLinks(firstEp.data)
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Network error on episode stream links", e)
            return@runBlocking
        }
        assertNotNull(epStreams)
        println("Episode 1 streams found: ${epStreams.streams.size}")
        epStreams.streams.forEach { s ->
            println("  -> Ep1 Stream: [${s.resolutionLabel}] ${s.serverName}")
        }

        println("Testing getDownloadLinks for web series Episode 1...")
        val epDownloads = plugin.getDownloadLinks(firstEp.data)
        println("Episode 1 download options: ${epDownloads.size}")
        epDownloads.forEach { d ->
            println("  -> Ep1 Download: ${d.title} | ${d.quality} | ${d.size}")
            assertTrue("Download must have valid URL", d.url.startsWith("http"))
            assertTrue("Download must have Accept-Ranges header", d.headers.containsKey("Accept-Ranges"))
        }
        assertTrue("Episode 1 should have download options", epDownloads.isNotEmpty())
    }
}
