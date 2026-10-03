package com.euthopiar.core.provider

import com.euthopiar.core.model.MediaItem
import com.euthopiar.core.model.MediaType
import com.euthopiar.core.network.DohDns
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.util.concurrent.TimeUnit

class AetherPluginTest {

    private lateinit var plugin: AetherPlugin
    private lateinit var client: OkHttpClient

    @Before
    fun setUp() {
        client = DohDns.createOkHttpClient(15, 25)
        plugin = AetherPlugin(client)
    }

    @Test
    fun testHomeCatalogLoadsNonEmpty() = runBlocking {
        println("Testing Aether getHomeCatalog()...")
        val catalogRows = plugin.getHomeCatalog()
        assertNotNull(catalogRows)
        assertTrue("Catalog rows should not be empty", catalogRows.isNotEmpty())

        val firstRow = catalogRows.first()
        println("First catalog row: '${firstRow.title}' with ${firstRow.items.size} items")
        assertTrue("Row title should not be blank", firstRow.title.isNotBlank())
        assertTrue("Row items should not be empty", firstRow.items.isNotEmpty())

        val firstItem = firstRow.items.first()
        println("First media item: ${firstItem.title} (ID: ${firstItem.id}, Type: ${firstItem.type})")
        assertTrue("Item ID should not be blank", firstItem.id.isNotBlank())
        assertTrue("Item title should not be blank", firstItem.title.isNotBlank())
        assertEquals("Provider should be Aether", "Aether", firstItem.provider)
    }

    @Test
    fun testSearchReturnsResults() = runBlocking {
        println("Testing Aether search('batman')...")
        val results = plugin.search("batman")
        assertNotNull(results)
        assertTrue("Search results should not be empty", results.isNotEmpty())

        val match = results.first()
        println("Top result: ${match.title} (ID: ${match.id}, Type: ${match.type}, Year: ${match.year})")
        assertTrue("Result title should contain 'Batman'", match.title.contains("Batman", ignoreCase = true))
        assertEquals("Provider should be Aether", "Aether", match.provider)
    }

    @Test
    fun testMovieDetailsAndTvDetails() = runBlocking {
        println("Testing Aether getDetails() for Fight Club (550)...")
        val movieItem = MediaItem(
            id = "550",
            title = "Fight Club",
            url = "https://aether.ist/movie/550",
            posterUrl = null,
            type = MediaType.MOVIE
        )
        val movieDetails = plugin.getDetails(movieItem)
        assertNotNull(movieDetails)
        assertEquals("550", movieDetails.id)
        assertNotNull("Synopsis should be loaded", movieDetails.synopsis)
        assertTrue("Episodes should have 1 item for movie", movieDetails.episodes.isNotEmpty())
        assertTrue("Cast should not be empty", movieDetails.cast.isNotEmpty())
        println("Movie cast count: ${movieDetails.cast.size}, top actor: ${movieDetails.cast.first().name}")
        println("Movie logoUrl: ${movieDetails.logoUrl}")
        assertNotNull("Movie clear logo should be resolved", movieDetails.logoUrl)
        assertTrue("Movie logo should be an http URL", movieDetails.logoUrl!!.startsWith("http"))

        println("Testing Aether getDetails() for Game of Thrones (1399)...")
        val tvItem = MediaItem(
            id = "1399",
            title = "Game of Thrones",
            url = "https://aether.ist/tv/1399",
            posterUrl = null,
            type = MediaType.TV_SERIES
        )
        val tvDetails = plugin.getDetails(tvItem)
        assertNotNull(tvDetails)
        assertEquals("1399", tvDetails.id)
        assertTrue("TV series should have multiple episodes", tvDetails.episodes.size > 1)
        println("TV episode count generated: ${tvDetails.episodes.size}")
        println("TV logoUrl: ${tvDetails.logoUrl}")
        assertNotNull("TV clear logo should be resolved", tvDetails.logoUrl)
        assertTrue("TV logo should be an http URL", tvDetails.logoUrl!!.startsWith("http"))

        val firstEp = tvDetails.episodes.first()
        println("First TV Episode: '${firstEp.title}' thumbnail='${firstEp.thumbnail}'")
        assertEquals("First episode should be 'Winter Is Coming'", "Winter Is Coming", firstEp.title)
        assertNotNull("Episode thumbnail should not be null", firstEp.thumbnail)
        assertTrue("Episode thumbnail should be TMDB still image", firstEp.thumbnail!!.contains("/w500/"))
    }

    @Test
    fun testStreamExtractionAndDecryption() = runBlocking {
        println("Testing Aether getStreamLinks() for Fight Club (550)...")
        val streamResult = try {
            plugin.getStreamLinks("550")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Aether stream extraction failed due to network", e)
            return@runBlocking
        }
        assertNotNull(streamResult)
        org.junit.Assume.assumeTrue("Streams list should not be empty", streamResult.streams.isNotEmpty())

        var foundMeridianOrLul = false
        streamResult.streams.forEach { s ->
            println("Aether Stream: ${s.quality} -> ${s.url.take(80)}...")
            assertTrue("Stream must be M3U8", s.isM3u8)
            assertFalse("Headers must not be empty", s.headers.isEmpty())
            if (s.quality.contains("Meridian") || s.quality.contains("Lul")) {
                foundMeridianOrLul = true
            }
        }
        println("Found Meridian/Lul server: $foundMeridianOrLul")

        println("Aether subtitles count: ${streamResult.subtitles.size}")
        if (streamResult.subtitles.isNotEmpty()) {
            println("Sample subtitle: ${streamResult.subtitles.first().language} -> ${streamResult.subtitles.first().url}")
        }

        println("Testing Aether TV stream extraction (1399:1:1)...")
        val tvResult = try {
            plugin.getStreamLinks("1399:1:1")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Aether TV stream extraction failed due to network", e)
            return@runBlocking
        }
        assertNotNull(tvResult)
        org.junit.Assume.assumeTrue("TV streams should not be empty", tvResult.streams.isNotEmpty())
        println("TV streams count: ${tvResult.streams.size}")
        tvResult.streams.forEach { s ->
            println("  [GOT Stream] ${s.quality} -> ${s.url.take(80)}...")
        }
    }

    @Test
    fun testReacherNativeStreams() = runBlocking {
        println("=== Testing Reacher on Aether (108978:1:1) ===")
        val reacherStreams = try {
            plugin.getStreamLinks("108978:1:1")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Reacher stream query failed", e)
            return@runBlocking
        }
        println("Reacher streams count: ${reacherStreams.streams.size}")
        org.junit.Assume.assumeTrue("Reacher streams should not be empty", reacherStreams.streams.isNotEmpty())
        reacherStreams.streams.forEach { s ->
            println("  [Aether Native Stream] ${s.quality} -> ${s.url.take(80)}...")
            assertTrue("Stream should be M3U8", s.isM3u8)
            assertTrue("Stream should have valid headers", s.headers.containsKey("Referer") || s.headers.containsKey("Origin"))
        }
    }

    @Test
    fun testDownloadLinks() = runBlocking {
        println("Testing Aether getDownloadLinks() for Fight Club (550)...")
        val downloads = try {
            plugin.getDownloadLinks("550")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Aether download extraction failed due to network", e)
            emptyList()
        }
        assertNotNull(downloads)
        org.junit.Assume.assumeTrue("Downloads should not be empty", downloads.isNotEmpty())
        println("Downloads count: ${downloads.size}")
        val first = downloads.first()
        println("Download option: ${first.title} -> ${first.url.take(80)}...")
        assertEquals("Aether", first.provider)
        assertTrue("Download option should have playback headers", first.headers.isNotEmpty())
    }

    @Test
    fun testAphroditeSourceIsPresentAndValid() = runBlocking {
        println("Testing Aether Aphrodite source presence for Fight Club (550)...")
        val streamResult = try {
            plugin.getStreamLinks("550")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Aether stream extraction failed due to network", e)
            return@runBlocking
        }
        val aphroditeStreams = streamResult.streams.filter {
            it.serverName.equals("Aphrodite", ignoreCase = true) || it.quality.contains("Aphrodite", ignoreCase = true)
        }
        println("Aphrodite streams count: ${aphroditeStreams.size}")
        assertTrue("Aphrodite source MUST be present in Aether streams", aphroditeStreams.isNotEmpty())
        aphroditeStreams.forEach { s ->
            println("Aphrodite stream: ${s.quality} -> ${s.url.take(80)}...")
            assertFalse("Aphrodite must not be honeypot 8-segment stream", s.url.contains("totallyacdn.org/cdn-m3u8?payload="))
        }
    }
}
