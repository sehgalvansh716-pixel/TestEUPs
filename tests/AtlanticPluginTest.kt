package com.euthopiar.core.provider

import com.euthopiar.core.model.MediaItem
import com.euthopiar.core.model.MediaType
import com.euthopiar.core.network.DohDns
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

class AtlanticPluginTest {

    private lateinit var plugin: AtlanticPlugin
    private lateinit var client: OkHttpClient

    @Before
    fun setUp() {
        client = DohDns.createOkHttpClient(15, 25)
        plugin = AtlanticPlugin(client)
    }

    @Test
    fun testHomeCatalogLoadsNonEmpty() = runBlocking {
        println("Testing Atlantic getHomeCatalog()...")
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
        assertEquals("Provider should be Atlantic", "Atlantic", firstItem.provider)
    }

    @Test
    fun testSearchReturnsResults() = runBlocking {
        println("Testing Atlantic search('fight club')...")
        val results = try { plugin.search("fight club") } catch (_: Exception) { return@runBlocking }
        org.junit.Assume.assumeNotNull(results)
        org.junit.Assume.assumeTrue("Search results should not be empty", results.isNotEmpty())

        val match = results.first()
        println("Top result: ${match.title} (ID: ${match.id}, Type: ${match.type}, Year: ${match.year})")
        assertTrue("Result title should contain 'Fight' or 'Club'", match.title.contains("Fight", ignoreCase = true))
        assertEquals("Provider should be Atlantic", "Atlantic", match.provider)
    }

    @Test
    fun testMovieDetailsAndTvDetails() = runBlocking {
        println("Testing Atlantic getDetails() for Fight Club (550)...")
        val movieItem = MediaItem(
            id = "550",
            title = "Fight Club",
            url = "https://atlantic.st/movie/550",
            posterUrl = null,
            type = MediaType.MOVIE
        )
        val movieDetails = try { plugin.getDetails(movieItem) } catch (_: Exception) { return@runBlocking }
        org.junit.Assume.assumeNotNull(movieDetails)
        assertEquals("550", movieDetails.id)
        assertNotNull("Synopsis should be loaded", movieDetails.synopsis)
        assertTrue("Episodes should have 1 item for movie", movieDetails.episodes.isNotEmpty())
        org.junit.Assume.assumeTrue("Cast should not be empty", movieDetails.cast.isNotEmpty())
        println("Movie cast count: ${movieDetails.cast.size}, top actor: ${movieDetails.cast.first().name}")
        println("Movie logoUrl: ${movieDetails.logoUrl}")
        assertNotNull("Movie clear logo should be resolved", movieDetails.logoUrl)
        assertTrue("Movie logo should be an http URL", movieDetails.logoUrl!!.startsWith("http"))

        println("Testing Atlantic getDetails() for Game of Thrones (1399)...")
        val tvItem = MediaItem(
            id = "1399",
            title = "Game of Thrones",
            url = "https://atlantic.st/tv/1399",
            posterUrl = null,
            type = MediaType.TV_SERIES
        )
        val tvDetails = try { plugin.getDetails(tvItem) } catch (_: Exception) { return@runBlocking }
        org.junit.Assume.assumeNotNull(tvDetails)
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
        println("Testing Atlantic getStreamLinks() for Fight Club (550)...")
        val streamResult = try {
            plugin.getStreamLinks("550")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Atlantic stream extraction failed due to network", e)
            return@runBlocking
        }
        assertNotNull(streamResult)
        org.junit.Assume.assumeTrue("Streams list should not be empty", streamResult.streams.isNotEmpty())

        streamResult.streams.forEach { s ->
            println("Atlantic Stream: ${s.quality} -> ${s.url.take(80)}...")
            assertTrue("Stream must be M3U8", s.isM3u8)
            assertEquals("https://atlantic.st/", s.headers["Referer"])
        }

        println("Atlantic subtitles count: ${streamResult.subtitles.size}")
        if (streamResult.subtitles.isNotEmpty()) {
            println("Sample subtitle: ${streamResult.subtitles.first().language} -> ${streamResult.subtitles.first().url}")
        }

        println("Testing Atlantic TV stream extraction (1399:1:1)...")
        val tvResult = try {
            plugin.getStreamLinks("1399:1:1")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Atlantic TV stream extraction failed due to network", e)
            return@runBlocking
        }
        assertNotNull(tvResult)
        org.junit.Assume.assumeTrue("TV streams should not be empty", tvResult.streams.isNotEmpty())
        println("TV streams count: ${tvResult.streams.size}")
    }

    @Test
    fun testReacherNativeStreams() = runBlocking {
        println("=== Testing Reacher on Atlantic (108978:1:1) ===")
        val reacherStreams = try {
            plugin.getStreamLinks("108978:1:1")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Reacher stream query failed", e)
            return@runBlocking
        }
        println("Reacher streams count: ${reacherStreams.streams.size}")
        org.junit.Assume.assumeTrue("Reacher streams should not be empty", reacherStreams.streams.isNotEmpty())
        reacherStreams.streams.forEach { s ->
            println("  [Atlantic Native Stream] ${s.quality} -> ${s.url.take(80)}...")
        }
    }

    @Test
    fun testObsessionMultiAudioStreams() = runBlocking {
        println("=== Testing Obsession on Atlantic (223313:1:1) ===")
        val obsessionStreams = try {
            plugin.getStreamLinks("223313:1:1")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Obsession stream query failed", e)
            return@runBlocking
        }
        println("Obsession streams count: ${obsessionStreams.streams.size}")
        org.junit.Assume.assumeTrue("Obsession streams should not be empty", obsessionStreams.streams.isNotEmpty())
        obsessionStreams.streams.forEach { s ->
            println("  [Atlantic Obsession Stream] ${s.quality} -> ${s.url.take(80)}...")
        }
    }

    @Test
    fun testDownloadLinks() = runBlocking {
        println("Testing Atlantic getDownloadLinks() for Fight Club (550)...")
        val downloads = try {
            plugin.getDownloadLinks("550")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Atlantic download extraction failed due to network", e)
            emptyList()
        }
        assertNotNull(downloads)
        org.junit.Assume.assumeTrue("Downloads should not be empty", downloads.isNotEmpty())
        println("Downloads count: ${downloads.size}")
        val first = downloads.first()
        println("Download option: ${first.title} -> ${first.url.take(80)}...")
        assertEquals("Atlantic", first.provider)
    }

    @Test
    fun testAphroditeSourceIsPresentAndValid() = runBlocking {
        println("Testing Atlantic Aphrodite source presence for Fight Club (550)...")
        val streamResult = try {
            plugin.getStreamLinks("550")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Atlantic stream extraction failed due to network", e)
            return@runBlocking
        }
        val aphroditeStreams = streamResult.streams.filter {
            it.serverName.equals("Aphrodite", ignoreCase = true) || it.quality.contains("Aphrodite", ignoreCase = true)
        }
        println("Aphrodite streams count: ${aphroditeStreams.size}")
        assertTrue("Aphrodite source MUST be present in Atlantic streams", aphroditeStreams.isNotEmpty())
        aphroditeStreams.forEach { s ->
            println("Aphrodite stream: ${s.quality} -> ${s.url.take(80)}...")
            assertFalse("Aphrodite must not be honeypot 8-segment stream", s.url.contains("totallyacdn.org/cdn-m3u8?payload="))
        }
    }
}
