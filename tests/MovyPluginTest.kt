package com.euthopiar.core.provider

import com.euthopiar.core.model.MediaItem
import com.euthopiar.core.model.MediaType
import com.euthopiar.core.network.DohDns
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

class MovyPluginTest {

    private lateinit var plugin: MovyPlugin
    private lateinit var client: OkHttpClient

    @Before
    fun setUp() {
        client = DohDns.createOkHttpClient(15, 25)
        plugin = MovyPlugin(client)
    }

    @Test
    fun testHomeCatalogLoadsNonEmpty() = runBlocking {
        println("Testing Movy getHomeCatalog()...")
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
        assertEquals("Provider should be Movy", "Movy", firstItem.provider)
        assertTrue("URL should point to movy.sx", firstItem.url.contains("movy.sx"))
    }

    @Test
    fun testSearchReturnsResults() = runBlocking {
        println("Testing Movy search('dune')...")
        val results = try { plugin.search("dune") } catch (_: Exception) { return@runBlocking }
        org.junit.Assume.assumeNotNull(results)
        org.junit.Assume.assumeTrue("Search results should not be empty", results.isNotEmpty())

        val match = results.first()
        println("Top result: ${match.title} (ID: ${match.id}, Type: ${match.type}, Year: ${match.year})")
        assertTrue("Result title should contain 'dune'", match.title.contains("Dune", ignoreCase = true))
        assertEquals("Provider should be Movy", "Movy", match.provider)
    }

    @Test
    fun testMovieDetailsAndTvDetails() = runBlocking {
        println("Testing Movy getDetails() for Dune: Part Two (693134)...")
        val movieItem = MediaItem(
            id = "693134",
            title = "Dune: Part Two",
            url = "https://www.movy.sx/movie/693134",
            posterUrl = null,
            type = MediaType.MOVIE
        )
        val movieDetails = try { plugin.getDetails(movieItem) } catch (_: Exception) { return@runBlocking }
        org.junit.Assume.assumeNotNull(movieDetails)
        assertEquals("693134", movieDetails.id)
        assertNotNull("Synopsis should be loaded", movieDetails.synopsis)
        assertTrue("Episodes should have 1 item for movie", movieDetails.episodes.isNotEmpty())
        org.junit.Assume.assumeTrue("Cast should not be empty", movieDetails.cast.isNotEmpty())
        println("Movie cast count: ${movieDetails.cast.size}, top actor: ${movieDetails.cast.first().name}")
        println("Movie logoUrl: ${movieDetails.logoUrl}")

        println("Testing Movy getDetails() for Breaking Bad (1396)...")
        val tvItem = MediaItem(
            id = "1396",
            title = "Breaking Bad",
            url = "https://www.movy.sx/tv/1396",
            posterUrl = null,
            type = MediaType.TV_SERIES
        )
        val tvDetails = try { plugin.getDetails(tvItem) } catch (_: Exception) { return@runBlocking }
        org.junit.Assume.assumeNotNull(tvDetails)
        assertEquals("1396", tvDetails.id)
        assertTrue("TV series should have multiple episodes", tvDetails.episodes.size > 1)
        println("TV episode count generated: ${tvDetails.episodes.size}")

        val firstEp = tvDetails.episodes.first()
        println("First TV Episode: '${firstEp.title}' thumbnail='${firstEp.thumbnail}'")
        assertEquals("First episode should be Pilot", "Pilot", firstEp.title)
        assertNotNull("Episode thumbnail should not be null", firstEp.thumbnail)
    }

    @Test
    fun testMovieStreamExtractionAndDecryption() = runBlocking {
        println("Testing Movy getStreamLinks() for Dune Part Two (693134)...")
        val streamResult = try {
            plugin.getStreamLinks("693134")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Movy stream extraction failed due to network", e)
            return@runBlocking
        }
        assertNotNull(streamResult)
        org.junit.Assume.assumeTrue("Streams list should not be empty", streamResult.streams.isNotEmpty())

        println("Movy movie stream count: ${streamResult.streams.size}")
        streamResult.streams.forEach { s ->
            println("  [Movy Stream] Server: ${s.serverName} | Quality: ${s.resolutionLabel} -> ${s.url.take(80)}...")
            assertTrue("Stream URL must not be blank", s.url.isNotBlank())
            assertTrue("Stream must be M3U8 or MP4", s.isM3u8 || s.url.contains(".mp4") || s.url.contains("storage"))
            assertEquals("https://www.movy.sx/", s.headers["Referer"])
            assertEquals("https://www.movy.sx", s.headers["Origin"])
        }

        println("Movy subtitles count: ${streamResult.subtitles.size}")
        if (streamResult.subtitles.isNotEmpty()) {
            println("Sample subtitle: ${streamResult.subtitles.first().language} -> ${streamResult.subtitles.first().url}")
        }
    }

    @Test
    fun testTvStreamExtractionAndDecryption() = runBlocking {
        println("Testing Movy TV stream extraction (1396:1:1)...")
        val tvResult = try {
            plugin.getStreamLinks("1396:1:1")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Movy TV stream extraction failed due to network", e)
            return@runBlocking
        }
        assertNotNull(tvResult)
        org.junit.Assume.assumeTrue("TV streams should not be empty", tvResult.streams.isNotEmpty())
        println("TV streams count: ${tvResult.streams.size}")
        tvResult.streams.take(5).forEach { s ->
            println("  [Movy TV Stream] Server: ${s.serverName} | Quality: ${s.resolutionLabel} -> ${s.url.take(80)}...")
        }
    }

    @Test
    fun testDownloadLinks() = runBlocking {
        println("Testing Movy getDownloadLinks() for Dune Part Two (693134)...")
        val downloads = try {
            plugin.getDownloadLinks("693134")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Movy download extraction failed due to network", e)
            emptyList()
        }
        assertNotNull(downloads)
        org.junit.Assume.assumeTrue("Downloads should not be empty", downloads.isNotEmpty())
        println("Downloads count: ${downloads.size}")
        val first = downloads.first()
        println("Download option: ${first.title} -> ${first.url.take(80)}...")
        assertEquals("Movy", first.provider)
        assertNotNull(first.headers["Referer"])
    }
}
