package com.euthopiar.core.provider

import com.euthopiar.core.model.MediaItem
import com.euthopiar.core.model.MediaType
import com.euthopiar.core.model.StreamEmission
import com.euthopiar.core.network.DohDns
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

class PvrPlayPluginTest {

    private lateinit var plugin: PvrPlayPlugin
    private lateinit var client: OkHttpClient

    @Before
    fun setUp() {
        client = DohDns.createOkHttpClient(
            connectTimeoutSeconds = 15,
            readTimeoutSeconds = 25
        )
        plugin = PvrPlayPlugin(client)
    }

    @Test
    fun testHomeCatalogLoads() = runBlocking {
        println("Testing PvrPlay getHomeCatalog()...")
        val catalog = plugin.getHomeCatalog()
        assertNotNull(catalog)
        assertTrue("Catalog rows should not be empty", catalog.isNotEmpty())
        println("Resolved ${catalog.size} native catalog rows:")
        catalog.forEach { row ->
            println("  -> Row: '${row.title}' (${row.items.size} items)")
        }
        assertTrue("Should have multiple native category rows", catalog.size >= 3)

        val firstRow = catalog.first()
        assertTrue("First catalog row items should not be empty", firstRow.items.isNotEmpty())
        val firstItem = firstRow.items.first()
        println("Sample Item: ${firstItem.title} (Year: ${firstItem.year}, Type: ${firstItem.type})")
        assertNotNull(firstItem.title)
        assertEquals("PvrPlay", firstItem.provider)
    }

    @Test
    fun testSearch() = runBlocking {
        println("Testing PvrPlay search('Loki')...")
        val results = plugin.search("Loki")
        assertNotNull(results)
        assertTrue("Search results for 'Loki' should not be empty", results.isNotEmpty())

        val match = results.first()
        println("Search match: ${match.title} (Type: ${match.type}, URL: ${match.url})")
        assertTrue("Title should contain 'Loki'", match.title.contains("Loki", ignoreCase = true))
    }

    @Test
    fun testMovieDetailsAndStreamFlow() = runBlocking {
        println("Testing PvrPlay getDetails() and stream flow for Insidious...")
        val movieItem = MediaItem(
            id = "insidious-out-of-the-further-2026-1291595",
            title = "Insidious: Out of the Further",
            url = "${plugin.mainUrl}/movie/insidious-out-of-the-further-2026-1291595",
            posterUrl = null,
            type = MediaType.MOVIE,
            provider = "PvrPlay"
        )

        val detail = plugin.getDetails(movieItem)
        assertNotNull(detail)
        println("Movie Detail Title: ${detail.title}")
        println("Movie Detail Backdrop: ${detail.backdropUrl}")
        println("Movie Detail Rating: ${detail.rating}")
        println("Movie Detail Cast: ${detail.cast.size} members")
        assertTrue("Title should contain Insidious", detail.title.contains("Insidious", ignoreCase = true))
        assertTrue("Episodes should contain at least 1 movie item", detail.episodes.isNotEmpty())

        val episode = detail.episodes.first()
        println("Episode payload data: ${episode.data}")

        val emissions = plugin.getStreamFlow(episode.data).toList()
        println("Received ${emissions.size} stream emissions:")
        val sources = emissions.filterIsInstance<StreamEmission.SourceFound>()
        val subtitles = emissions.filterIsInstance<StreamEmission.SubtitleFound>()

        sources.forEach { s ->
            println("  -> Stream: ${s.source.serverName} | Quality: ${s.source.quality} | Tracks: ${s.source.audioTracks.map { it.languageName }} | URL: ${s.source.url.take(65)}...")
        }
        subtitles.forEach { sub ->
            println("  -> Subtitle: ${sub.track.language} (${sub.track.url.take(50)}...)")
        }

        assertTrue("Should resolve direct stream sources", sources.isNotEmpty())

        // Verify downloads
        val downloads = plugin.getDownloadLinks(episode.data)
        println("Resolved download options: ${downloads.size}")
        downloads.take(3).forEach { d ->
            println("  -> Download: ${d.title} | ${d.quality} | ${d.size}")
            assertTrue(d.headers.containsKey("Accept-Ranges"))
        }
        assertTrue("Download options should not be empty", downloads.isNotEmpty())
    }

    @Test
    fun testTvSeriesAndEpisodes() = runBlocking {
        println("Testing PvrPlay getDetails() for TV series...")
        val tvItem = MediaItem(
            id = "loki-2021-84958",
            title = "Loki",
            url = "${plugin.mainUrl}/tv/loki-2021-84958",
            posterUrl = null,
            type = MediaType.TV_SERIES,
            provider = "PvrPlay"
        )

        val detail = plugin.getDetails(tvItem)
        assertNotNull(detail)
        println("Series Detail Title: ${detail.title}")
        println("Series Episodes Count: ${detail.episodes.size}")
        assertTrue("Series should have at least 1 episode", detail.episodes.isNotEmpty())

        val ep1 = detail.episodes.first()
        println("Ep1 Title: ${ep1.title}, Data: ${ep1.data}")

        val emissions = plugin.getStreamFlow(ep1.data).toList()
        val sources = emissions.filterIsInstance<StreamEmission.SourceFound>()
        println("Ep1 resolved streams count: ${sources.size}")
        assertTrue("Should resolve streams for TV episode", sources.isNotEmpty())
    }
}
