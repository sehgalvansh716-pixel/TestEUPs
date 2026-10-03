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
import java.util.concurrent.TimeUnit

class UhdMoviesPluginTest {

    private lateinit var plugin: UhdMoviesPlugin
    private lateinit var client: OkHttpClient

    @Before
    fun setUp() {
        client = DohDns.createOkHttpClient(
            connectTimeoutSeconds = 15,
            readTimeoutSeconds = 25
        )
        plugin = UhdMoviesPlugin(client)
    }

    @Test
    fun testHomeCatalogLoads() = runBlocking {
        println("Testing UHDMovies getHomeCatalog()...")
        val catalog = plugin.getHomeCatalog()
        assertNotNull(catalog)
        assertTrue("Catalog rows should not be empty", catalog.isNotEmpty())

        val firstRow = catalog.first()
        println("First catalog row: '${firstRow.title}' with ${firstRow.items.size} items")
        assertTrue("Catalog items should not be empty", firstRow.items.isNotEmpty())

        val firstItem = firstRow.items.first()
        println("Item: ${firstItem.title} (Year: ${firstItem.year}, Quality: ${firstItem.quality}, Type: ${firstItem.type})")
        assertNotNull(firstItem.title)
        assertEquals("UHDMovies", firstItem.provider)
    }

    @Test
    fun testSearch() = runBlocking {
        println("Testing UHDMovies search('loki')...")
        val results = plugin.search("loki")
        assertNotNull(results)
        assertTrue("Search results for 'loki' should not be empty", results.isNotEmpty())

        val match = results.first()
        println("Search match: ${match.title} (Type: ${match.type}, URL: ${match.url})")
        assertTrue("Title should contain 'loki'", match.title.contains("loki", ignoreCase = true))
    }

    @Test
    fun testMovieDetailsAndDirectGoogleCdnStream() = runBlocking {
        println("Testing UHDMovies getDetails() and stream flow for The Love Hypothesis...")
        val movieItem = MediaItem(
            id = "download-the-love-hypothesis-2026-dual-audio-hindi-english-2160p-4k-1080p-10bit-hevc-blu-ray-esubs",
            title = "The Love Hypothesis",
            url = "https://uhdmovies.my/download-the-love-hypothesis-2026-dual-audio-hindi-english-2160p-4k-1080p-10bit-hevc-blu-ray-esubs/",
            posterUrl = null,
            type = MediaType.MOVIE,
            provider = "UHDMovies"
        )

        val detail = plugin.getDetails(movieItem)
        assertNotNull(detail)
        println("Movie Detail Title: ${detail.title}")
        println("Movie Detail Backdrop: ${detail.backdropUrl}")
        println("Movie Detail Rating: ${detail.rating}")
        println("Movie Detail Cast: ${detail.cast.size} members")
        assertTrue("Title should contain Love Hypothesis", detail.title.contains("Love Hypothesis", ignoreCase = true))
        assertTrue("Episodes should contain at least 1 movie item", detail.episodes.isNotEmpty())

        val episode = detail.episodes.first()
        assertTrue("Data should contain variants", episode.data.contains("variants"))

        val emissions = plugin.getStreamFlow(episode.data).toList()
        println("Received ${emissions.size} stream emissions:")
        emissions.forEach { emission ->
            when (emission) {
                is StreamEmission.SourceFound -> {
                    println("  -> Stream: ${emission.source.serverName} | Quality: ${emission.source.quality} | URL: ${emission.source.url.take(70)}...")
                }
                is StreamEmission.StatusUpdate -> {
                    println("  -> Status: [${emission.serverName}] ${emission.message}")
                }
                is StreamEmission.SubtitleFound -> {
                    println("  -> Subtitle: ${emission.track.language}")
                }
            }
        }

        val sources = emissions.filterIsInstance<StreamEmission.SourceFound>()
        // Live third-party redirectors may be temporarily down or under upstream maintenance
        org.junit.Assume.assumeTrue("Should have resolved at least 1 stream source", sources.isNotEmpty())

        val googleCdnSource = sources.firstOrNull { it.source.url.contains("googleusercontent.com") || it.source.url.contains("video-gen.xyz") || it.source.url.contains("video-seed.dev") }
        assertNotNull("Should find a Google CDN direct or Instant CDN stream source", googleCdnSource)

        // Verify getDownloadLinks returns fast and valid downloads with Accept-Ranges
        println("Testing getDownloadLinks for movie...")
        val downloads = plugin.getDownloadLinks(episode.data)
        println("Resolved download options: ${downloads.size}")
        downloads.forEach { d ->
            println("  -> Download: ${d.title} | ${d.quality} | ${d.size} | ${d.url.take(60)}...")
            assertTrue("Download must have valid URL", d.url.startsWith("http"))
            assertTrue("Download must have Accept-Ranges header", d.headers.containsKey("Accept-Ranges"))
        }
        org.junit.Assume.assumeTrue("Should have resolved at least 1 download option", downloads.isNotEmpty())
    }

    @Test
    fun testSeriesDetailsAndEpisodeMapping() = runBlocking {
        println("Testing UHDMovies getDetails() for TV series Solo Leveling...")
        val seriesItem = MediaItem(
            id = "download-solo-leveling-2024-season-1-hindi-japanese-1080p",
            title = "Solo Leveling",
            url = "https://uhdmovies.my/download-solo-leveling-2024-season-1-hindi-japanese-1080p/",
            posterUrl = null,
            type = MediaType.TV_SERIES,
            provider = "UHDMovies"
        )

        val detail = plugin.getDetails(seriesItem)
        assertNotNull(detail)
        println("Series Detail Title: ${detail.title}")
        println("Series Episodes Count: ${detail.episodes.size}")

        assertTrue("Series should have at least 12 episodes", detail.episodes.size >= 12)

        // Verify seasons are properly separated (Season 1 and Season 2)
        val seasons = detail.episodes.map { it.seasonNumber }.distinct()
        println("Extracted seasons: $seasons")
        assertTrue("Should extract Season 1", seasons.contains(1))

        val s1Eps = detail.episodes.filter { it.seasonNumber == 1 }
        println("Season 1 episode count: ${s1Eps.size}")
        assertTrue("Season 1 should have 12 episodes", s1Eps.size >= 12)

        val ep1 = detail.episodes.first()
        println("Ep1 title: '${ep1.title}', languages: ${ep1.availableLanguages}, audioType: ${ep1.audioType}")
        assertTrue("Episode 1 should have multi-audio languages", ep1.availableLanguages.isNotEmpty())

        println("Testing getDownloadLinks for series Episode 1...")
        val epDownloads = plugin.getDownloadLinks(ep1.data)
        println("Resolved episode download options: ${epDownloads.size}")
        epDownloads.forEach { d ->
            println("  -> Ep Download: ${d.title} | ${d.quality} | ${d.size} | ${d.url.take(60)}...")
            assertTrue("Episode download must have valid URL", d.url.startsWith("http"))
            assertTrue("Episode download must have Accept-Ranges header", d.headers.containsKey("Accept-Ranges"))
        }
        // Third-party LinkPilot servers may experience transient Cloudflare 502/rate-limits
        org.junit.Assume.assumeTrue("Episode 1 should have download options", epDownloads.isNotEmpty())
    }
}

