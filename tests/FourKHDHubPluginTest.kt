package com.euthopiar.core.provider

import com.euthopiar.core.model.MediaItem
import com.euthopiar.core.model.MediaType
import com.euthopiar.core.model.StreamEmission
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.*
import okhttp3.OkHttpClient
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.util.concurrent.TimeUnit

class FourKHDHubPluginTest {

    private lateinit var plugin: FourKHDHubPlugin
    private lateinit var client: OkHttpClient

    @Before
    fun setUp() {
        client = OkHttpClient.Builder()
            .connectTimeout(15, TimeUnit.SECONDS)
            .readTimeout(20, TimeUnit.SECONDS)
            .followRedirects(true)
            .followSslRedirects(true)
            .build()
        plugin = FourKHDHubPlugin(client)
    }

    @Test
    fun testBypassDecryptionLogic() {
        // Known payload from greenmotors test
        val testPayload = "Y214WE0xWjNZbXRhVUdwMmIxQldObFo2ZFRCeFZVOXRRbmxxYVV0UU9XRndla2w1YjNveGFYRlVPV3h3YkRWM1RERnFhVzVVT1dkTlNtdDFiM3BGZVhCNWFtbFdkbXAyYjJ4V05sWjZVMVpJZDA5M1JsSXdNa2RWZURWdk1rVkxSbnBqZGtWdGVHdEtlRm94Y0ZSYWJVaExUVzVHVW1OcVRWUXhTWEY2VnpaTk0zbEJXakZYWkUxU2RWaHhlbGRXUm5wMVRVcFNVakZXWVRBOQ=="

        // Test decoding steps directly
        val s1 = String(java.util.Base64.getDecoder().decode(testPayload), Charsets.UTF_8)
        val s2 = String(java.util.Base64.getDecoder().decode(s1), Charsets.UTF_8)

        // ROT13
        val sb = StringBuilder()
        for (c in s2) {
            when (c) {
                in 'a'..'z' -> sb.append(((c - 'a' + 13) % 26 + 'a'.code).toChar())
                in 'A'..'Z' -> sb.append(((c - 'A' + 13) % 26 + 'A'.code).toChar())
                else -> sb.append(c)
            }
        }
        val s3 = sb.toString()
        val s4 = String(java.util.Base64.getDecoder().decode(s3), Charsets.UTF_8)
        assertTrue("Decrypted JSON should contain target URL field", s4.contains("\"o\""))

        val root = kotlinx.serialization.json.Json.parseToJsonElement(s4).jsonObject
        val oB64 = root["o"]?.jsonPrimitive?.content ?: ""
        val finalUrl = String(java.util.Base64.getDecoder().decode(oB64), Charsets.UTF_8)
        assertTrue("Decoded URL must contain hubcloud drive link", finalUrl.contains("hubcloud"))
    }

    @Test
    fun testHomeCatalogLoads() = runBlocking {
        println("Testing 4KHDHub getHomeCatalog()...")
        val catalog = plugin.getHomeCatalog()
        assertNotNull(catalog)
        assertTrue("Catalog rows should not be empty", catalog.isNotEmpty())

        val firstRow = catalog.first()
        println("First catalog row: '${firstRow.title}' with ${firstRow.items.size} items")
        assertTrue("Catalog items should not be empty", firstRow.items.isNotEmpty())

        val firstItem = firstRow.items.first()
        println("Item: ${firstItem.title} (Year: ${firstItem.year}, Quality: ${firstItem.quality}, Type: ${firstItem.type})")
        assertNotNull(firstItem.title)
        assertEquals("4KHDHub", firstItem.provider)
    }

    @Test
    fun testSearch() = runBlocking {
        println("Testing 4KHDHub search('punisher')...")
        val results = plugin.search("punisher")
        assertNotNull(results)
        assertTrue("Search results should not be empty", results.isNotEmpty())

        val match = results.first()
        println("Search match: ${match.title} (Type: ${match.type}, URL: ${match.url})")
        assertTrue("Title should contain 'punisher'", match.title.contains("punisher", ignoreCase = true))
    }

    @Test
    fun testMovieDetailsAndStreamFlow() = runBlocking {
        println("Testing 4KHDHub getDetails() and stream flow for Death of a Unicorn...")
        val movieItem = MediaItem(
            id = "death-of-a-unicorn-movie-8212",
            title = "Death of a Unicorn",
            url = "https://4khdhub.one/death-of-a-unicorn-movie-8212/",
            posterUrl = null,
            type = MediaType.MOVIE,
            provider = "4KHDHub"
        )

        val detail = plugin.getDetails(movieItem)
        assertNotNull(detail)
        assertTrue("Title should contain Death of a Unicorn", detail.title.contains("Death of a Unicorn"))
        assertTrue("Episodes should contain at least 1 movie item", detail.episodes.isNotEmpty())

        val episode = detail.episodes.first()
        println("Movie episode data payload length: ${episode.data.length}")
        assertTrue("Data should contain variants", episode.data.contains("variants"))

        val emissions = plugin.getStreamFlow(episode.data).toList()
        println("Received ${emissions.size} stream emissions:")
        emissions.forEach { emission ->
            when (emission) {
                is StreamEmission.SourceFound -> {
                    println("  -> Stream: ${emission.source.serverName} | Quality: ${emission.source.quality} | URL: ${emission.source.url.take(60)}...")
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
        assertTrue("Should have resolved at least 1 stream source", sources.isNotEmpty())
    }

    @Test
    fun testDetailsFromIdOnlyWithEmptyUrl() = runBlocking {
        println("Testing 4KHDHub getDetails() with empty URL (simulating App Details Navigation)...")
        val dummyItem = MediaItem(
            id = "death-of-a-unicorn-movie-8212",
            title = "",
            url = "",
            posterUrl = null,
            type = MediaType.MOVIE,
            provider = "4KHDHub"
        )

        val detail = plugin.getDetails(dummyItem)
        assertNotNull(detail)
        println("Resolved Title: ${detail.title}")
        println("Resolved Backdrop: ${detail.backdropUrl}")
        println("Resolved Rating: ${detail.rating}")
        println("Resolved Synopsis: ${detail.synopsis?.take(60)}...")
        println("Resolved Cast: ${detail.cast.size} members")
        assertTrue("Title should not be blank", detail.title.isNotBlank())
        assertTrue("Synopsis should not be blank", !detail.synopsis.isNullOrBlank())
        assertTrue("Poster should not be blank", !detail.posterUrl.isNullOrBlank())
    }

    @Test
    fun testSeriesDetailsAndEpisodes() = runBlocking {
        println("Testing 4KHDHub getDetails() for TV Series...")
        val seriesItem = MediaItem(
            id = "the-final-problem-series-8203",
            title = "The Final Problem",
            url = "https://4khdhub.one/the-final-problem-series-8203/",
            posterUrl = null,
            type = MediaType.TV_SERIES,
            provider = "4KHDHub"
        )

        val detail = plugin.getDetails(seriesItem)
        assertNotNull(detail)
        println("Resolved Series: ${detail.title}, Episodes count: ${detail.episodes.size}")
        detail.episodes.forEach { ep ->
            println("  Ep S${ep.seasonNumber}E${ep.episodeNumber}: '${ep.title}' (Audio: ${ep.audioType}, Languages: ${ep.availableLanguages})")
        }
        assertTrue("Episodes should contain at least 2 episodes", detail.episodes.size > 1)
        assertEquals("Should have exactly 4 episodes for S01 of The Final Problem", 4, detail.episodes.size)

        // Test stream flow on first episode to verify audio tracks
        val ep1 = detail.episodes.first()
        val emissions = plugin.getStreamFlow(ep1.data).toList()
        val sources = emissions.filterIsInstance<StreamEmission.SourceFound>()
        println("Resolved ${sources.size} direct media stream sources for Ep 1:")
        sources.forEach { s ->
            println("  -> ${s.source.serverName} | Quality: ${s.source.quality} | Tracks: ${s.source.audioTracks.map { "${it.languageName} (${it.codec})" }}")
        }
        assertTrue("Must have resolved direct stream sources", sources.isNotEmpty())

        // Test Marvel's The Punisher (Episode 1, 2, 3)
        println("Testing Marvel's The Punisher series...")
        val punisherItem = MediaItem(
            id = "marvels-the-punisher-series-2288",
            title = "Marvel's The Punisher",
            url = "https://4khdhub.one/marvels-the-punisher-series-2288/",
            posterUrl = null,
            type = MediaType.TV_SERIES,
            provider = "4KHDHub"
        )
        val punisherDetail = plugin.getDetails(punisherItem)
        println("Punisher episodes count: ${punisherDetail.episodes.size}")
        punisherDetail.episodes.forEach {
            println("  -> Punisher Ep S${it.seasonNumber}E${it.episodeNumber}: '${it.title}' (Languages: ${it.availableLanguages})")
        }
        assertTrue("Punisher should have multiple episodes", punisherDetail.episodes.size >= 2)

        // Test MobLand (Season 1 and Season 2)
        println("Testing MobLand series...")
        val moblandItem = MediaItem(
            id = "mobland-series-98",
            title = "MobLand",
            url = "https://4khdhub.one/mobland-series-98/",
            posterUrl = null,
            type = MediaType.TV_SERIES,
            provider = "4KHDHub"
        )
        val moblandDetail = plugin.getDetails(moblandItem)
        println("Mobland episodes count: ${moblandDetail.episodes.size}")
        moblandDetail.episodes.forEach {
            println("  -> Mobland Ep S${it.seasonNumber}E${it.episodeNumber}: '${it.title}'")
        }
        assertTrue("Mobland should have multiple episodes across seasons", moblandDetail.episodes.size >= 2)
    }

    @Test
    fun testSoloLevelingEpisodeNormalizationAndStreams() = runBlocking {
        println("Testing 4KHDHub Solo Leveling series episode normalization & streams...")
        val soloItem = MediaItem(
            id = "solo-leveling-series-272",
            title = "Solo Leveling",
            url = "https://4khdhub.one/solo-leveling-series-272/",
            posterUrl = null,
            type = MediaType.TV_SERIES,
            provider = "4KHDHub"
        )
        val detail = plugin.getDetails(soloItem)
        assertNotNull(detail)
        println("Solo Leveling total episodes count: ${detail.episodes.size}")

        val s2Episodes = detail.episodes.filter { it.seasonNumber == 2 }
        println("Season 2 episodes count: ${s2Episodes.size}")
        s2Episodes.forEach {
            println("  S02E${it.episodeNumber}: '${it.title}'")
        }
        assertEquals("Season 2 should have exactly 13 episodes (not 25)", 13, s2Episodes.size)
        assertEquals("Season 2 first episode should be 1", 1, s2Episodes.first().episodeNumber)
        assertEquals("Season 2 last episode should be 13", 13, s2Episodes.last().episodeNumber)

        // Test stream extraction for Season 2 Episode 1
        val ep1 = s2Episodes.first()
        val emissions = plugin.getStreamFlow(ep1.data).toList()
        val sources = emissions.filterIsInstance<StreamEmission.SourceFound>()
        println("Solo Leveling S02E01 direct stream sources count: ${sources.size}")
        sources.forEach { s ->
            println("  -> Server: ${s.source.serverName} | Quality: ${s.source.quality} | URL: ${s.source.url.take(80)}...")
            assertFalse("Stream URL must NOT be an intermediate redirect HTML page", s.source.url.contains("hubcloud.ist/?id="))
        }
        assertTrue("Should have resolved at least 1 direct stream source for Solo Leveling S02E01", sources.isNotEmpty())
    }
}
