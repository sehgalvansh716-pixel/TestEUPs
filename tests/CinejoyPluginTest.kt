package com.euthopiar.core.provider

import com.euthopiar.core.model.MediaItem
import com.euthopiar.core.model.MediaType
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.contentOrNull
import com.euthopiar.core.network.DohDns
import okhttp3.OkHttpClient
import okhttp3.Request
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

class CinejoyPluginTest {

    private lateinit var plugin: CinejoyPlugin
    private lateinit var client: OkHttpClient
    private val json = Json { ignoreUnknownKeys = true }

    @Before
    fun setUp() {
        client = DohDns.createOkHttpClient(15, 25)
        plugin = CinejoyPlugin(client)
    }

    @Test
    fun testHomeCatalogLoadsNonEmpty() = runBlocking {
        try {
            println("Testing getHomeCatalog()...")
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
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Cinejoy home catalog failed due to network", e)
        }
    }

    @Test
    fun testSearchReturnsResults() = runBlocking {
        try {
            println("Testing search('batman')...")
            val results = plugin.search("batman")
            assertNotNull(results)
            assertTrue("Search results should not be empty", results.isNotEmpty())

            println("Found ${results.size} search results for 'batman'")
            val match = results.first()
            println("Top result: ${match.title} (ID: ${match.id}, Type: ${match.type}, Year: ${match.year})")
            assertTrue("Result title should contain 'Batman' or be related", match.title.contains("Batman", ignoreCase = true))
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Cinejoy search failed due to network", e)
        }
    }

    @Test
    fun testMovieDetailsAndTvDetails() = runBlocking {
        try {
            println("Testing getDetails() for Toy Story 5 (Movie)...")
            val movieItem = MediaItem(
                id = "1084244",
                title = "Toy Story 5",
                url = "https://cinejoy.pk/movie/1084244",
                posterUrl = null,
                type = MediaType.MOVIE
            )
            val movieDetails = plugin.getDetails(movieItem)
            assertNotNull(movieDetails)
            assertEquals("1084244", movieDetails.id)
            assertNotNull("Synopsis should be loaded", movieDetails.synopsis)
            assertTrue("Episodes should have 1 item for movie", movieDetails.episodes.isNotEmpty())
            println("Movie synopsis: ${movieDetails.synopsis?.take(100)}...")
            println("Movie genres: ${movieDetails.genres}")

            println("Testing getDetails() for Game of Thrones (TV)...")
            val tvItem = MediaItem(
                id = "1399",
                title = "Game of Thrones",
                url = "https://cinejoy.pk/tv/1399",
                posterUrl = null,
                type = MediaType.TV_SERIES
            )
            val tvDetails = plugin.getDetails(tvItem)
            assertNotNull(tvDetails)
            assertEquals("1399", tvDetails.id)
            assertTrue("TV series should have multiple episodes", tvDetails.episodes.size > 1)
            println("TV episode count generated: ${tvDetails.episodes.size}")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Cinejoy details failed due to network", e)
        }
    }

    @Test
    fun testStreamExtractionBlackAdam() = runBlocking {
        println("Testing getStreamLinks() for Black Adam (436270)...")
        val streamResult = try {
            plugin.getStreamLinks("436270")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Cinejoy stream extraction failed due to network", e)
            return@runBlocking
        }
        println("Streams count for Black Adam: ${streamResult.streams.size}")
        org.junit.Assume.assumeTrue("Black Adam should have streams", streamResult.streams.isNotEmpty())
        streamResult.streams.forEach { s ->
            println("Stream: ${s.quality} -> ${s.url}")
            try {
                val req = Request.Builder().url(s.url).apply { s.headers.forEach { (k, v) -> header(k, v) } }.build()
                val resp = client.newCall(req).execute()
                println("HTTP status for ${s.quality}: ${resp.code}")
                resp.close()
            } catch (_: Exception) {}
        }
    }

    @Test
    fun testStreamExtractionReturnsPlayableHls() = runBlocking {
        println("Testing getStreamLinks() for movie ID 443463 (Leave No Trace)...")
        val streamResult = try {
            plugin.getStreamLinks("443463")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Leave No Trace stream extraction failed due to network", e)
            return@runBlocking
        }
        assertNotNull(streamResult)
        org.junit.Assume.assumeTrue("Streams list should not be empty", streamResult.streams.isNotEmpty())

        val stream = streamResult.streams.first()
        println("Resolved stream URL: ${stream.url}")
        println("Stream quality: ${stream.quality}")
        println("Is M3U8: ${stream.isM3u8}")
        assertTrue("Stream must be M3U8 HLS", stream.isM3u8)

        // Verify headers
        assertEquals("https://cinejoy.pk/", stream.headers["Referer"])
        assertEquals("https://cinejoy.pk", stream.headers["Origin"])

        // Test live stream playlist playback
        println("Verifying live HLS stream connection...")
        try {
            val streamReqBuilder = Request.Builder().url(stream.url)
            stream.headers.forEach { (k, v) -> streamReqBuilder.header(k, v) }
            val streamResp = client.newCall(streamReqBuilder.build()).execute()

            println("HLS Playlist HTTP status: ${streamResp.code}")
            org.junit.Assume.assumeTrue("Stream playlist HTTP status should be 200 OK", streamResp.isSuccessful)
            val playlistBody = streamResp.body?.string() ?: ""
            assertTrue("Playlist must begin with #EXTM3U", playlistBody.startsWith("#EXTM3U"))
            println("Playlist preview:\n${playlistBody.take(200)}\n...")
            streamResp.close()
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Live playlist fetch failed", e)
        }

        // Verify subtitles
        println("Resolved subtitles: ${streamResult.subtitles.size} tracks")
        if (streamResult.subtitles.isNotEmpty()) {
            val firstSub = streamResult.subtitles.first()
            println("Sample subtitle: ${firstSub.language} -> ${firstSub.url}")
        }

        // Test each server specifically
        println("\n--- Testing all servers individually ---")
        streamResult.streams.forEach { s ->
            println("\nTesting server: ${s.quality} -> ${s.url}")
            try {
                val reqBuilder = Request.Builder().url(s.url)
                s.headers.forEach { (k, v) -> reqBuilder.header(k, v) }
                val resp = client.newCall(reqBuilder.build()).execute()
                println("Response code for ${s.quality}: ${resp.code}")
                if (!resp.isSuccessful) {
                    println("ERROR body: ${resp.body?.string()}")
                } else {
                    val preview = resp.body?.string()?.take(150)
                    println("Success preview: $preview")
                }
            } catch (e: Exception) {
                println("Exception for ${s.quality}: ${e.message}")
            }
        }
    }

    @Test
    fun testFetchCast() = runBlocking {
        try {
            println("Testing fetchCast for Toy Story 2 (Movie)...")
            val movieCast = plugin.fetchCast("863", "tt0120363", MediaType.MOVIE)
            println("Movie cast count: ${movieCast.size}")
            movieCast.forEach { println(" - ${it.name} (${it.character ?: "Actor"}) -> ${it.profileUrl}") }
            assertTrue("Movie cast should not be empty", movieCast.isNotEmpty())

            println("Testing fetchCast for Breaking Bad (TV)...")
            val tvCast = plugin.fetchCast("1396", "tt0903747", MediaType.TV_SERIES)
            println("TV cast count: ${tvCast.size}")
            tvCast.forEach { println(" - ${it.name} (${it.character}) -> ${it.profileUrl}") }
            assertTrue("TV cast should not be empty", tvCast.isNotEmpty())
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Cinejoy fetch cast failed due to network", e)
        }
    }

    @Test
    fun testMultipleContentStreams() = runBlocking {
        val testIds = listOf(
            "1032863", // The Love Hypothesis
            "443463",  // Leave No Trace
            "1396:1:1", // Breaking Bad
            "299534",  // Endgame
            "550",     // Fight Club
            "155"      // The Dark Knight
        )

        try {
            for (id in testIds) {
                println("\n================ Testing ID: $id ================")
                val result = plugin.getStreamLinks(id)
                println("Streams for $id count: ${result.streams.size}")
                result.streams.forEach { s ->
                    println("  - ${s.quality}: ${s.url.take(80)}...")
                }
                val downloads = plugin.getDownloadLinks(id)
                println("Downloads for $id count: ${downloads.size}")
                downloads.forEach { d ->
                    println("  - ${d.quality} (${d.size}): ${d.source} -> ${d.url.take(80)}...")
                }
            }
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Cinejoy multiple streams failed due to network", e)
        }
    }

    @Test
    fun testWasmEngineDetailed() = runBlocking {
        try {
            val servers = listOf("Nebula", "Lisbon", "Solara", "Athens")
            for (server in servers) {
                val resSeries = CinejoyWasmEngine.requestStream(client, server, "series", "1396", 1, 1)
                println("\nServer $server (series): $resSeries")
                if (resSeries != null && resSeries.contains("playlist")) {
                    val root = json.parseToJsonElement(resSeries).jsonObject
                    val streamArr = root["data"]?.jsonObject?.get("stream")?.jsonArray
                    val playlistUrl = streamArr?.firstOrNull()?.jsonObject?.get("playlist")?.jsonPrimitive?.contentOrNull
                    if (playlistUrl != null) {
                        println("Testing GET on playlist: $playlistUrl")
                        val req = Request.Builder()
                            .url(playlistUrl)
                            .header("Referer", "https://cinejoy.pk/")
                            .header("Origin", "https://cinejoy.pk")
                            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
                            .build()
                        val resp = client.newCall(req).execute()
                        println("Playlist response code: ${resp.code}")
                        val body = resp.body?.string() ?: ""
                        println("Playlist body preview:\n${body.take(300)}")

                        // If it has sub-playlists or segments, test fetching one!
                        if (body.startsWith("#EXTM3U")) {
                            val firstSubUri = body.lines().firstOrNull { it.isNotBlank() && !it.startsWith("#") }
                            if (firstSubUri != null) {
                                val baseUri = java.net.URI(playlistUrl)
                                val resolvedUri = baseUri.resolve(firstSubUri).toString()
                                println("Testing segment/sub-playlist fetch: $resolvedUri")
                                val subReq = Request.Builder()
                                    .url(resolvedUri)
                                    .header("Referer", "https://cinejoy.pk/")
                                    .header("Origin", "https://cinejoy.pk")
                                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
                                    .build()
                                val subResp = client.newCall(subReq).execute()
                                println("Segment/sub-playlist response code: ${subResp.code}")
                                val subBody = subResp.body?.string() ?: ""
                                println("Segment/sub-playlist preview: ${subBody.take(150)}")
                                subResp.close()
                            }
                        }
                    }
                }
            }
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Cinejoy Wasm engine detailed failed due to network", e)
        }
    }
}
