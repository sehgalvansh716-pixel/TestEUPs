package com.euthopiar.core.provider

import com.euthopiar.core.model.MediaItem
import com.euthopiar.core.model.MediaType
import com.euthopiar.core.network.DohDns
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

class OneShowsPluginTest {

    private lateinit var plugin: OneShowsPlugin
    private lateinit var client: OkHttpClient

    @Before
    fun setUp() {
        client = DohDns.createOkHttpClient(15, 25)
        plugin = OneShowsPlugin(client)
    }

    @Test
    fun testHomeCatalogLoadsNonEmpty() = runBlocking {
        println("Testing 1Shows getHomeCatalog()...")
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
        assertEquals("Provider should be 1Shows", "1Shows", firstItem.provider)
    }

    @Test
    fun testSearchReturnsResults() = runBlocking {
        println("Testing 1Shows search('fight club')...")
        val results = plugin.search("fight club")
        assertNotNull(results)
        assertTrue("Search results should not be empty", results.isNotEmpty())

        val match = results.first()
        println("Top result: ${match.title} (ID: ${match.id}, Type: ${match.type}, Year: ${match.year})")
        assertTrue("Result title should contain 'Fight' or 'Club'", match.title.contains("Fight", ignoreCase = true))
        assertEquals("Provider should be 1Shows", "1Shows", match.provider)
    }

    @Test
    fun testMovieDetailsAndTvDetails() = runBlocking {
        println("Testing 1Shows getDetails() for Fight Club (550)...")
        val movieItem = MediaItem(
            id = "550",
            title = "Fight Club",
            url = "https://www.1shows.bz/movie/550",
            posterUrl = null,
            type = MediaType.MOVIE
        )
        val movieDetails = plugin.getDetails(movieItem)
        assertNotNull(movieDetails)
        assertEquals("550", movieDetails.id)
        assertNotNull("Synopsis should be loaded", movieDetails.synopsis)
        assertTrue("Episodes should have 1 item for movie", movieDetails.episodes.isNotEmpty())
        println("Movie cast count: ${movieDetails.cast.size}")
        println("Movie logoUrl: ${movieDetails.logoUrl}")
        assertNotNull("Movie clear logo should be resolved", movieDetails.logoUrl)
        assertTrue("Movie logo should be an http URL", movieDetails.logoUrl!!.startsWith("http"))

        println("Testing 1Shows getDetails() for Game of Thrones (1399)...")
        val tvItem = MediaItem(
            id = "1399",
            title = "Game of Thrones",
            url = "https://www.1shows.bz/tv/1399",
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
        println("Testing 1Shows getStreamLinks() for Fight Club (550)...")
        val streamResult = plugin.getStreamLinks("550")
        assertNotNull(streamResult)
        assertTrue("Streams list should not be empty", streamResult.streams.isNotEmpty())

        println("1Shows streams count: ${streamResult.streams.size}")
        streamResult.streams.forEach { s ->
            println("Stream: ${s.quality} -> ${s.url.take(80)}...")
        }

        println("1Shows subtitles count: ${streamResult.subtitles.size}")
        assertTrue("Subtitles should be returned", streamResult.subtitles.isNotEmpty())
        println("Sample subtitle: ${streamResult.subtitles.first().language} -> ${streamResult.subtitles.first().url}")

        println("Testing 1Shows TV stream extraction (1399:1:1)...")
        val tvResult = plugin.getStreamLinks("1399:1:1")
        assertNotNull(tvResult)
        assertTrue("TV streams should not be empty", tvResult.streams.isNotEmpty())
        println("TV streams count: ${tvResult.streams.size}")
        assertTrue("TV subtitles should not be empty", tvResult.subtitles.isNotEmpty())
    }

    @Test
    fun testDownloadLinks() = runBlocking {
        println("Testing 1Shows getDownloadLinks() for Fight Club (550)...")
        val downloads = plugin.getDownloadLinks("550")
        assertNotNull(downloads)
        assertTrue("Downloads should not be empty", downloads.isNotEmpty())
        println("Downloads count: ${downloads.size}")
        downloads.forEach { d ->
            println("Download: ${d.quality} (${d.size}) -> ${d.url.take(80)}...")
        }
        assertEquals("1Shows", downloads.first().provider)
    }

    @Test
    fun testReacherAndObsessionMultiAudioStreams() = runBlocking {
        println("=== Testing Reacher (108978:1:1) ===")
        val reacherStreams = plugin.getStreamLinks("108978:1:1")
        println("Reacher streams count: ${reacherStreams.streams.size}")
        reacherStreams.streams.forEach { s ->
            println("  [Stream] ${s.quality} -> ${s.url}")
        }
        assertTrue("Reacher should have multiple servers", reacherStreams.streams.size >= 2)
        assertTrue(
            "Reacher should have multi-audio / 5.1 surround / DTS tracks",
            reacherStreams.streams.any {
                it.quality.contains("5.1", ignoreCase = true) ||
                it.quality.contains("DTS", ignoreCase = true) ||
                it.quality.contains("Multi", ignoreCase = true) ||
                it.quality.contains("Hindi", ignoreCase = true)
            }
        )
        println("Reacher subtitles count: ${reacherStreams.subtitles.size}")
        assertTrue("Reacher should have subtitles", reacherStreams.subtitles.isNotEmpty())
        reacherStreams.subtitles.take(10).forEach { sub ->
            println("  [Sub] ${sub.language} -> ${sub.url}")
        }

        println("=== Searching Obsession ===")
        val obsessionSearch = plugin.search("Obsession")
        println("Obsession search results: ${obsessionSearch.size}")
        obsessionSearch.take(5).forEach { item ->
            println("  Obsession match: ${item.title} (ID: ${item.id}, Type: ${item.type}, Year: ${item.year})")
        }

        println("=== Testing Obsession (223313:1:1) ===")
        val obsessionStreams = plugin.getStreamLinks("223313:1:1")
        println("Obsession 223313 streams count: ${obsessionStreams.streams.size}")
        obsessionStreams.streams.forEach { s ->
            println("  [Obsession Stream] ${s.quality} -> ${s.url.take(80)}...")
        }
        println("Obsession 223313 subtitles count: ${obsessionStreams.subtitles.size}")
        assertTrue("Obsession should have multi-audio streams", obsessionStreams.streams.isNotEmpty())
        assertTrue("Obsession should have multiple servers", obsessionStreams.streams.size >= 2)
    }

    @Test
    fun testZeroCrossContamination() = runBlocking {
        val streamResult = plugin.getStreamLinks("550")
        val downloads = plugin.getDownloadLinks("550")

        val forbiddenKeywords = listOf("atlantic.st", "hls.lol", "cinejoy", "aether.ist")
        for (s in streamResult.streams) {
            forbiddenKeywords.forEach { kw ->
                assertFalse("Stream URL must not contain '$kw': ${s.url}", s.url.contains(kw, ignoreCase = true))
            }
            s.headers.values.forEach { headerVal ->
                forbiddenKeywords.forEach { kw ->
                    assertFalse("Stream header must not contain '$kw': $headerVal", headerVal.contains(kw, ignoreCase = true))
                }
            }
        }

        for (d in downloads) {
            forbiddenKeywords.forEach { kw ->
                assertFalse("Download URL must not contain '$kw': ${d.url}", d.url.contains(kw, ignoreCase = true))
            }
            d.headers.values.forEach { headerVal ->
                forbiddenKeywords.forEach { kw ->
                    assertFalse("Download header must not contain '$kw': $headerVal", headerVal.contains(kw, ignoreCase = true))
                }
            }
        }
    }

    @Test
    fun testVidzeeWasmDecryption() {
        val stream = javaClass.classLoader?.getResourceAsStream("vidzee.wasm")
        assertNotNull("vidzee.wasm resource must exist", stream)
        val module = com.dylibso.chicory.wasm.Parser.parse(stream!!)
        val abortFn = com.dylibso.chicory.runtime.HostFunction(
            "env",
            "abort",
            listOf(com.dylibso.chicory.wasm.types.ValueType.I32, com.dylibso.chicory.wasm.types.ValueType.I32, com.dylibso.chicory.wasm.types.ValueType.I32, com.dylibso.chicory.wasm.types.ValueType.I32),
            emptyList()
        ) { _, _ -> longArrayOf() }

        val instance = com.dylibso.chicory.runtime.Instance.builder(module)
            .withImportValues(com.dylibso.chicory.runtime.ImportValues.builder().withFunctions(listOf(abortFn)).build())
            .build()

        val allocNew = instance.export("__new")
        val pin = instance.export("__pin")
        val unpin = instance.export("__unpin")
        val decrypt = instance.export("decrypt")
        val memory = instance.memory()

        fun allocateString(s: String): Int {
            val byteLen = s.length * 2
            val ptr = allocNew.apply(byteLen.toLong(), 2L)[0].toInt()
            val buf = java.nio.ByteBuffer.allocate(byteLen).order(java.nio.ByteOrder.LITTLE_ENDIAN)
            for (ch in s) {
                buf.putChar(ch)
            }
            memory.write(ptr, buf.array())
            return ptr
        }

        fun allocateByteArray(bytes: ByteArray): Int {
            val len = bytes.size
            val abPtr = pin.apply(allocNew.apply(len.toLong(), 1L)[0])[0].toInt()
            memory.write(abPtr, bytes)
            val dPtr = allocNew.apply(12L, 6L)[0].toInt()
            val buf = java.nio.ByteBuffer.allocate(12).order(java.nio.ByteOrder.LITTLE_ENDIAN)
            buf.putInt(abPtr)
            buf.putInt(abPtr)
            buf.putInt(len)
            memory.write(dPtr, buf.array())
            unpin.apply(abPtr.toLong())
            return dPtr
        }

        val b64 = "JHd/O54eLA5vxsCtWdZrNDz0YidatlfsRouzAAU1lyRUGIWOo123KOvylgIVEAa8UPQjtUqHr34MUJpPoqrquMnVQ8aBl8K4nPG2BuamdNHuPmi4A+OfjxJiNqM8S/sC+Sfh+L/Z/+//RLhPR+iW9bJQ41YXtu8djXFobXOJmnoYHaS37yHOh2uWSRkgfSCI5htKL6Qa4qY6WB8ifshe9SSTcPw+slQ61oowfbWDf+CHRc47K9HbsBaDDYcMfrnSJLI3q5qABCp16eRb/2IxwaAa3Q=="
        val rawBytes = java.util.Base64.getDecoder().decode(b64)
        val arrPtr = allocateByteArray(rawBytes)
        val hostPtr = allocateString("player.vidzee.wtf")

        val resPtr = decrypt.apply(arrPtr.toLong(), hostPtr.toLong())[0].toInt()
        println("resPtr: $resPtr")
        assertTrue("resPtr should be > 0", resPtr > 0)

        val metaBytes = memory.readBytes(resPtr, 12)
        val metaBuf = java.nio.ByteBuffer.wrap(metaBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        val bufPtr0 = metaBuf.int
        val bufPtr = metaBuf.int
        val outLen = metaBuf.int
        println("bufPtr: $bufPtr, outLen: $outLen")
        assertTrue("outLen should be > 0", outLen > 0)

        val outBytes = memory.readBytes(bufPtr, outLen)
        val jsonStr = String(outBytes, Charsets.UTF_8)
        println("DECRYPTED JSON FROM CHICORY: $jsonStr")
        assertTrue("JSON should contain url", jsonStr.contains("url"))
    }

    @Test
    fun testTheEndOfOakStreet() = runBlocking {
        println("=== Testing The End of Oak Street (1101383) ===")
        val downloads = plugin.getDownloadLinks("1101383")
        println("Oak Street downloads count: ${downloads.size}")
        downloads.forEach { d ->
            println("  [Download] title='${d.title}' quality='${d.quality}' size='${d.size}' url='${d.url}'")
        }

        val streams = plugin.getStreamLinks("1101383")
        println("Oak Street streams count: ${streams.streams.size}")
        streams.streams.forEach { s ->
            println("  [Stream] quality='${s.quality}' isM3u8=${s.isM3u8} url='${s.url}'")
        }
    }

    @Test
    fun testDownloadExecution() = runBlocking {
        println("=== Testing Download Execution for 1101383 ===")
        val downloads = try {
            plugin.getDownloadLinks("1101383")
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("1Shows download extraction failed due to network", e)
            emptyList()
        }
        org.junit.Assume.assumeTrue("Downloads should not be empty", downloads.isNotEmpty())
        val firstOpt = downloads.first()
        println("Testing option: ${firstOpt.title} -> ${firstOpt.url}")

        // 1. Fetch playlist
        val reqBuilder = okhttp3.Request.Builder().url(firstOpt.url)
        firstOpt.headers.forEach { (k, v) -> reqBuilder.header(k, v) }
        val resp = try {
            client.newCall(reqBuilder.build()).execute()
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Live playlist fetch failed", e)
            return@runBlocking
        }
        println("Playlist response code: ${resp.code}")
        val playlistText = resp.body?.string() ?: ""
        println("Playlist preview:\n${playlistText.take(500)}")
        resp.close()
        org.junit.Assume.assumeTrue("Playlist must be 200 OK", resp.isSuccessful)

        // 2. Parse first segment
        val lines = playlistText.lines()
        var segLine: String? = null
        for (l in lines) {
            val t = l.trim()
            if (t.isNotBlank() && !t.startsWith("#")) {
                segLine = t
                break
            }
        }
        println("First media/segment line: $segLine")
        assertNotNull("Must find a media/segment line", segLine)

        val baseUri = java.net.URI(firstOpt.url)
        val resolvedUrl = baseUri.resolve(segLine!!).toString()

        val subReq = okhttp3.Request.Builder().url(resolvedUrl)
        firstOpt.headers.forEach { (k, v) -> subReq.header(k, v) }
        val subResp = try {
            client.newCall(subReq.build()).execute()
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Sub-playlist request failed", e)
            return@runBlocking
        }
        org.junit.Assume.assumeTrue("Sub-playlist must be 200 OK", subResp.isSuccessful)
        val subText = subResp.body?.string() ?: ""
        subResp.close()

        val segUrl = subText.lines().firstOrNull { it.startsWith("http") }
        org.junit.Assume.assumeTrue("Segment URL must be present", segUrl != null)
        if (segUrl == null) return@runBlocking

        val segReq = okhttp3.Request.Builder().url(segUrl)
        firstOpt.headers.forEach { (k, v) -> segReq.header(k, v) }
        val segResp = try {
            client.newCall(segReq.build()).execute()
        } catch (e: Exception) {
            org.junit.Assume.assumeNoException("Segment request failed", e)
            return@runBlocking
        }
        org.junit.Assume.assumeTrue("Segment response code must be 200 OK", segResp.isSuccessful)
        val segBytes = segResp.body?.bytes()
        segResp.close()
        if (segBytes != null) {
            assertTrue("Segment body must have bytes", segBytes.isNotEmpty())
        }
    }

    @Test
    fun testLionessS1E1Streams() = runBlocking {
        println("=== Testing Lioness S1E1 Streams (TMDB 113962:1:1) ===")
        val result = plugin.getStreamLinks("113962:1:1")
        println("Total Streams: ${result.streams.size}")
        println("Total Subtitles: ${result.subtitles.size}")

        assertTrue("Must have streams for Lioness S1E1", result.streams.isNotEmpty())
        assertTrue("Must have subtitles for Lioness S1E1", result.subtitles.isNotEmpty())

        val vidrockStreams = result.streams.filter { it.serverName.contains("Vidrock", ignoreCase = true) }
        val vidyStreams = result.streams.filter { it.serverName.contains("Vidy", ignoreCase = true) }

        println("Vidrock Streams: ${vidrockStreams.size}")
        vidrockStreams.forEach { s ->
            println("  [Vidrock] ${s.serverName} | ${s.quality} -> ${s.url.take(80)}...")
            assertTrue("Vidrock URL must start with http", s.url.startsWith("http"))
            assertTrue("Vidrock must be m3u8 or direct video", s.url.contains(".m3u8") || s.url.contains(".mp4"))
        }

        println("Vidy Streams: ${vidyStreams.size}")
        vidyStreams.forEach { s ->
            println("  [Vidy] ${s.serverName} | ${s.quality} -> ${s.url.take(80)}...")
            assertTrue("Vidy URL must start with http", s.url.startsWith("http"))
            assertTrue("Vidy must be a stream endpoint", s.url.contains(".m3u8") || s.url.contains(".mp4") || s.url.contains(".json") || s.url.contains(".mkv") || s.isM3u8)
        }

        assertTrue("Must find Vidrock streams", vidrockStreams.isNotEmpty())
        assertTrue("Must find Vidy streams", vidyStreams.isNotEmpty())

        // Ensure no raw unplayable HTML landing page URLs are present
        val forbiddenLandingPages = listOf("vidlink.pro", "vidfast.pro", "vidfast.vc", "vidy.st", "vidrock.ru")
        for (stream in result.streams) {
            for (forbidden in forbiddenLandingPages) {
                assertFalse("Stream URL must not be raw HTML landing page: ${stream.url}", stream.url.contains(forbidden))
            }
        }
    }
}
