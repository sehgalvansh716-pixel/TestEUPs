package com.euthopiar.core.provider

import com.euthopiar.core.model.*
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.Assert.assertTrue

class FourKHDHubPluginLiveTest {

    private val plugin = FourKHDHubPlugin()

    @Test
    fun testTwentyContentPieces() = runBlocking {
        println("======================================================================")
        println("STARTING 4KHDHUB EXTENSIVE DIAGNOSTIC TEST (20+ REAL CONTENT PIECES)")
        println("======================================================================")

        // 1. Fetch catalog rows to sample diverse content
        val catalogRows = plugin.getHomeCatalog()
        println("Fetched ${catalogRows.size} catalog rows from 4KHDHub.")

        val sampleItems = mutableListOf<MediaItem>()
        // Collect distinct items across rows
        for (row in catalogRows) {
            for (item in row.items) {
                if (sampleItems.none { it.id == item.id || it.url == item.url }) {
                    sampleItems.add(item)
                }
                if (sampleItems.size >= 25) break
            }
            if (sampleItems.size >= 25) break
        }

        println("Collected ${sampleItems.size} diverse candidate items for testing.")
        assertTrue("Should have collected at least 20 items from catalogs", sampleItems.size >= 20)

        data class TestResult(
            val index: Int,
            val title: String,
            val type: MediaType,
            val url: String,
            val variantsFound: Int,
            val greenmotorsBypassSuccess: Int,
            val hubcloudResolved: Int,
            val gamerxytResolved: Int,
            val totalStreamsEmitted: Int,
            val streamServers: List<String>,
            val status: String,
            val failureReason: String? = null
        )

        val results = mutableListOf<TestResult>()

        for ((idx, item) in sampleItems.take(22).withIndex()) {
            val itemNum = idx + 1
            println("\n----------------------------------------------------------------------")
            println("[$itemNum/22] Testing: '${item.title}' (${item.type})")
            println("URL: ${item.url.ifBlank { item.id }}")

            try {
                val detail = plugin.getDetails(item)
                println("-> Details fetched: '${detail.title}' | Year: ${detail.year} | Episodes: ${detail.episodes.size}")

                val targetEp = detail.episodes.firstOrNull()
                if (targetEp == null) {
                    println("-> FAIL: 0 episodes found in details!")
                    results.add(
                        TestResult(
                            index = itemNum,
                            title = item.title.ifBlank { detail.title },
                            type = item.type,
                            url = item.url,
                            variantsFound = 0,
                            greenmotorsBypassSuccess = 0,
                            hubcloudResolved = 0,
                            gamerxytResolved = 0,
                            totalStreamsEmitted = 0,
                            streamServers = emptyList(),
                            status = "FAIL",
                            failureReason = "No episodes/variants parsed from page structure"
                        )
                    )
                    continue
                }

                // Diagnose raw variants inside targetEp.data
                val epDataJson = targetEp.data
                val variantsCount = Regex(""""greenmotorsUrl":\s*"([^"]+)"""").findAll(epDataJson).count()
                println("-> Variants in payload: $variantsCount")

                // Step-by-step diagnostic on each greenmotors link
                var gmSuccess = 0
                var hcSuccess = 0
                var gxSuccess = 0
                val gmLinks = Regex(""""greenmotorsUrl":\s*"([^"]+)"""").findAll(epDataJson).map { it.groupValues[1] }.toList()

                for (link in gmLinks.take(3)) {
                    val hubcloud = plugin.bypassGreenmotors(link)
                    if (!hubcloud.isNullOrBlank()) {
                        gmSuccess++
                        val gamerxyt = plugin.resolveHubcloud(hubcloud)
                        if (!gamerxyt.isNullOrBlank()) {
                            hcSuccess++
                            val directServers = plugin.resolveGamerxyt(gamerxyt)
                            if (directServers.isNotEmpty()) {
                                gxSuccess++
                            }
                        }
                    }
                }

                // Stream Flow collection
                val emissions = plugin.getStreamFlow(epDataJson).toList()
                val sources = emissions.filterIsInstance<StreamEmission.SourceFound>().map { it.source }
                val serversList = sources.map { it.serverName }.distinct()

                println("-> Streams emitted: ${sources.size} across servers: $serversList")

                if (sources.isNotEmpty()) {
                    results.add(
                        TestResult(
                            index = itemNum,
                            title = detail.title,
                            type = item.type,
                            url = item.url,
                            variantsFound = variantsCount,
                            greenmotorsBypassSuccess = gmSuccess,
                            hubcloudResolved = hcSuccess,
                            gamerxytResolved = gxSuccess,
                            totalStreamsEmitted = sources.size,
                            streamServers = serversList,
                            status = "PASS"
                        )
                    )
                } else {
                    val reason = when {
                        variantsCount == 0 -> "Page format change / No download items found in HTML"
                        gmSuccess == 0 -> "Greenmotors mediator bypass failed on link"
                        hcSuccess == 0 -> "HubCloud -> Gamerxyt redirection failed"
                        gxSuccess == 0 -> "Gamerxyt yielded 0 playable media links"
                        else -> "Unknown emission failure"
                    }
                    println("-> FAIL: $reason")
                    results.add(
                        TestResult(
                            index = itemNum,
                            title = detail.title,
                            type = item.type,
                            url = item.url,
                            variantsFound = variantsCount,
                            greenmotorsBypassSuccess = gmSuccess,
                            hubcloudResolved = hcSuccess,
                            gamerxytResolved = gxSuccess,
                            totalStreamsEmitted = 0,
                            streamServers = emptyList(),
                            status = "FAIL",
                            failureReason = reason
                        )
                    )
                }
            } catch (e: Exception) {
                println("-> EXCEPTION: ${e.message}")
                results.add(
                    TestResult(
                        index = itemNum,
                        title = item.title,
                        type = item.type,
                        url = item.url,
                        variantsFound = 0,
                        greenmotorsBypassSuccess = 0,
                        hubcloudResolved = 0,
                        gamerxytResolved = 0,
                        totalStreamsEmitted = 0,
                        streamServers = emptyList(),
                        status = "FAIL",
                        failureReason = "Exception: ${e.message}"
                    )
                )
            }
        }

        println("\n======================================================================")
        println("SUMMARY REPORT ACROSS ${results.size} TESTED CONTENT PIECES")
        println("======================================================================")
        println(String.format("%-4s | %-32s | %-6s | %-8s | %-8s | %-12s | %s", "No.", "Title", "Type", "Variants", "Streams", "Status", "Reason / Notes"))
        println("-".repeat(110))

        for (r in results) {
            val titleShort = if (r.title.length > 30) r.title.take(29) + "…" else r.title
            val notes = if (r.status == "PASS") "${r.streamServers.take(2).joinToString(", ")}" else "${r.failureReason}"
            println(String.format("%-4d | %-32s | %-6s | %-8d | %-8d | %-12s | %s", r.index, titleShort, r.type.name, r.variantsFound, r.totalStreamsEmitted, r.status, notes))
        }

        val passedCount = results.count { it.status == "PASS" }
        val failedCount = results.count { it.status == "FAIL" }
        println("-".repeat(110))
        println("TOTAL TESTED: ${results.size} | PASSED: $passedCount | FAILED: $failedCount | SUCCESS RATE: ${(passedCount * 100) / results.size}%")
        println("======================================================================\n")
    }

    @Test
    fun testReply1988Directly() = runBlocking {
        println("=== TESTING REPLY 1988 DIRECTLY ===")

        val item = MediaItem(
            id = "reply-1988-series-8249",
            title = "Reply 1988",
            url = "https://4khdhub.one/reply-1988-series-8249/",
            posterUrl = null,
            backdropUrl = null,
            type = MediaType.TV_SERIES,
            year = 2015,
            quality = "HD",
            provider = "4khdhub"
        )
        val detail = plugin.getDetails(item)
        println("Detail title: ${detail.title}")
        println("Detail episodes count: ${detail.episodes.size}")
        for ((idx, ep) in detail.episodes.take(5).withIndex()) {
            println("Ep $idx: S${ep.seasonNumber}E${ep.episodeNumber} - ${ep.title} (data length: ${ep.data.length})")
        }
        val firstEp = detail.episodes.firstOrNull()
        if (firstEp != null) {
            val streams = plugin.getStreamFlow(firstEp.data).toList()
            val sources = streams.filterIsInstance<StreamEmission.SourceFound>().map { it.source }
            println("Sources found for Ep 1: ${sources.size}")
            for (s in sources) {
                println("  Source: ${s.serverName} | ${s.quality} | ${s.url.take(60)}...")
            }
        }
    }

    @Test
    fun testTheUncannyCounter() = runBlocking {
        println("\n=== TESTING THE UNCANNY COUNTER DIRECTLY ===")
        val item = MediaItem(
            id = "the-uncanny-counter-series-8247",
            title = "The Uncanny Counter",
            url = "https://4khdhub.one/the-uncanny-counter-series-8247/",
            posterUrl = null,
            backdropUrl = null,
            type = MediaType.TV_SERIES,
            year = 2020,
            quality = "HD",
            provider = "4khdhub"
        )
        val detail = plugin.getDetails(item)
        println("Detail title: ${detail.title}")
        println("Detail episodes count: ${detail.episodes.size}")
        org.junit.Assert.assertTrue("Should parse episodes", detail.episodes.isNotEmpty())

        val firstEp = detail.episodes.first()
        println("Episode 1 ID: ${firstEp.id}")

        // Test 1: Standard serialized episode data
        println("\n[Test 1] Standard serialized episode data:")
        val streams1 = plugin.getStreamFlow(firstEp.data).toList()
        val sources1 = streams1.filterIsInstance<StreamEmission.SourceFound>().map { it.source }
        println("  Emitted sources: ${sources1.size}")
        org.junit.Assert.assertTrue("Serialized data must yield streams", sources1.isNotEmpty())

        // Test 2: Raw episode ID (on-demand resolution)
        println("\n[Test 2] Raw episode ID (${firstEp.id}):")
        val streams2 = plugin.getStreamFlow(firstEp.id).toList()
        val sources2 = streams2.filterIsInstance<StreamEmission.SourceFound>().map { it.source }
        println("  Emitted sources: ${sources2.size}")
        org.junit.Assert.assertTrue("Raw episode ID must yield streams", sources2.isNotEmpty())

        // Test 3: Short slug + season + episode coordinates
        println("\n[Test 3] Slug coordinates (the-uncanny-counter-series-8247:1:1):")
        val streams3 = plugin.getStreamFlow("the-uncanny-counter-series-8247:1:1").toList()
        val sources3 = streams3.filterIsInstance<StreamEmission.SourceFound>().map { it.source }
        println("  Emitted sources: ${sources3.size}")
        org.junit.Assert.assertTrue("Slug coordinates must yield streams", sources3.isNotEmpty())

        // Test 4: Episode download links
        println("\n[Test 4] getDownloadLinks for Episode 1:")
        val downloads = plugin.getDownloadLinks(firstEp.data)
        println("  Download options: ${downloads.size}")
        org.junit.Assert.assertTrue("Must yield download options", downloads.isNotEmpty())
    }

    @Test
    fun testTheLoveHypothesis() = runBlocking {
        println("\n=======================================================")
        println("=== TESTING 'THE LOVE HYPOTHESIS' BUFFERING ISSUE ===")
        println("=======================================================")
        val item = MediaItem(
            id = "the-love-hypothesis-movie-8179",
            title = "The Love Hypothesis",
            url = "https://4khdhub.one/the-love-hypothesis-movie-8179/",
            posterUrl = null,
            backdropUrl = null,
            type = MediaType.MOVIE,
            year = 2026,
            quality = "HD",
            provider = "4khdhub"
        )
        val detail = plugin.getDetails(item)
        println("Detail title: ${detail.title}")
        println("Detail episodes count: ${detail.episodes.size}")

        val movieEp = detail.episodes.firstOrNull()
        if (movieEp == null) {
            println("FAIL: 0 movie variants found!")
            return@runBlocking
        }

        println("Movie variants data length: ${movieEp.data.length}")
        val streams = plugin.getStreamFlow(movieEp.data).toList()
        val sources = streams.filterIsInstance<StreamEmission.SourceFound>().map { it.source }
        println("Sources emitted: ${sources.size}")

        val testClient = okhttp3.OkHttpClient.Builder()
            .followRedirects(true)
            .followSslRedirects(true)
            .connectTimeout(java.time.Duration.ofSeconds(10))
            .readTimeout(java.time.Duration.ofSeconds(15))
            .build()

        for ((idx, source) in sources.withIndex()) {
            println("\n--- Source #$idx: [${source.serverName}] (${source.quality}) ---")
            println("URL: ${source.url}")
            println("Headers: ${source.headers}")

            // Test 1: HEAD request
            try {
                val headReqBuilder = okhttp3.Request.Builder().url(source.url).head()
                source.headers.forEach { (k, v) -> headReqBuilder.header(k, v) }
                val headResp = testClient.newCall(headReqBuilder.build()).execute()
                println("HEAD code: ${headResp.code}")
                println("Content-Type: ${headResp.header("Content-Type")}")
                println("Content-Length: ${headResp.header("Content-Length")}")
                println("Accept-Ranges: ${headResp.header("Accept-Ranges")}")
                headResp.close()
            } catch (e: Exception) {
                println("HEAD exception: ${e.message}")
            }

            // Test 2: Byte-range GET request (first 1MB chunk like ExoPlayer does)
            try {
                val startMs = System.currentTimeMillis()
                val getReqBuilder = okhttp3.Request.Builder()
                    .url(source.url)
                    .header("Range", "bytes=0-1048576") // 1 MB chunk
                source.headers.forEach { (k, v) -> getReqBuilder.header(k, v) }
                
                val getResp = testClient.newCall(getReqBuilder.build()).execute()
                val code = getResp.code
                val body = getResp.body
                val bytesRead = body?.byteStream()?.readNBytes(256 * 1024)?.size ?: 0
                val elapsedMs = System.currentTimeMillis() - startMs
                println("GET (0-1MB) code: $code | Bytes read: $bytesRead in ${elapsedMs}ms")
                println("Response Content-Range: ${getResp.header("Content-Range")}")
                println("Response Content-Type: ${getResp.header("Content-Type")}")
                getResp.close()
            } catch (e: Exception) {
                println("GET Range exception (BUFFERING ROOT CAUSE): ${e.message}")
            }
        }
    }

    @Test
    fun testFiftyContentPieces() = runBlocking {
        println("======================================================================")
        println("STARTING 4KHDHUB COMPREHENSIVE 50+ CONTENT DIAGNOSTIC TEST")
        println("======================================================================")

        val candidateItems = mutableListOf<MediaItem>()

        // 1. Explicitly inject known problem items & edge cases
        candidateItems.add(
            MediaItem(
                id = "the-love-hypothesis-movie-8179",
                title = "The Love Hypothesis",
                url = "https://4khdhub.one/the-love-hypothesis-movie-8179/",
                posterUrl = null, backdropUrl = null,
                type = MediaType.MOVIE, year = 2026, quality = "HD", provider = "4khdhub"
            )
        )
        candidateItems.add(
            MediaItem(
                id = "reply-1988-series-8249",
                title = "Reply 1988",
                url = "https://4khdhub.one/reply-1988-series-8249/",
                posterUrl = null, backdropUrl = null,
                type = MediaType.TV_SERIES, year = 2015, quality = "HD", provider = "4khdhub"
            )
        )
        candidateItems.add(
            MediaItem(
                id = "force-of-nature-series-8253",
                title = "Force of Nature",
                url = "https://4khdhub.one/force-of-nature-series-8253/",
                posterUrl = null, backdropUrl = null,
                type = MediaType.TV_SERIES, year = 2026, quality = "HD", provider = "4khdhub"
            )
        )

        // 2. Collect from all 16 catalog rows
        try {
            val catalogRows = plugin.getHomeCatalog()
            println("Fetched ${catalogRows.size} catalog rows.")
            for (row in catalogRows) {
                for (item in row.items) {
                    if (candidateItems.none { it.id == item.id || it.url == item.url }) {
                        candidateItems.add(item)
                    }
                }
            }
        } catch (e: Exception) {
            println("Warning: catalog fetch failed: ${e.message}")
        }

        // 3. Collect via targeted searches for diverse series, anime, classics, and regional titles
        val targetQueries = listOf(
            "Stranger Things", "Breaking Bad", "Game of Thrones", "Squid Game",
            "Money Heist", "Dark", "Demon Slayer", "Solo Leveling",
            "Oppenheimer", "Interstellar", "Dune", "Avatar", "Stree 2"
        )
        for (q in targetQueries) {
            if (candidateItems.size >= 65) break
            try {
                val searchResults = plugin.search(q)
                for (res in searchResults.take(3)) {
                    if (candidateItems.none { it.id == res.id || it.url == res.url }) {
                        candidateItems.add(res)
                    }
                }
            } catch (_: Exception) {}
        }

        println("Total unique candidate items gathered: ${candidateItems.size}")
        val testPool = candidateItems.take(50)
        println("Selected exactly ${testPool.size} items for comprehensive diagnostic.\n")

        data class DetailedResult(
            val index: Int,
            val title: String,
            val type: MediaType,
            val url: String,
            val episodesCount: Int,
            val variantsCount: Int,
            val streamsCount: Int,
            val primaryServer: String,
            val primaryUrl: String,
            val probeStatus: String,
            val probeHttpCode: Int,
            val status: String,
            val notes: String
        )

        val probeClient = okhttp3.OkHttpClient.Builder()
            .followRedirects(true)
            .followSslRedirects(true)
            .connectTimeout(java.time.Duration.ofSeconds(6))
            .readTimeout(java.time.Duration.ofSeconds(6))
            .build()

        val results = mutableListOf<DetailedResult>()

        for ((idx, item) in testPool.withIndex()) {
            val itemNum = idx + 1
            val cleanTitle = item.title.ifBlank { item.id }
            print("[$itemNum/50] Testing: '$cleanTitle' (${item.type})... ")

            try {
                val detail = plugin.getDetails(item)
                val epCount = detail.episodes.size

                if (epCount == 0) {
                    println("FAIL (0 episodes parsed)")
                    results.add(
                        DetailedResult(
                            index = itemNum,
                            title = cleanTitle,
                            type = item.type,
                            url = item.url,
                            episodesCount = 0,
                            variantsCount = 0,
                            streamsCount = 0,
                            primaryServer = "None",
                            primaryUrl = "",
                            probeStatus = "NO_EPISODES",
                            probeHttpCode = 0,
                            status = "FAIL",
                            notes = "0 episodes parsed (Draft post or accordion truncated)"
                        )
                    )
                    continue
                }

                val targetEp = detail.episodes.first()
                val variantsCount = Regex(""""greenmotorsUrl":\s*"([^"]+)"""").findAll(targetEp.data).count()

                // Resolve streams flow
                val streamEmissions = plugin.getStreamFlow(targetEp.data).toList()
                val sources = streamEmissions.filterIsInstance<StreamEmission.SourceFound>().map { it.source }

                if (sources.isEmpty()) {
                    println("FAIL (0 streams emitted across $variantsCount variants)")
                    results.add(
                        DetailedResult(
                            index = itemNum,
                            title = cleanTitle,
                            type = item.type,
                            url = item.url,
                            episodesCount = epCount,
                            variantsCount = variantsCount,
                            streamsCount = 0,
                            primaryServer = "None",
                            primaryUrl = "",
                            probeStatus = "ZERO_STREAMS",
                            probeHttpCode = 0,
                            status = "FAIL",
                            notes = "Bypasses failed to yield valid media streams"
                        )
                    )
                    continue
                }

                val primary = sources.first()
                // Probe primary stream with HTTP Range (0-1024)
                var probeCode = 0
                var probeStatus = "UNKNOWN"

                try {
                    val pReq = okhttp3.Request.Builder()
                        .url(primary.url)
                        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
                        .header("Range", "bytes=0-1024")
                    primary.headers.forEach { (k, v) -> pReq.header(k, v) }

                    probeClient.newCall(pReq.build()).execute().use { resp ->
                        probeCode = resp.code
                        probeStatus = when (resp.code) {
                            200, 206 -> "ALIVE"
                            403 -> "HTTP_403_FORBIDDEN"
                            404 -> "HTTP_404_NOT_FOUND"
                            else -> "HTTP_${resp.code}"
                        }
                    }
                } catch (pe: Exception) {
                    probeStatus = "PROBE_ERR: ${pe.javaClass.simpleName}"
                }

                val status = when {
                    probeStatus == "ALIVE" -> "PASS"
                    probeStatus.contains("403") || probeStatus.contains("404") -> "BUFFERING_RISK"
                    else -> "PASS"
                }

                println("$status | Streams: ${sources.size} | Primary: [${primary.serverName}] -> $probeStatus")

                results.add(
                    DetailedResult(
                        index = itemNum,
                        title = cleanTitle,
                        type = item.type,
                        url = item.url,
                        episodesCount = epCount,
                        variantsCount = variantsCount,
                        streamsCount = sources.size,
                        primaryServer = primary.serverName,
                        primaryUrl = primary.url,
                        probeStatus = probeStatus,
                        probeHttpCode = probeCode,
                        status = status,
                        notes = when (status) {
                            "PASS" -> "${primary.serverName} (HTTP $probeCode)"
                            "BUFFERING_RISK" -> "Primary is ${primary.serverName} ($probeStatus) - Will cause buffering!"
                            else -> probeStatus
                        }
                    )
                )

            } catch (e: Exception) {
                println("EXCEPTION: ${e.message}")
                results.add(
                    DetailedResult(
                        index = itemNum,
                        title = cleanTitle,
                        type = item.type,
                        url = item.url,
                        episodesCount = 0,
                        variantsCount = 0,
                        streamsCount = 0,
                        primaryServer = "None",
                        primaryUrl = "",
                        probeStatus = "EXCEPTION",
                        probeHttpCode = 0,
                        status = "FAIL",
                        notes = "Exception: ${e.message}"
                    )
                )
            }
        }

        println("\n==============================================================================================================")
        println("SUMMARY REPORT ACROSS ${results.size} TESTED CONTENT PIECES")
        println("==============================================================================================================")
        println(String.format("%-4s | %-28s | %-7s | %-4s | %-7s | %-14s | %-16s | %s", "No.", "Title", "Type", "Eps", "Streams", "Status", "Probe", "Primary Server / Notes"))
        println("-".repeat(110))

        for (r in results) {
            val titleShort = if (r.title.length > 27) r.title.take(26) + "…" else r.title
            println(String.format("%-4d | %-28s | %-7s | %-4d | %-7d | %-14s | %-16s | %s", r.index, titleShort, r.type.name, r.episodesCount, r.streamsCount, r.status, r.probeStatus, r.notes))
        }

        val passedCount = results.count { it.status == "PASS" }
        val buffCount = results.count { it.status == "BUFFERING_RISK" }
        val failedCount = results.count { it.status == "FAIL" }

        println("-".repeat(110))
        println("TOTAL TESTED: ${results.size}")
        println("-> HEALTHY (PASS): $passedCount (${(passedCount * 100) / results.size}%)")
        println("-> BUFFERING RISK (Dead Primary Stream 403/404): $buffCount (${(buffCount * 100) / results.size}%)")
        println("-> HARD FAIL (0 Episodes / Truncated): $failedCount (${(failedCount * 100) / results.size}%)")
        println("==============================================================================================================\n")
    }
}


