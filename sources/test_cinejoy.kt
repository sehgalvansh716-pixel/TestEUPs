package com.euthopiar.scratch

import com.euthopiar.core.provider.CinejoyPlugin
import com.euthopiar.core.provider.CinejoyWasmEngine
import com.euthopiar.core.network.DohDns
import kotlinx.coroutines.runBlocking
import okhttp3.Request

fun main() = runBlocking {
    println("=== TESTING CINEJOY PLUGIN ===")
    val client = okhttp3.OkHttpClient.Builder()
        .connectTimeout(15, java.util.concurrent.TimeUnit.SECONDS)
        .readTimeout(25, java.util.concurrent.TimeUnit.SECONDS)
        .build()
    val plugin = CinejoyPlugin(client)

    // 1. Test WasmEngine across different candidate server names for Inception (27205)
    println("\n--- Testing Servers on CinejoyWasmEngine ---")
    val candidateServers = listOf(
        "Lisbon", "Solara", "Nebula", "Athens",
        "Madrid", "Tokyo", "Paris", "Berlin", "London", "Rome", "Aura", "Nova"
    )

    for (server in candidateServers) {
        try {
            val res = CinejoyWasmEngine.requestStream(
                client = client,
                server = server,
                type = "movie",
                tmdbId = "27205"
            )
            if (res != null && res.contains("playlist")) {
                println("  [SUCCESS] Server '$server' returned stream!")
                println("    Snippet: ${res.take(150)}")
            } else if (res != null) {
                println("  [RESPONSE] Server '$server': ${res.take(100)}")
            } else {
                println("  [FAIL] Server '$server' returned null")
            }
        } catch (e: Exception) {
            println("  [ERROR] Server '$server': ${e.message}")
        }
    }

    // 2. Test getStreamLinks for Movie
    println("\n--- Testing getStreamLinks for Movie (Inception: 27205) ---")
    val movieStreams = plugin.getStreamLinks("27205")
    println("Found ${movieStreams.streams.size} streams, ${movieStreams.subtitles.size} subtitles")
    for (s in movieStreams.streams) {
        println("  Stream: ${s.serverName} | Quality: ${s.quality} | isM3u8: ${s.isM3u8} | URL: ${s.url.take(80)}")
        // Check live HTTP status
        val req = Request.Builder().url(s.url)
        s.headers.forEach { (k, v) -> req.header(k, v) }
        try {
            client.newCall(req.build()).execute().use { resp ->
                println("    -> Live HTTP: ${resp.code} (${resp.message})")
            }
        } catch (e: Exception) {
            println("    -> Live HTTP Error: ${e.message}")
        }
    }

    // 3. Test getStreamLinks for TV (Breaking Bad: 1396:1:1)
    println("\n--- Testing getStreamLinks for TV (Breaking Bad: 1396:1:1) ---")
    val tvStreams = plugin.getStreamLinks("1396:1:1")
    println("Found ${tvStreams.streams.size} streams, ${tvStreams.subtitles.size} subtitles")
    for (s in tvStreams.streams) {
        println("  Stream: ${s.serverName} | Quality: ${s.quality} | isM3u8: ${s.isM3u8} | URL: ${s.url.take(80)}")
        val req = Request.Builder().url(s.url)
        s.headers.forEach { (k, v) -> req.header(k, v) }
        try {
            client.newCall(req.build()).execute().use { resp ->
                println("    -> Live HTTP: ${resp.code} (${resp.message})")
            }
        } catch (e: Exception) {
            println("    -> Live HTTP Error: ${e.message}")
        }
    }

    // 4. Test getDownloadLinks
    println("\n--- Testing getDownloadLinks for Movie (27205) ---")
    val movieDownloads = plugin.getDownloadLinks("27205")
    println("Found ${movieDownloads.size} download options:")
    for (d in movieDownloads) {
        println("  Download: ${d.title} | Quality: ${d.quality} | Size: ${d.size} | URL: ${d.url.take(80)}")
    }

    println("\n--- Testing getDownloadLinks for TV (1396:1:1) ---")
    val tvDownloads = plugin.getDownloadLinks("1396:1:1")
    println("Found ${tvDownloads.size} download options:")
    for (d in tvDownloads) {
        println("  Download: ${d.title} | Quality: ${d.quality} | Size: ${d.size} | URL: ${d.url.take(80)}")
    }
}
