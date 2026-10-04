package com.euthopiar.scratch

import com.euthopiar.core.model.*
import com.euthopiar.core.provider.CinejoyPlugin
import kotlinx.coroutines.*
import okhttp3.OkHttpClient
import okhttp3.Request
import java.util.concurrent.TimeUnit

data class TestTarget(
    val title: String,
    val tmdbId: String,
    val isTv: Boolean,
    val season: Int = 1,
    val episode: Int = 1
)

fun main() = runBlocking {
    val client = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(20, TimeUnit.SECONDS)
        .build()

    val plugin = CinejoyPlugin(client)

    val targets = listOf(
        // 20 Movies
        TestTarget("Inception", "27205", false),
        TestTarget("Interstellar", "157336", false),
        TestTarget("The Dark Knight", "155", false),
        TestTarget("Oppenheimer", "872585", false),
        TestTarget("Avatar: The Way of Water", "76600", false),
        TestTarget("Dune: Part Two", "693134", false),
        TestTarget("Spider-Man: Across the Spider-Verse", "569094", false),
        TestTarget("The Batman", "414906", false),
        TestTarget("Top Gun: Maverick", "361743", false),
        TestTarget("Gladiator", "98", false),
        TestTarget("Fight Club", "550", false),
        TestTarget("Pulp Fiction", "680", false),
        TestTarget("The Matrix", "603", false),
        TestTarget("Avengers: Endgame", "299534", false),
        TestTarget("Parasite", "496243", false),
        TestTarget("Whiplash", "244786", false),
        TestTarget("Spirited Away", "129", false),
        TestTarget("Toy Story", "862", false),
        TestTarget("Titanic", "597", false),
        TestTarget("Jurassic Park", "329", false),

        // 20 TV Series / Web Series
        TestTarget("Stranger Things", "66732", true, 1, 1),
        TestTarget("Breaking Bad", "1396", true, 1, 1),
        TestTarget("Game of Thrones", "1399", true, 1, 1),
        TestTarget("The Last of Us", "100088", true, 1, 1),
        TestTarget("House of the Dragon", "94997", true, 1, 1),
        TestTarget("Better Call Saul", "60059", true, 1, 1),
        TestTarget("The Boys", "76479", true, 1, 1),
        TestTarget("Succession", "76331", true, 1, 1),
        TestTarget("Severance", "93405", true, 1, 1),
        TestTarget("Shogun", "126308", true, 1, 1),
        TestTarget("Chernobyl", "87108", true, 1, 1),
        TestTarget("Arcane", "94605", true, 1, 1),
        TestTarget("Peaky Blinders", "60574", true, 1, 1),
        TestTarget("Rick and Morty", "60625", true, 1, 1),
        TestTarget("Dark", "70523", true, 1, 1),
        TestTarget("The Bear", "136315", true, 1, 1),
        TestTarget("Loki", "84958", true, 1, 1),
        TestTarget("Ted Lasso", "97546", true, 1, 1),
        TestTarget("Wednesday", "119051", true, 1, 1),
        TestTarget("The Witcher", "71912", true, 1, 1)
    )

    println("================================================================================")
    println("          CINEJOY PLUGIN INTENSIVE 40-ITEM VERIFICATION AUDIT")
    println("================================================================================")
    println("Total test items: ${targets.size} (20 Movies, 20 TV Series)\n")

    var passCount = 0
    var failCount = 0

    for ((index, item) in targets.withIndex()) {
        val num = index + 1
        val itemTypeStr = if (item.isTv) "TV (S${item.season}E${item.episode})" else "Movie"
        val episodeData = if (item.isTv) "${item.tmdbId}:${item.season}:${item.episode}" else item.tmdbId

        print(String.format("[%02d/40] %-35s [%-7s] -> ", num, item.title, itemTypeStr))
        System.out.flush()

        try {
            // 1. Details & Metadata
            val mediaItem = MediaItem(
                id = item.tmdbId,
                title = item.title,
                url = "https://cinejoy.pk/${if (item.isTv) "tv" else "movie"}/${item.tmdbId}",
                posterUrl = null,
                backdropUrl = null,
                type = if (item.isTv) MediaType.TV_SERIES else MediaType.MOVIE
            )
            val detail = plugin.getDetails(mediaItem)
            val hasMeta = detail.title.isNotBlank() && !detail.synopsis.isNullOrBlank() && detail.cast.isNotEmpty()

            // 2. Streams & Subtitles
            val streamResult = plugin.getStreamLinks(episodeData)
            val streams = streamResult.streams
            val subs = streamResult.subtitles
            val hasStreams = streams.isNotEmpty()

            // Verify top stream is live
            var streamLive = false
            if (hasStreams) {
                val topStream = streams.first()
                try {
                    val req = Request.Builder()
                        .url(topStream.url)
                        .header("User-Agent", "Mozilla/5.0")
                        .header("Range", "bytes=0-1024")
                    topStream.headers.forEach { (k, v) -> req.header(k, v) }
                    client.newCall(req.build()).execute().use { resp ->
                        streamLive = resp.isSuccessful && resp.code in 200..299
                    }
                } catch (_: Exception) {}
            }

            // 3. Downloads
            val downloads = plugin.getDownloadLinks(episodeData)
            val hasDownloads = downloads.isNotEmpty()

            var downloadLive = false
            if (hasDownloads) {
                val topDownload = downloads.first()
                try {
                    val req = Request.Builder()
                        .url(topDownload.url)
                        .header("User-Agent", "Mozilla/5.0")
                        .header("Range", "bytes=0-1024")
                    topDownload.headers.forEach { (k, v) -> req.header(k, v) }
                    client.newCall(req.build()).execute().use { resp ->
                        downloadLive = resp.isSuccessful && (resp.code == 200 || resp.code == 206)
                    }
                } catch (_: Exception) {}
            }

            val passed = hasMeta && hasStreams && streamLive && hasDownloads && downloadLive

            if (passed) {
                passCount++
                val streamNames = streams.map { it.serverName }.distinct().joinToString(",")
                println("PASS | Streams: ${streams.size} ($streamNames) | Subs: ${subs.size} | DLs: ${downloads.size}")
            } else {
                failCount++
                println("FAIL | Meta: $hasMeta, Streams: ${streams.size} (Live: $streamLive), Subs: ${subs.size}, DLs: ${downloads.size} (Live: $downloadLive)")
            }
        } catch (e: Exception) {
            failCount++
            println("ERROR: ${e.message}")
        }
    }

    println("\n================================================================================")
    println("AUDIT SUMMARY: $passCount / ${targets.size} PASSED (${String.format("%.1f", passCount * 100.0 / targets.size)}%)")
    println("================================================================================")
}
