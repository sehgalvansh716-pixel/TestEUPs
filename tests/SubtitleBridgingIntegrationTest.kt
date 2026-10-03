package com.euthopiar.core.provider

import com.euthopiar.core.model.MediaType
import com.euthopiar.core.model.SubtitleTrack
import com.euthopiar.core.network.DohDns
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONObject
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

class SubtitleBridgingIntegrationTest {

    private lateinit var client: OkHttpClient
    private lateinit var atlanticPlugin: AtlanticPlugin
    private lateinit var movyPlugin: MovyPlugin

    data class ContentTarget(
        val name: String,
        val tmdbId: String,
        val imdbId: String,
        val type: MediaType,
        val season: Int? = null,
        val episode: Int? = null
    )

    private val moviesList = listOf(
        ContentTarget("The Dark Knight", "155", "tt0468569", MediaType.MOVIE),
        ContentTarget("Inception", "27205", "tt1375666", MediaType.MOVIE),
        ContentTarget("Interstellar", "157336", "tt0816692", MediaType.MOVIE),
        ContentTarget("Fight Club", "550", "tt0137523", MediaType.MOVIE),
        ContentTarget("The Matrix", "603", "tt0133093", MediaType.MOVIE),
        ContentTarget("Pulp Fiction", "680", "tt0110912", MediaType.MOVIE),
        ContentTarget("Avengers: Endgame", "299534", "tt4154796", MediaType.MOVIE),
        ContentTarget("Gladiator", "98", "tt0172495", MediaType.MOVIE),
        ContentTarget("The Godfather", "238", "tt0068646", MediaType.MOVIE),
        ContentTarget("Into the Spider-Verse", "324857", "tt4633694", MediaType.MOVIE),
        ContentTarget("Oppenheimer", "872585", "tt15398776", MediaType.MOVIE),
        ContentTarget("Dune: Part Two", "693134", "tt15239678", MediaType.MOVIE),
        ContentTarget("Black Adam", "436270", "tt6443346", MediaType.MOVIE),
        ContentTarget("Toy Story 2", "863", "tt0120363", MediaType.MOVIE),
        ContentTarget("The Shawshank Redemption", "278", "tt0111161", MediaType.MOVIE),
        ContentTarget("Forrest Gump", "13", "tt0109830", MediaType.MOVIE),
        ContentTarget("Titanic", "597", "tt0120338", MediaType.MOVIE),
        ContentTarget("Avatar", "19995", "tt0499549", MediaType.MOVIE),
        ContentTarget("The Wolf of Wall Street", "106646", "tt0993846", MediaType.MOVIE),
        ContentTarget("Leave No Trace", "443463", "tt3892172", MediaType.MOVIE)
    )

    private val seriesList = listOf(
        ContentTarget("The Mentalist", "5920", "tt1196946", MediaType.TV_SERIES, 1, 1),
        ContentTarget("Breaking Bad", "1396", "tt0903747", MediaType.TV_SERIES, 1, 1),
        ContentTarget("Game of Thrones", "1399", "tt0944947", MediaType.TV_SERIES, 1, 1),
        ContentTarget("Stranger Things", "66732", "tt4574334", MediaType.TV_SERIES, 1, 1),
        ContentTarget("The Boys", "76479", "tt1190634", MediaType.TV_SERIES, 1, 1),
        ContentTarget("Chernobyl", "87108", "tt7366338", MediaType.TV_SERIES, 1, 1),
        ContentTarget("Better Call Saul", "60059", "tt3032476", MediaType.TV_SERIES, 1, 1),
        ContentTarget("Sherlock", "19885", "tt1475582", MediaType.TV_SERIES, 1, 1),
        ContentTarget("The Last of Us", "100088", "tt3581920", MediaType.TV_SERIES, 1, 1),
        ContentTarget("House of the Dragon", "94997", "tt11198330", MediaType.TV_SERIES, 1, 1),
        ContentTarget("Peaky Blinders", "60574", "tt2442560", MediaType.TV_SERIES, 1, 1),
        ContentTarget("Rick and Morty", "60625", "tt2861424", MediaType.TV_SERIES, 1, 1),
        ContentTarget("Succession", "76331", "tt7660850", MediaType.TV_SERIES, 1, 1),
        ContentTarget("Fargo", "60622", "tt2802850", MediaType.TV_SERIES, 1, 1),
        ContentTarget("True Detective", "46648", "tt2356777", MediaType.TV_SERIES, 1, 1),
        ContentTarget("Dark", "70523", "tt5753856", MediaType.TV_SERIES, 1, 1),
        ContentTarget("Severance", "95396", "tt11280740", MediaType.TV_SERIES, 1, 1),
        ContentTarget("The Sopranos", "1398", "tt0141842", MediaType.TV_SERIES, 1, 1),
        ContentTarget("The Wire", "3297", "tt0306414", MediaType.TV_SERIES, 1, 1),
        ContentTarget("Mindhunter", "67744", "tt5290382", MediaType.TV_SERIES, 1, 1)
    )

    @Before
    fun setUp() {
        client = DohDns.createOkHttpClient(12, 20)
        atlanticPlugin = AtlanticPlugin(client)
        movyPlugin = MovyPlugin(client)
    }

    private val jsonParser = kotlinx.serialization.json.Json { ignoreUnknownKeys = true }

    @Test
    fun testOpenSubtitlesAcross20MoviesAnd20Series() = runBlocking {
        println("=== TESTING OPENSUBTITLES ACROSS 20 MOVIES & 20 SERIES ===")
        var resolvedMovies = 0
        var resolvedSeries = 0

        // Test 20 Movies on OpenSubtitles
        for ((index, item) in moviesList.withIndex()) {
            val url = "https://opensubtitles-v3.strem.io/subtitles/movie/${item.imdbId}.json"
            val req = Request.Builder().url(url).header("User-Agent", "Mozilla/5.0").build()
            try {
                client.newCall(req).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val body = resp.body?.string() ?: ""
                        val root = jsonParser.parseToJsonElement(body)
                        val arr = if (root is kotlinx.serialization.json.JsonObject) root["subtitles"] else null
                        val count = if (arr is kotlinx.serialization.json.JsonArray) arr.size else 0
                        if (count > 0) {
                            resolvedMovies++
                            println("[Movie ${index + 1}/20] OpenSubtitles verified for '${item.name}' (${item.imdbId}): $count tracks")
                        }
                    }
                }
            } catch (e: Exception) {
                println("[Movie ${index + 1}/20] OpenSubtitles failed for '${item.name}': ${e.message}")
            }
        }

        // Test 20 Series on OpenSubtitles
        for ((index, item) in seriesList.withIndex()) {
            val url = "https://opensubtitles-v3.strem.io/subtitles/series/${item.imdbId}:${item.season}:${item.episode}.json"
            val req = Request.Builder().url(url).header("User-Agent", "Mozilla/5.0").build()
            try {
                client.newCall(req).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val body = resp.body?.string() ?: ""
                        val root = jsonParser.parseToJsonElement(body)
                        val arr = if (root is kotlinx.serialization.json.JsonObject) root["subtitles"] else null
                        val count = if (arr is kotlinx.serialization.json.JsonArray) arr.size else 0
                        if (count > 0) {
                            resolvedSeries++
                            println("[Series ${index + 1}/20] OpenSubtitles verified for '${item.name}' S${item.season}E${item.episode} (${item.imdbId}): $count tracks")
                        }
                    }
                }
            } catch (e: Exception) {
                println("[Series ${index + 1}/20] OpenSubtitles failed for '${item.name}': ${e.message}")
            }
        }

        println("\nOpenSubtitles Result: $resolvedMovies/20 movies, $resolvedSeries/20 series verified")
        assertTrue("OpenSubtitles must resolve at least 16/20 movies", resolvedMovies >= 16)
        assertTrue("OpenSubtitles must resolve at least 16/20 series", resolvedSeries >= 16)
    }

    @Test
    fun testMoviesSubtitlesAcrossProviders() = runBlocking {
        println("=== TESTING 20 MOVIES SUBTITLE BRIDGING & ACCURACY ===")
        var passedCount = 0

        for ((index, item) in moviesList.withIndex()) {
            println("\n[${index + 1}/20] Checking Movie: ${item.name} (TMDB: ${item.tmdbId}, IMDB: ${item.imdbId})")
            val subs = mutableListOf<SubtitleTrack>()

            // Test Atlantic subtitles
            try {
                val res = atlanticPlugin.getStreamLinks(item.tmdbId)
                subs.addAll(res.subtitles)
            } catch (_: Exception) {}

            println("  Total distinct language subtitle tracks: ${subs.size}")
            subs.take(15).forEach { println("    Track: ${it.language} -> ${it.url}") }

            // Verify clean deduplication: no language should exceed 2 variants (Regular and CC)
            val langGroups = subs.groupBy { it.language.replace(Regex("""\s*\[CC\]""", RegexOption.IGNORE_CASE), "").trim().lowercase() }
            langGroups.forEach { (lang, tracks) ->
                assertTrue("Language '$lang' in movie ${item.name} has ${tracks.size} tracks, expected <= 2", tracks.size <= 2)
            }

            val hasEnglish = subs.any { it.language.contains("english", ignoreCase = true) }
            if (subs.isNotEmpty()) {
                println("  English subtitle present: $hasEnglish")
                val sampleSub = subs.first()
                println("  Sample track: '${sampleSub.language}' -> ${sampleSub.url}")
                passedCount++
            }
        }

        println("\n=== MOVIES BRIDGING TEST SUMMARY: $passedCount / 20 resolved clean subtitles ===")
        assertTrue("At least 15/20 movies must have verified subtitles", passedCount >= 15)
    }

    @Test
    fun testSeriesSubtitlesAcrossProviders() = runBlocking {
        println("=== TESTING 20 WEB SERIES / TV SHOWS SUBTITLE BRIDGING & ACCURACY ===")
        var passedCount = 0

        for ((index, item) in seriesList.withIndex()) {
            println("\n[${index + 1}/20] Checking Series: ${item.name} S${item.season}E${item.episode} (TMDB: ${item.tmdbId})")
            val subs = mutableListOf<SubtitleTrack>()

            val episodeData = "${item.tmdbId}:${item.season}:${item.episode}"
            try {
                val res = atlanticPlugin.getStreamLinks(episodeData)
                subs.addAll(res.subtitles)
            } catch (_: Exception) {}

            println("  Total distinct language subtitle tracks: ${subs.size}")
            subs.take(15).forEach { println("    Track: ${it.language} -> ${it.url}") }

            // Verify clean deduplication: no language should exceed 2 variants (Regular and CC)
            val langGroups = subs.groupBy { it.language.replace(Regex("""\s*\[CC\]""", RegexOption.IGNORE_CASE), "").trim().lowercase() }
            langGroups.forEach { (lang, tracks) ->
                assertTrue("Language '$lang' in series ${item.name} has ${tracks.size} tracks, expected <= 2", tracks.size <= 2)
            }

            val hasEnglish = subs.any { it.language.contains("english", ignoreCase = true) }
            if (subs.isNotEmpty()) {
                println("  English subtitle present: $hasEnglish")
                val sampleSub = subs.first()
                println("  Sample track: '${sampleSub.language}' -> ${sampleSub.url}")
                passedCount++
            }
        }

        println("\n=== SERIES BRIDGING TEST SUMMARY: $passedCount / 20 resolved clean subtitles ===")
        assertTrue("At least 15/20 series must have verified subtitles", passedCount >= 15)
    }
}
