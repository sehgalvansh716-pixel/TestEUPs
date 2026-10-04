package com.euthopiar.scratch

import com.euthopiar.core.provider.CinejoyWasmEngine
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import okhttp3.Request
import java.util.concurrent.TimeUnit
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive

fun main() = runBlocking {
    val client = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(20, TimeUnit.SECONDS)
        .build()

    val servers = listOf("Nebula", "Lisbon", "Scout", "Solara", "Riga", "Athens")
    println("=== Testing servers for Interstellar (movie: 157336) ===")
    for (server in servers) {
        val res = CinejoyWasmEngine.requestStream(client, server, "movie", "157336")
        if (res != null) {
            val json = Json { ignoreUnknownKeys = true }
            val root = json.parseToJsonElement(res).jsonObject
            val dataObj = root["data"]?.jsonObject
            val streamArr = dataObj?.get("stream")?.jsonArray
            val url = streamArr?.firstOrNull()?.jsonObject?.get("playlist")?.jsonPrimitive?.content
            println("Server [$server]: playlist=$url")
            if (url != null) {
                try {
                    val req = Request.Builder()
                        .url(url)
                        .header("Referer", "https://cinejoy.pk/")
                        .header("Origin", "https://cinejoy.pk")
                        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                        .build()
                    val code = client.newCall(req).execute().use { it.code }
                    println("  -> HTTP status: $code")
                } catch (e: Exception) {
                    println("  -> Fetch error: ${e.message}")
                }
            }
        } else {
            println("Server [$server]: returned null")
        }
    }
}
