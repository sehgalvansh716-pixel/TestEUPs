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
        .readTimeout(25, TimeUnit.SECONDS)
        .build()

    println("Requesting fresh Solara stream for Inception (27205)...")
    val res = CinejoyWasmEngine.requestStream(
        client = client,
        server = "Solara",
        type = "movie",
        tmdbId = "27205"
    )
    println("Response raw: $res")

    if (res != null) {
        val json = Json { ignoreUnknownKeys = true }
        val root = json.parseToJsonElement(res).jsonObject
        val dataObj = root["data"]?.jsonObject
        val streamArr = dataObj?.get("stream")?.jsonArray
        val url = streamArr?.firstOrNull()?.jsonObject?.get("playlist")?.jsonPrimitive?.content
        println("Testing playlist URL immediately: $url")

        if (url != null) {
            val req = Request.Builder()
                .url(url)
                .header("Referer", "https://cinejoy.pk/")
                .header("Origin", "https://cinejoy.pk")
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                .build()

            client.newCall(req).execute().use { resp ->
                println("HTTP Code: ${resp.code}")
                println("Headers: ${resp.headers}")
                val body = resp.body?.string()?.take(500)
                println("Body: $body")
            }
        }
    }
}
