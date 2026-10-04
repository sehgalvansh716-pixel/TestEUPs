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

    println("Requesting Lisbon for Interstellar (157336)...")
    val res = CinejoyWasmEngine.requestStream(client, "Lisbon", "movie", "157336")
    println("Response: $res")
    if (res != null) {
        val json = Json { ignoreUnknownKeys = true }
        val root = json.parseToJsonElement(res).jsonObject
        val dataObj = root["data"]?.jsonObject
        val streamArr = dataObj?.get("stream")?.jsonArray
        val url = streamArr?.firstOrNull()?.jsonObject?.get("playlist")?.jsonPrimitive?.content
        println("Lisbon playlist: $url")
        if (url != null) {
            val req = Request.Builder()
                .url(url)
                .header("Referer", "https://cinejoy.pk/")
                .header("User-Agent", "Mozilla/5.0")
                .build()
            client.newCall(req).execute().use { resp ->
                println("Master code: ${resp.code}")
                val body = resp.body?.string() ?: ""
                println("Master snippet:\n${body.take(400)}")
            }
        }
    }
}
