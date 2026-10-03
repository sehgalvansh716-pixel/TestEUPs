package com.euthopiar.scratch

import com.euthopiar.core.model.*
import com.euthopiar.core.provider.CinejoyWasmEngine
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.toList
import kotlinx.serialization.json.*
import okhttp3.OkHttpClient
import okhttp3.Request
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

fun main() = runBlocking {
    val client = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(25, TimeUnit.SECONDS)
        .build()

    val json = Json { ignoreUnknownKeys = true }

    println("==================================================")
    println("1. TEST SERVER ROSTER DISCOVERY (api.wing.st/servers)")
    println("==================================================")
    val serverList = mutableListOf<String>()
    try {
        val sReq = Request.Builder()
            .url("https://api.wing.st/servers")
            .header("Referer", "https://cinejoy.pk/")
            .header("User-Agent", "Mozilla/5.0")
            .build()
        val sResp = client.newCall(sReq).execute()
        if (sResp.isSuccessful) {
            val body = sResp.body?.string() ?: ""
            val root = json.parseToJsonElement(body).jsonObject
            root["servers"]?.jsonArray?.forEach { elem ->
                val sObj = elem.jsonObject
                val sName = sObj["name"]?.jsonPrimitive?.contentOrNull
                val sStatus = sObj["status"]?.jsonPrimitive?.contentOrNull
                if (sName != null && sStatus == "ok") {
                    serverList.add(sName)
                }
            }
        }
    } catch (e: Exception) {
        println("Error fetching servers: $e")
    }
    if (serverList.isEmpty()) {
        serverList.addAll(listOf("Nebula", "Lisbon", "Scout", "Riga", "Solara", "Athens"))
    }
    println("Discovered active servers: $serverList")

    println("\n==================================================")
    println("2. TEST STREAM EXTRACTION & PROBING FOR MOVIE (27205)")
    println("==================================================")
    val testMovieId = "27205"
    coroutineScope {
        serverList.forEach { server ->
            launch {
                val res = withTimeoutOrNull(8000) {
                    CinejoyWasmEngine.requestStream(
                        client = client,
                        server = server,
                        type = "movie",
                        tmdbId = testMovieId
                    )
                }
                if (res != null) {
                    try {
                        val root = json.parseToJsonElement(res).jsonObject
                        val streamArr = root["data"]?.jsonObject?.get("stream")?.jsonArray
                        if (streamArr != null && streamArr.isNotEmpty()) {
                            for (sElem in streamArr) {
                                val sObj = sElem.jsonObject
                                val pUrl = sObj["playlist"]?.jsonPrimitive?.contentOrNull ?: continue
                                val probeReq = Request.Builder()
                                    .url(pUrl)
                                    .header("User-Agent", "Mozilla/5.0")
                                    .header("Referer", "https://cinejoy.pk/")
                                    .header("Range", "bytes=0-1024")
                                    .build()
                                val probeResp = client.newCall(probeReq).execute()
                                val code = probeResp.code
                                probeResp.close()
                                println("[$server] Stream URL: ${pUrl.take(70)}... -> HTTP $code ${if (code in 200..299) "LIVE [OK]" else "BLOCKED/FAILED"}")
                            }
                        } else {
                            println("[$server] No stream array (or error): ${res.take(80)}")
                        }
                    } catch (e: Exception) {
                        println("[$server] Parse error: $e")
                    }
                } else {
                    println("[$server] Timed out or returned null")
                }
            }
        }
    }

    println("\n==================================================")
    println("3. TEST SUBTITLES (subs.wing.st)")
    println("==================================================")
    try {
        val subReq = Request.Builder()
            .url("https://subs.wing.st/subtitles?type=movie&tmdb=27205")
            .header("Referer", "https://cinejoy.pk/")
            .header("User-Agent", "Mozilla/5.0")
            .build()
        val subResp = client.newCall(subReq).execute()
        if (subResp.isSuccessful) {
            val body = subResp.body?.string() ?: ""
            val root = json.parseToJsonElement(body).jsonObject
            val subsArr = root["subtitles"]?.jsonArray
            println("Retrieved ${subsArr?.size ?: 0} subtitles from subs.wing.st!")
            subsArr?.take(5)?.forEach { elem ->
                val sObj = elem.jsonObject
                println("  - [${sObj["language"]?.jsonPrimitive?.content}] ${sObj["display"]?.jsonPrimitive?.content}")
            }
        }
    } catch (e: Exception) {
        println("Sub error: $e")
    }

    println("\n==================================================")
    println("4. TEST DOWNLOAD OPTIONS FOR Inception (27205)")
    println("==================================================")
    val dReq = Request.Builder()
        .url("https://downloads.wing.st/movie/27205")
        .header("Referer", "https://cinejoy.pk/")
        .header("User-Agent", "Mozilla/5.0")
        .build()
    val dResp = client.newCall(dReq).execute()
    val dBody = dResp.body?.string() ?: ""
    val dRoot = json.parseToJsonElement(dBody).jsonObject
    val linksArr = dRoot["links"]?.jsonArray
    val validDownloads = mutableListOf<String>()
    linksArr?.forEach { elem ->
        val obj = elem.jsonObject
        val url = obj["url"]?.jsonPrimitive?.contentOrNull ?: return@forEach
        val name = obj["name"]?.jsonPrimitive?.contentOrNull ?: ""
        if (!url.contains(".workers.dev") && !url.contains("111477.xyz")) {
            validDownloads.add("[$name] -> $url")
        }
    }
    println("Direct R2 downloads found: ${validDownloads.size}")
    if (validDownloads.isEmpty()) {
        println("Fallback to verified live streams: Lisbon / Nebula")
    }
}
