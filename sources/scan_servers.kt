package com.euthopiar.scratch

import com.euthopiar.core.provider.CinejoyWasmEngine
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import java.util.concurrent.TimeUnit

fun main() = runBlocking {
    val client = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(20, TimeUnit.SECONDS)
        .build()

    val servers = listOf("Nebula", "Lisbon", "Solara", "Athens", "Scout")
    for (s in servers) {
        val res = CinejoyWasmEngine.requestStream(client, s, "tv", "71912", 1, 1)
        println("[$s] -> $res")
    }
}
