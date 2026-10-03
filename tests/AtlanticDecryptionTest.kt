package com.euthopiar.core.provider

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.OkHttpClient
import okhttp3.Request
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

class AtlanticDecryptionTest {

    private val client = OkHttpClient()
    private val json = Json { ignoreUnknownKeys = true }

    private fun hexToBytes(hex: String): ByteArray {
        val len = hex.length
        val data = ByteArray(len / 2)
        var i = 0
        while (i < len) {
            data[i / 2] = ((Character.digit(hex[i], 16) shl 4) + Character.digit(hex[i + 1], 16)).toByte()
            i += 2
        }
        return data
    }

    @Test
    fun testLiveAtlanticDecryption() {
        val req = Request.Builder()
            .url("https://stream.hls.lol/helios?tmdbId=533535&type=movie")
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .header("Origin", "https://atlantic.st")
            .header("Referer", "https://atlantic.st/")
            .build()

        val resp = try { client.newCall(req).execute() } catch (_: Exception) { return }
        org.junit.Assume.assumeTrue("Helios call was successful", resp.isSuccessful)
        val body = resp.body?.string() ?: ""
        val root = try { json.parseToJsonElement(body).jsonObject } catch (_: Exception) { return }
        val sources = root["sources"]?.jsonObject
        org.junit.Assume.assumeNotNull(sources)
        org.junit.Assume.assumeTrue("Sources non-empty", sources != null && sources.isNotEmpty())

        val moscow = sources?.get("Moscow")?.jsonObject
        val encUrl = moscow?.get("url")?.jsonPrimitive?.content ?: ""
        org.junit.Assume.assumeTrue("Must start with ns_", encUrl.startsWith("ns_"))

        val rawHex = encUrl.removePrefix("ns_")
        val rawBytes = hexToBytes(rawHex)

        val iv = rawBytes.copyOfRange(0, 12)
        val ctAndTag = rawBytes.copyOfRange(12, rawBytes.size)

        val key = hexToBytes("e4b8a1d6f2c9037b5a8e4d1c6f9b2085a7c3e9f6d1b4a8c2e5f7a0d3b6c9e2f5")
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        val keySpec = SecretKeySpec(key, "AES")
        val gcmSpec = GCMParameterSpec(128, iv)
        cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec)

        val decryptedBytes = cipher.doFinal(ctAndTag)
        val decryptedUrl = String(decryptedBytes, Charsets.UTF_8)
        println("Decrypted Atlantic Stream URL: $decryptedUrl")
        assertTrue("Decrypted URL must be valid workers URL", decryptedUrl.startsWith("https://"))

        // Now test requesting the stream
        val m3u8Req = Request.Builder()
            .url(decryptedUrl)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .header("Origin", "https://atlantic.st")
            .header("Referer", "https://atlantic.st/")
            .build()
        val m3u8Resp = client.newCall(m3u8Req).execute()
        assertTrue("M3U8 fetch successful", m3u8Resp.isSuccessful)
        val m3u8Content = m3u8Resp.body?.string() ?: ""
        assertTrue("Must start with #EXTM3U", m3u8Content.startsWith("#EXTM3U"))
        println("M3U8 verified:\n${m3u8Content.take(200)}...")
    }
}
