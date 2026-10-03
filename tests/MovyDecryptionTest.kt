package com.euthopiar.core.provider

import okhttp3.OkHttpClient
import okhttp3.Request
import org.junit.Assert.*
import org.junit.Test
import java.util.Base64
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

class MovyDecryptionTest {

    private val client = OkHttpClient()
    private val json = Json { ignoreUnknownKeys = true }

    object MovyCipher {
        private val D = intArrayOf(
            0x428a2f98.toInt(), 0x71374491.toInt(), 0xb5c0fbcf.toInt(), 0xe9b5dba5.toInt(),
            0x3956c25b, 0x59f111f1, 0x923f82a4.toInt(), 0xab1c5ed5.toInt(),
            0xd807aa98.toInt(), 0x12835b01, 0x243185be, 0x550c7dc3,
            0x72be5d74, 0x80deb1fe.toInt(), 0x9bdc06a7.toInt(), 0xc19bf174.toInt()
        )
        private val MAGIC_PREFIX = byteArrayOf(109, 118, 109, 49) // "mvm1"

        private fun o(e: Int): Int {
            var x = e xor (e ushr 16)
            x *= 0x85ebca6b.toInt()
            x = x xor (x ushr 13)
            x *= 0xc2b2ae35.toInt()
            return x xor (x ushr 16)
        }

        private fun u(e: Int, a: Int): Int {
            val shift = a and 31
            return if (shift == 0) e else (e shl shift) or (e ushr (32 - shift))
        }

        fun decrypt(encB64: String, seedStr: String, mediaId: Long): String {
            val normalized = encB64.trim().replace('-', '+').replace('_', '/')
            val padLen = (4 - (normalized.length % 4)) % 4
            val padded = normalized + "=".repeat(padLen)
            val raw = Base64.getDecoder().decode(padded)
            val t = raw.size

            val sValues = IntArray(61)
            val sPresent = BooleanArray(61)
            var h = 0x811c9dc5.toInt()
            for (i in 0 until seedStr.length) {
                h = (h xor seedStr[i].code) * 0x1000193
            }

            val mediaIdLow32 = (mediaId and 0xFFFFFFFFL).toInt()
            var n = o(o(h) xor o(mediaIdLow32 xor 0x9e3779b9.toInt()))

            for (e in 0 until 8) {
                if (((e * (e + 1)) and 1) == 0) {
                    val a = Integer.remainderUnsigned(n, 61)
                    n = u(n + 0x9e3779b9.toInt(), 7 + (7 and e))
                    sValues[a] = n xor o(n)
                    sPresent[a] = true
                    n = o(n + a)
                } else {
                    sValues[e] = D[15 and e]
                    sPresent[e] = true
                }
            }

            var acc = o(0xa5a5a5a5.toInt() xor n)

            val keystream = ByteArray(t)
            var lCounter = 0
            var e = 0
            while (e < t) {
                val dVal = Integer.remainderUnsigned(acc, 61)
                val isPresent = sPresent[dVal]
                val iFlag = if (isPresent) -1 else 0
                val rVal = if (isPresent) sValues[dVal] else 0
                val cVal = 0x9e3779b9.toInt() * (lCounter + 1)
                lCounter++
                val tXor = acc
                val sXor = rVal xor cVal
                val bVal = (tXor xor sXor) or (tXor and sXor and iFlag)
                val rot1 = u(bVal + acc, 31 and dVal)
                val rot2 = u(acc, 31 and (dVal * 7))
                acc = o((rot1 xor rot2) + 0x9e3779b9.toInt())
                sValues[dVal] = acc
                sPresent[dVal] = true
                val kw = acc

                keystream[e++] = (kw and 0xFF).toByte()
                if (e < t) keystream[e++] = ((kw ushr 8) and 0xFF).toByte()
                if (e < t) keystream[e++] = ((kw ushr 16) and 0xFF).toByte()
                if (e < t) keystream[e++] = ((kw ushr 24) and 0xFF).toByte()
            }

            for (i in 0 until t) {
                raw[i] = (raw[i].toInt() xor keystream[i].toInt()).toByte()
            }

            for (i in 0 until 4) {
                if (raw[i] != MAGIC_PREFIX[i]) {
                    throw IllegalArgumentException("Decrypt header mismatch")
                }
            }

            return String(raw, 4, t - 4, Charsets.UTF_8)
        }
    }

    @Test
    fun testLiveMovyDecryption() {
        val tmdbId = 693134L
        val seedReq = Request.Builder()
            .url("https://api.wecollege.net/seed?mediaId=$tmdbId")
            .header("Origin", "https://www.movy.sx")
            .header("Referer", "https://www.movy.sx/")
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .build()

        val seedResp = client.newCall(seedReq).execute()
        assertTrue("Seed status should be 200", seedResp.isSuccessful)
        val seedBody = seedResp.body?.string() ?: ""
        val seed = json.parseToJsonElement(seedBody).jsonObject["seed"]?.jsonPrimitive?.content
        assertNotNull("Seed should not be null", seed)

        println("Obtained session seed: $seed")

        val sourcesReq = Request.Builder()
            .url("https://api.wecollege.net/miami/sources?title=Dune%3A+Part+Two&mediaType=movie&year=2024&tmdbId=$tmdbId&imdbId=tt15239678&enc=2&seed=$seed")
            .header("Origin", "https://www.movy.sx")
            .header("Referer", "https://www.movy.sx/")
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .build()

        val sourcesResp = client.newCall(sourcesReq).execute()
        assertTrue("Sources status should be 200", sourcesResp.isSuccessful)
        val encPayload = sourcesResp.body?.string() ?: ""
        assertTrue("Encrypted payload should not be empty", encPayload.isNotEmpty())

        val decryptedJson = MovyCipher.decrypt(encPayload, seed!!, tmdbId)
        println("Decrypted JSON:\n${decryptedJson.take(300)}...")
        assertTrue("Decrypted JSON should contain sources", decryptedJson.contains("\"sources\":"))
        assertTrue("Decrypted JSON should contain moon.zenoak.top or m3u8", decryptedJson.contains(".m3u8"))
    }
}
