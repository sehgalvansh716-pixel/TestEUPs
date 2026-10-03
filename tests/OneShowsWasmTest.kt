package com.euthopiar.core.provider

import com.dylibso.chicory.runtime.HostFunction
import com.dylibso.chicory.runtime.ImportValues
import com.dylibso.chicory.runtime.Instance
import com.dylibso.chicory.wasm.Parser
import com.dylibso.chicory.wasm.types.ValueType
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.OkHttpClient
import okhttp3.Request
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.File
import java.io.FileInputStream

class OneShowsWasmTest {

    private val json = Json { ignoreUnknownKeys = true }
    private val client = OkHttpClient()

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
    fun testLiveOneShowsDecryption() {
        // 1. Fetch token
        val tokenReq = Request.Builder()
            .url("https://api.viduki.net/download-token")
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
            .header("Origin", "https://www.1shows.bz")
            .header("Referer", "https://www.1shows.bz/")
            .build()
        val tokenResp = try { client.newCall(tokenReq).execute() } catch (_: Exception) { return }
        org.junit.Assume.assumeTrue("Token fetch successful", tokenResp.isSuccessful)
        val tokenBody = tokenResp.body?.string() ?: ""
        val token = json.parseToJsonElement(tokenBody).jsonObject["token"]?.jsonPrimitive?.content ?: ""
        org.junit.Assume.assumeTrue("Token not empty", token.isNotBlank())
        println("Got Viduki Token: $token")

        // 2. Fetch encrypted download payload for movie 550
        val dlReq = Request.Builder()
            .url("https://api.viduki.net/download/movie/550")
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
            .header("Origin", "https://www.1shows.bz")
            .header("Referer", "https://www.1shows.bz/")
            .header("x-download-token", token)
            .build()
        val dlResp = client.newCall(dlReq).execute()
        assertTrue("Payload fetch successful", dlResp.isSuccessful)
        val dlBody = dlResp.body?.string() ?: ""
        val payloadObj = json.parseToJsonElement(dlBody).jsonObject

        val ivHex = payloadObj["iv"]?.jsonPrimitive?.content ?: ""
        val tagHex = payloadObj["tag"]?.jsonPrimitive?.content ?: ""
        val ctHex = payloadObj["ct"]?.jsonPrimitive?.content ?: ""
        assertTrue("ct not empty", ctHex.isNotBlank())

        val tokenBytes = hexToBytes(token)
        val ivBytes = hexToBytes(ivHex)
        val tagBytes = hexToBytes(tagHex)
        val ctBytes = hexToBytes(ctHex)

        // 3. Load Wasm via Chicory
        val file = File("src/main/resources/makimaDL.wasm")
        val stream = FileInputStream(if (file.exists()) file else File("core/src/main/resources/makimaDL.wasm"))
        val module = Parser.parse(stream)

        val abortFn = HostFunction(
            "env",
            "abort",
            listOf(ValueType.I32, ValueType.I32, ValueType.I32, ValueType.I32),
            emptyList()
        ) { _, _ -> longArrayOf() }

        val importValues = ImportValues.builder()
            .withFunctions(listOf(abortFn))
            .build()

        val instance = Instance.builder(module)
            .withImportValues(importValues)
            .build()

        val alloc = instance.export("_eyNV")
        val reset = instance.export("_YH7c")
        val decrypt = instance.export("_YtSb")
        val memory = instance.memory()

        fun ec(bytes: ByteArray): Pair<Int, Int> {
            val ptr = alloc.apply(bytes.size.toLong())[0].toInt()
            memory.write(ptr, bytes)
            return ptr to bytes.size
        }

        val f = ec(tokenBytes)
        val p = ec(ivBytes)
        val m = ec(ctBytes)
        val x = ec(tagBytes)

        val g = alloc.apply(ctBytes.size.toLong())[0].toInt()

        val v = decrypt.apply(
            f.first.toLong(), f.second.toLong(),
            p.first.toLong(), p.second.toLong(),
            m.first.toLong(), m.second.toLong(),
            x.first.toLong(), x.second.toLong(),
            g.toLong()
        )[0].toInt()

        assertTrue("Decryption return code must be > 0, got: $v", v > 0)

        val decryptedBytes = memory.readBytes(g, v)
        reset.apply()

        val decryptedText = String(decryptedBytes, Charsets.UTF_8)
        println("Decrypted JSON:\n${decryptedText.take(300)}...")
        assertTrue("Decrypted output contains sources", decryptedText.contains("sources"))

        val sourcesArr = json.parseToJsonElement(decryptedText).jsonObject["sources"]?.jsonArray
        assertNotNull(sourcesArr)
        assertTrue("Sources should not be empty", sourcesArr!!.isNotEmpty())
        println("Total sources decrypted: ${sourcesArr.size}")
    }

    @Test
    fun testVidukiMainDecryption() {
        val base = "https://api.viduki.net"
        val vidHeaders = mapOf(
            "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer" to "https://www.viduki.net/",
            "Origin" to "https://www.viduki.net"
        )

        // 1. Fetch manifest & wasm
        val manReq = Request.Builder().url("https://www.viduki.net/makima-manifest.json")
            .header("Referer", "https://www.viduki.net/").build()
        val manResp = try { client.newCall(manReq).execute() } catch (_: Exception) { return }
        org.junit.Assume.assumeTrue("Manifest fetch ok", manResp.isSuccessful)
        val manBody = manResp.body?.string() ?: ""
        val manObj = json.parseToJsonElement(manBody).jsonObject
        val wasmUrl = "https://www.viduki.net" + manObj["url"]!!.jsonPrimitive.content
        val expObj = manObj["exports"]!!.jsonObject

        val allocName = expObj["alloc"]!!.jsonPrimitive.content
        val resetName = expObj["reset"]!!.jsonPrimitive.content
        val writeByteName = expObj["writeByte"]!!.jsonPrimitive.content
        val readByteName = expObj["readByte"]!!.jsonPrimitive.content
        val decryptPepperName = expObj["decryptPepper"]!!.jsonPrimitive.content
        val decryptEnvelopeName = expObj["decryptEnvelope"]!!.jsonPrimitive.content

        val wasmReq = Request.Builder().url(wasmUrl).header("Referer", "https://www.viduki.net/").build()
        val wasmBytes = client.newCall(wasmReq).execute().use { it.body!!.bytes() }
        val module = Parser.parse(java.io.ByteArrayInputStream(wasmBytes))

        val abortFn = HostFunction(
            "env",
            "abort",
            listOf(ValueType.I32, ValueType.I32, ValueType.I32, ValueType.I32),
            emptyList()
        ) { _, _ -> longArrayOf() }

        val instance = Instance.builder(module)
            .withImportValues(ImportValues.builder().withFunctions(listOf(abortFn)).build())
            .build()

        val alloc = instance.export(allocName)
        val reset = instance.export(resetName)
        val writeByte = instance.export(writeByteName)
        val readByte = instance.export(readByteName)
        val decryptPepper = instance.export(decryptPepperName)
        val decryptEnvelope = instance.export(decryptEnvelopeName)

        fun writeBuf(bytes: ByteArray): Int {
            val ptr = alloc.apply(bytes.size.toLong())[0].toInt()
            for (i in bytes.indices) {
                writeByte.apply(ptr.toLong(), i.toLong(), (bytes[i].toInt() and 0xFF).toLong())
            }
            return ptr
        }

        fun readBuf(ptr: Int, len: Int): ByteArray {
            val out = ByteArray(len)
            for (i in 0 until len) {
                out[i] = readByte.apply(ptr.toLong(), i.toLong())[0].toByte()
            }
            return out
        }

        // 2. Altcha solver
        fun solveAltcha(challengeUrl: String): String {
            val chReq = Request.Builder().url(challengeUrl)
                .header("User-Agent", vidHeaders["User-Agent"]!!)
                .header("Referer", vidHeaders["Referer"]!!)
                .header("Origin", vidHeaders["Origin"]!!)
                .build()
            val chBody = client.newCall(chReq).execute().use { it.body?.string() ?: "" }
            val chObj = json.parseToJsonElement(chBody).jsonObject
            val salt = chObj["salt"]!!.jsonPrimitive.content
            val target = chObj["challenge"]!!.jsonPrimitive.content
            val maxNum = chObj["maxnumber"]?.jsonPrimitive?.content?.toLongOrNull() ?: 50000L
            val algo = chObj["algorithm"]!!.jsonPrimitive.content
            val sig = chObj["signature"]!!.jsonPrimitive.content

            val md = java.security.MessageDigest.getInstance("SHA-256")
            var found = 0L
            for (n in 0L..maxNum) {
                md.reset()
                val digest = md.digest((salt + n).toByteArray(Charsets.UTF_8))
                val hex = digest.joinToString("") { "%02x".format(it) }
                if (hex.equals(target, ignoreCase = true)) {
                    found = n
                    break
                }
            }

            val ansJson = """{"algorithm":"$algo","challenge":"$target","number":$found,"salt":"$salt","signature":"$sig","took":10}"""
            return java.util.Base64.getEncoder().encodeToString(ansJson.toByteArray(Charsets.UTF_8))
        }

        // Step 1: Bootstrap
        val altcha1 = solveAltcha("$base/altcha-challenge")
        val bootReq = Request.Builder().url("$base/bootstrap")
            .header("User-Agent", vidHeaders["User-Agent"]!!)
            .header("Referer", vidHeaders["Referer"]!!)
            .header("Origin", vidHeaders["Origin"]!!)
            .header("X-Altcha", altcha1)
            .header("Accept", "application/json")
            .build()
        val bootBody = client.newCall(bootReq).execute().use { it.body?.string() ?: "" }
        val nonce = json.parseToJsonElement(bootBody).jsonObject["n"]!!.jsonPrimitive.content
        assertTrue("Nonce not empty", nonce.isNotBlank())
        println("Bootstrap Nonce: $nonce")

        // Step 2: Pepper
        val altcha2 = solveAltcha("$base/altcha-challenge")
        val pepReq = Request.Builder().url("$base/pepper-key")
            .header("User-Agent", vidHeaders["User-Agent"]!!)
            .header("Referer", vidHeaders["Referer"]!!)
            .header("Origin", vidHeaders["Origin"]!!)
            .header("X-Nonce", nonce)
            .header("X-Altcha", altcha2)
            .header("Accept", "application/json")
            .build()
        val pepBody = client.newCall(pepReq).execute().use { it.body?.string() ?: "" }
        val pepObj = json.parseToJsonElement(pepBody).jsonObject
        val bucket = pepObj["bucket"]!!.jsonPrimitive.content.toLong()
        val pepIv = hexToBytes(pepObj["iv"]!!.jsonPrimitive.content)
        val pepCt = hexToBytes(pepObj["ct"]!!.jsonPrimitive.content)
        val pepTag = hexToBytes(pepObj["tag"]!!.jsonPrimitive.content)

        reset.apply()
        val nonceBytes = hexToBytes(nonce)
        val bucketBuf = java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.BIG_ENDIAN).putLong(bucket).array()

        val pNonce = writeBuf(nonceBytes)
        val pBucket = writeBuf(bucketBuf)
        val pIv = writeBuf(pepIv)
        val pCt = writeBuf(pepCt)
        val pTag = writeBuf(pepTag)

        val pepRes = decryptPepper.apply(
            pNonce.toLong(), nonceBytes.size.toLong(),
            pBucket.toLong(), 8L,
            pIv.toLong(), pepIv.size.toLong(),
            pCt.toLong(), pepCt.size.toLong(),
            pTag.toLong(), pepTag.size.toLong()
        )[0].toInt()
        assertTrue("Pepper decryption result > 0", pepRes > 0)
        println("Pepper decrypted successfully: $pepRes")

        // Step 3: Query Main Movie 550 with server Chris (Hindi)
        val random = java.security.SecureRandom()
        val cnBytes = ByteArray(16).also { random.nextBytes(it) }
        val reqIdBytes = ByteArray(16).also { random.nextBytes(it) }
        val clientNonceHex = cnBytes.joinToString("") { "%02x".format(it) }
        val reqIdHex = reqIdBytes.joinToString("") { "%02x".format(it) }

        val streamReq = Request.Builder().url("$base/main/movie/550?srv=Chris")
            .header("User-Agent", vidHeaders["User-Agent"]!!)
            .header("Referer", vidHeaders["Referer"]!!)
            .header("Origin", vidHeaders["Origin"]!!)
            .header("X-Nonce", nonce)
            .header("X-Client-Nonce", clientNonceHex)
            .header("X-Request-Id", reqIdHex)
            .header("Accept", "application/json")
            .build()
        val streamResp = client.newCall(streamReq).execute()
        val streamBody = streamResp.body?.string() ?: ""
        println("Encrypted Stream Body: ${streamBody.take(150)}...")
        val sObj = json.parseToJsonElement(streamBody).jsonObject

        val snBytes = hexToBytes(sObj["sn"]!!.jsonPrimitive.content)
        val tb = sObj["tb"]!!.jsonPrimitive.content.toLong()
        val tbBuf = java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.BIG_ENDIAN).putLong(tb).array()
        val iv1Bytes = hexToBytes(sObj["iv1"]!!.jsonPrimitive.content)
        val iv2Bytes = hexToBytes(sObj["iv2"]!!.jsonPrimitive.content)
        val tag1Bytes = hexToBytes(sObj["tag1"]!!.jsonPrimitive.content)
        val tag2Bytes = hexToBytes(sObj["tag2"]!!.jsonPrimitive.content)
        val wkBytes = hexToBytes(sObj["wk"]!!.jsonPrimitive.content)
        val ctBytes = hexToBytes(sObj["ct"]!!.jsonPrimitive.content)

        reset.apply()
        val pCN = writeBuf(cnBytes)
        val pSN = writeBuf(snBytes)
        val pTB = writeBuf(tbBuf)
        val pReqId = writeBuf(reqIdBytes)
        val pIv2 = writeBuf(iv2Bytes)
        val pWk = writeBuf(wkBytes)
        val pTag2 = writeBuf(tag2Bytes)
        val pIv1 = writeBuf(iv1Bytes)
        val pSCt = writeBuf(ctBytes)
        val pTag1 = writeBuf(tag1Bytes)
        val pOut = alloc.apply(ctBytes.size.toLong())[0].toInt()

        val outLen = decryptEnvelope.apply(
            pCN.toLong(), cnBytes.size.toLong(),
            pSN.toLong(), snBytes.size.toLong(),
            pTB.toLong(), 8L,
            pReqId.toLong(), reqIdBytes.size.toLong(),
            pIv2.toLong(), iv2Bytes.size.toLong(),
            pWk.toLong(), wkBytes.size.toLong(),
            pTag2.toLong(), tag2Bytes.size.toLong(),
            pIv1.toLong(), iv1Bytes.size.toLong(),
            pSCt.toLong(), ctBytes.size.toLong(),
            pTag1.toLong(), tag1Bytes.size.toLong(),
            pOut.toLong()
        )[0].toInt()

        assertTrue("Envelope decryption outLen > 0, was: $outLen", outLen > 0)
        val decBytes = readBuf(pOut, outLen)
        val decStr = String(decBytes, Charsets.UTF_8)
        println("DECRYPTED VIDUKI STREAM JSON: $decStr")
        assertTrue("Decrypted stream contains url", decStr.contains("http"))
    }

    @Test
    fun testVidrockLiveDecryption() {
        val req = Request.Builder()
            .url("https://vidrock.net/api/tv/113962/1/1")
            .header("Referer", "https://vidrock.to/")
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
            .build()
        val resp = client.newCall(req).execute()
        assertTrue("Vidrock API 200 OK", resp.isSuccessful)
        val body = resp.body?.string() ?: ""
        println("Vidrock API raw: ${body.take(100)}...")
        val root = json.parseToJsonElement(body).jsonObject
        var decryptedCount = 0
        for ((srv, srvElem) in root) {
            val encUrl = srvElem.jsonObject["url"]?.jsonPrimitive?.content ?: continue
            val decryptedUrl = OneShowsWasmEngine.decryptVidrockPayload(encUrl)
            if (decryptedUrl != null) {
                println("Decrypted Vidrock [$srv]: $decryptedUrl")
                assertTrue("Decrypted URL is valid HTTP", decryptedUrl.startsWith("http"))
                decryptedCount++
            }
        }
        assertTrue("Decrypted at least 2 Vidrock servers", decryptedCount >= 2)
    }

    @Test
    fun testVidyLiveDecryption() {
        val tmdbId = "113962"
        val seedReq = Request.Builder()
            .url("https://api.wecollege.net/seed?mediaId=$tmdbId")
            .header("Referer", "https://www.vidy.st/")
            .header("Origin", "https://www.vidy.st")
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
            .build()
        val seedResp = client.newCall(seedReq).execute()
        assertTrue("Seed API 200 OK", seedResp.isSuccessful)
        val seedBody = seedResp.body?.string() ?: ""
        val seed = json.parseToJsonElement(seedBody).jsonObject["seed"]?.jsonPrimitive?.content ?: ""
        assertTrue("Seed is not blank", seed.isNotBlank())
        println("Got Vidy seed: $seed")

        // Query Atlanta server
        val atlantaUrl = "https://api.wecollege.net/atlanta/sources?title=Special+Ops%3A+Lioness&mediaType=tv&year=2023&seasonId=1&episodeId=1&tmdbId=$tmdbId&enc=2&seed=$seed"
        val atlReq = Request.Builder()
            .url(atlantaUrl)
            .header("Referer", "https://www.vidy.st/")
            .header("Origin", "https://www.vidy.st")
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
            .build()
        val atlResp = try { client.newCall(atlReq).execute() } catch (_: Exception) { return }
        org.junit.Assume.assumeTrue("Atlanta API 200 OK", atlResp.isSuccessful)
        val atlBody = atlResp.body?.string() ?: ""
        val decryptedJson = OneShowsWasmEngine.decryptVidyPayload(atlBody, seed, tmdbId)
        assertNotNull("Decrypted Atlanta JSON is not null", decryptedJson)
        println("DECRYPTED VIDY ATLANTA: ${decryptedJson!!.take(200)}...")
        assertTrue("Contains sources", decryptedJson.contains("sources"))
    }
}
