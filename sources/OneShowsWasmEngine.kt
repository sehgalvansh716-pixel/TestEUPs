package com.euthopiar.core.provider

import com.dylibso.chicory.runtime.HostFunction
import com.dylibso.chicory.runtime.ImportValues
import com.dylibso.chicory.runtime.Instance
import com.dylibso.chicory.wasm.Parser
import com.dylibso.chicory.wasm.WasmModule
import com.dylibso.chicory.wasm.types.ValueType
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * Headless WebAssembly cryptographic engine for 1Shows / Viduki / Vidzee.
 *
 * Runs:
 * 1. `makimaDL.wasm` runtime for high-speed direct CDN streaming & downloads.
 * 2. `vidzee.wasm` runtime for multi-audio Vidzee player stream decryption.
 * 3. Dynamic `makima.wasm` runtime for authentic Viduki Main (Leon, Jill, Chris, Grace, etc.)
 *    stream and session decryption via Altcha PoW verification.
 */
object OneShowsWasmEngine {

    private const val WASM_URL = "https://www.1shows.bz/makimaDL.5505988f62c6f867.wasm"
    private const val REFERER = "https://www.1shows.bz/"
    private const val ORIGIN = "https://www.1shows.bz"
    private const val USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    private val json = Json { ignoreUnknownKeys = true }

    @Volatile
    private var parsedModule: WasmModule? = null

    @Volatile
    private var vidzeeModule: WasmModule? = null

    @Volatile
    private var makimaModule: WasmModule? = null

    @Volatile
    private var makimaExports: MakimaExports? = null

    private val sessionNonceCache = ConcurrentHashMap<String, Pair<String, Long>>()

    data class MakimaExports(
        val alloc: String,
        val reset: String,
        val writeByte: String,
        val readByte: String,
        val decryptPepper: String,
        val decryptEnvelope: String
    )

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

    /* ---------------------------------------------------------------------- */
    /* 1. Vidzee WASM Decryption                                               */
    /* ---------------------------------------------------------------------- */

    private fun getOrLoadVidzeeModule(client: OkHttpClient): WasmModule {
        vidzeeModule?.let { return it }
        synchronized(this) {
            vidzeeModule?.let { return it }

            val resourceStream = OneShowsWasmEngine::class.java.classLoader?.getResourceAsStream("vidzee.wasm")
            val bytes = if (resourceStream != null) {
                resourceStream.use { it.readBytes() }
            } else {
                val req = Request.Builder()
                    .url("https://player.vidzee.wtf/assets/streams-BHpSC3gU.js")
                    .header("Referer", "https://player.vidzee.wtf/")
                    .header("User-Agent", USER_AGENT)
                    .build()
                val js = client.newCall(req).execute().use { it.body?.string() ?: "" }
                val match = Regex("""const\s+R\s*=\s*"([^"]+)"""").find(js)
                if (match != null) {
                    Base64.getDecoder().decode(match.groupValues[1])
                } else {
                    throw IllegalStateException("Failed to load vidzee wasm module")
                }
            }

            val module = Parser.parse(ByteArrayInputStream(bytes))
            vidzeeModule = module
            return module
        }
    }

    /**
     * Decrypts encrypted payload 'c' returned by https://core.vidzee.wtf/streams/
     * using the pure-Java Chicory WebAssembly runtime.
     */
    suspend fun decryptVidzeePayload(
        client: OkHttpClient,
        cBase64: String,
        hostname: String = "player.vidzee.wtf"
    ): String? = withContext(Dispatchers.IO) {
        try {
            val module = getOrLoadVidzeeModule(client)

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

            val allocNew = instance.export("__new")
            val pin = instance.export("__pin")
            val unpin = instance.export("__unpin")
            val decrypt = instance.export("decrypt")
            val memory = instance.memory()

            fun allocateString(s: String): Int {
                val byteLen = s.length * 2
                val ptr = allocNew.apply(byteLen.toLong(), 2L)[0].toInt()
                val buf = ByteBuffer.allocate(byteLen).order(ByteOrder.LITTLE_ENDIAN)
                for (ch in s) {
                    buf.putChar(ch)
                }
                memory.write(ptr, buf.array())
                return ptr
            }

            fun allocateByteArray(bytes: ByteArray): Int {
                val len = bytes.size
                val abPtr = pin.apply(allocNew.apply(len.toLong(), 1L)[0])[0].toInt()
                memory.write(abPtr, bytes)
                val dPtr = allocNew.apply(12L, 6L)[0].toInt()
                val buf = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
                buf.putInt(abPtr)
                buf.putInt(abPtr)
                buf.putInt(len)
                memory.write(dPtr, buf.array())
                unpin.apply(abPtr.toLong())
                return dPtr
            }

            val rawBytes = Base64.getDecoder().decode(cBase64)
            val arrPtr = allocateByteArray(rawBytes)
            val hostPtr = allocateString(hostname)

            val resPtr = decrypt.apply(arrPtr.toLong(), hostPtr.toLong())[0].toInt()
            if (resPtr <= 0) return@withContext null

            val metaBytes = memory.readBytes(resPtr, 12)
            val metaBuf = ByteBuffer.wrap(metaBytes).order(ByteOrder.LITTLE_ENDIAN)
            metaBuf.int // skip bufPtr0
            val bufPtr = metaBuf.int
            val outLen = metaBuf.int

            if (outLen <= 0) return@withContext null

            val outBytes = memory.readBytes(bufPtr, outLen)
            String(outBytes, Charsets.UTF_8)
        } catch (_: Exception) {
            null
        }
    }

    /* ---------------------------------------------------------------------- */
    /* 2. MakimaDL Direct Downloads WASM Decryption                           */
    /* ---------------------------------------------------------------------- */

    private fun getOrLoadModule(client: OkHttpClient): WasmModule {
        parsedModule?.let { return it }
        synchronized(this) {
            parsedModule?.let { return it }

            // 1. Try classpath resource
            val resourceStream = OneShowsWasmEngine::class.java.classLoader?.getResourceAsStream("makimaDL.wasm")
            val bytes = if (resourceStream != null) {
                resourceStream.use { it.readBytes() }
            } else {
                // 2. Fallback to network download
                val request = Request.Builder()
                    .url(WASM_URL)
                    .header("Referer", REFERER)
                    .header("Origin", ORIGIN)
                    .header("User-Agent", USER_AGENT)
                    .build()
                client.newCall(request).execute().use { response ->
                    response.body?.bytes() ?: throw IllegalStateException("Empty body from $WASM_URL")
                }
            }

            val module = Parser.parse(ByteArrayInputStream(bytes))
            parsedModule = module
            return module
        }
    }

    suspend fun decryptPayload(
        client: OkHttpClient,
        tokenHex: String,
        ivHex: String,
        tagHex: String,
        ctHex: String
    ): String? = withContext(Dispatchers.IO) {
        try {
            val module = getOrLoadModule(client)

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

            fun writeBytes(bytes: ByteArray): Pair<Int, Int> {
                val ptr = alloc.apply(bytes.size.toLong())[0].toInt()
                memory.write(ptr, bytes)
                return ptr to bytes.size
            }

            val tokenBytes = hexToBytes(tokenHex)
            val ivBytes = hexToBytes(ivHex)
            val tagBytes = hexToBytes(tagHex)
            val ctBytes = hexToBytes(ctHex)

            val f = writeBytes(tokenBytes)
            val p = writeBytes(ivBytes)
            val m = writeBytes(ctBytes)
            val x = writeBytes(tagBytes)

            val g = alloc.apply(ctBytes.size.toLong())[0].toInt()

            val v = decrypt.apply(
                f.first.toLong(), f.second.toLong(),
                p.first.toLong(), p.second.toLong(),
                m.first.toLong(), m.second.toLong(),
                x.first.toLong(), x.second.toLong(),
                g.toLong()
            )[0].toInt()

            if (v <= 0) {
                try { reset.apply() } catch (_: Exception) {}
                return@withContext null
            }

            val decryptedBytes = memory.readBytes(g, v)
            try { reset.apply() } catch (_: Exception) {}

            String(decryptedBytes, Charsets.UTF_8)
        } catch (_: Exception) {
            null
        }
    }

    /* ---------------------------------------------------------------------- */
    /* 3. Viduki Main & Altcha Decryption (Leon, Jill, Chris, Grace, etc.)     */
    /* ---------------------------------------------------------------------- */

    fun solveAltcha(salt: String, challenge: String, maxNumber: Long, algorithm: String = "SHA-256", signature: String = ""): String {
        val md = MessageDigest.getInstance(algorithm)
        var found = 0L
        for (n in 0L..maxNumber) {
            md.reset()
            val text = salt + n
            val digest = md.digest(text.toByteArray(Charsets.UTF_8))
            val hex = digest.joinToString("") { "%02x".format(it) }
            if (hex.equals(challenge, ignoreCase = true)) {
                found = n
                break
            }
        }
        val payload = """{"algorithm":"$algorithm","challenge":"$challenge","number":$found,"salt":"$salt","signature":"$signature","took":10}"""
        return Base64.getEncoder().encodeToString(payload.toByteArray(Charsets.UTF_8))
    }

    suspend fun getVidukiSessionNonce(client: OkHttpClient, baseUrl: String): String? = withContext(Dispatchers.IO) {
        val cached = sessionNonceCache[baseUrl]
        if (cached != null && System.currentTimeMillis() - cached.second < 600_000L) {
            return@withContext cached.first
        }
        try {
            val chReq = Request.Builder()
                .url("$baseUrl/altcha-challenge")
                .header("Referer", "https://www.viduki.net/")
                .header("Origin", "https://www.viduki.net")
                .header("User-Agent", USER_AGENT)
                .build()
            val chBody = client.newCall(chReq).execute().use { it.body?.string() ?: "" }
            if (chBody.isBlank()) return@withContext null

            val chObj = json.parseToJsonElement(chBody).jsonObject
            val salt = chObj["salt"]?.jsonPrimitive?.content ?: return@withContext null
            val target = chObj["challenge"]?.jsonPrimitive?.content ?: return@withContext null
            val maxNum = chObj["maxnumber"]?.jsonPrimitive?.content?.toLongOrNull() ?: 50000L
            val algo = chObj["algorithm"]?.jsonPrimitive?.content ?: "SHA-256"
            val sig = chObj["signature"]?.jsonPrimitive?.content ?: ""

            val altcha = solveAltcha(salt, target, maxNum, algo, sig)

            val bootReq = Request.Builder()
                .url("$baseUrl/bootstrap")
                .header("Referer", "https://www.viduki.net/")
                .header("Origin", "https://www.viduki.net")
                .header("User-Agent", USER_AGENT)
                .header("X-Altcha", altcha)
                .header("Accept", "application/json")
                .build()
            val bootBody = client.newCall(bootReq).execute().use { it.body?.string() ?: "" }
            if (bootBody.isBlank()) return@withContext null

            val nonce = json.parseToJsonElement(bootBody).jsonObject["n"]?.jsonPrimitive?.content
            if (!nonce.isNullOrBlank()) {
                sessionNonceCache[baseUrl] = nonce to System.currentTimeMillis()
                nonce
            } else null
        } catch (_: Exception) {
            null
        }
    }

    private fun getOrLoadMakimaModule(client: OkHttpClient): Pair<WasmModule, MakimaExports>? {
        makimaModule?.let { mod ->
            makimaExports?.let { exp ->
                return mod to exp
            }
        }
        synchronized(this) {
            makimaModule?.let { mod ->
                makimaExports?.let { exp ->
                    return mod to exp
                }
            }

            try {
                val manReq = Request.Builder()
                    .url("https://www.viduki.net/makima-manifest.json")
                    .header("Referer", "https://www.viduki.net/")
                    .header("User-Agent", USER_AGENT)
                    .build()
                val manBody = client.newCall(manReq).execute().use { it.body?.string() ?: "" }
                val manObj = json.parseToJsonElement(manBody).jsonObject
                val wasmRelUrl = manObj["url"]?.jsonPrimitive?.content ?: "/makima.a0d5c2ffd2859979.wasm"
                val expObj = manObj["exports"]?.jsonObject ?: return null

                val exports = MakimaExports(
                    alloc = expObj["alloc"]?.jsonPrimitive?.content ?: "_gGry",
                    reset = expObj["reset"]?.jsonPrimitive?.content ?: "_kuez",
                    writeByte = expObj["writeByte"]?.jsonPrimitive?.content ?: "_0SGL",
                    readByte = expObj["readByte"]?.jsonPrimitive?.content ?: "_4vLu",
                    decryptPepper = expObj["decryptPepper"]?.jsonPrimitive?.content ?: "_iUeM",
                    decryptEnvelope = expObj["decryptEnvelope"]?.jsonPrimitive?.content ?: "_kGtw"
                )

                val wasmFullUrl = if (wasmRelUrl.startsWith("http")) wasmRelUrl else "https://www.viduki.net$wasmRelUrl"
                val wasmReq = Request.Builder()
                    .url(wasmFullUrl)
                    .header("Referer", "https://www.viduki.net/")
                    .header("User-Agent", USER_AGENT)
                    .build()
                val wasmBytes = client.newCall(wasmReq).execute().use { it.body!!.bytes() }
                val module = Parser.parse(ByteArrayInputStream(wasmBytes))

                makimaModule = module
                makimaExports = exports
                return module to exports
            } catch (_: Exception) {
                return null
            }
        }
    }

    /**
     * Decrypts an encrypted Viduki Main server payload into the decrypted JSON response.
     */
    suspend fun decryptVidukiMainStream(
        client: OkHttpClient,
        baseUrl: String,
        nonce: String,
        pathAndQuery: String
    ): String? = withContext(Dispatchers.IO) {
        try {
            val (module, exports) = getOrLoadMakimaModule(client) ?: return@withContext null

            val abortFn = HostFunction(
                "env",
                "abort",
                listOf(ValueType.I32, ValueType.I32, ValueType.I32, ValueType.I32),
                emptyList()
            ) { _, _ -> longArrayOf() }

            val instance = Instance.builder(module)
                .withImportValues(ImportValues.builder().withFunctions(listOf(abortFn)).build())
                .build()

            val alloc = instance.export(exports.alloc)
            val reset = instance.export(exports.reset)
            val writeByte = instance.export(exports.writeByte)
            val readByte = instance.export(exports.readByte)
            val decryptPepper = instance.export(exports.decryptPepper)
            val decryptEnvelope = instance.export(exports.decryptEnvelope)

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

            // Pepper handshake
            val chReq = Request.Builder()
                .url("$baseUrl/altcha-challenge")
                .header("Referer", "https://www.viduki.net/")
                .header("Origin", "https://www.viduki.net")
                .header("User-Agent", USER_AGENT)
                .build()
            val chBody = client.newCall(chReq).execute().use { it.body?.string() ?: "" }
            val chObj = json.parseToJsonElement(chBody).jsonObject
            val salt = chObj["salt"]?.jsonPrimitive?.content ?: return@withContext null
            val target = chObj["challenge"]?.jsonPrimitive?.content ?: return@withContext null
            val maxNum = chObj["maxnumber"]?.jsonPrimitive?.content?.toLongOrNull() ?: 50000L
            val algo = chObj["algorithm"]?.jsonPrimitive?.content ?: "SHA-256"
            val sig = chObj["signature"]?.jsonPrimitive?.content ?: ""

            val altcha = solveAltcha(salt, target, maxNum, algo, sig)

            val pepReq = Request.Builder()
                .url("$baseUrl/pepper-key")
                .header("Referer", "https://www.viduki.net/")
                .header("Origin", "https://www.viduki.net")
                .header("User-Agent", USER_AGENT)
                .header("X-Nonce", nonce)
                .header("X-Altcha", altcha)
                .header("Accept", "application/json")
                .build()
            val pepBody = client.newCall(pepReq).execute().use { it.body?.string() ?: "" }
            val pepObj = json.parseToJsonElement(pepBody).jsonObject
            val bucket = pepObj["bucket"]?.jsonPrimitive?.content?.toLongOrNull() ?: return@withContext null
            val pepIv = hexToBytes(pepObj["iv"]?.jsonPrimitive?.content ?: return@withContext null)
            val pepCt = hexToBytes(pepObj["ct"]?.jsonPrimitive?.content ?: return@withContext null)
            val pepTag = hexToBytes(pepObj["tag"]?.jsonPrimitive?.content ?: return@withContext null)

            reset.apply()
            val nonceBytes = hexToBytes(nonce)
            val bucketBuf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(bucket).array()

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

            if (pepRes <= 0) return@withContext null

            // Query server
            val random = SecureRandom()
            val cnBytes = ByteArray(16).also { random.nextBytes(it) }
            val reqIdBytes = ByteArray(16).also { random.nextBytes(it) }
            val clientNonceHex = cnBytes.joinToString("") { "%02x".format(it) }
            val reqIdHex = reqIdBytes.joinToString("") { "%02x".format(it) }

            val targetUrl = "$baseUrl$pathAndQuery"
            val streamReq = Request.Builder().url(targetUrl)
                .header("Referer", "https://www.viduki.net/")
                .header("Origin", "https://www.viduki.net")
                .header("User-Agent", USER_AGENT)
                .header("X-Nonce", nonce)
                .header("X-Client-Nonce", clientNonceHex)
                .header("X-Request-Id", reqIdHex)
                .header("Accept", "application/json")
                .build()
            val streamResp = client.newCall(streamReq).execute()
            val streamBody = streamResp.body?.string() ?: ""
            if (streamBody.isBlank()) return@withContext null

            val sObj = json.parseToJsonElement(streamBody).jsonObject
            if (sObj.containsKey("stream")) {
                return@withContext streamBody
            }

            val snHex = sObj["sn"]?.jsonPrimitive?.contentOrNull ?: return@withContext null
            val tb = sObj["tb"]?.jsonPrimitive?.contentOrNull?.toLongOrNull() ?: return@withContext null
            val iv1Hex = sObj["iv1"]?.jsonPrimitive?.contentOrNull ?: return@withContext null
            val iv2Hex = sObj["iv2"]?.jsonPrimitive?.contentOrNull ?: return@withContext null
            val tag1Hex = sObj["tag1"]?.jsonPrimitive?.contentOrNull ?: return@withContext null
            val tag2Hex = sObj["tag2"]?.jsonPrimitive?.contentOrNull ?: return@withContext null
            val wkHex = sObj["wk"]?.jsonPrimitive?.contentOrNull ?: return@withContext null
            val ctHex = sObj["ct"]?.jsonPrimitive?.contentOrNull ?: return@withContext null

            val snBytes = hexToBytes(snHex)
            val tbBuf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(tb).array()
            val iv1Bytes = hexToBytes(iv1Hex)
            val iv2Bytes = hexToBytes(iv2Hex)
            val tag1Bytes = hexToBytes(tag1Hex)
            val tag2Bytes = hexToBytes(tag2Hex)
            val wkBytes = hexToBytes(wkHex)
            val ctBytes = hexToBytes(ctHex)

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

            if (outLen <= 0) return@withContext null
            val decBytes = readBuf(pOut, outLen)
            String(decBytes, Charsets.UTF_8)
        } catch (_: Exception) {
            null
        }
    }

    /* ---------------------------------------------------------------------- */
    /* 4. Vidrock AES-256-GCM Native Stream Decryption                         */
    /* ---------------------------------------------------------------------- */

    private const val VIDROCK_KEY_HEX = "7f3e9c2a8b5d1f4e6a9c3b7d2e5f8a1c4b6d9e2f5a8c1b4d7e9f2a5c8b1d4e7f"

    fun decryptVidrockPayload(ciphertextB64Url: String): String? {
        return try {
            val clean = ciphertextB64Url.trim().replace('-', '+').replace('_', '/')
            val padded = clean.padEnd(4 * ((clean.length + 3) / 4), '=')
            val raw = Base64.getDecoder().decode(padded)
            if (raw.size <= 28) return null // 12 bytes IV + at least 1 byte ciphertext + 16 bytes tag

            val iv = raw.copyOfRange(0, 12)
            val cipherWithTag = raw.copyOfRange(12, raw.size)

            val keyBytes = hexToBytes(VIDROCK_KEY_HEX)
            val keySpec = SecretKeySpec(keyBytes, "AES")
            val gcmSpec = GCMParameterSpec(128, iv)
            val cipher = Cipher.getInstance("AES/GCM/NoPadding")
            cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec)
            val decryptedBytes = cipher.doFinal(cipherWithTag)
            String(decryptedBytes, Charsets.UTF_8)
        } catch (_: Exception) {
            null
        }
    }

    /* ---------------------------------------------------------------------- */
    /* 5. Vidy (wecollege) PRNG Stream Cipher Decryption                      */
    /* ---------------------------------------------------------------------- */

    private val VIDY_U = byteArrayOf(109, 118, 109, 49) // "mvm1"

    private fun vidyM(value: Int): Int {
        var e = value
        e = e xor (e ushr 16)
        e = (e.toLong() * 2246822507L).toInt()
        e = e xor (e ushr 13)
        e = (e.toLong() * 3266489909L).toInt()
        e = e xor (e ushr 16)
        return e
    }

    private fun vidyP(e: Int, t: Int): Int {
        return Integer.rotateLeft(e, t and 31)
    }

    fun decryptVidyPayload(ciphertextB64: String, seed: String, mediaIdStr: String): String? {
        return try {
            val clean = ciphertextB64.trim().replace('-', '+').replace('_', '/')
            val padded = clean.padEnd(4 * ((clean.length + 3) / 4), '=')
            val o = Base64.getDecoder().decode(padded)
            if (o.size < VIDY_U.size) return null

            val mediaIdNum = mediaIdStr.toLongOrNull()?.toInt() ?: 0
            var t1 = 2166136261L.toInt()
            for (i in seed.indices) {
                val code = seed[i].code
                t1 = ((t1 xor code).toLong() * 16777619L).toInt()
            }
            val t2 = vidyM(mediaIdNum xor 2654435769L.toInt())
            var s = vidyM(vidyM(t1) xor t2)

            val a = IntArray(61)
            val setIndices = BooleanArray(61)
            for (e in 0 until 8) {
                val t = ((s.toLong() and 0xFFFFFFFFL) % 61).toInt()
                s = vidyP((s.toLong() + 2654435769L).toInt(), 7 + (7 and e))
                a[t] = s xor vidyM(s)
                setIndices[t] = true
                s = vidyM(s + t)
            }

            var acc = vidyM(2779096485L.toInt() xor s)

            val n = ByteArray(o.size)
            var step = 0
            var byteIdx = 0
            while (byteIdx < o.size) {
                val d = ((acc.toLong() and 0xFFFFFFFFL) % 61).toInt()
                val r = if (setIndices[d]) -1 else 0
                val iVal = a[d]

                val term = iVal xor ((2654435769L * (step + 1)).toInt())
                val lVal = (acc xor term) or (acc and term and r)

                val rot1 = vidyP(lVal + acc, 31 and d)
                val rot2 = vidyP(acc, 31 and (d * 7))
                val newAcc = vidyM((rot1 xor rot2) + 2654435769L.toInt())

                a[d] = newAcc
                setIndices[d] = true
                acc = newAcc
                val tGen = newAcc

                step++

                n[byteIdx++] = (tGen and 0xFF).toByte()
                if (byteIdx < o.size) n[byteIdx++] = ((tGen ushr 8) and 0xFF).toByte()
                if (byteIdx < o.size) n[byteIdx++] = ((tGen ushr 16) and 0xFF).toByte()
                if (byteIdx < o.size) n[byteIdx++] = ((tGen ushr 24) and 0xFF).toByte()
            }

            for (i in o.indices) {
                o[i] = (o[i].toInt() xor n[i].toInt()).toByte()
            }

            for (i in VIDY_U.indices) {
                if (o[i] != VIDY_U[i]) {
                    return null
                }
            }

            String(o, VIDY_U.size, o.size - VIDY_U.size, Charsets.UTF_8)
        } catch (_: Exception) {
            null
        }
    }

    fun prewarm(client: OkHttpClient) {
        try {
            getOrLoadModule(client)
        } catch (_: Exception) {}
    }
}
