package com.euthopiar.core.provider

import com.dylibso.chicory.runtime.Instance
import com.dylibso.chicory.wasm.Parser
import com.dylibso.chicory.wasm.WasmModule
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.ByteArrayInputStream
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * Headless WebAssembly cryptographic engine for Cinejoy (wing.st lumen-gate-v2 protocol).
 *
 * Executes the `crush.wasm` module using the zero-dependency pure-Java Chicory WebAssembly runtime,
 * encrypts requests via `seal_request`, submits encrypted octet-streams to `https://api.wing.st/g`,
 * and decrypts the resulting AES-256-GCM response payloads.
 */
object CinejoyWasmEngine {

    private const val WASM_URL = "https://api.wing.st/crush.wasm"
    private const val GATEWAY_URL = "https://api.wing.st/g"
    private const val REFERER = "https://cinejoy.pk/"
    private const val ORIGIN = "https://cinejoy.pk"
    private const val USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    @Volatile
    private var parsedModule: WasmModule? = null
    private val random = SecureRandom()

    private fun getOrLoadModule(client: OkHttpClient): WasmModule {
        parsedModule?.let { return it }
        synchronized(this) {
            parsedModule?.let { return it }

            // 1. Try classpath resource
            val resourceStream = CinejoyWasmEngine::class.java.classLoader?.getResourceAsStream("crush.wasm")
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

    suspend fun requestStream(
        client: OkHttpClient,
        server: String,
        type: String, // "movie" or "tv"
        tmdbId: String,
        season: Int? = null,
        episode: Int? = null
    ): String? = withContext(Dispatchers.IO) {
        try {
            val module = getOrLoadModule(client)
            val instance = Instance.builder(module).build()

            val alloc = instance.export("alloc")
            val dealloc = instance.export("dealloc")
            val sealRequest = instance.export("seal_request")
            val memory = instance.memory()

            // Prepare JSON payload: e.g. {"path":"/Nebula/movie","payload":{"tmdb":"1084244"}}
            val endpointType = if (type == "tv") "series" else type
            val path = "/$server/$endpointType"
            val jsonPayload = if (season != null && episode != null) {
                """{"path":"$path","payload":{"tmdb":"$tmdbId","season":"$season","episode":"$episode"}}"""
            } else {
                """{"path":"$path","payload":{"tmdb":"$tmdbId"}}"""
            }

            val plaintextBytes = jsonPayload.toByteArray(Charsets.UTF_8)
            val randomBytes = ByteArray(44)
            random.nextBytes(randomBytes)

            val maxLen = plaintextBytes.size + 512
            val t = alloc.apply(plaintextBytes.size.toLong())[0].toInt()
            val m = alloc.apply(randomBytes.size.toLong())[0].toInt()
            val n = alloc.apply(maxLen.toLong())[0].toInt()

            memory.write(t, plaintextBytes)
            memory.write(m, randomBytes)

            val outLen = sealRequest.apply(
                t.toLong(),
                plaintextBytes.size.toLong(),
                m.toLong(),
                randomBytes.size.toLong(),
                n.toLong(),
                maxLen.toLong()
            )[0].toInt()

            if (outLen <= 98) {
                dealloc.apply(t.toLong(), plaintextBytes.size.toLong())
                dealloc.apply(m.toLong(), randomBytes.size.toLong())
                dealloc.apply(n.toLong(), maxLen.toLong())
                return@withContext null
            }

            val output = memory.readBytes(n, outLen)

            dealloc.apply(t.toLong(), plaintextBytes.size.toLong())
            dealloc.apply(m.toLong(), randomBytes.size.toLong())
            dealloc.apply(n.toLong(), maxLen.toLong())

            val responseKey = output.copyOfRange(0, 32)
            val keyId = output[32]
            val ephemeralPublic = output.copyOfRange(33, 98)
            val body = output.copyOfRange(98, outLen)

            // POST to gateway
            val mediaType = "application/octet-stream".toMediaType()
            val requestBody = body.toRequestBody(mediaType)
            val request = Request.Builder()
                .url(GATEWAY_URL)
                .header("Referer", REFERER)
                .header("Origin", ORIGIN)
                .header("User-Agent", USER_AGENT)
                .post(requestBody)
                .build()

            val response = client.newCall(request).execute()
            if (!response.isSuccessful) {
                return@withContext null
            }

            val encryptedResp = response.body?.bytes() ?: return@withContext null
            if (encryptedResp.size < 28) { // 12 IV + 16 Tag minimum
                return@withContext null
            }

            // Decrypt AES-256-GCM
            val iv = encryptedResp.copyOfRange(0, 12)
            val ciphertextWithTag = encryptedResp.copyOfRange(12, encryptedResp.size)

            // AAD: "lumen-gate-v2" (13 bytes) + 0x00, 0x02, keyId + ephemeralPublic (65 bytes)
            val prefix = "lumen-gate-v2".toByteArray(Charsets.UTF_8)
            val aad = ByteArray(prefix.size + 3 + ephemeralPublic.size)
            System.arraycopy(prefix, 0, aad, 0, prefix.size)
            aad[prefix.size] = 0
            aad[prefix.size + 1] = 2
            aad[prefix.size + 2] = keyId
            System.arraycopy(ephemeralPublic, 0, aad, prefix.size + 3, ephemeralPublic.size)

            val cipher = Cipher.getInstance("AES/GCM/NoPadding")
            val keySpec = SecretKeySpec(responseKey, "AES")
            val gcmSpec = GCMParameterSpec(128, iv)
            cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec)
            cipher.updateAAD(aad)

            val decryptedBytes = cipher.doFinal(ciphertextWithTag)
            String(decryptedBytes, Charsets.UTF_8)
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
