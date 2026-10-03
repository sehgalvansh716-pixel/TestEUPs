package com.euthopiar.core.provider

import android.annotation.SuppressLint
import android.app.Application
import android.os.Handler
import android.os.Looper
import android.webkit.CookieManager
import android.webkit.JavascriptInterface
import android.webkit.WebView
import android.webkit.WebViewClient
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.json.JSONObject

/**
 * Runs Atlantic's own production signing module inside a hidden WebView whose base URL is
 * https://atlantic.st/. The module performs the (rotating-key) handshake and solves the Cloudflare
 * Turnstile gate exactly as the website does, then fetches the cdn.hls.lol content JSON.
 * Session state lives inside the WebView's JS module, so Turnstile is only solved once per ~hour.
 */
internal object AtlanticWebSigner {
    private val mutex = Mutex()
    private var webView: WebView? = null
    private var loadedChunk: String? = null
    private var pageReady: CompletableDeferred<Boolean>? = null
    private val pending = HashMap<String, CompletableDeferred<String?>>()
    private val main = Handler(Looper.getMainLooper())

    private fun appContext(): Application? = try {
        Class.forName("android.app.ActivityThread")
            .getMethod("currentApplication").invoke(null) as? Application
    } catch (_: Exception) { null }

    private class Bridge {
        @JavascriptInterface
        fun ready() { pageReady?.complete(true) }

        @JavascriptInterface
        fun result(id: String, json: String?) {
            synchronized(pending) { pending.remove(id) }?.complete(json)
        }
    }

    private fun pageHtml(chunkPath: String) = """
        <!doctype html><html><head><meta charset="utf-8">
        <script src="https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit"></script>
        </head><body><div id="cf"></div>
        <script type="module">
        let mod = null;
        async function load() {
          if (!mod) {
            mod = await import('https://atlantic.st$chunkPath');
            if (mod && mod.w) {
              try { mod.w('https://cdn.hls.lol'); } catch(e) {}
            }
          }
          return mod;
        }
        window.run = async function(id, path) {
          try {
            const m = await load();
            for (let attempt = 0; attempt < 3; attempt++) {
              const h = await m.s('https://cdn.hls.lol', path);
              if (!h) {
                await new Promise(r => setTimeout(r, 800));
                continue;
              }
              const r = await fetch('https://cdn.hls.lol' + path, { headers: h });
              if (!r.ok) { m.i(); continue; }
              const j = await r.json();
              if (j && j.renew) { m.i(); continue; }
              Android.result(id, JSON.stringify(j));
              return;
            }
            Android.result(id, null);
          } catch (e) { Android.result(id, null); }
        };
        load().then(() => Android.ready()).catch(() => Android.ready());
        </script></body></html>
    """.trimIndent()

    @SuppressLint("SetJavaScriptEnabled")
    private suspend fun ensureWebView(chunkPath: String): Boolean = withContext(Dispatchers.Main) {
        val ctx = appContext() ?: return@withContext false
        if (webView != null && loadedChunk == chunkPath) return@withContext true
        webView?.destroy()
        val ready = CompletableDeferred<Boolean>()
        pageReady = ready
        val wv = WebView(ctx)
        val chromeUa = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        wv.settings.userAgentString = chromeUa
        wv.settings.javaScriptEnabled = true
        wv.settings.domStorageEnabled = true
        wv.settings.databaseEnabled = true
        wv.settings.mediaPlaybackRequiresUserGesture = false
        try {
            val cookieManager = CookieManager.getInstance()
            cookieManager.setAcceptCookie(true)
            cookieManager.setAcceptThirdPartyCookies(wv, true)
        } catch (_: Throwable) {}
        wv.addJavascriptInterface(Bridge(), "Android")
        wv.webViewClient = WebViewClient()
        wv.loadDataWithBaseURL("https://atlantic.st/", pageHtml(chunkPath), "text/html", "utf-8", null)
        webView = wv
        loadedChunk = chunkPath
        withTimeoutOrNull(20_000) { ready.await() } == true
    }

    /** Returns the raw content JSON from cdn.hls.lol for [path], or null on any failure. */
    suspend fun fetchContent(chunkPath: String, path: String): String? = mutex.withLock {
        try {
            if (!ensureWebView(chunkPath)) return@withLock null
            val id = System.nanoTime().toString()
            val deferred = CompletableDeferred<String?>()
            synchronized(pending) { pending[id] = deferred }
            withContext(Dispatchers.Main) {
                webView?.evaluateJavascript("window.run(${JSONObject.quote(id)}, ${JSONObject.quote(path)});", null)
            }
            val out = withTimeoutOrNull(40_000) { deferred.await() }
            synchronized(pending) { pending.remove(id) }
            out
        } catch (_: Exception) {
            null
        }
    }
}
