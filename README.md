# Vansh EUPs v2 Official Architecture Repository

Official decoupled Universal Plugin (EUP v2) distribution repository for **Euthopiar**.

## 🚀 How to Add to Euthopiar
In Euthopiar, open **Settings -> Extensions / Plugins -> Repository Manager -> Add Repository**, and paste:

```
https://raw.githubusercontent.com/sehgalvansh716-pixel/VanshEUPs-V2-Architecture/main/plugins.json
```

---

## 📦 Available Plugins (15)

### 🎬 Main Streaming & Catalog Providers (9)
| Plugin | Version | Supported Types | Capabilities |
| :--- | :---: | :---: | :--- |
| **Atlantic** | v1.1.0 | Movies, TV, Anime | Helios Moscow, Aphrodite CDN, Natsuki/Granite Subs, Multi-Audio |
| **Cinejoy** | v1.1.0 | Movies, TV | Lisbon & Nebula Wasm, Helios, Aphrodite, Direct HLS |
| **Aether** | v1.1.0 | Movies, TV | UpCloud, VidCloud, Multi-Server, 1080p FHD |
| **1Shows** | v1.1.0 | Movies, TV | Fast multi-mirror streaming, 1080p, Subtitles |
| **4KHDHub** | v1.1.0 | Movies, TV | 4K UHD, 1080p, HubCloud, DriveSeed, 10Gbps CDN Downloads |
| **UHDMovies** | v1.1.0 | Movies, TV | 4K UHD, HDR, 10Bit HEVC, DriveSeed, Direct Downloads |
| **MoviesLeech** | v1.1.0 | Movies, TV | Bollywood, Indian Cinema, South Indian, Hollywood, Google CDN |
| **Movy** | v1.1.0 | Movies, TV | PRNG decrypted mirrors, 4K/1080p HLS, Multi-dub audio |
| **PvrPlay** | v1.1.0 | Movies, TV | Regional, South Indian & Hindi cinema streaming |

### 🔒 Vault / Private Space Providers (6)
*Private Space Isolation: Vault plugins are federated into the secure Private Space and never write to history, TMDB, AniSkip, or external scrobblers.*

| Plugin | Version | Supported Types | Capabilities |
| :--- | :---: | :---: | :--- |
| **HQPorner** | v1.1.0 | Vault Video | 1080p/720p Full HD direct MP4 streams and downloads |
| **XVideos** | v1.1.0 | Vault Video | Multi-resolution HLS & direct MP4 streams (1080p, 720p, 480p) |
| **EPorner** | v1.1.0 | Vault Video | 4K UHD & 1080p 60fps streams and direct downloads |
| **Rule34Video**| v1.1.0 | Vault Video | Animated & stylized media streaming across quality tiers |
| **Beeg** | v1.1.0 | Vault Video | Native REST API 1080p/720p direct seekable MP4 streams |
| **Faphouse** | v1.1.0 | Vault Video | Direct MP4 streams and trailer resolution |

---

## 🏛️ EUP v2 Architecture Highlights
- **Zero Android Framework Dependencies**: Compiled against pure JVM contracts (`:eup-api`).
- **Resilient Token Single-Flighting**: `TokenStore` mutex prevents CDN token rotation race conditions.
- **Virtual URIs (`eup://`)**: Demuxed HLS audio track resolution and offline download stability.
- **TMDB Match Candidate Scoring**: Strict Jaro-Winkler + Year check prevents false positive mismatches.
- **AniSkip Integration**: Community skip intro (`OP`) and outro (`ED`) timestamps for anime.
- **OpenSubtitles Integration**: Dynamic external subtitle synchronization via Cinemeta IMDb matching.
