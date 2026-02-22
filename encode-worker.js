/**
 * Encoding worker for Cloudflare D1 encoding_jobs queue.
 *
 * Flow:
 * 1) Claim queued job from /api/admin/encoding-jobs/claim
 * 2) Download source_url to temp folder
 * 3) Transcode to multi-bitrate HLS
 * 4) Upload HLS output to Backblaze B2
 * 5) Mark job complete (or failed) via API
 */

const { execSync, spawn } = require("child_process")
const fs = require("fs")
const os = require("os")
const path = require("path")
const { Readable } = require("stream")
const { pipeline } = require("stream/promises")
const { S3Client, PutObjectCommand, GetObjectCommand } = require("@aws-sdk/client-s3")
require("dotenv").config()

const DEFAULT_QUALITIES = [
    { name: "360p", width: 640, height: 360, bitrate: "600k", maxrate: "750k", bufsize: "1200k", audioBitrate: "96k" },
    { name: "540p", width: 960, height: 540, bitrate: "1200k", maxrate: "1500k", bufsize: "2400k", audioBitrate: "128k" },
    { name: "720p", width: 1280, height: 720, bitrate: "2200k", maxrate: "2800k", bufsize: "4200k", audioBitrate: "128k" },
]
const MAX_SUBTITLE_TRACKS = 10
const AUTO_SUBTITLE_LANG_TOKEN = "auto"
const SUBTITLE_ONLY_QUALITY_TOKEN = "__subtitle_only__"

function normalizeOptionalString(value) {
    if (typeof value !== "string") {
        if (value == null) return null
        const parsed = String(value).trim()
        return parsed.length > 0 ? parsed : null
    }
    const trimmed = value.trim()
    return trimmed.length > 0 ? trimmed : null
}

function parseDbBoolean(value) {
    if (typeof value === "boolean") return value
    if (typeof value === "number") return value === 1
    if (typeof value === "string") {
        const normalized = value.trim().toLowerCase()
        return normalized === "1" || normalized === "true" || normalized === "yes"
    }
    return false
}

function parseEnvBoolean(value, fallbackValue = false) {
    if (value === undefined || value === null || value === "") return fallbackValue
    return parseDbBoolean(value)
}

function normalizeSubtitleLanguage(value) {
    const normalized = normalizeOptionalString(value)?.toLowerCase().replace(/_/g, "-") || "und"
    const safe = normalized.replace(/[^a-z0-9-]+/g, "")
    if (!safe) return "und"
    return safe.length > 16 ? safe.slice(0, 16) : safe
}

function normalizeSubtitleKind(value) {
    const normalized = normalizeOptionalString(value)?.toLowerCase()
    if (normalized === "captions" || normalized === "caption" || normalized === "cc") {
        return "captions"
    }
    return "subtitles"
}

function parseSubtitleMode(value) {
    const normalized = normalizeOptionalString(value)?.toLowerCase()
    if (normalized === "off" || normalized === "manual" || normalized === "auto" || normalized === "hybrid") {
        return normalized
    }
    return "manual"
}

function parseSubtitleLanguages(value) {
    const raw = normalizeOptionalString(value)
    if (!raw) return []
    const parsed = []
    const seen = new Set()
    const parts = raw
        .split(",")
        .map((part) => part.trim())
        .filter((part) => part.length > 0)
        .slice(0, MAX_SUBTITLE_TRACKS)

    for (const part of parts) {
        const normalized = part.toLowerCase().replace(/_/g, "-")
        const language = normalized === "detect" ? AUTO_SUBTITLE_LANG_TOKEN : normalizeSubtitleLanguage(normalized)
        if (seen.has(language)) continue
        seen.add(language)
        parsed.push(language)
    }

    // Keep detection stable: if auto-detect is configured, ignore additional forced languages.
    if (parsed.includes(AUTO_SUBTITLE_LANG_TOKEN)) {
        return [AUTO_SUBTITLE_LANG_TOKEN]
    }
    return parsed
}

function sanitizeFileToken(value, fallbackValue) {
    const normalized = normalizeOptionalString(value)?.replace(/[^a-zA-Z0-9_-]+/g, "_") || ""
    const compact = normalized.replace(/_+/g, "_").replace(/^_+|_+$/g, "")
    if (!compact) return fallbackValue
    return compact.slice(0, 32)
}

function normalizeSubtitleTrack(rawTrack, index) {
    if (!rawTrack || typeof rawTrack !== "object" || Array.isArray(rawTrack)) return null
    const track = rawTrack
    const language = normalizeSubtitleLanguage(
        track.lang ?? track.language ?? track.code ?? `track-${index + 1}`,
    )
    const url = normalizeOptionalString(
        track.url
        ?? track.src
        ?? track.file_url
        ?? track.fileUrl
        ?? track.media_url
        ?? track.mediaUrl
        ?? track.href,
    )
    if (!url) return null

    return {
        lang: language,
        label: normalizeOptionalString(track.label ?? track.name ?? track.title)?.slice(0, 64) || language.toUpperCase(),
        url,
        kind: normalizeSubtitleKind(track.kind),
        default: parseDbBoolean(track.default ?? track.is_default ?? track.isDefault),
    }
}

function dedupeSubtitleTracks(tracks) {
    const deduplicated = new Map()
    for (const track of tracks) {
        const normalizedTrack = {
            lang: normalizeSubtitleLanguage(track.lang),
            label: normalizeOptionalString(track.label)?.slice(0, 64) || normalizeSubtitleLanguage(track.lang).toUpperCase(),
            url: normalizeOptionalString(track.url) || "",
            kind: normalizeSubtitleKind(track.kind),
            default: !!track.default,
        }
        if (!normalizedTrack.url) continue
        const dedupeKey = `${normalizedTrack.lang}|${normalizedTrack.kind}`
        const existing = deduplicated.get(dedupeKey)
        if (!existing || (!existing.default && normalizedTrack.default)) {
            deduplicated.set(dedupeKey, normalizedTrack)
        }
    }
    return Array.from(deduplicated.values())
        .sort((a, b) => {
            if (a.default !== b.default) return a.default ? -1 : 1
            return a.label.localeCompare(b.label)
        })
        .slice(0, MAX_SUBTITLE_TRACKS)
}

function ensureDefaultSubtitleTrack(tracks) {
    if (!Array.isArray(tracks) || tracks.length === 0) return []
    if (tracks.some((track) => !!track.default)) return tracks
    return tracks.map((track, index) => ({ ...track, default: index === 0 }))
}

function mergeSubtitleTrackSets(manualTracks, autoTracks) {
    const merged = []
    const seen = new Set()

    for (const track of manualTracks) {
        const key = `${normalizeSubtitleLanguage(track.lang)}|${normalizeSubtitleKind(track.kind)}`
        if (seen.has(key)) continue
        seen.add(key)
        merged.push(track)
    }
    for (const track of autoTracks) {
        const key = `${normalizeSubtitleLanguage(track.lang)}|${normalizeSubtitleKind(track.kind)}`
        if (seen.has(key)) continue
        seen.add(key)
        merged.push(track)
    }

    return ensureDefaultSubtitleTrack(dedupeSubtitleTracks(merged))
}

function extractSubtitleTracksFromJob(job) {
    const candidates = []
    if (Array.isArray(job?.subtitle_tracks)) {
        candidates.push(...job.subtitle_tracks)
    }

    const contentMetadata = job?.content_metadata && typeof job.content_metadata === "object" && !Array.isArray(job.content_metadata)
        ? job.content_metadata
        : {}
    const metadataCandidates = [
        contentMetadata.subtitle_tracks,
        contentMetadata.subtitles,
        contentMetadata?.encoding?.subtitle_tracks,
        contentMetadata?.playback?.subtitles,
    ]
    for (const value of metadataCandidates) {
        if (Array.isArray(value)) {
            candidates.push(...value)
        }
    }

    const normalizedTracks = []
    candidates.forEach((entry, index) => {
        const normalized = normalizeSubtitleTrack(entry, index)
        if (normalized) normalizedTracks.push(normalized)
    })
    return dedupeSubtitleTracks(normalizedTracks)
}

function getContentMetadataFromJob(job) {
    return job?.content_metadata && typeof job.content_metadata === "object" && !Array.isArray(job.content_metadata)
        ? job.content_metadata
        : {}
}

function extractQualityUrlsFromMetadata(contentMetadata) {
    const qualityUrls = {}

    const collectFromRecord = (recordValue) => {
        if (!recordValue || typeof recordValue !== "object" || Array.isArray(recordValue)) return
        for (const [label, urlRaw] of Object.entries(recordValue)) {
            const normalizedLabel = normalizeQualityName(label)
            const url = normalizeOptionalString(urlRaw)
            if (!normalizedLabel || normalizedLabel === "auto" || !url) continue
            if (!qualityUrls[normalizedLabel]) {
                qualityUrls[normalizedLabel] = url
            }
        }
    }

    collectFromRecord(contentMetadata.quality_urls)
    collectFromRecord(contentMetadata?.playback?.quality_urls)
    collectFromRecord(contentMetadata?.encoding?.quality_urls)
    return qualityUrls
}

function resolveExistingMasterUrl(job) {
    const contentMetadata = getContentMetadataFromJob(job)
    const candidates = [
        contentMetadata.output_master_url,
        contentMetadata.master_url,
        contentMetadata?.encoding?.output_master_url,
        contentMetadata?.encoding?.master_url,
        contentMetadata?.playback?.master_url,
    ]
    for (const candidate of candidates) {
        const parsed = normalizeOptionalString(candidate)
        if (parsed) return parsed
    }
    return null
}

function deriveRemotePrefixFromMasterUrl(masterUrl) {
    const normalized = normalizeOptionalString(masterUrl)
    if (!normalized) return null

    const parsePathname = (pathname) => {
        const withoutMaster = pathname.replace(/\/master\.m3u8$/i, "")
        const trimmed = withoutMaster.replace(/^\/+/, "").replace(/\/+$/, "")
        return trimmed || null
    }

    try {
        const parsed = new URL(normalized)
        return parsePathname(parsed.pathname)
    } catch {
        const withoutQuery = normalized.split("?")[0].split("#")[0]
        const pathname = /^[a-z]+:\/\//i.test(withoutQuery)
            ? withoutQuery.replace(/^[a-z]+:\/\/[^/]+/i, "")
            : withoutQuery
        return parsePathname(pathname)
    }
}

function isSubtitleOnlyRequestedQualities(requestedQualities) {
    if (!Array.isArray(requestedQualities)) return false
    return requestedQualities.some((item) => normalizeQualityName(item) === SUBTITLE_ONLY_QUALITY_TOKEN)
}

function isSubtitleOnlyJob(job) {
    return isSubtitleOnlyRequestedQualities(job?.requested_qualities)
}

function inferSubtitleExtension(url) {
    try {
        const parsed = new URL(url)
        const pathname = parsed.pathname.toLowerCase()
        if (pathname.endsWith(".srt")) return ".srt"
        return ".vtt"
    } catch {
        const normalized = String(url || "").toLowerCase().split("?")[0].split("#")[0]
        if (normalized.endsWith(".srt")) return ".srt"
        return ".vtt"
    }
}

function srtToVtt(input) {
    const normalizedInput = String(input || "").replace(/^\uFEFF/, "").replace(/\r\n/g, "\n").replace(/\r/g, "\n")
    const converted = normalizedInput.replace(
        /(\d{2}:\d{2}:\d{2}),(\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}),(\d{3})/g,
        "$1.$2 --> $3.$4",
    )
    return `WEBVTT\n\n${converted}`
}

function parsePositiveInt(value, fallbackValue) {
    const parsed = Number.parseInt(String(value || ""), 10)
    if (!Number.isFinite(parsed) || parsed <= 0) return fallbackValue
    return parsed
}

function normalizeQualityName(value) {
    return String(value || "").trim().toLowerCase()
}

function sanitizeQuality(rawQuality, index) {
    const quality = rawQuality && typeof rawQuality === "object" ? rawQuality : {}
    const name = String(quality.name || `q${index + 1}`).trim()
    const width = parsePositiveInt(quality.width, 0)
    const height = parsePositiveInt(quality.height, 0)
    const bitrate = String(quality.bitrate || "").trim()
    if (!name || width <= 0 || height <= 0 || !bitrate) return null
    return {
        name,
        width,
        height,
        bitrate,
        maxrate: String(quality.maxrate || bitrate),
        bufsize: String(quality.bufsize || quality.maxrate || bitrate),
        audioBitrate: String(quality.audioBitrate || "96k"),
    }
}

function sortQualitiesAscending(qualities) {
    return [...qualities].sort((a, b) => {
        if (a.height !== b.height) return a.height - b.height
        return a.width - b.width
    })
}

function parseQualities() {
    const raw = process.env.ENCODER_QUALITIES_JSON
    if (!raw) return DEFAULT_QUALITIES
    try {
        const parsed = JSON.parse(raw)
        if (Array.isArray(parsed) && parsed.length > 0) {
            const sanitized = parsed
                .map((item, index) => sanitizeQuality(item, index))
                .filter((item) => !!item)
            if (sanitized.length > 0) {
                return sortQualitiesAscending(sanitized)
            }
        }
    } catch (error) {
        log(`Invalid ENCODER_QUALITIES_JSON, fallback default: ${error.message}`, "warn")
    }
    return DEFAULT_QUALITIES
}

function parseB2RegionFromEndpoint(endpoint) {
    try {
        const host = new URL(endpoint).hostname
        const match = host.match(/^s3\.([a-z0-9-]+)\./i)
        if (match && match[1]) return match[1]
    } catch {}
    return "us-west-004"
}

const CONFIG = {
    adminApiBaseUrl: process.env.ADMIN_API_BASE_URL || process.env.CLOUDFLARE_API_URL || "https://ruwaq-jawi-api.ruwaqjawi.workers.dev",
    adminApiKey: process.env.ADMIN_API_KEY || process.env.CLOUDFLARE_ADMIN_API_KEY,
    allowDevNoKey:
        (process.env.ALLOW_DEV_ADMIN_NO_KEY || process.env.ADMIN_ALLOW_NO_KEY || "").toLowerCase() === "true",
    workerId: process.env.ENCODER_WORKER_ID || `encoder-${process.pid}`,
    pollIntervalMs: parsePositiveInt(process.env.ENCODER_POLL_INTERVAL_MS, 10000),
    tempDir: process.env.ENCODER_TEMP_DIR || path.join(os.tmpdir(), "ruwaq-encoder"),
    cdnBaseUrl: (process.env.CDN_BASE_URL || "https://videos.mujam.store").replace(/\/+$/, ""),
    segmentDurationSeconds: parsePositiveInt(process.env.ENCODER_SEGMENT_DURATION_SECONDS, 2),
    uploadConcurrency: parsePositiveInt(process.env.ENCODER_UPLOAD_CONCURRENCY, 4),
    ffmpegPreset: process.env.ENCODER_FFMPEG_PRESET || "veryfast",
    qualities: sortQualitiesAscending(parseQualities()),
    subtitles: {
        mode: parseSubtitleMode(process.env.SUBTITLE_MODE),
        languages: parseSubtitleLanguages(process.env.SUBTITLE_LANGS),
        model: normalizeOptionalString(process.env.SUBTITLE_MODEL) || "small",
        device: normalizeOptionalString(process.env.SUBTITLE_DEVICE) || "cpu",
        computeType: normalizeOptionalString(process.env.SUBTITLE_COMPUTE_TYPE) || "int8",
        beamSize: parsePositiveInt(process.env.SUBTITLE_BEAM_SIZE, 5),
        pythonBin: normalizeOptionalString(process.env.SUBTITLE_PYTHON_BIN) || "python3",
        scriptPath: path.resolve(
            normalizeOptionalString(process.env.SUBTITLE_SCRIPT_PATH) || path.join(__dirname, "generate-subtitles.py"),
        ),
        required: parseEnvBoolean(process.env.SUBTITLE_REQUIRED, false),
    },
    b2: {
        endpoint: process.env.B2_ENDPOINT,
        region: process.env.B2_REGION || parseB2RegionFromEndpoint(process.env.B2_ENDPOINT || ""),
        keyId: process.env.B2_KEY_ID,
        applicationKey: process.env.B2_APPLICATION_KEY,
        bucketName: process.env.B2_BUCKET_NAME || "ruwaq-videos",
        rawBucketName: process.env.B2_RAW_BUCKET_NAME || "ruwaq-video-raw",
    },
}

if (!CONFIG.adminApiKey && !CONFIG.allowDevNoKey) {
    throw new Error("Missing ADMIN_API_KEY or CLOUDFLARE_ADMIN_API_KEY")
}
if (!CONFIG.b2.endpoint || !CONFIG.b2.keyId || !CONFIG.b2.applicationKey) {
    throw new Error("Missing B2_ENDPOINT/B2_KEY_ID/B2_APPLICATION_KEY")
}

if (
    (CONFIG.subtitles.mode === "auto" || CONFIG.subtitles.mode === "hybrid")
    && !fs.existsSync(CONFIG.subtitles.scriptPath)
) {
    throw new Error(
        `Missing subtitle generation script at ${CONFIG.subtitles.scriptPath}. Set SUBTITLE_SCRIPT_PATH or disable auto subtitles.`,
    )
}

const s3Client = new S3Client({
    endpoint: CONFIG.b2.endpoint,
    region: CONFIG.b2.region,
    credentials: {
        accessKeyId: CONFIG.b2.keyId,
        secretAccessKey: CONFIG.b2.applicationKey,
    },
})

let shouldStop = false

function log(message, level = "info") {
    const ts = new Date().toISOString()
    const prefix = level.toUpperCase().padEnd(5)
    console.log(`[${ts}] [${prefix}] ${message}`)
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms))
}

function ensureDir(dirPath) {
    if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true })
    }
}

function cleanDir(dirPath) {
    if (fs.existsSync(dirPath)) {
        fs.rmSync(dirPath, { recursive: true, force: true })
    }
}

async function apiRequest(method, endpoint, body) {
    const headers = {
        "Content-Type": "application/json",
    }
    if (CONFIG.adminApiKey) {
        headers["X-API-Key"] = CONFIG.adminApiKey
    }

    const response = await fetch(`${CONFIG.adminApiBaseUrl}${endpoint}`, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
    })
    const text = await response.text()
    let payload = {}
    if (text) {
        try {
            payload = JSON.parse(text)
        } catch {
            payload = {}
        }
    }
    if (!response.ok) {
        const message = payload.error || payload.message || `HTTP ${response.status}`
        throw new Error(`API ${method} ${endpoint} failed: ${message}`)
    }
    return payload
}

async function claimJob() {
    const payload = await apiRequest("POST", "/api/admin/encoding-jobs/claim", {
        worker_id: CONFIG.workerId,
    })
    return payload.data || null
}

async function markJobFailed(jobId, errorMessage) {
    await apiRequest("POST", `/api/admin/encoding-jobs/${jobId}/fail`, {
        error_message: errorMessage,
    })
}

async function markJobComplete(jobId, result) {
    await apiRequest("POST", `/api/admin/encoding-jobs/${jobId}/complete`, result)
}

async function markJobProgress(jobId, progress) {
    try {
        await apiRequest("POST", `/api/admin/encoding-jobs/${jobId}/progress`, progress)
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        log(`Progress update failed for job ${jobId}: ${message}`, "warn")
    }
}

async function downloadFile(url, destinationPath) {
    const response = await fetch(url)
    if (!response.ok || !response.body) {
        throw new Error(`Download failed (${response.status}) for ${url}`)
    }
    ensureDir(path.dirname(destinationPath))
    await pipeline(Readable.fromWeb(response.body), fs.createWriteStream(destinationPath))
}

async function downloadFromRawBucket(storagePath, destinationPath) {
    const result = await s3Client.send(
        new GetObjectCommand({
            Bucket: CONFIG.b2.rawBucketName,
            Key: storagePath,
        }),
    )
    if (!result.Body) {
        throw new Error(`Raw bucket download returned empty body for ${storagePath}`)
    }

    ensureDir(path.dirname(destinationPath))
    if (typeof result.Body.pipe === "function") {
        await pipeline(result.Body, fs.createWriteStream(destinationPath))
        return
    }
    if (typeof result.Body.transformToWebStream === "function") {
        await pipeline(Readable.fromWeb(result.Body.transformToWebStream()), fs.createWriteStream(destinationPath))
        return
    }
    if (typeof result.Body.transformToByteArray === "function") {
        const bytes = await result.Body.transformToByteArray()
        fs.writeFileSync(destinationPath, Buffer.from(bytes))
        return
    }
    throw new Error("Unsupported S3 response body type")
}

async function downloadSourceForJob(job, destinationPath) {
    const sourceUrl = job.source_url
    if (sourceUrl) {
        try {
            await downloadFile(sourceUrl, destinationPath)
            return
        } catch (error) {
            log(`Direct source URL download failed for job ${job.id}: ${error.message}`, "warn")
        }
    }
    if (job.source_storage_path) {
        await downloadFromRawBucket(job.source_storage_path, destinationPath)
        return
    }
    throw new Error("No downloadable source available (source_url and source_storage_path missing)")
}

function getVideoDuration(inputPath) {
    try {
        const result = execSync(
            `ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "${inputPath}"`,
            { encoding: "utf8" },
        )
        return Math.max(0, Math.round(parseFloat(result.trim())))
    } catch {
        return null
    }
}

function getVideoInfo(inputPath) {
    try {
        const result = execSync(
            `ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=s=x:p=0 "${inputPath}"`,
            { encoding: "utf8" },
        )
        const [width, height] = result.trim().split("x").map(Number)
        return { width, height }
    } catch {
        return { width: null, height: null }
    }
}

function getVideoFps(inputPath) {
    try {
        const result = execSync(
            `ffprobe -v error -select_streams v:0 -show_entries stream=avg_frame_rate -of default=noprint_wrappers=1:nokey=1 "${inputPath}"`,
            { encoding: "utf8" },
        )
        const raw = result.trim()
        if (!raw) return 30
        if (raw.includes("/")) {
            const [numStr, denStr] = raw.split("/")
            const num = Number(numStr)
            const den = Number(denStr)
            if (Number.isFinite(num) && Number.isFinite(den) && den > 0) {
                const fps = num / den
                if (Number.isFinite(fps) && fps > 0) return fps
            }
        }
        const parsed = Number(raw)
        return Number.isFinite(parsed) && parsed > 0 ? parsed : 30
    } catch {
        return 30
    }
}

function hasAudioStream(inputPath) {
    try {
        const result = execSync(
            `ffprobe -v error -select_streams a:0 -show_entries stream=index -of csv=p=0 "${inputPath}"`,
            { encoding: "utf8" },
        )
        return result.trim().length > 0
    } catch {
        return false
    }
}

function selectQualitiesForJob(job, sourceVideoInfo) {
    const configuredQualities = CONFIG.qualities
    if (configuredQualities.length === 0) {
        throw new Error("No encoder qualities configured")
    }

    const requested = Array.isArray(job.requested_qualities)
        ? job.requested_qualities
            .map((item) => normalizeQualityName(item))
            .filter((item) => !!item && item !== "auto" && item !== SUBTITLE_ONLY_QUALITY_TOKEN)
        : []
    const requestedSet = new Set(requested)

    let selected = configuredQualities.filter((quality) => {
        if (requestedSet.size === 0) return true
        return requestedSet.has(normalizeQualityName(quality.name))
    })
    if (selected.length === 0) selected = [...configuredQualities]

    const sourceHeight = Number(sourceVideoInfo?.height)
    if (Number.isFinite(sourceHeight) && sourceHeight > 0) {
        const bounded = selected.filter((quality) => quality.height <= sourceHeight + 8)
        if (bounded.length > 0) {
            selected = bounded
        } else {
            selected = [selected[0]]
        }
    }

    return sortQualitiesAscending(selected)
}

async function transcodeToHls({
    inputPath,
    outputDir,
    qualities,
    fps = 30,
    hasAudio = true,
}) {
    const segmentDurationSeconds = CONFIG.segmentDurationSeconds
    const keyint = Math.max(24, Math.round(fps * segmentDurationSeconds))
    ensureDir(outputDir)

    let filterComplex = ""
    qualities.forEach((quality, index) => {
        filterComplex += `[0:v]scale=w=${quality.width}:h=${quality.height}:force_original_aspect_ratio=decrease,pad=ceil(iw/2)*2:ceil(ih/2)*2[v${index}];`
    })
    filterComplex = filterComplex.slice(0, -1)

    const ffmpegArgs = ["-i", inputPath, "-filter_complex", filterComplex]
    qualities.forEach((quality, index) => {
        ffmpegArgs.push("-map", `[v${index}]`)
        if (hasAudio) {
            ffmpegArgs.push("-map", "0:a:0")
        }
        ffmpegArgs.push(
            `-c:v:${index}`,
            "libx264",
            `-b:v:${index}`,
            quality.bitrate,
            `-maxrate:v:${index}`,
            quality.maxrate || quality.bitrate,
            `-bufsize:v:${index}`,
            quality.bufsize || quality.maxrate || quality.bitrate,
            `-preset:v:${index}`,
            CONFIG.ffmpegPreset,
        )
        if (hasAudio) {
            ffmpegArgs.push(
                `-c:a:${index}`,
                "aac",
                `-b:a:${index}`,
                quality.audioBitrate || "96k",
            )
        }
    })

    const streamMap = qualities
        .map((quality, index) => {
            if (hasAudio) {
                return `v:${index},a:${index},name:${quality.name}`
            }
            return `v:${index},name:${quality.name}`
        })
        .join(" ")

    ffmpegArgs.push(
        "-g",
        String(keyint),
        "-keyint_min",
        String(keyint),
        "-sc_threshold",
        "0",
        "-force_key_frames",
        `expr:gte(t,n_forced*${segmentDurationSeconds})`,
        "-f",
        "hls",
        "-hls_time",
        String(segmentDurationSeconds),
        "-hls_playlist_type",
        "vod",
        "-hls_flags",
        "independent_segments",
        "-hls_segment_type",
        "mpegts",
        "-hls_segment_filename",
        path.join(outputDir, "%v", "segment_%03d.ts"),
        "-var_stream_map",
        streamMap,
        path.join(outputDir, "%v", "playlist.m3u8"),
    )

    qualities.forEach((quality) => ensureDir(path.join(outputDir, quality.name)))

    await new Promise((resolve, reject) => {
        const ffmpeg = spawn("ffmpeg", ffmpegArgs, { stdio: "inherit" })
        ffmpeg.on("close", (code) => {
            if (code === 0) resolve()
            else reject(new Error(`ffmpeg exited with code ${code}`))
        })
        ffmpeg.on("error", (error) => reject(error))
    })

    let masterPlaylist = "#EXTM3U\n#EXT-X-VERSION:3\n"
    qualities.forEach((quality) => {
        const videoBitrate = parseInt(String(quality.bitrate).replace(/[^\d]/g, ""), 10) * 1000
        const audioBitrate = hasAudio
            ? parseInt(String(quality.audioBitrate || "96k").replace(/[^\d]/g, ""), 10) * 1000
            : 0
        const bandwidth = videoBitrate + audioBitrate
        masterPlaylist += `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${quality.width}x${quality.height},NAME="${quality.name}"\n`
        masterPlaylist += `${quality.name}/playlist.m3u8\n`
    })
    fs.writeFileSync(path.join(outputDir, "master.m3u8"), masterPlaylist)
}

async function uploadFile(localPath, remotePath) {
    const body = fs.createReadStream(localPath)
    const contentType = remotePath.endsWith(".m3u8")
        ? "application/vnd.apple.mpegurl"
        : remotePath.endsWith(".ts")
            ? "video/MP2T"
            : remotePath.endsWith(".vtt")
                ? "text/vtt; charset=utf-8"
                : remotePath.endsWith(".srt")
                    ? "application/x-subrip; charset=utf-8"
                    : "application/octet-stream"
    const cacheControl = remotePath.endsWith(".m3u8")
        ? "public, max-age=30, s-maxage=30, stale-while-revalidate=60"
        : "public, max-age=31536000, immutable"
    const command = new PutObjectCommand({
        Bucket: CONFIG.b2.bucketName,
        Key: remotePath,
        Body: body,
        ContentType: contentType,
        CacheControl: cacheControl,
    })
    await s3Client.send(command)
}

function uploadPriority(remotePath) {
    if (remotePath.endsWith("/master.m3u8") || remotePath.endsWith("\\master.m3u8")) return 3
    if (remotePath.endsWith(".m3u8")) return 2
    return 1
}

async function uploadDirectory(localDir, remotePrefix, concurrency = 4) {
    const files = []
    const walk = (dir, childPrefix = "") => {
        const entries = fs.readdirSync(dir)
        for (const entry of entries) {
            const fullPath = path.join(dir, entry)
            const relativePath = childPrefix ? `${childPrefix}/${entry}` : entry
            if (fs.statSync(fullPath).isDirectory()) {
                walk(fullPath, relativePath)
            } else {
                files.push({
                    localPath: fullPath,
                    remotePath: `${remotePrefix}/${relativePath}`,
                })
            }
        }
    }
    walk(localDir)

    files.sort((a, b) => {
        const priority = uploadPriority(a.remotePath) - uploadPriority(b.remotePath)
        if (priority !== 0) return priority
        return a.remotePath.localeCompare(b.remotePath)
    })

    const workers = Math.max(1, Math.min(concurrency, files.length))
    let cursor = 0
    let completed = 0

    const runWorker = async () => {
        for (;;) {
            const currentIndex = cursor
            cursor += 1
            if (currentIndex >= files.length) return
            const file = files[currentIndex]
            await uploadFile(file.localPath, file.remotePath)
            completed += 1
            if (completed === files.length || completed % 25 === 0) {
                log(`Upload progress: ${completed}/${files.length}`)
            }
        }
    }

    const pending = []
    for (let i = 0; i < workers; i++) {
        pending.push(runWorker())
    }
    await Promise.all(pending)
}

function isAutoSubtitleModeEnabled() {
    return CONFIG.subtitles.mode === "auto" || CONFIG.subtitles.mode === "hybrid"
}

function isManualSubtitleModeEnabled() {
    return CONFIG.subtitles.mode === "manual" || CONFIG.subtitles.mode === "hybrid"
}

function buildSubtitleLabel(language, mode = "manual") {
    const normalized = normalizeSubtitleLanguage(language)
    if (normalized === "und") {
        return mode === "auto" ? "Auto" : "Subtitle"
    }
    return mode === "auto" ? `Auto (${normalized.toUpperCase()})` : normalized.toUpperCase()
}

function readSubtitleMeta(metaPath) {
    if (!fs.existsSync(metaPath)) return {}
    try {
        const content = fs.readFileSync(metaPath, "utf8")
        const parsed = JSON.parse(content)
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
            return parsed
        }
    } catch {}
    return {}
}

async function generateAutoSubtitleTrack({
    sourcePath,
    outputPath,
    metaPath,
    language,
}) {
    const args = [
        CONFIG.subtitles.scriptPath,
        "--input",
        sourcePath,
        "--output",
        outputPath,
        "--meta-output",
        metaPath,
        "--model",
        CONFIG.subtitles.model,
        "--device",
        CONFIG.subtitles.device,
        "--compute-type",
        CONFIG.subtitles.computeType,
        "--beam-size",
        String(CONFIG.subtitles.beamSize),
    ]
    const explicitLanguage = normalizeSubtitleLanguage(language)
    if (explicitLanguage && explicitLanguage !== AUTO_SUBTITLE_LANG_TOKEN && explicitLanguage !== "und") {
        args.push("--language", explicitLanguage)
    }

    await new Promise((resolve, reject) => {
        const child = spawn(CONFIG.subtitles.pythonBin, args, { stdio: "inherit" })
        child.on("error", (error) => reject(error))
        child.on("close", (code) => {
            if (code === 0) resolve()
            else reject(new Error(`Subtitle generator exited with code ${code}`))
        })
    })
}

async function processSubtitleTracks({ subtitleTracks, workDir, remotePrefix }) {
    if (!Array.isArray(subtitleTracks) || subtitleTracks.length === 0) {
        return []
    }

    const subtitlesDir = path.join(workDir, "subs")
    ensureDir(subtitlesDir)

    const finalizedTracks = []
    const usedBaseNames = new Set()

    for (let index = 0; index < subtitleTracks.length; index += 1) {
        const track = subtitleTracks[index]
        const extension = inferSubtitleExtension(track.url)
        const tempSourcePath = path.join(subtitlesDir, `source_${index + 1}${extension}`)
        await downloadFile(track.url, tempSourcePath)

        let subtitleBody = fs.readFileSync(tempSourcePath, "utf8")
        if (extension === ".srt") {
            subtitleBody = srtToVtt(subtitleBody)
        }
        if (!subtitleBody.startsWith("WEBVTT")) {
            subtitleBody = `WEBVTT\n\n${subtitleBody}`
        }

        const baseTokenSeed = sanitizeFileToken(track.lang, `track_${index + 1}`)
        let baseToken = baseTokenSeed
        let suffix = 2
        while (usedBaseNames.has(baseToken)) {
            baseToken = `${baseTokenSeed}_${suffix}`
            suffix += 1
        }
        usedBaseNames.add(baseToken)

        const finalLocalPath = path.join(subtitlesDir, `${baseToken}.vtt`)
        fs.writeFileSync(finalLocalPath, subtitleBody, "utf8")

        const remoteSubtitlePath = `${remotePrefix}/subs/${baseToken}.vtt`
        await uploadFile(finalLocalPath, remoteSubtitlePath)

        finalizedTracks.push({
            lang: track.lang,
            label: track.label,
            url: `${CONFIG.cdnBaseUrl}/${remoteSubtitlePath}`,
            kind: normalizeSubtitleKind(track.kind),
            default: !!track.default,
        })
    }

    return ensureDefaultSubtitleTrack(dedupeSubtitleTracks(finalizedTracks))
}

async function processAutoSubtitleTracks({
    sourcePath,
    workDir,
    remotePrefix,
}) {
    if (!isAutoSubtitleModeEnabled()) {
        return []
    }

    const subtitlesDir = path.join(workDir, "subs-auto")
    ensureDir(subtitlesDir)

    const requestedLanguages = CONFIG.subtitles.languages.length > 0
        ? CONFIG.subtitles.languages.slice(0, MAX_SUBTITLE_TRACKS)
        : [AUTO_SUBTITLE_LANG_TOKEN]

    const generatedTracks = []
    const usedBaseNames = new Set()

    for (let index = 0; index < requestedLanguages.length; index += 1) {
        const requestedLanguage = normalizeSubtitleLanguage(requestedLanguages[index])
        const languageToken = requestedLanguage === "und" ? AUTO_SUBTITLE_LANG_TOKEN : requestedLanguage
        const outputName = sanitizeFileToken(`auto_${languageToken}`, `auto_${index + 1}`)
        const outputPath = path.join(subtitlesDir, `${outputName}.vtt`)
        const metaPath = path.join(subtitlesDir, `${outputName}.json`)

        const subtitleTarget = languageToken === AUTO_SUBTITLE_LANG_TOKEN ? "auto-detect" : languageToken
        log(`Generating auto subtitle (${subtitleTarget})`)
        await generateAutoSubtitleTrack({
            sourcePath,
            outputPath,
            metaPath,
            language: languageToken,
        })

        if (!fs.existsSync(outputPath)) {
            throw new Error(`Subtitle generator did not produce output file: ${outputPath}`)
        }

        const meta = readSubtitleMeta(metaPath)
        const detectedLanguage = normalizeSubtitleLanguage(
            meta.language
            || meta.detected_language
            || (languageToken === AUTO_SUBTITLE_LANG_TOKEN ? "und" : languageToken),
        )

        const baseTokenSeed = sanitizeFileToken(
            detectedLanguage === "und" ? `auto_${index + 1}` : detectedLanguage,
            `auto_${index + 1}`,
        )
        let baseToken = baseTokenSeed
        let suffix = 2
        while (usedBaseNames.has(baseToken)) {
            baseToken = `${baseTokenSeed}_${suffix}`
            suffix += 1
        }
        usedBaseNames.add(baseToken)

        const finalLocalPath = path.join(subtitlesDir, `${baseToken}.vtt`)
        if (finalLocalPath !== outputPath) {
            fs.renameSync(outputPath, finalLocalPath)
        }

        const remoteSubtitlePath = `${remotePrefix}/subs/${baseToken}.vtt`
        await uploadFile(finalLocalPath, remoteSubtitlePath)

        generatedTracks.push({
            lang: detectedLanguage,
            label: buildSubtitleLabel(detectedLanguage, "auto"),
            url: `${CONFIG.cdnBaseUrl}/${remoteSubtitlePath}`,
            kind: "subtitles",
            default: false,
        })
    }

    return dedupeSubtitleTracks(generatedTracks)
}

async function processJob(job) {
    const workDir = path.join(CONFIG.tempDir, job.id)
    const sourcePath = path.join(workDir, "input.mp4")
    const outputDir = path.join(workDir, "hls")
    ensureDir(workDir)

    try {
        await markJobProgress(job.id, {
            stage: "preparing_source",
            message: "Downloading source video",
        })
        log(`Downloading source for job ${job.id}`)
        await downloadSourceForJob(job, sourcePath)

        const duration = getVideoDuration(sourcePath)
        const hasAudio = hasAudioStream(sourcePath)
        const subtitleOnly = isSubtitleOnlyJob(job)
        const manualSubtitleTracks = extractSubtitleTracksFromJob(job)
        const requestedQualitiesLog = Array.isArray(job.requested_qualities) && job.requested_qualities.length > 0
            ? job.requested_qualities.join(",")
            : "default"
        const subtitleModeLog = CONFIG.subtitles.mode
        const manualSubtitleLog = manualSubtitleTracks.length > 0
            ? manualSubtitleTracks.map((track) => `${track.lang}:${track.label}`).join(",")
            : "none"
        const autoLanguagesLog = CONFIG.subtitles.languages.length > 0
            ? CONFIG.subtitles.languages.join(",")
            : AUTO_SUBTITLE_LANG_TOKEN
        const defaultRemotePrefix = `hls/${job.content_item_id}`
        const existingMasterUrl = resolveExistingMasterUrl(job)
        const remotePrefix = subtitleOnly
            ? deriveRemotePrefixFromMasterUrl(existingMasterUrl) || defaultRemotePrefix
            : defaultRemotePrefix
        let masterUrl = existingMasterUrl || `${CONFIG.cdnBaseUrl}/${remotePrefix}/master.m3u8`
        let qualityUrls = subtitleOnly
            ? extractQualityUrlsFromMetadata(getContentMetadataFromJob(job))
            : {}

        if (subtitleOnly) {
            const existingQualitiesLog = Object.keys(qualityUrls).length > 0
                ? Object.keys(qualityUrls).join(",")
                : "unknown"
            log(
                `Subtitle-only job ${job.id}: duration=${duration || "?"}s audio=${hasAudio} requested=${requestedQualitiesLog} existing_qualities=${existingQualitiesLog} subtitle_mode=${subtitleModeLog} manual_subtitles=${manualSubtitleLog} auto_languages=${autoLanguagesLog}`,
            )
            await markJobProgress(job.id, {
                stage: "processing",
                message: "Skipping HLS transcode (subtitle regeneration only)",
            })
        } else {
            const videoInfo = getVideoInfo(sourcePath)
            const fps = getVideoFps(sourcePath)
            const selectedQualities = selectQualitiesForJob(job, videoInfo)
            const selectedQualitiesLog = selectedQualities.map((quality) => quality.name).join(",")

            log(
                `Transcoding job ${job.id}: duration=${duration || "?"}s size=${videoInfo.width || "?"}x${videoInfo.height || "?"} fps=${fps.toFixed(
                    2,
                )} audio=${hasAudio} requested=${requestedQualitiesLog} selected=${selectedQualitiesLog} subtitle_mode=${subtitleModeLog} manual_subtitles=${manualSubtitleLog} auto_languages=${autoLanguagesLog}`,
            )

            await markJobProgress(job.id, {
                stage: "transcoding_hls",
                message: `Transcoding HLS renditions (${selectedQualitiesLog})`,
            })
            await transcodeToHls({
                inputPath: sourcePath,
                outputDir,
                qualities: selectedQualities,
                fps,
                hasAudio,
            })

            log(`Uploading HLS output for job ${job.id} to ${remotePrefix}`)
            await markJobProgress(job.id, {
                stage: "uploading_hls",
                message: "Uploading HLS segments and playlists",
            })
            await uploadDirectory(outputDir, remotePrefix, CONFIG.uploadConcurrency)

            masterUrl = `${CONFIG.cdnBaseUrl}/${remotePrefix}/master.m3u8`
            qualityUrls = {}
            for (const quality of selectedQualities) {
                qualityUrls[quality.name] = `${CONFIG.cdnBaseUrl}/${remotePrefix}/${quality.name}/playlist.m3u8`
            }
        }

        let uploadedManualTracks = []
        if (isManualSubtitleModeEnabled()) {
            await markJobProgress(job.id, {
                stage: "processing_manual_subtitles",
                message: manualSubtitleTracks.length > 0
                    ? `Processing ${manualSubtitleTracks.length} manual subtitle track(s)`
                    : "No manual subtitles provided",
            })
            if (manualSubtitleTracks.length > 0) {
                uploadedManualTracks = await processSubtitleTracks({
                    subtitleTracks: manualSubtitleTracks,
                    workDir,
                    remotePrefix,
                })
            }
        }

        let uploadedAutoTracks = []
        if (isAutoSubtitleModeEnabled()) {
            await markJobProgress(job.id, {
                stage: "generating_auto_subtitles",
                message: CONFIG.subtitles.languages.length > 0
                    ? `Generating auto subtitles (${CONFIG.subtitles.languages.join(",")})`
                    : "Generating auto subtitles (auto-detect)",
            })
            if (!hasAudio) {
                const message = `Auto subtitle generation skipped for job ${job.id}: source has no audio stream`
                if (CONFIG.subtitles.required) {
                    throw new Error(`${message} and SUBTITLE_REQUIRED=true`)
                }
                log(message, "warn")
            } else {
                try {
                    uploadedAutoTracks = await processAutoSubtitleTracks({
                        sourcePath,
                        workDir,
                        remotePrefix,
                    })
                } catch (error) {
                    const message = error instanceof Error ? error.message : String(error)
                    if (CONFIG.subtitles.required) {
                        throw new Error(`Auto subtitle generation failed: ${message}`)
                    }
                    log(`Auto subtitle generation failed for job ${job.id}: ${message}`, "warn")
                }
            }
        }

        let finalizedSubtitleTracks = []
        if (CONFIG.subtitles.mode === "manual") {
            finalizedSubtitleTracks = uploadedManualTracks
        } else if (CONFIG.subtitles.mode === "auto") {
            finalizedSubtitleTracks = uploadedAutoTracks
        } else if (CONFIG.subtitles.mode === "hybrid") {
            finalizedSubtitleTracks = mergeSubtitleTrackSets(uploadedManualTracks, uploadedAutoTracks)
        }
        finalizedSubtitleTracks = ensureDefaultSubtitleTrack(dedupeSubtitleTracks(finalizedSubtitleTracks))
        if (finalizedSubtitleTracks.length > 0) {
            log(`Uploaded ${finalizedSubtitleTracks.length} subtitle track(s) for job ${job.id}`)
        }

        await markJobProgress(job.id, {
            stage: "finalizing",
            message: "Finalizing playback metadata",
        })
        await markJobComplete(job.id, {
            master_url: masterUrl,
            quality_urls: qualityUrls,
            duration_seconds: duration,
            subtitle_tracks: finalizedSubtitleTracks,
        })
        log(`Job ${job.id} completed`)
    } finally {
        cleanDir(workDir)
    }
}

function setupSignalHandlers() {
    const requestStop = (signalName) => {
        if (shouldStop) return
        shouldStop = true
        log(`Received ${signalName}, will stop after current iteration`)
    }
    process.on("SIGTERM", () => requestStop("SIGTERM"))
    process.on("SIGINT", () => requestStop("SIGINT"))
}

async function runLoop() {
    ensureDir(CONFIG.tempDir)
    setupSignalHandlers()
    log(`Encoder worker started (worker_id=${CONFIG.workerId})`)
    log(`API base: ${CONFIG.adminApiBaseUrl}`)
    log(`B2 bucket: ${CONFIG.b2.bucketName}`)
    log(`B2 region: ${CONFIG.b2.region}`)
    log(`Upload concurrency: ${CONFIG.uploadConcurrency}`)
    log(`Subtitle mode: ${CONFIG.subtitles.mode}`)
    if (isAutoSubtitleModeEnabled()) {
        const languagesLog = CONFIG.subtitles.languages.length > 0
            ? CONFIG.subtitles.languages.join(",")
            : AUTO_SUBTITLE_LANG_TOKEN
        log(
            `Auto subtitle config: model=${CONFIG.subtitles.model} device=${CONFIG.subtitles.device} compute_type=${CONFIG.subtitles.computeType} languages=${languagesLog} required=${CONFIG.subtitles.required}`,
        )
    }
    if (!CONFIG.adminApiKey && CONFIG.allowDevNoKey) {
        log("Running without ADMIN_API_KEY (dev bypass mode)", "warn")
    }

    while (!shouldStop) {
        try {
            const job = await claimJob()
            if (!job) {
                await sleep(CONFIG.pollIntervalMs)
                continue
            }
            log(`Claimed job ${job.id} (content_item=${job.content_item_id})`)
            try {
                await processJob(job)
            } catch (error) {
                const message = error instanceof Error ? error.message : String(error)
                log(`Job ${job.id} failed: ${message}`, "error")
                await markJobFailed(job.id, message)
            }
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error)
            log(`Worker loop error: ${message}`, "error")
            await sleep(CONFIG.pollIntervalMs)
        }
    }
    log("Encoder worker stopped")
}

runLoop().catch((error) => {
    const message = error instanceof Error ? error.stack || error.message : String(error)
    console.error(message)
    process.exit(1)
})
