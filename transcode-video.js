/**
 * Video Transcoding Script for Windows
 * 
 * Transcodes video files to HLS format with multiple quality levels,
 * uploads to Backblaze B2, and updates Supabase database.
 * 
 * Usage:
 *   node transcode-video.js <video-path> <episode-id>
 * 
 * Example:
 *   node transcode-video.js "C:\Videos\lecture.mp4" "123e4567-e89b-12d3-a456-426614174000"
 */

const { execSync, spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

// ============================================================================
// CONFIGURATION
// ============================================================================

const CONFIG = {
    // Backblaze B2 (S3-compatible)
    b2: {
        endpoint: process.env.B2_ENDPOINT,
        keyId: process.env.B2_KEY_ID,
        applicationKey: process.env.B2_APPLICATION_KEY,
        bucketName: process.env.B2_BUCKET_NAME || 'ruwaq-videos',
    },
    // Supabase
    supabase: {
        url: process.env.SUPABASE_URL,
        serviceKey: process.env.SUPABASE_SERVICE_KEY,
    },
    // CDN
    cdnBaseUrl: process.env.CDN_BASE_URL || 'https://videos.mujam.store',
    // Temp directory for processing
    tempDir: path.join(process.env.TEMP || 'C:\\Temp', 'video-transcoding'),
    // HLS output tuning
    hlsSharedAudioTrack: (process.env.ENCODER_HLS_SHARED_AUDIO_TRACK || 'true').toLowerCase() === 'true',
    hlsSharedAudioBitrate: process.env.ENCODER_HLS_SHARED_AUDIO_BITRATE || '96k',
    // Quality presets
    qualities: [
        { name: '240p', width: 426, height: 240, bitrate: '280k', maxrate: '360k', bufsize: '560k', audioBitrate: '64k' },
        { name: '360p', width: 640, height: 360, bitrate: '500k', maxrate: '650k', bufsize: '1000k', audioBitrate: '96k' },
        { name: '540p', width: 960, height: 540, bitrate: '1000k', maxrate: '1300k', bufsize: '2000k', audioBitrate: '96k' },
        { name: '720p', width: 1280, height: 720, bitrate: '1800k', maxrate: '2400k', bufsize: '3600k', audioBitrate: '96k' },
    ],
};

// ============================================================================
// INITIALIZE CLIENTS
// ============================================================================

const s3Client = new S3Client({
    endpoint: CONFIG.b2.endpoint,
    region: 'us-west-004', // B2 region from endpoint
    credentials: {
        accessKeyId: CONFIG.b2.keyId,
        secretAccessKey: CONFIG.b2.applicationKey,
    },
});

const supabase = createClient(
    CONFIG.supabase.url,
    CONFIG.supabase.serviceKey
);

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function log(message, type = 'info') {
    const timestamp = new Date().toISOString();
    const prefix = {
        info: '📘',
        success: '✅',
        error: '❌',
        warning: '⚠️',
        progress: '🔄',
    }[type] || '📘';
    console.log(`${prefix} [${timestamp}] ${message}`);
}

function ensureDir(dirPath) {
    if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true });
    }
}

function cleanDir(dirPath) {
    if (fs.existsSync(dirPath)) {
        fs.rmSync(dirPath, { recursive: true, force: true });
    }
}

function parseBitrateToBps(value, fallback = 0) {
    const raw = String(value || '').trim().toLowerCase();
    if (!raw) return fallback;
    const parsed = Number.parseFloat(raw);
    if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
    if (raw.endsWith('k')) return Math.round(parsed * 1000);
    if (raw.endsWith('m')) return Math.round(parsed * 1000 * 1000);
    if (raw.endsWith('g')) return Math.round(parsed * 1000 * 1000 * 1000);
    if (parsed < 10000) return Math.round(parsed * 1000);
    return Math.round(parsed);
}

async function getVideoDuration(inputPath) {
    try {
        const result = execSync(
            `ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "${inputPath}"`,
            { encoding: 'utf8' }
        );
        return Math.round(parseFloat(result.trim()));
    } catch (error) {
        log('Could not get video duration', 'warning');
        return null;
    }
}

async function getVideoInfo(inputPath) {
    try {
        const result = execSync(
            `ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=s=x:p=0 "${inputPath}"`,
            { encoding: 'utf8' }
        );
        const [width, height] = result.trim().split('x').map(Number);
        return { width, height };
    } catch (error) {
        log('Could not get video dimensions', 'warning');
        return { width: null, height: null };
    }
}

async function getVideoFps(inputPath) {
    try {
        const result = execSync(
            `ffprobe -v error -select_streams v:0 -show_entries stream=avg_frame_rate -of default=noprint_wrappers=1:nokey=1 "${inputPath}"`,
            { encoding: 'utf8' }
        );
        const raw = result.trim();
        if (!raw) return 30;

        if (raw.includes('/')) {
            const [numStr, denStr] = raw.split('/');
            const num = Number(numStr);
            const den = Number(denStr);
            if (Number.isFinite(num) && Number.isFinite(den) && den !== 0) {
                const fps = num / den;
                if (Number.isFinite(fps) && fps > 0) {
                    return fps;
                }
            }
        }

        const fps = Number(raw);
        if (Number.isFinite(fps) && fps > 0) {
            return fps;
        }
        return 30;
    } catch (error) {
        log('Could not get video FPS, defaulting to 30', 'warning');
        return 30;
    }
}

// ============================================================================
// TRANSCODING FUNCTIONS
// ============================================================================

async function transcodeToHLS(inputPath, outputDir, qualities, fps = 30) {
    log(`Transcoding video to HLS with ${qualities.length} quality levels...`, 'progress');
    const segmentDurationSeconds = 2;
    const useSharedAudioTrack = CONFIG.hlsSharedAudioTrack;
    const keyint = Math.max(24, Math.round(fps * segmentDurationSeconds));
    log(`Using keyint=${keyint} for ${segmentDurationSeconds}s segments (fps=${fps.toFixed(2)})`, 'info');
    log(`Shared audio track: enabled=${useSharedAudioTrack} bitrate=${CONFIG.hlsSharedAudioBitrate}`, 'info');

    ensureDir(outputDir);

    // Build FFmpeg command for all qualities
    let filterComplex = '';

    qualities.forEach((q, i) => {
        // Scale with force_original_aspect_ratio=decrease, then pad to ensure even dimensions
        // The pad filter ensures width and height are divisible by 2 (required by libx264)
        filterComplex += `[0:v]scale=w=${q.width}:h=${q.height}:force_original_aspect_ratio=decrease,pad=ceil(iw/2)*2:ceil(ih/2)*2[v${i}];`;
    });

    // Remove trailing semicolon
    filterComplex = filterComplex.slice(0, -1);

    const ffmpegArgs = [
        '-i', inputPath,
        '-filter_complex', filterComplex,
    ];

    // Add mappings and codec settings for each quality
    qualities.forEach((q, i) => {
        ffmpegArgs.push('-map', `[v${i}]`);
        if (!useSharedAudioTrack) {
            ffmpegArgs.push('-map', '0:a?');
        }
        ffmpegArgs.push(
            `-c:v:${i}`, 'libx264',
            `-b:v:${i}`, q.bitrate,
            `-maxrate:v:${i}`, q.maxrate || q.bitrate,
            `-bufsize:v:${i}`, q.bufsize || q.maxrate || q.bitrate,
            `-preset:v:${i}`, 'veryfast',
        );
        if (!useSharedAudioTrack) {
            ffmpegArgs.push(
                `-c:a:${i}`, 'aac',
                `-b:a:${i}`, q.audioBitrate
            );
        }
    });

    if (useSharedAudioTrack) {
        ffmpegArgs.push(
            '-map', '0:a?',
            '-c:a:0', 'aac',
            '-ac:a:0', '2',
            '-ar:a:0', '48000',
            '-b:a:0', CONFIG.hlsSharedAudioBitrate,
        );
    }

    const varStreamMap = (() => {
        if (useSharedAudioTrack) {
            const videos = qualities.map((q, i) => `v:${i},agroup:audio,name:${q.name}`);
            return [...videos, 'a:0,agroup:audio,name:audio,default:yes,language:und'].join(' ');
        }
        return qualities.map((q, i) => `v:${i},a:${i},name:${q.name}`).join(' ');
    })();

    ffmpegArgs.push(
        '-g', String(keyint),
        '-keyint_min', String(keyint),
        '-sc_threshold', '0',
        '-force_key_frames', `expr:gte(t,n_forced*${segmentDurationSeconds})`,
        '-f', 'hls',
        '-hls_time', String(segmentDurationSeconds),  // 2-second segments for TikTok-like fast loading
        '-hls_playlist_type', 'vod',
        '-hls_flags', 'independent_segments',
        '-hls_segment_type', 'fmp4',
        '-hls_fmp4_init_filename', 'init.mp4',
        '-hls_segment_filename', path.join(outputDir, '%v', 'segment_%03d.m4s'),
        '-var_stream_map', varStreamMap,
        path.join(outputDir, '%v', 'playlist.m3u8')
    );

    // Create quality directories
    qualities.forEach(q => ensureDir(path.join(outputDir, q.name)));
    if (useSharedAudioTrack) {
        ensureDir(path.join(outputDir, 'audio'));
    }

    await new Promise((resolve, reject) => {
        const ffmpeg = spawn('ffmpeg', ffmpegArgs, { stdio: 'inherit' });

        ffmpeg.on('close', (code) => {
            if (code === 0) {
                log('FFmpeg transcoding completed!', 'success');
                resolve();
            } else {
                reject(new Error(`FFmpeg exited with code ${code}`));
            }
        });

        ffmpeg.on('error', (err) => {
            reject(new Error(`FFmpeg error: ${err.message}`));
        });
    });

    // Manually create master.m3u8 in the output directory
    // This ensures it's in the correct location for HLS playback
    log('Creating master playlist...', 'progress');

    let masterPlaylist = '#EXTM3U\n#EXT-X-VERSION:7\n';
    const sharedAudioBitrate = parseBitrateToBps(CONFIG.hlsSharedAudioBitrate, parseBitrateToBps('96k', 96000));
    if (useSharedAudioTrack) {
        masterPlaylist += '#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="Default",LANGUAGE="und",DEFAULT=YES,AUTOSELECT=YES,URI="audio/playlist.m3u8"\n';
    }

    qualities.forEach(q => {
        // Calculate bandwidth (bitrate in bits per second)
        const videoBitrate = parseBitrateToBps(q.bitrate, 0);
        const audioBitrate = useSharedAudioTrack
            ? sharedAudioBitrate
            : parseBitrateToBps(q.audioBitrate, parseBitrateToBps('96k', 96000));
        const bandwidth = videoBitrate + audioBitrate;
        if (useSharedAudioTrack) {
            masterPlaylist += `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${q.width}x${q.height},NAME="${q.name}",AUDIO="audio"\n`;
        } else {
            masterPlaylist += `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${q.width}x${q.height},NAME="${q.name}"\n`;
        }
        masterPlaylist += `${q.name}/playlist.m3u8\n`;
    });

    const masterPath = path.join(outputDir, 'master.m3u8');
    fs.writeFileSync(masterPath, masterPlaylist);
    log(`Created master playlist at ${masterPath}`, 'success');
}


// ============================================================================
// UPLOAD FUNCTIONS
// ============================================================================

async function uploadFile(localPath, remotePath) {
    const fileContent = fs.readFileSync(localPath);
    const contentType = remotePath.endsWith('.m3u8')
        ? 'application/vnd.apple.mpegurl'
        : remotePath.endsWith('.ts')
            ? 'video/MP2T'
            : remotePath.endsWith('.m4s')
                ? 'video/iso.segment'
                : remotePath.endsWith('.mp4')
                    ? 'video/mp4'
                    : 'application/octet-stream';

    const command = new PutObjectCommand({
        Bucket: CONFIG.b2.bucketName,
        Key: remotePath,
        Body: fileContent,
        ContentType: contentType,
    });

    await s3Client.send(command);
}

async function uploadDirectory(localDir, remotePrefix) {
    log(`Uploading HLS files to B2...`, 'progress');

    const files = [];

    function collectFiles(dir, prefix) {
        const items = fs.readdirSync(dir);
        for (const item of items) {
            const localPath = path.join(dir, item);
            const remotePath = prefix ? `${prefix}/${item}` : item;

            if (fs.statSync(localPath).isDirectory()) {
                collectFiles(localPath, remotePath);
            } else {
                files.push({ localPath, remotePath: `${remotePrefix}/${remotePath}` });
            }
        }
    }

    collectFiles(localDir, '');

    log(`Uploading ${files.length} files...`, 'info');

    let uploaded = 0;
    for (const file of files) {
        await uploadFile(file.localPath, file.remotePath);
        uploaded++;
        if (uploaded % 10 === 0 || uploaded === files.length) {
            log(`Uploaded ${uploaded}/${files.length} files`, 'progress');
        }
    }

    log(`All files uploaded to B2!`, 'success');
}

// ============================================================================
// DATABASE FUNCTIONS
// ============================================================================

async function createVideoFileRecord(episodeId) {
    const { data, error } = await supabase
        .from('video_files')
        .insert({
            episode_id: episodeId,
            status: 'processing',
        })
        .select()
        .single();

    if (error) throw error;
    return data;
}

async function updateVideoFileRecord(id, updates) {
    const { error } = await supabase
        .from('video_files')
        .update(updates)
        .eq('id', id);

    if (error) throw error;
}

// ============================================================================
// MAIN FUNCTION
// ============================================================================

async function main() {
    const args = process.argv.slice(2);

    if (args.length < 2) {
        console.log(`
Usage: node transcode-video.js <video-path> <episode-id>

Arguments:
  video-path    Path to the source video file
  episode-id    UUID of the video_episodes record in Supabase

Example:
  node transcode-video.js "C:\\Videos\\lecture.mp4" "123e4567-e89b-12d3-a456-426614174000"
    `);
        process.exit(1);
    }

    const [inputPath, episodeId] = args;

    // Validate input file
    if (!fs.existsSync(inputPath)) {
        log(`File not found: ${inputPath}`, 'error');
        process.exit(1);
    }

    // Validate UUID format
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    if (!uuidRegex.test(episodeId)) {
        log(`Invalid episode ID format: ${episodeId}`, 'error');
        process.exit(1);
    }

    log(`Starting video transcoding...`, 'info');
    log(`Input: ${inputPath}`, 'info');
    log(`Episode ID: ${episodeId}`, 'info');

    let videoFileRecord;
    const outputDir = path.join(CONFIG.tempDir, episodeId);

    try {
        // Step 1: Get video info
        log('Getting video information...', 'progress');
        const duration = await getVideoDuration(inputPath);
        const { width, height } = await getVideoInfo(inputPath);
        const fps = await getVideoFps(inputPath);
        const fileSize = fs.statSync(inputPath).size;

        log(
            `Duration: ${duration}s, Resolution: ${width}x${height}, FPS: ${fps.toFixed(2)}, Size: ${(fileSize / 1024 / 1024).toFixed(2)} MB`,
            'info'
        );

        // Step 2: Create database record
        log('Creating database record...', 'progress');
        videoFileRecord = await createVideoFileRecord(episodeId);
        log(`Created video_files record: ${videoFileRecord.id}`, 'success');

        // Step 3: Transcode to HLS
        await transcodeToHLS(inputPath, outputDir, CONFIG.qualities, fps);

        // Step 4: Upload to B2
        const hlsPath = `hls/${videoFileRecord.id}`;
        await uploadDirectory(outputDir, hlsPath);

        // Step 5: Update database record
        log('Updating database record...', 'progress');
        await updateVideoFileRecord(videoFileRecord.id, {
            status: 'ready',
            duration_seconds: duration,
            file_size_bytes: fileSize,
            width: width,
            height: height,
            hls_path: `${hlsPath}/master.m3u8`,
            cdn_base_url: `${CONFIG.cdnBaseUrl}/${hlsPath}`,
            qualities: CONFIG.qualities.map(q => q.name),
            processed_at: new Date().toISOString(),
        });

        log('Video processing completed successfully!', 'success');
        log(`CDN URL: ${CONFIG.cdnBaseUrl}/${hlsPath}/master.m3u8`, 'info');

    } catch (error) {
        log(`Error: ${error.message}`, 'error');

        // Update database with error
        if (videoFileRecord) {
            await updateVideoFileRecord(videoFileRecord.id, {
                status: 'failed',
                error_message: error.message,
            });
        }

        process.exit(1);
    } finally {
        // Cleanup temp files
        log('Cleaning up temporary files...', 'progress');
        cleanDir(outputDir);
    }
}

main();
