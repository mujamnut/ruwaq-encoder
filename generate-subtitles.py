#!/usr/bin/env python3
"""
Generate WebVTT subtitles from a video/audio file using faster-whisper.
"""

import argparse
import json
import os
import sys
from typing import List, Optional, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate WebVTT subtitles with faster-whisper")
    parser.add_argument("--input", required=True, help="Input media file path")
    parser.add_argument("--output", required=True, help="Output WebVTT file path")
    parser.add_argument("--meta-output", help="Optional output path for metadata JSON")
    parser.add_argument("--language", help="Optional language code (example: en, ms, ar)")
    parser.add_argument("--model", default="small", help="Whisper model name/path")
    parser.add_argument("--device", default="cpu", help="Device to run on (cpu or cuda)")
    parser.add_argument("--compute-type", default="int8", help="Compute type (int8/float16/etc)")
    parser.add_argument("--beam-size", type=int, default=5, help="Beam size used for transcription")
    parser.add_argument(
        "--no-vad-filter",
        action="store_true",
        help="Disable built-in voice activity detection filter",
    )
    return parser.parse_args()


def format_timestamp(seconds: float) -> str:
    total_milliseconds = max(0, int(round(seconds * 1000.0)))
    hours = total_milliseconds // 3_600_000
    total_milliseconds -= hours * 3_600_000
    minutes = total_milliseconds // 60_000
    total_milliseconds -= minutes * 60_000
    secs = total_milliseconds // 1_000
    milliseconds = total_milliseconds - (secs * 1_000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}.{milliseconds:03d}"


def ensure_parent_dir(file_path: str) -> None:
    parent = os.path.dirname(os.path.abspath(file_path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def normalize_text(value: str) -> str:
    return " ".join((value or "").strip().split())


def write_webvtt(output_path: str, cues: List[Tuple[float, float, str]]) -> None:
    ensure_parent_dir(output_path)
    with open(output_path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("WEBVTT\n\n")
        for index, (start, end, text) in enumerate(cues, start=1):
            handle.write(f"{index}\n")
            handle.write(f"{format_timestamp(start)} --> {format_timestamp(end)}\n")
            handle.write(f"{text}\n\n")


def write_metadata(meta_output: Optional[str], payload: dict) -> None:
    if not meta_output:
        return
    ensure_parent_dir(meta_output)
    with open(meta_output, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)


def main() -> int:
    args = parse_args()

    if not os.path.isfile(args.input):
        print(f"Input file does not exist: {args.input}", file=sys.stderr)
        return 1

    try:
        from faster_whisper import WhisperModel
    except Exception as error:
        print(
            f"Failed to import faster_whisper. Install dependency first. Error: {error}",
            file=sys.stderr,
        )
        return 1

    try:
        model = WhisperModel(
            args.model,
            device=args.device,
            compute_type=args.compute_type,
        )
        segments_iterable, info = model.transcribe(
            args.input,
            language=args.language or None,
            beam_size=max(1, int(args.beam_size)),
            vad_filter=not args.no_vad_filter,
            condition_on_previous_text=False,
        )

        cues: List[Tuple[float, float, str]] = []
        for segment in segments_iterable:
            text = normalize_text(getattr(segment, "text", ""))
            if not text:
                continue
            start = float(getattr(segment, "start", 0.0))
            end = float(getattr(segment, "end", start))
            if end <= start:
                end = start + 0.2
            cues.append((start, end, text))

        write_webvtt(args.output, cues)

        detected_language = getattr(info, "language", None)
        language_probability = getattr(info, "language_probability", None)
        duration_seconds = getattr(info, "duration", None)
        metadata = {
            "language": detected_language,
            "language_probability": language_probability,
            "duration_seconds": duration_seconds,
            "cue_count": len(cues),
            "input": os.path.abspath(args.input),
            "output": os.path.abspath(args.output),
            "model": args.model,
            "device": args.device,
            "compute_type": args.compute_type,
        }
        write_metadata(args.meta_output, metadata)
        print(json.dumps(metadata, ensure_ascii=True))
        return 0
    except Exception as error:
        print(f"Subtitle generation failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
