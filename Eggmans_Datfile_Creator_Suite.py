#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Eggmans_Datfile_Creator_Suite
Single unified datting engine with configurable options.

Dat Type:
  Mixed  — files hashed as-is (no archive inspection); adds forcepacking="fileonly"
  Zipped — zip contents analyzed; each zip is a game, internal paths preserved

Generation Mode:
  Per Root    — one dat per top-level folder; all subfolder content rolled in
  Per All     — one dat per every folder that contains relevant content (recursive)

Structure (Per Root only — 5 options matching dir2datUI):
  opt1  — Dirs
  opt2  — Archives as Games
  opt3  — First Level Dirs as Games
  opt4  — First Level Dirs as Games + Merge Dirs in Games

Format:
  Legacy  — all entries use <dir name=...>
  Modern  — root entries use <game> (or <machine>); subfolders use <dir>

RomVault DAT format (from RVWorld DATReader source):
  - ROM attrs order: name, size, crc, sha1 [, md5] [, date]
  - CRC = 8 lowercase hex chars of uncompressed data (from zip central directory)
  - SHA1/MD5 computed from decompressed file content
  - size = uncompressed file size
  - <game> = DatDir with DGame; <dir> = DatDir without DGame
  - date on <rom> informational only (RomVault reader has it commented out)
  - forcepacking absent on Zipped header → RomVault defaults to Zip mode
"""

import os, sys, json, re, time, queue, zlib, stat, io, gc
import ctypes, hashlib, threading, datetime
from concurrent.futures import (ThreadPoolExecutor, as_completed,
                                wait as _cf_wait, FIRST_COMPLETED)
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinterdnd2 import DND_FILES, TkinterDnD
from xml.sax.saxutils import escape as xml_escape

# ─── Hard-required pip packages ────────────────────────────────────────────
# Install with:  pip install zstandard zipfile-zstd
# Both are needed for streaming ZStandard (RVZSTD, method-93) zip entries.
try:
    import zipfile_zstd as _zipfile_mod   # drop-in zipfile + method-93 support
    import zstandard as _zstandard        # noqa: F401 — validates install
    import zstandard as _zstd_lib         # used directly for stream-path RVZSTD decompression
    _DEPS_OK    = True
    _DEPS_ERROR = ""
except ImportError as _dep_err:
    import zipfile as _zipfile_mod        # bare fallback so name is always defined
    _DEPS_OK    = False
    _DEPS_ERROR = str(_dep_err)

# ─── Optional: psutil for NIC speed auto-detection ─────────────────────────
# Install with:  pip install psutil
# If absent, auto network cap falls back to unlimited (user can set manually).
try:
    import psutil as _psutil
    _PSUTIL_OK = True
except ImportError:
    _psutil    = None   # type: ignore
    _PSUTIL_OK = False

CONFIG_FILENAME       = "Eggmans_Datfile_Creator_Suite_config.json"
FILE_ATTRIBUTE_HIDDEN = 0x2
FILE_ATTRIBUTE_SYSTEM = 0x4
MAX_SAFE_PATH         = 240

# ── SMB/network concurrency control ─────────────────────────────────────────
# BYTESIO_THRESHOLD: zips at or below this size are read entirely into a
#   BytesIO buffer in one sequential pass.  All ZipFile seeks then happen in
#   RAM — no further network traffic, no BufferedReader seek-invalidation.
#   Set to 500 MB.  At 4 workers peak RAM from BytesIO = 2 GB, acceptable
#   on modern hardware.  This covers virtually all multi-entry "medium" zips
#   that would otherwise stall due to per-entry seek overhead (see below).
#
# _LARGE_ZIP_LOCK: Semaphore(1) — only one zip >500 MB reads from the network
#   at a time.  Prevents SMB credit exhaustion from concurrent large streams.
#
# STREAM_OPEN_BUF: Buffer size for the stream (large-zip) path.  Must be
#   SMALL (4 MB, not 32 MB).  Python's BufferedReader.seek() unconditionally
#   clears its buffer on every call.  ZipFile.open(entry) always seeks to
#   the entry's header_offset.  With a 32 MB buffer, each of N entries
#   discards + re-reads 32 MB → N × 320 ms stall at 100 MB/s.  With 4 MB,
#   each seek-miss costs only ~40 ms and re-reads cover ~9 consecutive entries
#   before the next miss.
BYTESIO_THRESHOLD = 500 * 1024 * 1024   # 500 MB
STREAM_OPEN_BUF   =   4 * 1024 * 1024  # 4 MB  (stream path only)
STREAM_ENTRY_MEM  =  64 * 1024 * 1024  # 64 MB max compressed bytes held in RAM per entry
_LARGE_ZIP_LOCK   = threading.Semaphore(1)


# ═══════════════════════════════════════════════════════════════════════════
#  UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

def is_hidden_or_system(path: str) -> bool:
    if os.path.basename(path).startswith("."):
        return True
    if os.name != "nt":
        return False
    try:
        fn = ctypes.windll.kernel32.GetFileAttributesW
        fn.argtypes = [ctypes.c_wchar_p]
        fn.restype  = ctypes.c_uint32
        attrs = fn(path)
        if attrs == 0xFFFFFFFF:
            return False
        return bool(attrs & (FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM))
    except Exception:
        return False


def xa(value: str) -> str:
    """Escape for XML attribute values."""
    return xml_escape(value, {'"': "&quot;", "'": "&apos;"})


def script_dir() -> str:
    """Returns the directory of the script/exe — used for config file location."""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def resource_path(filename: str) -> str:
    """
    Returns the full path to a bundled resource file.

    When running as a PyInstaller --onefile exe, data files are extracted
    to a temporary sys._MEIPASS directory, NOT the exe's own directory.
    When running as a plain .py script, resources live beside the script.
    """
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, filename)
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)


def safe_makedirs(path: str) -> Optional[str]:
    try:
        os.makedirs(path, exist_ok=True)
        return None
    except Exception as e:
        return str(e)


def clean_dnd_path(data: str) -> str:
    data = data.strip()
    if data.startswith("{") and data.endswith("}"):
        data = data[1:-1]
    if " " in data and not os.path.exists(data):
        parts = data.split()
        if parts:
            return parts[0].strip("{}")
    return data


def safe_filename(s: str) -> str:
    """Strip characters illegal in Windows filenames."""
    return re.sub(r'[<>:"/\\|?*]', "_", s)


# ═══════════════════════════════════════════════════════════════════════════
#  SETTINGS
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Settings:
    # Paths
    input_root:   str = ""
    output_root:  str = ""
    parent_name:  str = ""          # optional prefix on all dat filenames

    # Header fields
    description:  str = ""
    category:     str = ""
    version:      str = ""
    date:         str = ""          # runtime only — not saved
    author:       str = ""
    url:          str = ""
    homepage:     str = ""
    comment:      str = ""

    # Dat type
    dat_type:     str = "mixed"     # "mixed" | "zipped"

    # Generation mode
    gen_mode:     str = "per_root"  # "per_root" | "per_all"

    # Structure (per_root only)
    structure:    str = "opt2"  # "opt1"|"opt2"|"opt3"|"opt4"

    # Format
    dat_format:   str = "modern"    # "legacy" | "modern"

    # Modern-only
    use_machine:  bool = False
    incl_game_desc: bool = True

    # Mixed-only
    forcepacking: bool = True

    # Zipped-only
    incl_file_date: bool = False

    # 7-Zip ZStandard path (for ZStandard-compressed zips)
    sevenzip_path: str = r"C:\Program Files\7-Zip-Zstandard\7z.exe"

    # Incremental update
    incremental:          bool = False
    incremental_dat_path: str  = ""    # path to existing dat (or folder of dats)
    retire_old_dats:      bool = True  # rename superseded dats to .old

    # Shared options
    include_md5:    bool = False
    include_sha256: bool = False
    # Extension filters (Mixed only) — comma-space-separated, case-insensitive.
    # Leading dots optional. Empty = no filter (include filter disabled).
    # Exclude filter always applied.
    ext_include:    str  = ""         # e.g. ".ima, .mfm, .86f"
    ext_exclude:    str  = ""         # e.g. ".nfo, .sfv, thumbs.db, .ds_store"
    multithread:    bool = True
    threads:        int  = 4
    net_cap_mbps:   int  = 0     # 0 = auto (85% of NIC speed); >0 = manual Mbit/s cap

    def to_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items()}
        d.pop("date", None)
        return d

    @staticmethod
    def from_dict(d: dict) -> "Settings":
        s = Settings()
        for k, v in d.items():
            if hasattr(s, k):
                setattr(s, k, v)
        try:
            s.threads = max(1, min(8, int(s.threads)))
        except Exception:
            s.threads = 4
        return s


def load_settings() -> "Settings":
    path = os.path.join(script_dir(), CONFIG_FILENAME)
    if not os.path.isfile(path):
        return Settings()
    try:
        with open(path, "r", encoding="utf-8") as f:
            return Settings.from_dict(json.load(f))
    except Exception:
        return Settings()


def save_settings(s: "Settings") -> None:
    path = os.path.join(script_dir(), CONFIG_FILENAME)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(s.to_dict(), f, indent=2, ensure_ascii=False)


# ═══════════════════════════════════════════════════════════════════════════
#  DAT HEADER WRITER
# ═══════════════════════════════════════════════════════════════════════════

def write_dat_header(f, dat_name: str, s: "Settings", header_date: str) -> None:
    """
    Writes the standard <header> block.
    Uses <name> tag (Logiqx standard).
    Always includes <romvault/> (base form); Mixed adds forcepacking attribute.
    """
    # Build <romvault .../> line
    if s.dat_type == "mixed" and s.forcepacking:
        rv_line = '\t\t<romvault forcepacking="fileonly"/>\n'
    else:
        rv_line = '\t\t<romvault/>\n'

    f.write('<?xml version="1.0"?>\n')
    f.write('<datafile>\n')
    f.write('\t<header>\n')
    # Tag built from character codes to avoid chat rendering collapsing <name> → <n>
    name_open  = '<' + chr(110)+chr(97)+chr(109)+chr(101) + '>'
    name_close = '</' + chr(110)+chr(97)+chr(109)+chr(101) + '>'
    f.write(f'\t\t{name_open}{xml_escape(dat_name)}{name_close}\n')
    f.write(f'\t\t<description>{xml_escape(s.description)}</description>\n')
    f.write(f'\t\t<category>{xml_escape(s.category)}</category>\n')
    f.write(f'\t\t<version>{xml_escape(s.version)}</version>\n')
    f.write(f'\t\t<date>{xml_escape(header_date)}</date>\n')
    f.write(f'\t\t<author>{xml_escape(s.author)}</author>\n')
    f.write(f'\t\t<url>{xml_escape(s.url)}</url>\n')
    f.write(f'\t\t<homepage>{xml_escape(s.homepage)}</homepage>\n')
    f.write(f'\t\t<comment>{xml_escape(s.comment)}</comment>\n')
    f.write(rv_line)
    f.write('\t</header>\n')


# ═══════════════════════════════════════════════════════════════════════════
#  NETWORK BANDWIDTH THROTTLE
# ═══════════════════════════════════════════════════════════════════════════

class _BandwidthThrottle:
    """
    Thread-safe token-bucket rate limiter for I/O reads.

    All worker threads share one instance.  Each call to consume(n) deducts
    n bytes from the token bucket; if the bucket is empty the call sleeps
    until enough tokens have refilled, effectively capping read throughput.

    rate_bytes_per_sec <= 0 means unlimited (consume() is a no-op).
    """
    def __init__(self, rate_bytes_per_sec: float):
        self._rate   = float(rate_bytes_per_sec)
        self._tokens = self._rate          # start full
        self._last   = time.monotonic()
        self._lock   = threading.Lock()

    def consume(self, nbytes: int,
                cancel: Optional[threading.Event] = None) -> None:
        if self._rate <= 0:
            return
        sleep_time = 0.0
        with self._lock:
            now     = time.monotonic()
            elapsed = now - self._last
            self._last = now
            # Refill — cap at one second's worth to prevent burst after idle
            self._tokens = min(self._rate,
                               self._tokens + elapsed * self._rate)
            if self._tokens >= nbytes:
                self._tokens -= nbytes
            else:
                deficit      = nbytes - self._tokens
                self._tokens = 0
                sleep_time   = deficit / self._rate
        # Sleep outside the lock in 50ms increments so cancel can interrupt
        if sleep_time > 0:
            deadline = time.monotonic() + sleep_time
            while time.monotonic() < deadline:
                if cancel and cancel.is_set():
                    return
                remaining = deadline - time.monotonic()
                time.sleep(min(0.05, remaining))


def _detect_net_cap_bytes_per_sec(pct: float = 0.85) -> float:
    """
    Return pct% of the fastest active NIC's speed in bytes/sec, or 0.0 if
    psutil is not installed / no speed can be determined.

    pct=0.85 leaves ~15% of NIC capacity free for other processes.
    """
    if not _PSUTIL_OK or _psutil is None:
        return 0.0
    try:
        max_mbps = 0
        for _nic, st in _psutil.net_if_stats().items():
            if st.isup and st.speed > 0:
                max_mbps = max(max_mbps, st.speed)
        if max_mbps > 0:
            return (max_mbps * 1_000_000 / 8) * pct
    except Exception:
        pass
    return 0.0


# ═══════════════════════════════════════════════════════════════════════════
#  HASHING — MIXED (file as-is)
# ═══════════════════════════════════════════════════════════════════════════

def hash_file(path: str, include_md5: bool,
              include_sha256: bool,
              cancel: threading.Event,
              chunk: int = 8*1024*1024,
              throttle: Optional["_BandwidthThrottle"] = None):
    """
    Hash a file with CRC32 + SHA1 (always) plus optional MD5 / SHA-256.
    Returns (size, crc, sha1, md5, sha256) — optional fields may be None.
    throttle: shared _BandwidthThrottle instance, or None for unlimited.
    """
    size = 0; crc = 0
    sha1   = hashlib.sha1()
    md5    = hashlib.md5()    if include_md5    else None
    sha256 = hashlib.sha256() if include_sha256 else None
    with open(path, "rb") as f:
        while True:
            if cancel.is_set():
                raise RuntimeError("CANCELLED")
            buf = f.read(chunk)
            if not buf:
                break
            if throttle:
                throttle.consume(len(buf), cancel=cancel)
            size += len(buf)
            crc   = zlib.crc32(buf, crc)
            sha1.update(buf)
            if md5:    md5.update(buf)
            if sha256: sha256.update(buf)
    return (size,
            f"{crc & 0xFFFFFFFF:08x}",
            sha1.hexdigest(),
            md5.hexdigest()    if md5    else None,
            sha256.hexdigest() if sha256 else None)

# ═══════════════════════════════════════════════════════════════════════════
#  HASHING — ZIPPED (analyze zip contents)
# ═══════════════════════════════════════════════════════════════════════════

def _get_entry_data_offset(raw_fh, entry) -> int:
    """
    Return the byte offset in raw_fh where this entry's compressed data begins.

    Python 3.11 added ZipInfo.start_offset; for 3.10 we read the local header.
    The local header's extra field length CAN differ from the central directory
    extra length, so we always read the local header to get the right offset.
    Falls back to central-dir calculation if the read fails.
    """
    # Python 3.11+
    try:
        so = entry.start_offset
        if so is not None:
            return so
    except AttributeError:
        pass
    # Read local header at header_offset + 26 to get fname_len and extra_len
    try:
        raw_fh.seek(entry.header_offset + 26)
        lens = raw_fh.read(4)
        if len(lens) == 4:
            fname_len = int.from_bytes(lens[0:2], 'little')
            extra_len = int.from_bytes(lens[2:4], 'little')
            return entry.header_offset + 30 + fname_len + extra_len
    except Exception:
        pass
    # Fallback using central directory values (usually correct)
    return entry.header_offset + 30 + len(entry.filename.encode('utf-8')) + len(entry.extra)


def _direct_hash_entry(raw_fh, entry, sha1_obj, md5_obj, sha256_obj, cancel, chunk_size):
    """
    Hash a zip entry by reading its compressed bytes in ONE large sequential
    read, then decompressing from memory.

    This replaces zf.open(entry) for the stream path.  The critical problem
    with zf.open() over SMB is that Python's ZipExtFile._read2() uses
    MIN_READ_SIZE=4096 bytes per read call.  For a 3MB entry that means
    ~750 sequential 4096-byte reads, each of which can cause SMB credit stalls
    in between.  One raw_fh.read(compress_size) = one large SMB request per
    entry — exactly what RomVault's ZipFileOpenReadStream does.

    Supports method 0 (Stored), method 8 (Deflate), method 93 (RVZSTD).
    Falls back to zf.open() for anything else (handled by caller).

    Returns True on success, False if method is unsupported (caller should
    fall back to zf.open()).
    """
    if entry.compress_type not in (0, 8, 93):
        return False   # caller falls back to zf.open()
    if entry.file_size == 0:
        return True    # nothing to hash; zero-byte empty hash already applied

    data_offset = _get_entry_data_offset(raw_fh, entry)

    if entry.compress_size > STREAM_ENTRY_MEM:
        # Entry is too large to hold in RAM — use streaming reads in chunks.
        # Still use direct raw_fh reads (large chunks, not 4096-byte reads).
        raw_fh.seek(data_offset)
        remaining = entry.compress_size
        if cancel.is_set():
            raise RuntimeError("CANCELLED")

        if entry.compress_type == 0:
            while remaining > 0:
                if cancel.is_set(): raise RuntimeError("CANCELLED")
                buf = raw_fh.read(min(chunk_size, remaining))
                if not buf: break
                remaining -= len(buf)
                sha1_obj.update(buf)
                if md5_obj:    md5_obj.update(buf)
                if sha256_obj: sha256_obj.update(buf)

        elif entry.compress_type == 8:
            dec = zlib.decompressobj(-15)
            while remaining > 0:
                if cancel.is_set(): raise RuntimeError("CANCELLED")
                chunk = raw_fh.read(min(chunk_size, remaining))
                if not chunk: break
                remaining -= len(chunk)
                out = dec.decompress(chunk)
                if out:
                    sha1_obj.update(out)
                    if md5_obj:    md5_obj.update(out)
                    if sha256_obj: sha256_obj.update(out)
            try:
                out = dec.flush()
                if out:
                    sha1_obj.update(out)
                    if md5_obj:    md5_obj.update(out)
                    if sha256_obj: sha256_obj.update(out)
            except Exception:
                pass

        elif entry.compress_type == 93:
            dctx = _zstd_lib.ZstdDecompressor()
            # Wrap the raw limited region in a reader
            lim = _LimitedReader(raw_fh, entry.compress_size)
            with dctx.stream_reader(lim, read_size=chunk_size) as reader:
                while True:
                    if cancel.is_set(): raise RuntimeError("CANCELLED")
                    out = reader.read(chunk_size)
                    if not out: break
                    sha1_obj.update(out)
                    if md5_obj:    md5_obj.update(out)
                    if sha256_obj: sha256_obj.update(out)
        return True

    # ── Normal path: ONE large read of entire compressed entry ───────────────
    raw_fh.seek(data_offset)
    if cancel.is_set():
        raise RuntimeError("CANCELLED")
    compressed = raw_fh.read(entry.compress_size)   # ← the key: ONE SMB request

    if entry.compress_type == 0:
        sha1_obj.update(compressed)
        if md5_obj:    md5_obj.update(compressed)
        if sha256_obj: sha256_obj.update(compressed)

    elif entry.compress_type == 8:
        dec = zlib.decompressobj(-15)
        for i in range(0, len(compressed), chunk_size):
            if cancel.is_set(): raise RuntimeError("CANCELLED")
            out = dec.decompress(compressed[i:i + chunk_size])
            if out:
                sha1_obj.update(out)
                if md5_obj:    md5_obj.update(out)
                if sha256_obj: sha256_obj.update(out)
        try:
            out = dec.flush()
            if out:
                sha1_obj.update(out)
                if md5_obj:    md5_obj.update(out)
                if sha256_obj: sha256_obj.update(out)
        except Exception:
            pass

    elif entry.compress_type == 93:
        dctx = _zstd_lib.ZstdDecompressor()
        with dctx.stream_reader(io.BytesIO(compressed), read_size=chunk_size) as reader:
            while True:
                if cancel.is_set(): raise RuntimeError("CANCELLED")
                out = reader.read(chunk_size)
                if not out: break
                sha1_obj.update(out)
                if md5_obj:    md5_obj.update(out)
                if sha256_obj: sha256_obj.update(out)

    del compressed   # free immediately
    return True


class _LimitedReader:
    """
    Wraps a file-like object and limits reads to at most `limit` bytes.
    Used to feed a ZstdDecompressor exactly one entry's compressed data
    without it reading past the entry boundary.
    """
    __slots__ = ('_fh', '_remaining')
    def __init__(self, fh, limit):
        self._fh        = fh
        self._remaining = limit
    def read(self, n=-1):
        if self._remaining <= 0:
            return b''
        if n < 0 or n > self._remaining:
            n = self._remaining
        data = self._fh.read(n)
        self._remaining -= len(data)
        return data
    def readinto(self, b):
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n


def _pipeline_hash(fh, sha1_obj, md5_obj, sha256_obj, cancel, chunk_size):
    """
    Double-buffer pipeline matching RomVault's ThreadReadBuffer pattern.

    Reader thread decompresses the NEXT chunk while main thread hashes
    the CURRENT chunk.  Both zlib/zstd and hashlib are C extensions that
    release the GIL, so they run in true parallel on separate CPU cores.

    Used for ALL paths (BytesIO and stream) for entries large enough to
    make the thread overhead worthwhile.

    buf_q.get() uses a 30-second timeout so a stalled network read never
    blocks Hard Stop indefinitely.
    """
    buf_q = queue.Queue(maxsize=2)

    def _reader():
        try:
            while not cancel.is_set():
                data = fh.read(chunk_size)
                buf_q.put(data)   # blocks if queue full — that's fine
                if not data:
                    return
        except Exception as exc:
            buf_q.put(exc)        # put (not put_nowait) so it's never lost

    reader_thread = threading.Thread(target=_reader, daemon=True)
    reader_thread.start()
    try:
        while True:
            try:
                item = buf_q.get(timeout=30)
            except queue.Empty:
                # Reader stalled for 30 s — check cancel and retry
                if cancel.is_set():
                    raise RuntimeError("CANCELLED")
                continue   # keep waiting; could be heavy decompression
            if isinstance(item, Exception):
                raise item
            if not item:
                break
            if cancel.is_set():
                raise RuntimeError("CANCELLED")
            sha1_obj.update(item)
            if md5_obj:    md5_obj.update(item)
            if sha256_obj: sha256_obj.update(item)
    finally:
        reader_thread.join(timeout=5)   # short — it's daemon, will die with process


def analyze_zip(zip_path: str, include_md5: bool,
                include_sha256: bool,
                incl_date: bool,
                cancel: threading.Event,
                sevenzip_path: str = "",
                throttle: Optional["_BandwidthThrottle"] = None) -> Tuple[List[tuple], str]:
    """
    Analyze a zip archive using Python's zipfile module with zipfile_zstd.
    Returns (results_list, diag_string).

    Two execution paths chosen by zip file size vs BYTESIO_THRESHOLD:

    SMALL PATH (zip <= BYTESIO_THRESHOLD, default 64 MB):
      - Reads the entire zip file into a BytesIO buffer in one sequential pass.
      - All ZipFile seeks then happen in RAM — no further network traffic.
      - Safe for parallel execution: multiple threads can each do this without
        competing for the same SMB stream.
      - Throttle post-consumed (after the read) so the read runs at full speed.

    LARGE PATH (zip > BYTESIO_THRESHOLD):
      - Acquires _LARGE_ZIP_LOCK (Semaphore(1)) so only one large zip is read
        from the network at a time.  This is the root fix for the SMB credit
        exhaustion that caused throughput collapse at ~50 zips.
      - Reads with a 32 MB OS-level buffer and entries sorted by header_offset
        so all reads are sequential forward seeks (SMB prefetch-friendly).
      - Throttle consumed per-entry in compressed bytes (actual network traffic).
      - Lock released as soon as the file is closed, before result sorting.
    """
    CHUNK       = 4 * 1024 * 1024   # 4 MB — matches RomVault's Buffersize constant
    SLOW_THRESH = 5.0               # MB/s below which a [SLOW] tag is prepended

    t_start = time.monotonic()
    results = []
    try:
        zip_size = os.path.getsize(zip_path)
    except OSError:
        zip_size = 0

    use_bytesio   = (0 < zip_size <= BYTESIO_THRESHOLD)
    lock_acquired = False

    # Canonical hashes for zero-byte content (empty file or empty folder entry).
    _EMPTY_SHA1   = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
    _EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    _EMPTY_MD5    = "d41d8cd98f00b204e9800998ecf8427e"

    # ── Pipeline threshold ───────────────────────────────────────────────────
    # Entries with uncompressed size >= this use the decompression/hash pipeline.
    # Entries below this use a simple loop (thread overhead > benefit).
    PIPELINE_THRESH = CHUNK   # 4 MB — one full read chunk

    # ── Per-entry slow threshold ─────────────────────────────────────────────
    # Entries taking longer than this are logged individually in the diag string
    # so you can identify which specific entries are bottlenecks.
    ENTRY_SLOW_S = 3.0   # seconds

    slow_entries = []   # [(name, seconds, MB_uncompressed)]

    def _hash_entries(zf, raw_fh_=None):
        """
        Hash all entries.
        raw_fh_: provided for stream path only — enables _direct_hash_entry
        which reads each entry's compressed bytes as ONE large raw read (one
        SMB request per entry), eliminating the 4096-byte-read stalls that
        zipfile.ZipExtFile._read2() causes internally.
        When raw_fh_ is None (BytesIO path), uses zf.open() with pipeline.
        """
        all_entries = zf.infolist()
        if not all_entries:
            return

        dir_entries  = [e for e in all_entries if     e.is_dir()]
        file_entries = [e for e in all_entries if not e.is_dir()]
        read_order   = sorted(file_entries, key=lambda e: e.header_offset)

        for entry in dir_entries:
            if cancel.is_set():
                raise RuntimeError("CANCELLED")
            rom_name = entry.filename.replace("\\", "/")
            if not rom_name.endswith("/"):
                rom_name += "/"
            results.append((
                rom_name, 0, "00000000", _EMPTY_SHA1,
                _EMPTY_MD5    if include_md5    else None,
                _EMPTY_SHA256 if include_sha256 else None,
                None,
            ))

        for entry in read_order:
            if cancel.is_set():
                raise RuntimeError("CANCELLED")

            sha1_obj   = hashlib.sha1()
            md5_obj    = hashlib.md5()    if include_md5    else None
            sha256_obj = hashlib.sha256() if include_sha256 else None

            t_entry = time.monotonic()

            if raw_fh_ is not None:
                # STREAM PATH: one large raw read per entry via _direct_hash_entry.
                ok = _direct_hash_entry(raw_fh_, entry, sha1_obj, md5_obj,
                                        sha256_obj, cancel, CHUNK)
                if not ok:
                    # Unknown compression method — fall back to zf.open()
                    with zf.open(entry) as fh:
                        _pipeline_hash(fh, sha1_obj, md5_obj, sha256_obj,
                                       cancel, CHUNK)
                # Post-consume throttle after the read
                if throttle and entry.compress_size > 0:
                    throttle.consume(entry.compress_size, cancel=cancel)
            else:
                # BYTESIO PATH: all seeks in RAM, use pipeline for large entries
                with zf.open(entry) as fh:
                    if entry.file_size >= PIPELINE_THRESH:
                        _pipeline_hash(fh, sha1_obj, md5_obj, sha256_obj,
                                       cancel, CHUNK)
                    else:
                        while True:
                            if cancel.is_set():
                                raise RuntimeError("CANCELLED")
                            buf = fh.read(CHUNK)
                            if not buf:
                                break
                            sha1_obj.update(buf)
                            if md5_obj:    md5_obj.update(buf)
                            if sha256_obj: sha256_obj.update(buf)

            entry_elapsed = time.monotonic() - t_entry
            if entry_elapsed >= ENTRY_SLOW_S:
                unc_mb = entry.file_size / (1024*1024)
                slow_entries.append(
                    entry.filename + " ("
                    + f"{unc_mb:.0f}" + " MB uncomp, "
                    + f"{entry_elapsed:.1f}" + "s)")

            crc32_hex = f"{entry.CRC & 0xFFFFFFFF:08x}"
            file_size = entry.file_size

            date_str = None
            if incl_date and entry.date_time and len(entry.date_time) >= 6:
                yr, mo, dy, h, mi, sc = entry.date_time[:6]
                try:
                    date_str = (str(yr) + "/" + f"{mo:02d}" + "/" +
                                f"{dy:02d}" + " " + f"{h:02d}" + "-" +
                                f"{mi:02d}" + "-" + f"{sc:02d}")
                except Exception:
                    pass

            results.append((
                entry.filename.replace("\\", "/"),
                file_size, crc32_hex,
                sha1_obj.hexdigest(),
                md5_obj.hexdigest()    if md5_obj    else None,
                sha256_obj.hexdigest() if sha256_obj else None,
                date_str,
            ))

    try:
        if use_bytesio:
            # ── SMALL PATH ────────────────────────────────────────────────
            if cancel.is_set():
                raise RuntimeError("CANCELLED")
            with io.open(zip_path, "rb") as f:
                raw_bytes = f.read()
            # Post-consume throttle: rate-limit AFTER the read so the actual
            # read happens at full speed; the sleep falls between zips.
            if throttle and raw_bytes:
                throttle.consume(len(raw_bytes), cancel=cancel)
            if cancel.is_set():
                raise RuntimeError("CANCELLED")
            bio = io.BytesIO(raw_bytes)
            del raw_bytes   # drop the duplicate; BytesIO holds its own copy
            with _zipfile_mod.ZipFile(bio, "r") as zf:
                _hash_entries(zf)           # BytesIO path — no raw_fh
        else:
            # ── LARGE PATH ────────────────────────────────────────────────
            while not _LARGE_ZIP_LOCK.acquire(timeout=0.5):
                if cancel.is_set():
                    raise RuntimeError("CANCELLED")
            lock_acquired = True
            if cancel.is_set():
                raise RuntimeError("CANCELLED")
            raw_fh = io.open(zip_path, "rb", buffering=STREAM_OPEN_BUF)
            try:
                with _zipfile_mod.ZipFile(raw_fh, "r") as zf:
                    _hash_entries(zf, raw_fh_=raw_fh)   # stream path — pass raw_fh
            finally:
                raw_fh.close()
                _LARGE_ZIP_LOCK.release()
                lock_acquired = False

    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(
            "Failed to read " + os.path.basename(zip_path) + ": " + str(exc))
    finally:
        if lock_acquired:
            _LARGE_ZIP_LOCK.release()

    # Sort results by rom_name for deterministic dat output
    results.sort(key=lambda r: r[0].lower())

    # Diagnostic string
    elapsed = time.monotonic() - t_start
    mb_read = zip_size / (1024 * 1024)
    if elapsed > 0:
        rate_mbs = mb_read / elapsed
        path_tag = "mem" if use_bytesio else "stream"
        diag = (f"{mb_read:.1f} MB in {elapsed:.1f}s = {rate_mbs:.1f} MB/s"
                f" ({len(results)} entries, {path_tag})")
        if rate_mbs < SLOW_THRESH and mb_read > 1.0:
            diag = "[SLOW] " + diag
        if slow_entries:
            diag += "  SLOW ENTRIES: " + "; ".join(slow_entries[:5])
            if len(slow_entries) > 5:
                diag += " ...+" + str(len(slow_entries)-5) + " more"
    else:
        diag = ""

    return results, diag


# ═══════════════════════════════════════════════════════════════════════════
#  ROM LINE BUILDER
# ═══════════════════════════════════════════════════════════════════════════

def rom_line(name: str, size, crc: str, sha1: str,
             md5: Optional[str],
             sha256: Optional[str],
             date_str: Optional[str],
             include_md5: bool,
             include_sha256: bool,
             incl_date: bool) -> str:
    """
    Build a <rom .../> line.
    Attribute order matches RomVault DatXMLWriter.cs:
      name, size, crc, sha1, sha256, md5, date
    """
    attrs = [f'name="{xa(name)}"', f'size="{size}"',
             f'crc="{crc}"', f'sha1="{sha1}"']
    if include_sha256 and sha256:
        attrs.append(f'sha256="{sha256}"')
    if include_md5 and md5:
        attrs.append(f'md5="{md5}"')
    if incl_date and date_str:
        attrs.append(f'date="{date_str}"')
    return "<rom " + " ".join(attrs) + "/>"


# ═══════════════════════════════════════════════════════════════════════════
#  EXTENSION FILTER HELPERS  (Mixed mode)
# ═══════════════════════════════════════════════════════════════════════════

def parse_ext_list(raw: str) -> set:
    """
    Parse a comma-separated extension list into a set of lowercase extensions
    with leading dots.

    Accepts: ".ima, .mfm, .86f"  or  "ima, mfm, 86f"  or  mixed.
    Also accepts full filenames (e.g. "thumbs.db") — stored as ".db" equivalents
    ONLY if they start with a dot. Bare filenames like "thumbs.db" are kept
    verbatim so they can match exact-file excludes.
    Returns a set — empty set means "no filter".
    """
    if not raw or not raw.strip():
        return set()
    out = set()
    for part in raw.split(","):
        p = part.strip().lower()
        if not p:
            continue
        # If it has a path separator, just keep basename
        p = p.replace("\\", "/").split("/")[-1]
        # Ensure leading dot on pure-extension entries (e.g. "ima" → ".ima")
        # but keep full filenames as-is (e.g. "thumbs.db" stays a full match key)
        if "." not in p:
            p = "." + p
        out.add(p)
    return out


def file_matches_filter(filename: str,
                        ext_include: set, ext_exclude: set) -> bool:
    """
    Test whether filename passes the include/exclude filters.
    Matching is case-insensitive on both extension AND full basename.
    A filter entry matches if:
      - it starts with '.' and matches the file's extension, OR
      - it equals the file's full basename (allows Thumbs.db-style entries).
    """
    base = os.path.basename(filename).lower()
    ext  = os.path.splitext(base)[1]   # includes leading dot, "" if none

    def _matches_any(flt: set) -> bool:
        for entry in flt:
            if entry == base:
                return True
            if entry.startswith(".") and entry == ext:
                return True
        return False

    if ext_include and not _matches_any(ext_include):
        return False
    if ext_exclude and _matches_any(ext_exclude):
        return False
    return True


# ═══════════════════════════════════════════════════════════════════════════
#  FOLDER TREE NODES  (shared by both dat types)
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class FolderNode:
    """
    Represents one folder in the input tree.
    For Mixed:  items = list of (filename, full_path)
    For Zipped: items = list of zip full_paths
    """
    name:    str
    relpath: str           # relative path from the job root (empty for root)
    items:   List[str]     = field(default_factory=list)   # full paths
    subdirs: List["FolderNode"] = field(default_factory=list)


@dataclass
class PreviewEntry:
    """One completed dat held in memory for the preview window."""
    dat_name:    str
    header_date: str
    node:        "FolderNode"   # full tree (per_root) or shallow node (per_all)
    data:        dict            # item_path → hash tuple
    settings:    "Settings"      # snapshot at time of creation
    is_perroot:  bool


def render_preview(entry: "PreviewEntry", structure_override: str) -> str:
    """
    Re-render a completed dat to a string using a different structure option.
    Uses the same writer functions as the actual dat writer — just targets a
    StringIO buffer instead of a file.
    For per_all entries, the node has no subdirs so all structure options
    produce an equivalent flat output (correct behaviour).
    """
    import io
    s_copy = Settings.from_dict(entry.settings.to_dict())
    s_copy.date      = entry.settings.date
    s_copy.structure = structure_override
    buf = io.StringIO()
    write_dat_header(buf, entry.dat_name, s_copy, entry.header_date)
    writers = _MIXED_WRITERS if s_copy.dat_type == "mixed" else _ZIPPED_WRITERS
    writer  = writers.get(structure_override, writers["opt2"])
    writer(buf, entry.node, entry.data, s_copy)
    buf.write("</datafile>\n")
    return buf.getvalue()


def scan_tree_mixed(dir_path: str, rel: str,
                    stop: threading.Event, ui_queue,
                    ext_include: Optional[set] = None,
                    ext_exclude: Optional[set] = None) -> FolderNode:
    """Recursively build a FolderNode tree for Mixed mode (all files).
    Extension filter applied to files only; subdirs still walked."""
    node = FolderNode(name=os.path.basename(dir_path), relpath=rel)
    if stop.is_set():
        return node
    ui_queue.put(("scan", dir_path))
    try:
        entries = sorted(os.scandir(dir_path), key=lambda e: e.name.lower())
    except Exception:
        return node
    inc = ext_include or set()
    exc = ext_exclude or set()
    for entry in entries:
        if stop.is_set():
            break
        if is_hidden_or_system(entry.path) or entry.is_symlink():
            continue
        if entry.is_file(follow_symlinks=False):
            if inc or exc:
                if not file_matches_filter(entry.name, inc, exc):
                    continue
            node.items.append(entry.path)
        elif entry.is_dir(follow_symlinks=False):
            child_rel = os.path.join(rel, entry.name) if rel else entry.name
            child = scan_tree_mixed(entry.path, child_rel, stop, ui_queue,
                                     inc, exc)
            node.subdirs.append(child)
    node.items.sort(key=lambda p: os.path.basename(p).lower())
    node.subdirs.sort(key=lambda n: n.name.lower())
    return node


def scan_tree_zipped(dir_path: str, rel: str,
                     stop: threading.Event, ui_queue) -> FolderNode:
    """Recursively build a FolderNode tree for Zipped mode (zip files only)."""
    node = FolderNode(name=os.path.basename(dir_path), relpath=rel)
    if stop.is_set():
        return node
    ui_queue.put(("scan", dir_path))
    try:
        entries = sorted(os.scandir(dir_path), key=lambda e: e.name.lower())
    except Exception:
        return node
    for entry in entries:
        if stop.is_set():
            break
        if is_hidden_or_system(entry.path) or entry.is_symlink():
            continue
        if entry.is_file(follow_symlinks=False) and entry.name.lower().endswith(".zip"):
            node.items.append(entry.path)
        elif entry.is_dir(follow_symlinks=False):
            child_rel = os.path.join(rel, entry.name) if rel else entry.name
            child = scan_tree_zipped(entry.path, child_rel, stop, ui_queue)
            node.subdirs.append(child)
    node.items.sort(key=lambda p: os.path.basename(p).lower())
    node.subdirs.sort(key=lambda n: n.name.lower())
    return node


def count_items(node: FolderNode) -> int:
    total = len(node.items)
    for s in node.subdirs:
        total += count_items(s)
    return total


def collect_all_items(node: FolderNode) -> List[str]:
    result = list(node.items)
    for s in node.subdirs:
        result.extend(collect_all_items(s))
    return result


# ═══════════════════════════════════════════════════════════════════════════
#  DAT WRITERS
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
#  DAT WRITERS — per_root  (5 structure options)
#
#  Option 1  – Dirs
#    All folders at every depth → <dir>.  Files → <rom>.  No <game> anywhere.
#
#  Option 2  – Archives as Games
#    Folder WITH direct files  → <game> + desc; subdirs merged (path/file in name).
#    Folder WITHOUT direct files (container) → <dir>; children processed same rule.
#
#  Option 3a – Archives as Games + First Level Dirs as Games
#    First-level subdirs → ALWAYS <game> + desc.
#    If game folder has files: files → <rom>, subdirs merged.
#    If game folder is a container: children processed as Archives as Games.
#
#  Option 3b – First Level Dirs as Games
#    First-level subdirs → ALWAYS <game> + desc.
#    If game folder has files: files → <rom>, subdirs merged.
#    If game folder is a container: children → <dir> (not game).
#
#  Option 5  – First Level Dirs as Games + Merge Dirs in Games
#    First-level subdirs → ALWAYS <game> + desc.
#    Direct files → <rom>.
#    All subdirs (game or container) → merged as flat roms with path prefix.
#    Each merged subdir gets an empty dir marker: <rom name="sub/" size="0" crc="00000000"/>
#
# ═══════════════════════════════════════════════════════════════════════════

# ── Shared atom helpers ─────────────────────────────────────────────────────

def _gtag(s: "Settings") -> str:
    """Return the game-level tag name based on settings."""
    if s.dat_format == "legacy":
        return "dir"
    return "machine" if s.use_machine else "game"


def _write_game_open(f, name: str, s: "Settings", depth: int) -> str:
    """Write opening game/dir tag + optional description. Returns tag used."""
    t  = "\t" * depth
    ti = "\t" * (depth + 1)
    tag = _gtag(s)
    f.write(f'{t}<{tag} name="{xa(name)}">\n')
    if tag != "dir" and s.incl_game_desc:
        f.write(f'{ti}<description>{xml_escape(name)}</description>\n')
    return tag


# ── Mixed atom helpers ──────────────────────────────────────────────────────

def _m_rom(f, item_path: str, prefix: str, data: dict, s: "Settings", indent: str):
    entry = data.get(item_path)
    if not entry:
        return
    size, crc, sha1, md5, sha256 = entry
    fname = os.path.basename(item_path)
    name  = f"{prefix}/{fname}" if prefix else fname
    f.write(f"{indent}{rom_line(name, size, crc, sha1, md5, sha256, None, s.include_md5, s.include_sha256, False)}\n")


def _m_merge(f, node: "FolderNode", data: dict, s: "Settings",
             prefix: str, indent: str):
    """Recursively flatten a Mixed subtree into path-prefixed <rom> entries."""
    for item in node.items:
        _m_rom(f, item, prefix, data, s, indent)
    for sub in node.subdirs:
        _m_merge(f, sub, data, s, f"{prefix}/{sub.name}" if prefix else sub.name, indent)


# ── Zipped atom helpers ─────────────────────────────────────────────────────

def _z_block(f, zip_path: str, data: dict, s: "Settings",
             depth: int, as_game: bool, name_override: str = ""):
    """Write one zip as a <game>/<dir> block containing its internal <rom> entries."""
    t    = "\t" * depth
    ti   = "\t" * (depth + 1)
    stem = os.path.splitext(os.path.basename(zip_path))[0]
    name = name_override if name_override else stem
    tag  = _gtag(s) if as_game else "dir"
    f.write(f'{t}<{tag} name="{xa(name)}">\n')
    if tag != "dir" and s.incl_game_desc:
        f.write(f'{ti}<description>{xml_escape(name)}</description>\n')
    for (rn, sz, crc, sha1, md5, sha256, ds) in data.get(zip_path, []):
        f.write(f"{ti}{rom_line(rn, sz, crc, sha1, md5, sha256, ds, s.include_md5, s.include_sha256, s.incl_file_date)}\n")
    f.write(f'{t}</{tag}>\n')


def _z_merge(f, node: "FolderNode", data: dict, s: "Settings",
             prefix: str, indent: str):
    """Recursively flatten a Zipped subtree into path-prefixed <rom> entries.
    Each zip's internal files appear as prefix/stem/internal_path."""
    for zp in node.items:
        stem = os.path.splitext(os.path.basename(zp))[0]
        p    = f"{prefix}/{stem}" if prefix else stem
        for (rn, sz, crc, sha1, md5, sha256, ds) in data.get(zp, []):
            f.write(f"{indent}{rom_line(f'{p}/{rn}', sz, crc, sha1, md5, sha256, ds, s.include_md5, s.include_sha256, s.incl_file_date)}\n")
    for sub in node.subdirs:
        _z_merge(f, sub, data, s, f"{prefix}/{sub.name}" if prefix else sub.name, indent)


# ── Option 1 — Dirs ─────────────────────────────────────────────────────────

def _write_mixed_opt1(f, node: "FolderNode", data: dict, s: "Settings", depth: int = 1):
    t  = "\t" * depth
    ti = "\t" * (depth + 1)
    # Root-level items wrapped in a game tag named after the folder (fix bare-rom bug)
    if depth == 1 and node.items:
        tag = _write_game_open(f, node.name, s, depth)
        for item in node.items:
            _m_rom(f, item, "", data, s, ti)
        f.write(f'{t}</{tag}>\n')
    else:
        for item in node.items:
            _m_rom(f, item, "", data, s, t)
    for sub in node.subdirs:
        f.write(f'{t}<dir name="{xa(sub.name)}">\n')
        _write_mixed_opt1(f, sub, data, s, depth + 1)
        f.write(f'{t}</dir>\n')


def _write_zipped_opt1(f, node: "FolderNode", data: dict, s: "Settings", depth: int = 1):
    t = "\t" * depth
    for zp in node.items:
        _z_block(f, zp, data, s, depth, as_game=False)
    for sub in node.subdirs:
        f.write(f'{t}<dir name="{xa(sub.name)}">\n')
        _write_zipped_opt1(f, sub, data, s, depth + 1)
        f.write(f'{t}</dir>\n')


# ── Option 2 — Archives as Games ─────────────────────────────────────────────

def _write_mixed_opt2_node(f, node: "FolderNode", data: dict,
                            s: "Settings", depth: int):
    """Recursively process one node per option 2 rules."""
    t  = "\t" * depth
    ti = "\t" * (depth + 1)
    if node.items:
        # Folder has direct files → game; merge all subdirs
        tag = _write_game_open(f, node.name, s, depth)
        for item in node.items:
            _m_rom(f, item, "", data, s, ti)
        for sub in node.subdirs:
            _m_merge(f, sub, data, s, sub.name, ti)
        f.write(f'{t}</{tag}>\n')
    else:
        # Container (no direct files) → dir; children processed same rule
        f.write(f'{t}<dir name="{xa(node.name)}">\n')
        for sub in node.subdirs:
            _write_mixed_opt2_node(f, sub, data, s, depth + 1)
        f.write(f'{t}</dir>\n')


def _write_mixed_opt2(f, node: "FolderNode", data: dict, s: "Settings", depth: int = 1):
    t  = "\t" * depth
    ti = "\t" * (depth + 1)
    # Root-level items wrapped in a game tag named after the folder (fix bare-rom bug)
    if depth == 1 and node.items:
        tag = _write_game_open(f, node.name, s, depth)
        for item in node.items:
            _m_rom(f, item, "", data, s, ti)
        f.write(f'{t}</{tag}>\n')
    else:
        for item in node.items:
            _m_rom(f, item, "", data, s, t)
    for sub in node.subdirs:
        _write_mixed_opt2_node(f, sub, data, s, depth)


def _write_zipped_opt2(f, node: "FolderNode", data: dict, s: "Settings", depth: int = 1):
    """
    Archives as Games -- fully recursive, correct at every depth.
    Every zip archive -> <game>.  Every physical filesystem dir -> <dir>.
    Internal zip paths (e.g. original/file.ima) are already preserved in rom
    names by analyze_zip and flow through _z_block unchanged.
    """
    t = "\t" * depth
    for zp in node.items:
        _z_block(f, zp, data, s, depth, as_game=True)
    for sub in node.subdirs:
        f.write(f'{t}<dir name="{xa(sub.name)}">\n')
        _write_zipped_opt2(f, sub, data, s, depth + 1)
        f.write(f'{t}</dir>\n')


# ── Option 3 — First Level Dirs as Games ─────────────────────────────────────

def _write_mixed_opt3(f, node: "FolderNode", data: dict, s: "Settings", depth: int = 1):
    t  = "\t" * depth
    ti = "\t" * (depth + 1)
    # Root-level items wrapped in a game tag named after the folder (fix bare-rom bug)
    if depth == 1 and node.items:
        tag = _write_game_open(f, node.name, s, depth)
        for item in node.items:
            _m_rom(f, item, "", data, s, ti)
        f.write(f'{t}</{tag}>\n')
    else:
        for item in node.items:
            _m_rom(f, item, "", data, s, t)
    for sub in node.subdirs:
        # First-level: always game
        tag = _write_game_open(f, sub.name, s, depth)
        if sub.items:
            # Has files: files as rom, subdirs merged (same as 3a for game folders)
            for item in sub.items:
                _m_rom(f, item, "", data, s, ti)
            for ssub in sub.subdirs:
                _m_merge(f, ssub, data, s, ssub.name, ti)
        else:
            # Container: children as <dir> (NOT game)
            for ssub in sub.subdirs:
                f.write(f'{ti}<dir name="{xa(ssub.name)}">\n')
                _write_mixed_opt1(f, ssub, data, s, depth + 2)
                f.write(f'{ti}</dir>\n')
        f.write(f'{t}</{tag}>\n')


def _write_zipped_opt3(f, node: "FolderNode", data: dict, s: "Settings", depth: int = 1):
    """
    First Level Dirs as Games (container dirs stay as <dir>).
    """
    t  = "\t" * depth
    ti = "\t" * (depth + 1)
    for zp in node.items:
        _z_block(f, zp, data, s, depth, as_game=True)
    for sub in node.subdirs:
        tag = _write_game_open(f, sub.name, s, depth)
        for zp in sub.items:
            _z_block(f, zp, data, s, depth + 1, as_game=True)
        for ssub in sub.subdirs:
            f.write(f'{ti}<dir name="{xa(ssub.name)}">\n')
            _write_zipped_opt2(f, ssub, data, s, depth + 2)
            f.write(f'{ti}</dir>\n')
        f.write(f'{t}</{tag}>\n')


# ── Option 5 — First Level Dirs as Games + Merge Dirs in Games ───────────────

def _write_mixed_opt4(f, node: "FolderNode", data: dict, s: "Settings", depth: int = 1):
    t  = "\t" * depth
    ti = "\t" * (depth + 1)
    # Root-level items wrapped in a game tag named after the folder (fix bare-rom bug)
    if depth == 1 and node.items:
        tag = _write_game_open(f, node.name, s, depth)
        for item in node.items:
            _m_rom(f, item, "", data, s, ti)
        f.write(f'{t}</{tag}>\n')
    else:
        for item in node.items:
            _m_rom(f, item, "", data, s, t)
    for sub in node.subdirs:
        # First-level: always game
        tag = _write_game_open(f, sub.name, s, depth)
        # Direct files of this game
        for item in sub.items:
            _m_rom(f, item, "", data, s, ti)
        # All subdirs merged: empty dir marker + path-prefixed roms
        for ssub in sub.subdirs:
            f.write(f'{ti}<rom name="{xa(ssub.name)}/" size="0" crc="00000000"/>\n')
            _m_merge(f, ssub, data, s, ssub.name, ti)
        f.write(f'{t}</{tag}>\n')


def _write_zipped_opt4(f, node: "FolderNode", data: dict, s: "Settings", depth: int = 1):
    t  = "\t" * depth
    ti = "\t" * (depth + 1)
    for zp in node.items:
        _z_block(f, zp, data, s, depth, as_game=True)
    for sub in node.subdirs:
        # First-level dir: always game
        tag = _write_game_open(f, sub.name, s, depth)
        # Direct zips of this subfolder
        for zp in sub.items:
            _z_block(f, zp, data, s, depth + 1, as_game=True)
        # Deeper subdirs merged: empty dir marker + path-prefixed content
        for ssub in sub.subdirs:
            f.write(f'{ti}<rom name="{xa(ssub.name)}/" size="0" crc="00000000"/>\n')
            _z_merge(f, ssub, data, s, ssub.name, ti)
        f.write(f'{t}</{tag}>\n')


# ── Dispatch table ───────────────────────────────────────────────────────────

_MIXED_WRITERS = {
    "opt1":  _write_mixed_opt1,
    "opt2":  _write_mixed_opt2,
    "opt3":  _write_mixed_opt3,
    "opt4":  _write_mixed_opt4,
}

_ZIPPED_WRITERS = {
    "opt1":  _write_zipped_opt1,
    "opt2":  _write_zipped_opt2,
    "opt3":  _write_zipped_opt3,
    "opt4":  _write_zipped_opt4,
}

# ── per_root dat wrapper (writes header + calls appropriate body writer) ────

def write_perroot_dat(dat_path: str, node: FolderNode,
                      data: dict, dat_name: str,
                      s: "Settings", header_date: str,
                      errors: list) -> None:
    writers = _MIXED_WRITERS if s.dat_type == "mixed" else _ZIPPED_WRITERS
    writer  = writers.get(s.structure, writers["opt2"])
    try:
        with open(dat_path, "w", encoding="utf-8", newline="\n") as f:
            write_dat_header(f, dat_name, s, header_date)
            writer(f, node, data, s)
            f.write('</datafile>\n')
    except Exception as e:
        errors.append(f"ERROR writing dat: {dat_path} :: {repr(e)}")


# ═══════════════════════════════════════════════════════════════════════════
#  DAT FILENAME HELPER
# ═══════════════════════════════════════════════════════════════════════════

def make_dat_name(folder_name: str, input_root: str, s: "Settings") -> str:
    """
    Build the <n> header value and dat filename stem.
    Always includes the top-level input folder name between parent and subfolder.
    e.g. parent="Commodore", root="C16-C116-Plus4", folder="[ARK]"
         -> "Commodore - C16-C116-Plus4 - [ARK]"
    """
    root_name = os.path.basename(os.path.normpath(input_root))
    parts = []
    if s.parent_name.strip():
        parts.append(s.parent_name.strip())
    parts.append(root_name)
    parts.append(folder_name)
    return " - ".join(parts)

def make_dat_filename(dat_name: str, header_date: str,
                      incomplete: bool = False) -> str:
    ext = ".xml" if True else ".dat"   # always .xml for Logiqx
    prefix = "[INCOMPLETE] " if incomplete else ""
    return f"{prefix}{safe_filename(dat_name)} ({header_date}_RomVault).xml"


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN WORKER
# ═══════════════════════════════════════════════════════════════════════════

def process(s: "Settings", ui_queue,
            soft_stop: threading.Event,
            hard_stop: threading.Event,
            preview_results: Optional[list] = None) -> None:
    """
    Unified processing engine for all dat types and generation modes.
    preview_results: if provided, completed PreviewEntry objects are appended.
    """
    start_time  = time.time()
    errors: list = []
    input_root  = os.path.abspath(s.input_root)
    output_root = os.path.abspath(s.output_root)
    header_date = s.date or datetime.date.today().isoformat()
    is_mixed    = (s.dat_type == "mixed")
    is_perroot  = (s.gen_mode == "per_root")

    err = safe_makedirs(output_root)
    if err:
        ui_queue.put(("done", False, [f"Cannot create output root: {err}"],
                      0, 0, 0, 0.0, ""))
        return

    max_workers = max(1, min(8, int(s.threads))) if s.multithread else 1

    # ── Network bandwidth throttle ───────────────────────────────────────────
    # Target: leave ~15% of NIC capacity free for other processes.
    if s.net_cap_mbps > 0:
        _net_rate = s.net_cap_mbps * 1_000_000 / 8   # Mbit/s → bytes/s
    else:
        _net_rate = _detect_net_cap_bytes_per_sec(pct=0.85)
    throttle = _BandwidthThrottle(_net_rate)
    if _net_rate > 0:
        _cap_mbs = _net_rate / (1_000_000 / 8)
        ui_queue.put(("status",
            "Network cap: " + f"{_cap_mbs:.0f}" + " Mbit/s  ("
            + ("auto-detected" if s.net_cap_mbps == 0 else "manual") + ")"
            + "  |  BytesIO threshold: " + str(BYTESIO_THRESHOLD // (1024*1024)) + " MB"
            + "  |  Large-zip serialised (SMB lock active)"))
    else:
        ui_queue.put(("status",
            "Network cap: unlimited  (psutil not installed or NIC undetected)"
            + "  |  BytesIO threshold: " + str(BYTESIO_THRESHOLD // (1024*1024)) + " MB"
            + "  |  Large-zip serialised (SMB lock active)"))

    # ── Discover immediate children of input_root ────────────────────────
    ui_queue.put(("status", "Phase 1 of 2 — Discovering folders and files... (please wait)"))
    try:
        top_entries = sorted(os.scandir(input_root), key=lambda e: e.name.lower())
    except Exception as e:
        ui_queue.put(("done", False, [f"Cannot scan input: {repr(e)}"],
                      0, 0, 0, 0.0, ""))
        return

    # ── Build job list ───────────────────────────────────────────────────
    # Each job = (folder_path, FolderNode, output_dir)
    # For per_root: one job per immediate subfolder (recursive content inside)
    # For per_all:  one job per every folder that has content at any depth

    jobs: List[Tuple[str, "FolderNode", str]] = []

    # Parse extension filters once (Mixed-only; ignored for Zipped where
    # the item is always the whole .zip)
    ext_inc = parse_ext_list(s.ext_include) if is_mixed else set()
    ext_exc = parse_ext_list(s.ext_exclude) if is_mixed else set()

    # Output always mirrors input structure under a folder named after input_root.
    # e.g. output_root / "Access Software PC Floppy Disk Image Collection" / "Docs"
    root_folder_name = os.path.basename(input_root)
    root_out_base    = os.path.join(output_root, root_folder_name)

    # ── Root-level items (files/zips sitting directly in input_root) ─────────
    # These form one extra job representing the collection root itself.
    root_items = []
    for entry in top_entries:
        if is_hidden_or_system(entry.path) or entry.is_symlink():
            continue
        if entry.is_file(follow_symlinks=False):
            if is_mixed:
                if not (ext_inc or ext_exc) or file_matches_filter(
                        entry.name, ext_inc, ext_exc):
                    root_items.append(entry.path)
            elif entry.name.lower().endswith(".zip"):
                root_items.append(entry.path)

    if root_items:
        root_items.sort(key=lambda p: os.path.basename(p).lower())
        root_node = FolderNode(name=root_folder_name, relpath="")
        root_node.items = root_items
        jobs.append((input_root, root_node, root_out_base))

    # ── Subfolders ───────────────────────────────────────────────────────────
    for entry in top_entries:
        if hard_stop.is_set():
            break
        if is_hidden_or_system(entry.path) or entry.is_symlink():
            continue
        if not entry.is_dir(follow_symlinks=False):
            continue

        folder_path = entry.path
        folder_name = entry.name

        if is_perroot:
            if is_mixed:
                node = scan_tree_mixed(folder_path, "", hard_stop, ui_queue,
                                        ext_inc, ext_exc)
            else:
                node = scan_tree_zipped(folder_path, "", hard_stop, ui_queue)
            if count_items(node) == 0:
                continue
            out_dir = os.path.join(root_out_base, folder_name)
            jobs.append((folder_path, node, out_dir))
        else:
            # per_all: one dat per folder that has direct content
            def collect_perall_jobs(dir_path, rel_from_root):
                if hard_stop.is_set():
                    return
                ui_queue.put(("scan", dir_path))
                shallow = FolderNode(
                    name=os.path.basename(dir_path),
                    relpath=rel_from_root)
                try:
                    entries = sorted(os.scandir(dir_path),
                                     key=lambda e: e.name.lower())
                except Exception:
                    return
                for entry2 in entries:
                    if hard_stop.is_set():
                        break
                    if is_hidden_or_system(entry2.path) or entry2.is_symlink():
                        continue
                    if is_mixed:
                        if entry2.is_file(follow_symlinks=False):
                            if ext_inc or ext_exc:
                                if not file_matches_filter(entry2.name,
                                                            ext_inc, ext_exc):
                                    continue
                            shallow.items.append(entry2.path)
                    else:
                        if (entry2.is_file(follow_symlinks=False) and
                                entry2.name.lower().endswith(".zip")):
                            shallow.items.append(entry2.path)
                shallow.items.sort(key=lambda p: os.path.basename(p).lower())
                if shallow.items:
                    out_dir = os.path.join(root_out_base, rel_from_root)
                    jobs.append((dir_path, shallow, out_dir))
                for entry2 in entries:
                    if hard_stop.is_set():
                        break
                    if is_hidden_or_system(entry2.path) or entry2.is_symlink():
                        continue
                    if entry2.is_dir(follow_symlinks=False):
                        child_rel = os.path.join(rel_from_root, entry2.name)
                        collect_perall_jobs(entry2.path, child_rel)

            collect_perall_jobs(folder_path, folder_name)

    if hard_stop.is_set():
        ui_queue.put(("done", False, ["Hard stop during scan."],
                      0, 0, 0, time.time()-start_time, ""))
        return
    if soft_stop.is_set():
        ui_queue.put(("done", False, ["Soft stop during scan."],
                      0, 0, 0, time.time()-start_time, ""))
        return
    if not jobs:
        ui_queue.put(("done", False, ["No content found in input folder."],
                      0, 0, 0, time.time()-start_time, ""))
        return

    # Count total items across all jobs
    total_items = sum(count_items(j[1]) for j in jobs)
    total_jobs  = len(jobs)
    ui_queue.put(("totals", total_jobs, total_items))
    ui_queue.put(("status",
                  f"Phase 1 complete — found {total_jobs} folder(s), "
                  f"{total_items} item(s) to process."))
    ui_queue.put(("status",
                  f"Phase 2 of 2 — Hashing and writing dat files..."))

    done_items   = 0
    written_dats = 0
    total_carried = 0
    total_hashed  = 0

    # ── Build incremental index map (if incremental mode) ────────────────
    # Maps out_dir → (game_index, existing_dat_path) for each job
    incr_index_map: dict = {}
    if s.incremental and s.incremental_dat_path:
        dat_src = os.path.abspath(s.incremental_dat_path)
        if os.path.isfile(dat_src):
            # Single dat file → applies to first job only (per_root single folder)
            gi, hd, err_s = _read_dat_index(dat_src)
            if err_s:
                errors.append(f"Could not read dat: {dat_src} :: {err_s}")
            else:
                # Map it to the first job's out_dir
                if jobs:
                    first_out = jobs[0][2]
                    incr_index_map[first_out] = (gi, dat_src)
        elif os.path.isdir(dat_src):
            # Recursively scan dat source folder for all .xml files.
            # Primary match: mirror relative path from dat_src onto jobs.
            #   dat at dats/Activision PC.../foo.xml
            #   → job whose folder_path ends with /Activision PC.../
            # Fallback: match by <n> header vs make_dat_name().

            # Build lookup: normcase(folder_path) -> (fp, nd, od)
            job_folderpath_map = {}
            job_name_map       = {}
            for fp, nd, od in jobs:
                job_folderpath_map[os.path.normcase(fp)] = (fp, nd, od)
                expected = make_dat_name(os.path.basename(fp), input_root, s)
                job_name_map[expected] = (fp, nd, od)

            for dirpath, _, filenames in os.walk(dat_src):
                for fn in sorted(filenames):
                    if not (fn.lower().endswith(".xml") or fn.lower().endswith(".dat")):
                        continue
                    full = os.path.join(dirpath, fn)
                    gi, hd, err_s = _read_dat_index(full)
                    if err_s:
                        errors.append("Could not read dat: " + full + " :: " + err_s)
                        continue

                    matched = False

                    # Primary: mirror relative path onto input_root
                    try:
                        rel_dir = os.path.relpath(dirpath, dat_src)
                    except ValueError:
                        rel_dir = "."
                    if rel_dir and rel_dir != ".":
                        # The dat lives in dats/RelDir/foo.xml
                        # → corresponding source folder is input_root/RelDir
                        candidate_fp = os.path.join(input_root, rel_dir)
                        key = os.path.normcase(candidate_fp)
                        pair = job_folderpath_map.get(key)
                        if pair:
                            _, _, out_dir = pair
                            incr_index_map[out_dir] = (gi, full)
                            matched = True

                    # Fallback: match by name header
                    if not matched:
                        dat_name_hdr = hd.get("name", "")
                        if dat_name_hdr:
                            pair = job_name_map.get(dat_name_hdr)
                            if pair:
                                _, _, out_dir = pair
                                incr_index_map[out_dir] = (gi, full)

    # ── Process each job ─────────────────────────────────────────────────
    for folder_path, node, out_dir in jobs:
        if hard_stop.is_set():
            break

        folder_name = os.path.basename(folder_path)
        dat_name    = make_dat_name(folder_name, input_root, s)
        items       = collect_all_items(node) if is_perroot else node.items

        ui_queue.put(("folder", folder_path, len(items)))

        err = safe_makedirs(out_dir)
        if err:
            errors.append(f"ERROR creating output folder: {out_dir} :: {err}")
            continue

        # ── Incremental: look up existing dat index for this job ─────────
        job_game_index: dict = {}
        job_existing_dat: str = ""
        if s.incremental:
            pair = incr_index_map.get(out_dir)
            if pair:
                job_game_index, job_existing_dat = pair
            else:
                ui_queue.put(("status",
                    f"No existing dat found for {folder_name} — full hash."))

        # ── Hash / analyze all items ──────────────────────────────────
        data: dict = {}   # item_path → hash result tuple
        incomplete = False

        if s.incremental and job_game_index:
            # Incremental mode: carry forward unchanged items, hash only new/changed
            data, done_items, job_carried, job_hashed, job_errs = \
                build_incremental_data(items, job_game_index, s,
                                       hard_stop, ui_queue, done_items,
                                       throttle=throttle)
            errors.extend(job_errs)
            total_carried += job_carried
            total_hashed  += job_hashed
            if hard_stop.is_set():
                incomplete = True
        else:
            # Full hash mode
            # ── Sort items by (parent_dir, basename) so items from the same
            # subfolder are processed consecutively.  This lets us emit
            # "subfolder" log markers, and also improves cache locality.
            items = sorted(items,
                           key=lambda p: (os.path.dirname(p).lower(),
                                          os.path.basename(p).lower()))

            def do_mixed(fp):
                return fp, hash_file(fp, s.include_md5,
                                      s.include_sha256,
                                      hard_stop,
                                      throttle=throttle), None, ""

            def do_zipped(zp):
                res_list, diag = analyze_zip(zp, s.include_md5,
                                              s.include_sha256,
                                              s.incl_file_date,
                                              hard_stop, s.sevenzip_path,
                                              throttle=throttle)
                return zp, res_list, None, diag

            work_fn = do_mixed if is_mixed else do_zipped

            def safe_work(item_path):
                try:
                    p, result, _, diag = work_fn(item_path)
                    return p, result, None, diag
                except Exception as exc:
                    msg = str(exc)
                    return item_path, None, ("CANCELLED" if "CANCELLED" in msg else repr(exc)), ""

            # Helper: emit a subfolder header when parent dir changes
            _last_subdir = [None]
            def _maybe_emit_subfolder(item_path):
                parent = os.path.dirname(item_path)
                if parent != _last_subdir[0]:
                    _last_subdir[0] = parent
                    try:
                        rel = os.path.relpath(parent, folder_path)
                    except ValueError:
                        rel = parent
                    ui_queue.put(("subfolder", rel))

            if max_workers == 1:
                for item in items:
                    if hard_stop.is_set():
                        incomplete = True; break
                    bname = os.path.basename(item)
                    _maybe_emit_subfolder(item)
                    _, result, err_s, diag = safe_work(item)
                    if err_s == "CANCELLED":
                        incomplete = True; break
                    done_items += 1
                    ui_queue.put(("progress", done_items))
                    if done_items % 100 == 0:
                        gc.collect()   # prevent heap growth from accumulated zlib/zipfile objects
                    if err_s or result is None:
                        err_detail = err_s or "unknown error"
                        errors.append("ERROR: " + item + " :: " + err_detail)
                        ui_queue.put(("item_error", bname, err_detail))
                        continue
                    log_detail = ("  (" + diag + ")") if diag else ""
                    ui_queue.put(("item_hashed", bname, log_detail))
                    data[item] = result
            else:
                # Non-blocking executor — do NOT use context manager so Hard Stop
                # can escape even if worker threads are blocked on network I/O.
                # Items are pre-sorted by directory so subfolder markers stay
                # roughly aligned with their zip entries even in async completion.
                ex = ThreadPoolExecutor(max_workers=max_workers)
                try:
                    fmap    = {ex.submit(safe_work, it): it for it in items}
                    pending = set(fmap.keys())
                    # Build dir→item mapping for subfolder tracking by completion
                    item_dir = {it: os.path.dirname(it) for it in items}
                    while pending:
                        if hard_stop.is_set():
                            incomplete = True
                            for ft in pending: ft.cancel()
                            break
                        done_set, pending = _cf_wait(
                            pending, timeout=0.5,
                            return_when=FIRST_COMPLETED)
                        for fut in done_set:
                            item  = fmap[fut]
                            bname = os.path.basename(item)
                            _maybe_emit_subfolder(item)
                            try:
                                _, result, err_s, diag = fut.result()
                            except Exception as exc:
                                result, err_s, diag = None, repr(exc), ""
                            if err_s == "CANCELLED":
                                incomplete = True
                                for ft in pending: ft.cancel()
                                pending = set()
                                break
                            done_items += 1
                            ui_queue.put(("progress", done_items))
                            if done_items % 100 == 0:
                                gc.collect()
                            if err_s or result is None:
                                err_detail = err_s or "unknown error"
                                errors.append("ERROR: " + item + " :: " + err_detail)
                                ui_queue.put(("item_error", bname, err_detail))
                                continue
                            log_detail = ("  (" + diag + ")") if diag else ""
                            ui_queue.put(("item_hashed", bname, log_detail))
                            data[item] = result
                finally:
                    ex.shutdown(wait=False)   # release threads immediately on hard stop

        # ── Write dat ─────────────────────────────────────────────────
        dat_filename = make_dat_filename(dat_name, header_date, incomplete)
        dat_path     = os.path.join(out_dir, dat_filename)

        if len(dat_path) >= MAX_SAFE_PATH:
            errors.append(f"PATH LENGTH WARNING ({len(dat_path)}): {dat_path}")

        # Both per_root and per_all use the same writers — per_all nodes are
        # shallow (no subdirs) so the 5 structure options produce equivalent
        # flat output. Per_root nodes are full trees and use the chosen structure.
        write_perroot_dat(dat_path, node, data, dat_name, s, header_date, errors)

        if os.path.isfile(dat_path):
            written_dats += 1
            ui_queue.put(("dat_written", dat_path, written_dats))

            # Incremental: optionally rename original to .old
            if (s.incremental and s.retire_old_dats
                    and job_existing_dat and os.path.isfile(job_existing_dat)):
                old_final, rename_err = retire_old_dat(job_existing_dat)
                if rename_err:
                    errors.append(rename_err)
                else:
                    ui_queue.put(("status",
                        f"Retired: {os.path.basename(old_final)}"))

        # Store for preview window (only if not incomplete)
        if preview_results is not None and not incomplete and data:
            import copy as _copy
            preview_results.append(PreviewEntry(
                dat_name    = dat_name,
                header_date = header_date,
                node        = node,
                data        = data,
                settings    = Settings.from_dict(s.to_dict()),
                is_perroot  = is_perroot,
            ))

        if incomplete:
            errors.append(f"INCOMPLETE dat: {folder_path}")
            break
        if soft_stop.is_set() and not hard_stop.is_set():
            break

    elapsed = time.time() - start_time
    ok = not hard_stop.is_set() and not soft_stop.is_set()
    if s.incremental and (total_carried + total_hashed) > 0:
        ui_queue.put(("status",
            f"Incremental summary: {total_carried} carried, "
            f"{total_hashed} hashed, {done_items} total items."))
    ui_queue.put(("done", ok, errors, done_items, total_items,
                  written_dats, elapsed, ""))


# ═══════════════════════════════════════════════════════════════════════════
#  GUI
# ═══════════════════════════════════════════════════════════════════════════

class App:
    def __init__(self, root: TkinterDnD.Tk):
        self.root        = root
        self.root.title("Eggman's Datfile Creator Suite")
        self.root.geometry("920x800+125+125")
        self.root.minsize(920, 800)

        # ── Hard dependency check — must happen before building any UI ────────
        if not _DEPS_OK:
            self.root.withdraw()
            messagebox.showerror(
                "Missing required packages",
                "The following pip packages are required and must be installed "
                "before running this application:\n\n"
                "  pip install zstandard zipfile-zstd\n\n"
                "Error detail:\n  " + _DEPS_ERROR,
                parent=self.root)
            self.root.after(100, self.root.destroy)
            return

        self.settings    = load_settings()
        self.ui_queue    = queue.Queue()
        self.worker: Optional[threading.Thread] = None
        self.soft_stop   = threading.Event()
        self.hard_stop   = threading.Event()

        # Counters
        self.total_items  = 0
        self.done_items   = 0
        self.total_jobs   = 0
        self.dats_written = 0
        self._log_lines: List[str] = []
        self._preview_results: List = []   # PreviewEntry objects from last run

        self._apply_theme()
        self._build_ui()
        self._apply_settings()
        self._setup_dnd()
        self._poll_queue()

    # ── Theme ────────────────────────────────────────────────────────────────

    def _apply_theme(self):
        FG  = "#2A2A2A"
        SEP = "#B8B0A4"

        # Section background palette
        C_PATHS    = "#D6E8F5"   # sky blue   — input/output paths
        C_HEADER   = "#F5F0D6"   # warm amber — dat header fields
        C_OPTIONS  = "#D6EDD6"   # sage green — dat type / structure / options
        C_BUTTONS  = "#DDD8CE"   # warm grey  — action buttons bar
        C_PROGRESS = "#E8D6F0"   # lavender   — progress + activity log
        C_LIST     = "#F3EEF8"   # pale lav.  — listbox background
        C_ENTRY    = "#F7F4EF"   # off-white  — entry field background
        SEL_BG     = "#C8B89A"   # warm sand  — selection highlight

        self.root.configure(bg="#EDE8E0")
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        # Base
        style.configure(".",
            background="#EDE8E0", foreground=FG,
            fieldbackground=C_ENTRY, selectbackground=SEL_BG,
            selectforeground=FG, troughcolor="#EDE8E0",
            bordercolor=SEP, darkcolor=SEP, lightcolor="#EDE8E0",
            focuscolor=SEL_BG)

        style.configure("TEntry",
            fieldbackground=C_ENTRY, foreground=FG,
            insertcolor=FG, selectbackground=SEL_BG)
        style.configure("TSpinbox",
            fieldbackground=C_ENTRY, foreground=FG,
            selectbackground=SEL_BG, arrowcolor=FG)
        style.configure("TSeparator", background=SEP)
        style.configure("Horizontal.TProgressbar",
            troughcolor="#D4CECA", background="#8A7A6A",
            bordercolor=SEP, darkcolor="#8A7A6A", lightcolor="#8A7A6A")

        # Per-section widget styles
        for sec, bg in [
            ("Paths",    C_PATHS),
            ("Header",   C_HEADER),
            ("Options",  C_OPTIONS),
            ("Buttons",  C_BUTTONS),
            ("Progress", C_PROGRESS),
        ]:
            style.configure(f"{sec}.TFrame",
                background=bg)
            style.configure(f"{sec}.TLabelframe",
                background=bg, bordercolor=SEP,
                darkcolor=SEP, lightcolor=bg, relief="groove")
            style.configure(f"{sec}.TLabelframe.Label",
                background=bg, foreground=FG, font=("Segoe UI", 9, "bold"))
            style.configure(f"{sec}.TLabel",
                background=bg, foreground=FG)
            style.configure(f"{sec}.TCheckbutton",
                background=bg, foreground=FG,
                indicatorbackground=C_ENTRY, indicatorforeground=FG)
            style.configure(f"{sec}.TRadiobutton",
                background=bg, foreground=FG,
                indicatorbackground=C_ENTRY)
            style.map(f"{sec}.TCheckbutton",
                background=[("active", bg), ("!active", bg)])
            style.map(f"{sec}.TRadiobutton",
                background=[("active", bg), ("!active", bg)])

        # Named button styles
        for name, color, hover in [
            ("Start",    "#A8D8A8", "#80C080"),
            ("SoftStop", "#F5D78E", "#D8B860"),
            ("HardStop", "#F5A8A8", "#D88080"),
            ("Preview",  "#A8C8E8", "#80A8D0"),
            ("Save",     "#C8A8D8", "#A880C0"),
            ("Log",      "#A8D8D0", "#80C0B8"),
            ("Browse",   "#D4CEC8", "#B8B0A4"),
            ("About",    "#C8D8A8", "#A8C080"),
        ]:
            style.configure(f"{name}.TButton",
                background=color, foreground=FG,
                bordercolor=SEP, relief="flat", padding=(6, 3))
            style.map(f"{name}.TButton",
                background=[
                    ("active",   hover),
                    ("pressed",  hover),
                    ("disabled", "#C8C4BF")])

        # Incremental section — coral/warm orange to stand out
        C_INCR = "#F5DDD6"
        style.configure("Incremental.TLabelframe",
            background=C_INCR, bordercolor=SEP,
            darkcolor=SEP, lightcolor=C_INCR, relief="groove")
        style.configure("Incremental.TLabelframe.Label",
            background=C_INCR, foreground=FG, font=("Segoe UI", 9, "bold"))
        style.configure("Incremental.TLabel",
            background=C_INCR, foreground=FG)
        style.configure("Incremental.TFrame",
            background=C_INCR)
        style.configure("IncrementalBold.TCheckbutton",
            background=C_INCR, foreground=FG,
            font=("Segoe UI", 9, "bold"),
            indicatorbackground=C_ENTRY, indicatorforeground=FG)
        style.map("IncrementalBold.TCheckbutton",
            background=[("active", C_INCR), ("!active", C_INCR)])
        style.configure("Incremental.TCheckbutton",
            background=C_INCR, foreground=FG,
            indicatorbackground=C_ENTRY, indicatorforeground=FG)
        style.map("Incremental.TCheckbutton",
            background=[("active", C_INCR), ("!active", C_INCR)])

        # Store for manual widget access
        self._c = {
            "fg": FG, "entry": C_ENTRY, "sel": SEL_BG, "sep": SEP,
            "paths": C_PATHS, "header": C_HEADER, "options": C_OPTIONS,
            "buttons": C_BUTTONS, "progress": C_PROGRESS,
            "list": C_LIST, "incr": C_INCR,
        }
        # Keep legacy refs for Listbox (tk widget, needs explicit colors)
        self._list_bg  = C_LIST
        self._list_fg  = FG
        self._list_sel = SEL_BG

    # ── Menu bar ─────────────────────────────────────────────────────────────

    def _build_menu(self):
        menubar = tk.Menu(self.root, bg="#EDE8E0", fg=self._c["fg"],
                          activebackground=self._c["sel"],
                          activeforeground=self._c["fg"],
                          relief="flat", bd=0)
        tools_menu = tk.Menu(menubar, tearoff=0,
                             bg="#EDE8E0", fg=self._c["fg"],
                             activebackground=self._c["sel"],
                             activeforeground=self._c["fg"])
        tools_menu.add_command(
            label="Analyze Folder Structure...",
            command=self.open_analyzer)
        tools_menu.add_separator()
        tools_menu.add_command(
            label="Bulk Datfile Header Updater...",
            command=self.open_bulk_header_updater)
        tools_menu.add_separator()
        tools_menu.add_command(
            label="Game and ROM Counter...",
            command=self.open_game_rom_counter)
        tools_menu.add_separator()
        tools_menu.add_command(
            label="Recursive Archive Extractor...",
            command=self.open_archive_extractor)
        tools_menu.add_command(
            label="ZIP Store Packer...",
            command=self.open_zip_packer)
        tools_menu.add_command(
            label="Remove ReadOnly Attribute...",
            command=self.open_remove_readonly)
        menubar.add_cascade(label="Tools", menu=tools_menu)

        help_menu = tk.Menu(menubar, tearoff=0,
                            bg="#EDE8E0", fg=self._c["fg"],
                            activebackground=self._c["sel"],
                            activeforeground=self._c["fg"])
        help_menu.add_command(label="README / Documentation",
                              command=self.open_readme_link)
        help_menu.add_separator()
        help_menu.add_command(label="About Eggman's Datfile Creator Suite",
                              command=self.show_about)
        menubar.add_cascade(label="Help", menu=help_menu)
        self.root.config(menu=menubar)

    # ── About window ─────────────────────────────────────────────────────────

    def open_analyzer(self):
        AnalyzerWindow(self.root, self)

    def open_bulk_header_updater(self):
        BulkHeaderUpdaterWindow(self.root, self)

    def open_game_rom_counter(self):
        GameRomCounterWindow(self.root, self)

    def open_archive_extractor(self):
        RecursiveArchiveExtractorWindow(self.root, self)

    def open_zip_packer(self):
        ZipStorePackerWindow(self.root, self)

    def open_remove_readonly(self):
        RemoveReadOnlyWindow(self.root, self)

    def show_about(self):
        win = tk.Toplevel(self.root)
        win.title("About — Eggman's Datfile Creator Suite")
        win.resizable(False, False)
        win.configure(bg=self._c["paths"])
        win.grab_set()

        # Banner image (optional — silently skipped if file not found)
        banner_path = resource_path("Eggmans_Datfile_Creator_Suite_banner.png")
        if os.path.isfile(banner_path):
            try:
                img = tk.PhotoImage(file=banner_path)
                lbl_img = tk.Label(win, image=img, bg=self._c["paths"], bd=0)
                lbl_img.image = img   # keep reference
                lbl_img.pack(pady=(0, 0))
            except Exception:
                pass

        # Text content — centered
        content = tk.Frame(win, bg=self._c["paths"], padx=30, pady=16)
        content.pack(fill="x")

        def cline(text, size=9, bold=False, color=None):
            font = ("Segoe UI", size, "bold" if bold else "normal")
            tk.Label(content, text=text,
                     font=font, bg=self._c["paths"],
                     fg=color or self._c["fg"]).pack(pady=1)

        cline("Eggman's Datfile Creator Suite", size=13, bold=True)
        cline("")
        cline("Created by Eggman for the benefit of the users", size=9)
        cline("of the preservation community.", size=9)
        cline("")
        cline("Developed in Python with assistance from", size=9)
        cline("Anthropic's Claude AI.", size=9)
        cline("")

        # Clickable GitHub link
        gh_url  = "https://github.com/Eggmansworld"
        lbl_url = tk.Label(content, text=gh_url,
                           font=("Segoe UI", 9, "underline"),
                           bg=self._c["paths"], fg="#1155CC",
                           cursor="hand2")
        lbl_url.pack(pady=(0, 4))
        lbl_url.bind("<Button-1>",
                     lambda e: __import__("webbrowser").open(gh_url))

        cline("")
        ttk.Button(content, text="Close", style="Browse.TButton",
                   command=win.destroy).pack(pady=(4, 0))

    def open_readme_link(self):
        import webbrowser
        webbrowser.open(
            "https://github.com/Eggmansworld/datfile_creator/"
            "blob/main/README.md")

    # ── UI construction ─────────────────────────────────────────────────────

    def _build_ui(self):
        self._build_menu()

        PAD = 6   # outer padding inside each section

        outer = ttk.Frame(self.root)
        outer.pack(fill="both", expand=True, padx=6, pady=4)
        outer.columnconfigure(0, weight=1)

        row = 0

        # ── PATHS section ────────────────────────────────────────────────────
        pf = ttk.LabelFrame(outer, text="  Paths  ",
                             style="Paths.TLabelframe", padding=PAD)
        pf.grid(row=row, column=0, sticky="ew", pady=(0, 4))
        pf.columnconfigure(1, weight=1)

        def path_row(parent, r, label, var_name, btn_cmd, btn_text="Browse..."):
            ttk.Label(parent, text=label,
                      style="Paths.TLabel").grid(row=r, column=0, sticky="w")
            v = tk.StringVar()
            setattr(self, var_name, v)
            ent = ttk.Entry(parent, textvariable=v)
            ent.grid(row=r, column=1, sticky="ew", padx=(4, 4))
            ttk.Button(parent, text=btn_text, style="Browse.TButton",
                       command=btn_cmd).grid(row=r, column=2, sticky="ew")
            return ent

        self.ent_input   = path_row(pf, 0, "Input top-level folder:",
                                     "var_input",   self.browse_input)
        self.ent_output  = path_row(pf, 1, "Output folder (dat root):",
                                     "var_output",  self.browse_output)

        ttk.Label(pf, text="Parent name (optional prefix):",
                  style="Paths.TLabel").grid(row=2, column=0, sticky="w")
        self.var_parent = tk.StringVar()
        ttk.Entry(pf, textvariable=self.var_parent).grid(
            row=2, column=1, sticky="ew", padx=(4, 4))
        ttk.Label(pf,
                  text='e.g. "MyName" → "MyName - FolderName"',
                  style="Paths.TLabel",
                  foreground="#5A6070").grid(row=2, column=2, sticky="w")

        self.ent_sevenzip = path_row(pf, 3, "7-Zip-ZStandard (7z.exe):",
                                      "var_sevenzip", self.browse_sevenzip)

        row += 1

        # ── HEADER section ───────────────────────────────────────────────────
        hf = ttk.LabelFrame(outer, text="  DAT Header  ",
                             style="Header.TLabelframe", padding=PAD)
        hf.grid(row=row, column=0, sticky="ew", pady=(0, 4))
        hf.columnconfigure(1, weight=1)

        def hdr_row(parent, r, label, var_name, hint="(blank = omit)"):
            ttk.Label(parent, text=label,
                      style="Header.TLabel").grid(row=r, column=0, sticky="w")
            v = tk.StringVar()
            setattr(self, var_name, v)
            ttk.Entry(parent, textvariable=v).grid(
                row=r, column=1, sticky="ew", padx=(4, 4))
            ttk.Label(parent, text=hint,
                      style="Header.TLabel",
                      foreground="#7A7050").grid(row=r, column=2, sticky="w")

        hdr_row(hf, 0, "Description:", "var_description", "")
        hdr_row(hf, 1, "Category:",    "var_category")
        hdr_row(hf, 2, "Version:",     "var_version")
        hdr_row(hf, 3, "Date (yyyy-mm-dd):", "var_date", "")
        hdr_row(hf, 4, "Author:",      "var_author", "")
        hdr_row(hf, 5, "URL:",         "var_url")
        hdr_row(hf, 6, "Homepage:",    "var_homepage")
        hdr_row(hf, 7, "Comment:",     "var_comment")

        row += 1

        # ── OPTIONS section ──────────────────────────────────────────────────
        of = ttk.LabelFrame(outer, text="  Dat Settings  ",
                             style="Options.TLabelframe", padding=PAD)
        of.grid(row=row, column=0, sticky="ew", pady=(0, 4))
        of.columnconfigure(1, weight=1)

        # Dat Type
        self.var_dat_type = tk.StringVar(value="mixed")
        ttk.Label(of, text="Dat type:",
                  style="Options.TLabel").grid(row=0, column=0, sticky="w", padx=(0,8))
        rf = ttk.Frame(of, style="Options.TFrame")
        rf.grid(row=0, column=1, columnspan=2, sticky="w")
        for txt, val in [("Mixed (Archive as File)", "mixed"), ("Zipped", "zipped")]:
            ttk.Radiobutton(rf, text=txt, style="Options.TRadiobutton",
                            variable=self.var_dat_type, value=val,
                            command=self._on_options_change).pack(
                side="left", padx=(0, 18))

        # Generation
        self.var_gen_mode = tk.StringVar(value="per_root")
        ttk.Label(of, text="Generation:",
                  style="Options.TLabel").grid(row=1, column=0, sticky="w",
                                               padx=(0,8), pady=(4,0))
        gf2 = ttk.Frame(of, style="Options.TFrame")
        gf2.grid(row=1, column=1, columnspan=2, sticky="w", pady=(4,0))
        for txt, val in [("1 dat per root folder", "per_root"),
                         ("1 dat per root folder & all subfolders", "per_all")]:
            ttk.Radiobutton(gf2, text=txt, style="Options.TRadiobutton",
                            variable=self.var_gen_mode, value=val,
                            command=self._on_options_change).pack(
                side="left", padx=(0, 18))

        # Structure + help link
        self.var_structure = tk.StringVar(value="opt2")
        ttk.Label(of, text="Structure:",
                  style="Options.TLabel").grid(row=2, column=0, sticky="nw",
                                               padx=(0,8), pady=(4,0))
        sf2 = ttk.Frame(of, style="Options.TFrame")
        sf2.grid(row=2, column=1, columnspan=2, sticky="w", pady=(4,0))
        self._struct_rbs = []
        # 2×2 grid: two rows, two columns
        structs = [
            ("opt1",  "1 — Dirs"),
            ("opt2",  "2 — Archives as Games"),
            ("opt3",  "3 — First Level Dirs as Games"),
            ("opt4",  "4 — First Level Dirs as Games + Merge Dirs in Games"),
        ]
        sf2.columnconfigure(0, weight=1)
        sf2.columnconfigure(1, weight=1)
        for idx, (val, lbl) in enumerate(structs):
            rb = ttk.Radiobutton(sf2, text=lbl,
                                 style="Options.TRadiobutton",
                                 variable=self.var_structure, value=val)
            rb.grid(row=idx // 2, column=idx % 2, sticky="w",
                    padx=(0, 24), pady=(0, 2))
            self._struct_rbs.append(rb)

        # Help link below structure options (row 2, spans both columns)
        link_lbl = tk.Label(sf2,
            text="📖  Click here to review these dat settings in the README",
            fg="#1155CC", bg=self._c["options"],
            font=("Segoe UI", 8, "underline"), cursor="hand2")
        link_lbl.grid(row=2, column=0, columnspan=2, sticky="w", pady=(3, 0))
        link_lbl.bind("<Button-1>", lambda e: self.open_readme_link())

        # Format
        self.var_format = tk.StringVar(value="modern")
        ttk.Label(of, text="Format:",
                  style="Options.TLabel").grid(row=3, column=0, sticky="w",
                                               padx=(0,8), pady=(4,0))
        ff2 = ttk.Frame(of, style="Options.TFrame")
        ff2.grid(row=3, column=1, columnspan=2, sticky="w", pady=(4,0))
        for txt, val in [("Modern  (<game> / <dir>)", "modern"),
                         ("Legacy  (all <dir>)",      "legacy")]:
            ttk.Radiobutton(ff2, text=txt, style="Options.TRadiobutton",
                            variable=self.var_format, value=val,
                            command=self._on_options_change).pack(
                side="left", padx=(0, 18))

        # Checkboxes row 1
        self.var_machine   = tk.BooleanVar()
        self.var_inc_desc  = tk.BooleanVar(value=True)
        self.var_forcepack = tk.BooleanVar(value=True)
        self.var_filedate  = tk.BooleanVar()

        ck1 = ttk.Frame(of, style="Options.TFrame")
        ck1.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(6,0))

        self.cb_machine = ttk.Checkbutton(
            ck1, text="Use <machine> instead of <game>",
            style="Options.TCheckbutton", variable=self.var_machine)
        self.cb_machine.pack(side="left", padx=(0, 16))

        self.cb_inc_desc = ttk.Checkbutton(
            ck1, text="Include <description> in game entries",
            style="Options.TCheckbutton", variable=self.var_inc_desc)
        self.cb_inc_desc.pack(side="left", padx=(0, 16))

        self.cb_forcepack = ttk.Checkbutton(
            ck1, text='forcepacking="fileonly"',
            style="Options.TCheckbutton", variable=self.var_forcepack)
        self.cb_forcepack.pack(side="left", padx=(0, 16))

        self.cb_filedate = ttk.Checkbutton(
            ck1, text="File date & time  (yyyy/mm/dd hh-mm-ss)",
            style="Options.TCheckbutton", variable=self.var_filedate)
        self.cb_filedate.pack(side="left")

        # Checkboxes row 2 — hashes + threading
        self.var_md5         = tk.BooleanVar()
        self.var_sha256      = tk.BooleanVar()
        self.var_multithread = tk.BooleanVar(value=True)
        self.var_threads     = tk.IntVar(value=4)
        self.var_net_cap     = tk.IntVar(value=0)

        ck2 = ttk.Frame(of, style="Options.TFrame")
        ck2.grid(row=5, column=0, columnspan=3, sticky="ew", pady=(4,0))

        ttk.Label(ck2, text="Extra hashes:",
                  style="Options.TLabel",
                  foreground="#4A6040").pack(side="left", padx=(0,6))
        ttk.Checkbutton(ck2, text="MD5", style="Options.TCheckbutton",
                        variable=self.var_md5).pack(side="left", padx=(0,12))
        ttk.Checkbutton(ck2, text="SHA-256", style="Options.TCheckbutton",
                        variable=self.var_sha256,
                        command=self._on_sha256_toggle).pack(side="left", padx=(0,12))
        ttk.Separator(ck2, orient="vertical").pack(
            side="left", fill="y", padx=(0,12))
        ttk.Checkbutton(ck2, text="Multithread",
                        style="Options.TCheckbutton",
                        variable=self.var_multithread,
                        command=self._on_mt_toggle).pack(side="left", padx=(0,6))
        ttk.Label(ck2, text="Threads (1–8):",
                  style="Options.TLabel").pack(side="left")
        self.spin_threads = ttk.Spinbox(ck2, from_=1, to=8,
                                        textvariable=self.var_threads, width=4)
        self.spin_threads.pack(side="left", padx=(4, 0))
        ttk.Separator(ck2, orient="vertical").pack(
            side="left", fill="y", padx=(10, 8))
        ttk.Label(ck2, text="Net cap Mbit/s (0=auto):",
                  style="Options.TLabel").pack(side="left")
        self.spin_net_cap = ttk.Spinbox(ck2, from_=0, to=100000,
                                        textvariable=self.var_net_cap, width=7)
        self.spin_net_cap.pack(side="left", padx=(4, 0))

        # Extension filters
        ef = ttk.Frame(of, style="Options.TFrame")
        ef.columnconfigure(1, weight=1)
        ef.grid(row=6, column=0, columnspan=3, sticky="ew", pady=(6,0))

        ttk.Label(ef, text="Include only extensions:",
                  style="Options.TLabel").grid(row=0, column=0, sticky="w")
        self.var_ext_include = tk.StringVar()
        self.ent_ext_include = ttk.Entry(ef, textvariable=self.var_ext_include)
        self.ent_ext_include.grid(row=0, column=1, sticky="ew", padx=(4,4))
        ttk.Label(ef, text='e.g. ".ima, .mfm"  (blank = all)',
                  style="Options.TLabel",
                  foreground="#4A6040").grid(row=0, column=2, sticky="w")

        ttk.Label(ef, text="Exclude extensions / files:",
                  style="Options.TLabel").grid(row=1, column=0, sticky="w")
        self.var_ext_exclude = tk.StringVar()
        self.ent_ext_exclude = ttk.Entry(ef, textvariable=self.var_ext_exclude)
        self.ent_ext_exclude.grid(row=1, column=1, sticky="ew", padx=(4,4))
        ttk.Label(ef, text='e.g. ".nfo, .sfv, thumbs.db"',
                  style="Options.TLabel",
                  foreground="#4A6040").grid(row=1, column=2, sticky="w")

        row += 1

        # ── INCREMENTAL UPDATE section ────────────────────────────────────────
        incr_lf = ttk.LabelFrame(outer, text="  Incremental Update  ",
                                  style="Incremental.TLabelframe", padding=PAD)
        incr_lf.grid(row=row, column=0, sticky="ew", pady=(0, 4))
        incr_lf.columnconfigure(1, weight=1)

        self.var_incremental = tk.BooleanVar()
        self.cb_incremental = ttk.Checkbutton(
            incr_lf,
            text="Incremental update — skip already-hashed files",
            style="IncrementalBold.TCheckbutton",
            variable=self.var_incremental,
            command=self._on_incremental_toggle)
        self.cb_incremental.grid(row=0, column=0, columnspan=3,
                                  sticky="w", pady=(0, 6))

        ttk.Label(incr_lf, text="Existing dat file or folder:",
                  style="Incremental.TLabel").grid(row=1, column=0, sticky="w")
        self.var_incr_dat = tk.StringVar()
        self.ent_incr_dat = ttk.Entry(incr_lf, textvariable=self.var_incr_dat,
                                       state="disabled")
        self.ent_incr_dat.grid(row=1, column=1, sticky="ew", padx=(4, 4))
        self.btn_incr_browse = ttk.Button(incr_lf, text="Browse...",
                                           style="Browse.TButton",
                                           command=self.browse_incr_dat,
                                           state="disabled")
        self.btn_incr_browse.grid(row=1, column=2, sticky="ew")

        self.var_retire = tk.BooleanVar(value=True)
        self.cb_retire = ttk.Checkbutton(incr_lf,
                                          text="Rename superseded dat to .old",
                                          style="Incremental.TCheckbutton",
                                          variable=self.var_retire)
        self.cb_retire.grid(row=2, column=0, columnspan=3,
                             sticky="w", pady=(4, 0))

        # Store reference so _on_incremental_toggle can access it
        self._incr_lf = incr_lf

        row += 1

        # ── BUTTONS section ──────────────────────────────────────────────────
        bf2 = ttk.Frame(outer, style="Buttons.TFrame", padding=(PAD, 4))
        bf2.grid(row=row, column=0, sticky="ew", pady=(0, 4))

        self.btn_start = ttk.Button(bf2, text="▶  Start",
                                    style="Start.TButton", command=self.start)
        self.btn_start.pack(side="left")
        self.btn_soft = ttk.Button(bf2, text="⏸  Soft Stop",
                                   style="SoftStop.TButton",
                                   command=self.soft_stop_cmd, state="disabled")
        self.btn_soft.pack(side="left", padx=(8, 0))
        self.btn_hard = ttk.Button(bf2, text="⏹  Hard Stop",
                                   style="HardStop.TButton",
                                   command=self.hard_stop_cmd, state="disabled")
        self.btn_hard.pack(side="left", padx=(8, 0))

        # Right side — Save Settings | Preview Dats | Show Progress
        ttk.Button(bf2, text="💾  Save Settings",
                   style="Save.TButton",
                   command=self.save_settings_cmd).pack(side="right")
        self.btn_preview = ttk.Button(bf2, text="🔍  Preview Dats",
                                      style="Preview.TButton",
                                      command=self.open_preview,
                                      state="disabled")
        self.btn_preview.pack(side="right", padx=(0, 12))
        self.btn_show_progress = ttk.Button(
            bf2, text="📋  Show Progress",
            style="Log.TButton",
            command=self.show_progress_window,
            state="disabled")
        self.btn_show_progress.pack(side="right", padx=(0, 8))

        row += 1


    # ── DnD ─────────────────────────────────────────────────────────────────

    def _setup_dnd(self):
        for ent, cb in [(self.ent_input,   self._drop_input),
                        (self.ent_output,  self._drop_output),
                        (self.ent_sevenzip, self._drop_sevenzip),
                        (self.ent_incr_dat, self._drop_incr_dat)]:
            ent.drop_target_register(DND_FILES)
            ent.dnd_bind("<<Drop>>", cb)

    def _drop_input(self, event):
        p = clean_dnd_path(event.data)
        if p and os.path.isdir(p):
            self.var_input.set(p)
        return event.action

    def _drop_output(self, event):
        p = clean_dnd_path(event.data)
        if p and os.path.isdir(p):
            self.var_output.set(p)
        return event.action

    def _drop_sevenzip(self, event):
        p = clean_dnd_path(event.data)
        if p and os.path.isfile(p) and p.lower().endswith("7z.exe"):
            self.var_sevenzip.set(p)
        return event.action

    def _drop_incr_dat(self, event):
        """DnD onto the incremental dat field.
        File vs folder is determined from the filesystem — no dialog needed."""
        p = clean_dnd_path(event.data)
        if p:
            if os.path.isfile(p) and (p.lower().endswith(".xml") or
                                       p.lower().endswith(".dat")):
                self.var_incr_dat.set(p)
            elif os.path.isdir(p):
                self.var_incr_dat.set(p)
            # else: silently ignore non-dat files dropped onto the field
        return event.action

    # ── Option interdependencies ─────────────────────────────────────────────

    def _on_options_change(self):
        is_modern  = (self.var_format.get()   == "modern")
        is_mixed   = (self.var_dat_type.get() == "mixed")
        is_perroot = (self.var_gen_mode.get() == "per_root")

        # machine / description only in Modern
        state_modern = "normal" if is_modern else "disabled"
        self.cb_machine.configure(state=state_modern)
        self.cb_inc_desc.configure(state=state_modern)

        # forcepacking only for Mixed
        self.cb_forcepack.configure(state="normal" if is_mixed else "disabled")

        # file date only for Zipped
        self.cb_filedate.configure(state="normal" if not is_mixed else "disabled")

        # extension filters only for Mixed (Zipped always hashes the .zip as-is)
        ext_state = "normal" if is_mixed else "disabled"
        self.ent_ext_include.configure(state=ext_state)
        self.ent_ext_exclude.configure(state=ext_state)

        # structure only for per_root
        state_struct = "normal" if is_perroot else "disabled"
        for rb in self._struct_rbs:
            rb.configure(state=state_struct)

    def _on_mt_toggle(self):
        self.spin_threads.configure(
            state="normal" if self.var_multithread.get() else "disabled")

    # ── Settings apply/read ──────────────────────────────────────────────────

    def _apply_settings(self):
        s = self.settings
        self.var_input.set(s.input_root)
        self.var_output.set(s.output_root)
        self.var_parent.set(s.parent_name)
        self.var_description.set(s.description)
        self.var_category.set(s.category)
        self.var_version.set(s.version)
        self.var_date.set(datetime.date.today().isoformat())
        self.var_author.set(s.author)
        self.var_url.set(s.url)
        self.var_homepage.set(s.homepage)
        self.var_comment.set(s.comment)
        self.var_dat_type.set(s.dat_type)
        self.var_gen_mode.set(s.gen_mode)
        self.var_structure.set(s.structure)
        self.var_format.set(s.dat_format)
        self.var_machine.set(s.use_machine)
        self.var_inc_desc.set(s.incl_game_desc)
        self.var_forcepack.set(s.forcepacking)
        self.var_filedate.set(s.incl_file_date)
        self.var_md5.set(s.include_md5)
        self.var_sha256.set(s.include_sha256)
        self.var_ext_include.set(s.ext_include)
        self.var_ext_exclude.set(s.ext_exclude)
        self.var_sevenzip.set(s.sevenzip_path)
        self.var_incremental.set(s.incremental)
        self.var_incr_dat.set(s.incremental_dat_path)
        self.var_retire.set(s.retire_old_dats)
        self._on_incremental_toggle()
        self.var_multithread.set(s.multithread)
        self.var_threads.set(s.threads)
        self.var_net_cap.set(s.net_cap_mbps)
        self._on_options_change()
        self._on_mt_toggle()

    def _read_settings(self) -> "Settings":
        return Settings(
            input_root   = self.var_input.get().strip(),
            output_root  = self.var_output.get().strip(),
            parent_name  = self.var_parent.get().strip(),
            description  = self.var_description.get().strip(),
            category     = self.var_category.get().strip(),
            version      = self.var_version.get().strip(),
            date         = self.var_date.get().strip() or
                           datetime.date.today().isoformat(),
            author       = self.var_author.get().strip() or "Eggman",
            url          = self.var_url.get().strip(),
            homepage     = self.var_homepage.get().strip(),
            comment      = self.var_comment.get().strip(),
            dat_type     = self.var_dat_type.get(),
            gen_mode     = self.var_gen_mode.get(),
            structure    = self.var_structure.get(),
            dat_format   = self.var_format.get(),
            use_machine  = bool(self.var_machine.get()),
            incl_game_desc = bool(self.var_inc_desc.get()),
            forcepacking = bool(self.var_forcepack.get()),
            incl_file_date = bool(self.var_filedate.get()),
            include_md5    = bool(self.var_md5.get()),
            include_sha256 = bool(self.var_sha256.get()),
            ext_include    = self.var_ext_include.get().strip(),
            ext_exclude    = self.var_ext_exclude.get().strip(),
            sevenzip_path          = self.var_sevenzip.get().strip(),
            incremental            = bool(self.var_incremental.get()),
            incremental_dat_path   = self.var_incr_dat.get().strip(),
            retire_old_dats        = bool(self.var_retire.get()),
            multithread  = bool(self.var_multithread.get()),
            threads      = max(1, min(8, int(self.var_threads.get() or 1))),
            net_cap_mbps = max(0, int(self.var_net_cap.get() or 0)),
        )

    # ── Browse ───────────────────────────────────────────────────────────────

    def browse_input(self):
        p = filedialog.askdirectory(title="Select input top-level folder")
        if p:
            self.var_input.set(p)

    def browse_output(self):
        p = filedialog.askdirectory(title="Select output folder")
        if p:
            self.var_output.set(p)

    def browse_sevenzip(self):
        p = filedialog.askopenfilename(
            title="Select 7z.exe from 7-Zip-ZStandard",
            filetypes=[("7z executable", "7z.exe"), ("All files", "*.*")])
        if p:
            self.var_sevenzip.set(p)

    def browse_incr_dat(self):
        """Browse for an existing dat file or a folder of dat files."""
        # Ask whether they want a file or a folder
        choice = messagebox.askquestion(
            "Existing Dat",
            "Single dat file — Yes.  Folder of dat files — No.",
            icon="question")
        if choice == "yes":
            p = filedialog.askopenfilename(
                title="Select existing dat file",
                filetypes=[("Dat files", "*.xml *.dat"),
                           ("All files", "*.*")])
        else:
            p = filedialog.askdirectory(
                title="Select folder containing dat files")
        if p:
            self.var_incr_dat.set(p)

    def _on_sha256_toggle(self):
        """Warn user about SHA-256 overhead and limited utility."""
        if self.var_sha256.get():
            messagebox.showwarning(
                "SHA-256 — Limited Utility",
                "SHA-256 hashing is enabled.\n\n"
                "No ROM manager, emulator, platform, or preservation system "
                "currently requires SHA-256 in datfiles.\n\n"
                "The only known use is No-Intro's Dat-o-Matic database. "
                "RomVault shows the value but does not use it for matching, "
                "verification, or fixing.\n\n"
                "SHA-256 adds significant computation time on large files.\n\n"
                "Proceed only if you specifically need SHA-256 for your project.")

    def _on_incremental_toggle(self):
        state = "normal" if self.var_incremental.get() else "disabled"
        self.ent_incr_dat.configure(state=state)
        self.btn_incr_browse.configure(state=state)

    def save_settings_cmd(self):
        s = self._read_settings()
        try:
            save_settings(s)
            self.settings = s
            messagebox.showinfo("Saved",
                f"Settings saved to:\n{os.path.join(script_dir(), CONFIG_FILENAME)}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save:\n{repr(e)}")

    def show_progress_window(self):
        """Re-show the RunProgressWindow after it has been closed/hidden."""
        rw = getattr(self, "_run_win", None)
        if rw and rw.winfo_exists():
            rw.deiconify()
            rw.lift()
            rw.focus_force()

    # ── Run control ──────────────────────────────────────────────────────────

    def start(self):
        if self.worker and self.worker.is_alive():
            return
        s = self._read_settings()
        if not s.input_root or not os.path.isdir(s.input_root):
            messagebox.showerror("Input folder",
                                 "Please select a valid input folder."); return
        if not s.output_root or not os.path.isdir(s.output_root):
            messagebox.showerror("Output folder",
                                 "Please select a valid output folder."); return

        self.settings = s
        try: save_settings(s)
        except Exception: pass

        # Incremental mode: show confirmation dialog before starting
        if s.incremental:
            dlg = IncrementalConfirmDialog(self.root, self, s)
            self.root.wait_window(dlg)
            if not dlg.proceed:
                return   # user cancelled
            # Apply version override if set
            if dlg.new_version:
                s.version = dlg.new_version
                self.var_version.set(dlg.new_version)
            # "Rehash entire folder" button disables incremental for this run
            if hasattr(dlg, '_s_override_incremental') and not dlg._s_override_incremental:
                s.incremental = False
                ui_msg = "Full rehash mode (incremental disabled for this run)."
            else:
                ui_msg = "Incremental update started."
        else:
            ui_msg = None

        self.soft_stop.clear(); self.hard_stop.clear()
        self.total_items = 0; self.done_items = 0
        self.total_jobs  = 0; self.dats_written = 0
        self._preview_results.clear()
        self.btn_preview.configure(state="disabled")
        self._log_lines.clear()
        self._start_msg = ui_msg if ui_msg else "Starting..."
        self.btn_start.configure(state="disabled")
        self.btn_soft.configure(state="normal")
        self.btn_hard.configure(state="normal")

        # Open the detached progress window
        if not hasattr(self, "_run_win") or not self._run_win.winfo_exists():
            self._run_win = RunProgressWindow(self.root, self)
        self._run_win.reset()
        self.btn_show_progress.configure(state="normal")

        self.worker = threading.Thread(
            target=process,
            args=(s, self.ui_queue, self.soft_stop, self.hard_stop,
                  self._preview_results),
            daemon=True)
        self.worker.start()

    def soft_stop_cmd(self):
        self.soft_stop.set()
        rw = getattr(self, "_run_win", None)
        if rw and rw.winfo_exists():
            rw.set_status("Soft stop: finishing current dat then stopping.")
        self.btn_soft.configure(state="disabled")

    def hard_stop_cmd(self):
        self.hard_stop.set()
        rw = getattr(self, "_run_win", None)
        if rw and rw.winfo_exists():
            rw.set_status("Hard stop: stopping ASAP.")
        self.btn_hard.configure(state="disabled")
        self.btn_soft.configure(state="disabled")

    # ── Queue polling ─────────────────────────────────────────────────────────

    def _poll_queue(self):
        try:
            while True:
                self._handle(self.ui_queue.get_nowait())
        except queue.Empty:
            pass
        self.root.after(100, self._poll_queue)

    def _handle(self, msg):
        """Route all worker messages to the RunProgressWindow."""
        rw = getattr(self, "_run_win", None)
        if rw and rw.winfo_exists():
            rw.handle(msg)
        # Re-enable main window buttons on done
        if msg[0] == "done":
            self.btn_start.configure(state="normal")
            self.btn_soft.configure(state="disabled")
            self.btn_hard.configure(state="disabled")
            if self._preview_results:
                self.btn_preview.configure(state="normal")

    def _log(self, line: str, fg: str = ""):
        """Keep log lines in memory (RunProgressWindow displays them live)."""
        self._log_lines.append(line)

    def save_log(self):
        if not self._log_lines:
            messagebox.showinfo("Log", "Nothing to save."); return
        ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        path = filedialog.asksaveasfilename(
            title="Save Activity Log",
            initialfile=f"dat_creator_log_{ts}.txt",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")])
        if not path: return
        try:
            with open(path, "w", encoding="utf-8", newline="\n") as f:
                f.write("\n".join(self._log_lines) + "\n")
            messagebox.showinfo("Saved", f"Log saved to:\n{path}")
        except Exception as e:
            messagebox.showerror("Error", repr(e))



    def open_preview(self):
        if not self._preview_results:
            return
        PreviewWindow(self.root, self._preview_results, self.settings)


# ═══════════════════════════════════════════════════════════════════════════
#  PREVIEW WINDOW
# ═══════════════════════════════════════════════════════════════════════════

class PreviewWindow(tk.Toplevel):
    """
    Shows each completed dat as rendered XML, with live structure switching.
    The text pane is fully selectable/copyable:
      - Click to place cursor, Ctrl+A selects all, Ctrl+C copies selection.
      - Right-click context menu: Select All / Copy.
    Changing the structure radio re-renders from in-memory data instantly.
    """

    STRUCT_OPTS = [
        ("opt1",  "1 — Dirs"),
        ("opt2",  "2 — Archives as Games"),
        ("opt3",  "3 — First Level Dirs as Games"),
        ("opt4",  "4 — First Level Dirs as Games + Merge Dirs in Games"),
    ]

    def __init__(self, parent, preview_results: list, main_settings: "Settings"):
        super().__init__(parent)
        self.title("Eggman\'s Datfile Creator Suite — Dat Preview")
        self.geometry("1100x780")
        self.minsize(800, 600)
        self._results = preview_results
        self._current_idx = 0

        # Apply same warm theme
        BG      = "#EDE8E0"
        BG_ENT  = "#F7F4EF"
        FG      = "#2A2A2A"
        SEL     = "#C8B89A"
        BTN     = "#D8D0C4"
        SEP     = "#C8C0B4"
        self.configure(bg=BG)

        style = ttk.Style(self)
        for cls, opts in [
            ("TFrame",       {"background": BG}),
            ("TLabel",       {"background": BG, "foreground": FG}),
            ("TRadiobutton", {"background": BG, "foreground": FG}),
            ("TButton",      {"background": BTN, "foreground": FG,
                              "bordercolor": SEP, "relief": "flat", "padding": 4}),
        ]:
            style.configure(cls, **opts)
        style.map("TButton", background=[("active", SEL), ("pressed", "#B8A890")])

        self._bg     = BG
        self._bg_ent = BG_ENT
        self._fg     = FG
        self._sel    = SEL
        self._sep    = SEP

        # Start with the structure from settings
        self._var_struct = tk.StringVar(value=main_settings.structure)

        self._build_ui()
        self._refresh()
        self.grab_set()

    def _build_ui(self):
        BG    = self._bg
        FG    = self._fg
        SEL   = self._sel

        # ── Top: dat selector listbox ────────────────────────────────────────
        top = ttk.Frame(self)
        top.pack(fill="x", padx=8, pady=(8, 0))
        ttk.Label(top, text="Completed dats:").pack(side="left")

        sel_frm = ttk.Frame(self)
        sel_frm.pack(fill="x", padx=8, pady=(2, 4))

        self._dat_lb = tk.Listbox(
            sel_frm, height=4, selectmode="single",
            bg=self._bg_ent, fg=FG, selectbackground=SEL,
            selectforeground=FG, relief="flat",
            highlightthickness=1, highlightcolor=self._sep)
        sb_y = ttk.Scrollbar(sel_frm, orient="vertical",
                              command=self._dat_lb.yview)
        self._dat_lb.configure(yscrollcommand=sb_y.set)
        self._dat_lb.pack(side="left", fill="x", expand=True)
        sb_y.pack(side="left", fill="y")

        for entry in self._results:
            self._dat_lb.insert(tk.END, entry.dat_name)
        self._dat_lb.selection_set(0)
        self._dat_lb.bind("<<ListboxSelect>>", self._on_dat_select)

        # ── Middle: structure radios ─────────────────────────────────────────
        rf = ttk.Frame(self)
        rf.pack(fill="x", padx=8, pady=(0, 4))
        ttk.Label(rf, text="Structure:").pack(side="left", padx=(0, 10))
        for val, label in self.STRUCT_OPTS:
            ttk.Radiobutton(
                rf, text=label,
                variable=self._var_struct, value=val,
                command=self._refresh).pack(side="left", padx=(0, 14))

        # ── Main: XML preview text pane ──────────────────────────────────────
        txt_frm = ttk.Frame(self)
        txt_frm.pack(fill="both", expand=True, padx=8, pady=(0, 4))

        self._txt = tk.Text(
            txt_frm,
            font=("Consolas", 9),
            bg=self._bg_ent, fg=FG,
            selectbackground=SEL, selectforeground=FG,
            insertbackground=FG,
            relief="flat", borderwidth=1,
            highlightthickness=1, highlightcolor=self._sep,
            wrap="none",
            undo=False)

        # XML syntax highlighting — Notepad++ "XML" style palette
        # adapted to the warm off-white theme
        self._txt.tag_configure("xml_decl",   foreground="#7A7A7A")   # <?xml ... ?>
        self._txt.tag_configure("xml_bracket",foreground="#0F57A1")   # < > </ />
        self._txt.tag_configure("xml_tag",    foreground="#0F57A1")   # tag names
        self._txt.tag_configure("xml_attr",   foreground="#7A1F99")   # attribute names
        self._txt.tag_configure("xml_string", foreground="#9C3F00")   # "values"
        self._txt.tag_configure("xml_text",   foreground="#1F6B3A")   # text content
        self._txt.tag_configure("xml_comment",foreground="#808080",
                                 font=("Consolas", 9, "italic"))

        vsb = ttk.Scrollbar(txt_frm, orient="vertical",   command=self._txt.yview)
        hsb = ttk.Scrollbar(txt_frm, orient="horizontal", command=self._txt.xview)
        self._txt.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        self._txt.grid(row=0, column=0, sticky="nsew")
        txt_frm.rowconfigure(0, weight=1)
        txt_frm.columnconfigure(0, weight=1)

        # Make text selectable and copyable but not editable
        # Allow all navigation and copy keys; block all editing keys
        self._txt.bind("<Key>", self._block_edit)
        self._txt.bind("<Control-a>", self._select_all)
        self._txt.bind("<Control-A>", self._select_all)
        self._txt.bind("<Control-c>", lambda e: None)   # allow default copy
        self._txt.bind("<Control-C>", lambda e: None)

        # Right-click context menu
        self._ctx = tk.Menu(self._txt, tearoff=0, bg=self._bg_ent, fg=FG)
        self._ctx.add_command(label="Select All", command=self._select_all_cmd)
        self._ctx.add_command(label="Copy",       command=self._copy_cmd)
        self._txt.bind("<Button-3>", self._show_ctx)

        # Line / char count label
        self._var_info = tk.StringVar(value="")
        ttk.Label(self, textvariable=self._var_info,
                  foreground="#888").pack(anchor="w", padx=8)

        # ── Bottom: save + close ─────────────────────────────────────────────
        bot = ttk.Frame(self)
        bot.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(bot, text="💾  Save Chosen Dat Structure As...",
                   command=self._save_as).pack(side="left")
        ttk.Button(bot, text="Close",
                   command=self.destroy).pack(side="right")

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _on_dat_select(self, _event=None):
        sel = self._dat_lb.curselection()
        if sel:
            self._current_idx = sel[0]
            self._refresh()

    def _refresh(self):
        if not self._results:
            return
        entry   = self._results[self._current_idx]
        struct  = self._var_struct.get()
        try:
            xml = render_preview(entry, struct)
        except Exception as exc:
            xml = f"[Render error: {exc}]"
        self._txt.configure(state="normal")
        self._txt.delete("1.0", tk.END)
        self._txt.insert("1.0", xml)
        self._txt.configure(state="normal")   # keep normal so selection works
        # Defer highlighting so the window repaints first (avoids UI hang on large dats)
        self.after_idle(lambda x=xml: self._apply_xml_highlight(x))
        lines = xml.count("\n")
        chars = len(xml)
        self._var_info.set(f"{lines} lines | {chars:,} chars")

    def _apply_xml_highlight(self, xml: str) -> None:
        """
        Apply XML syntax highlighting to the text widget.
        Uses sorted interval list to efficiently track masked ranges —
        O(n log n) instead of the previous O(n²) per-character array.
        """
        import re, bisect

        def add_tag(tag: str, start: int, end: int):
            if start >= end:
                return
            self._txt.tag_add(tag, f"1.0+{start}c", f"1.0+{end}c")

        # Clear previous highlighting
        for tag in ("xml_decl", "xml_bracket", "xml_tag", "xml_attr",
                    "xml_string", "xml_text", "xml_comment"):
            self._txt.tag_remove(tag, "1.0", tk.END)

        # Masked ranges stored as sorted list of (start, end) pairs.
        # We only add non-overlapping ranges in strict order so a bisect
        # check is sufficient.
        masked_starts: list = []   # sorted list of range starts
        masked_ends:   list = []   # parallel list of range ends

        def mask(start: int, end: int):
            bisect.insort(masked_starts, start)
            idx = bisect.bisect_right(masked_starts, start) - 1
            masked_ends.insert(idx, end)

        def is_masked(pos: int) -> bool:
            # Find any masked range whose start <= pos
            idx = bisect.bisect_right(masked_starts, pos) - 1
            if idx < 0:
                return False
            return masked_ends[idx] > pos

        # 1. XML comments <!-- ... -->
        for m in re.finditer(r"<!--.*?-->", xml, re.DOTALL):
            add_tag("xml_comment", m.start(), m.end())
            mask(m.start(), m.end())

        # 2. XML/processing declarations <?xml ... ?>
        for m in re.finditer(r"<\?.*?\?>", xml, re.DOTALL):
            if not is_masked(m.start()):
                add_tag("xml_decl", m.start(), m.end())
                mask(m.start(), m.end())

        # 3. Tags — full tag including attributes
        tag_re = re.compile(
            r"(</?)"                      # opening bracket + optional slash
            r"([A-Za-z_][\w.-]*)"         # tag name
            r"([^>]*)"                    # everything up to closing bracket
            r"(/?>)",                     # closing bracket
            re.DOTALL)
        attr_re = re.compile(
            r'([A-Za-z_][\w.-]*)'
            r'\s*=\s*'
            r'("(?:[^"\\]|\\.)*"|'
            r"'(?:[^'\\]|\\.)*')",
            re.DOTALL)

        for m in tag_re.finditer(xml):
            if is_masked(m.start()):
                continue
            add_tag("xml_bracket", m.start(1), m.start(2))     # < or </
            add_tag("xml_tag",     m.start(2), m.end(2))       # tag name
            add_tag("xml_bracket", m.start(4), m.end(4))       # > or />
            attrs_base = m.start(3)
            for am in attr_re.finditer(xml[m.start(3):m.end(3)]):
                add_tag("xml_attr",   attrs_base + am.start(1), attrs_base + am.end(1))
                add_tag("xml_string", attrs_base + am.start(2), attrs_base + am.end(2))
            mask(m.start(), m.end())

        # 4. Text content between tags — use regex to find non-whitespace
        # runs that land entirely in unmasked territory
        for m in re.finditer(r"\S+", xml):
            if not is_masked(m.start()):
                add_tag("xml_text", m.start(), m.end())

    def _block_edit(self, event):
        # Allow: navigation, selection (Shift+arrows), copy (Ctrl+C/A), F-keys
        allowed = {
            "Left", "Right", "Up", "Down", "Home", "End",
            "Prior", "Next",                # Page Up / Page Down
            "Control_L", "Control_R",
            "Shift_L", "Shift_R",
            "Alt_L", "Alt_R",
            "F1","F2","F3","F4","F5","F6","F7","F8","F9","F10","F11","F12",
        }
        if event.keysym in allowed:
            return None
        # Allow Ctrl+C, Ctrl+A, Ctrl+X (copy only, no cut effect since readonly)
        if event.state & 0x4:   # Control held
            if event.keysym.lower() in ("c", "a", "x"):
                return None
        return "break"

    def _select_all(self, event=None):
        self._txt.tag_add("sel", "1.0", tk.END)
        return "break"

    def _select_all_cmd(self):
        self._select_all()

    def _copy_cmd(self):
        try:
            text = self._txt.get("sel.first", "sel.last")
        except tk.TclError:
            text = self._txt.get("1.0", tk.END)
        self.clipboard_clear()
        self.clipboard_append(text)

    def _show_ctx(self, event):
        try:
            self._ctx.tk_popup(event.x_root, event.y_root)
        finally:
            self._ctx.grab_release()

    def _save_as(self):
        if not self._results:
            return
        entry  = self._results[self._current_idx]
        struct = self._var_struct.get()
        struct_label = dict(self.STRUCT_OPTS).get(struct, struct)
        default_name = f"{safe_filename(entry.dat_name)} ({entry.header_date}_RomVault) [{struct_label}].xml"
        path = filedialog.asksaveasfilename(
            parent=self,
            title="Save Chosen Dat Structure As...",
            initialfile=default_name,
            defaultextension=".xml",
            filetypes=[("XML dat files", "*.xml"), ("All files", "*.*")])
        if not path:
            return
        try:
            xml = render_preview(entry, struct)
            with open(path, "w", encoding="utf-8", newline="\n") as f:
                f.write(xml)
            messagebox.showinfo("Saved", f"Dat saved to:\n{path}", parent=self)
        except Exception as exc:
            messagebox.showerror("Error", repr(exc), parent=self)



# ═══════════════════════════════════════════════════════════════════════════
#  INCREMENTAL UPDATE ENGINE
# ═══════════════════════════════════════════════════════════════════════════

# ── Dat XML reader ───────────────────────────────────────────────────────────

def _read_dat_index(dat_path: str) -> Tuple[dict, dict, str]:
    """
    Parse an existing Logiqx XML dat into an in-memory index.

    Returns:
        game_index  — {game_name: {"roms": [rom_dict, ...], ...}}
        header_dict — {field: value} from the <header> block
        error_str   — non-empty string if parse failed, else ""

    rom_dict keys (all present, value may be None):
        name, size, crc, sha1, sha256, md5, date

    The index key is the game name as-is from name="..." attribute.
    For Mixed dats:  game_name == archive stem, rom name == archive filename.
    For Zipped dats: game_name == zip stem, rom names == internal paths.
    """
    import xml.etree.ElementTree as ET
    game_index:  dict = {}
    header_dict: dict = {}

    try:
        tree = ET.parse(dat_path)
        root = tree.getroot()
    except Exception as e:
        return {}, {}, f"XML parse error: {e}"

    # Header fields — handle both <name> and the <n> shorthand
    hdr = root.find("header")
    if hdr is not None:
        for tag in ("name", "n", "description", "category", "version",
                    "date", "author", "url", "homepage", "comment"):
            el = hdr.find(tag)
            if el is not None and el.text:
                store_key = "name" if tag == "n" else tag
                header_dict[store_key] = el.text.strip()
        rv = hdr.find("romvault")
        if rv is not None:
            fp = rv.get("forcepacking", "")
            header_dict["forcepacking"] = fp

    # Game / machine / dir entries
    for game_el in root.iter():
        if game_el.tag not in ("game", "machine", "dir", "set"):
            continue
        gname = game_el.get("name", "")
        if not gname:
            continue
        roms = []
        for rom_el in game_el:
            if rom_el.tag not in ("rom", "disk", "file"):
                continue
            roms.append({
                "name":   rom_el.get("name",   ""),
                "size":   rom_el.get("size",   None),
                "crc":    rom_el.get("crc",    None),
                "sha1":   rom_el.get("sha1",   None),
                "sha256": rom_el.get("sha256", None),
                "md5":    rom_el.get("md5",    None),
                "date":   rom_el.get("date",   None),
            })
        game_index[gname] = {"roms": roms}

    return game_index, header_dict, ""


# ── CRC fast-check (Zipped) ──────────────────────────────────────────────────

def _zip_crc_fast(zip_path: str, sevenzip_path: str = "") -> Tuple[int, str]:
    """
    Read CRC32 and total uncompressed size from zip central directory.
    Returns (total_uncompressed_size, xor_crc_hex) or (0, "") on failure.

    Uses _zipfile_mod (zipfile_zstd) — reads only the central directory,
    no decompression.  No subprocess, no temp files.

    sevenzip_path is kept for call-site compatibility but is not used.
    """
    try:
        total_size = 0
        crc_xor    = 0
        found      = False
        with _zipfile_mod.ZipFile(zip_path, "r") as zf:
            for entry in zf.infolist():
                if entry.is_dir():
                    continue
                total_size += entry.file_size
                crc_xor    ^= entry.CRC
                found       = True
        if not found:
            return 0, ""
        return total_size, f"{crc_xor & 0xFFFFFFFF:08x}"
    except Exception:
        return 0, ""


# ── Dat validation (name-only cross-check) ───────────────────────────────────

def validate_dat_vs_folder(game_index: dict, folder_path: str,
                            dat_type: str) -> dict:
    """
    Check how many game entries in the dat correspond to files/zips/folders
    actually present in folder_path (shallow check, no hashing).

    Mixed mode handles two layouts:
      - Flat Mixed:   each game's first rom name is a file directly in folder
      - Folder Mixed: each game name is a subfolder (Structure 3/4)
    A game is considered "found" if EITHER the expected filename exists as a
    file OR the game_name exists as a subfolder — covering both layouts.

    Returns:
        {
          "total_in_dat":    int,
          "found_in_folder": int,
          "missing":         [game_name, ...],   # in dat, not in folder
          "extra":           [filename, ...],     # in folder, not in dat
          "match_pct":       float,
        }
    """
    is_zipped = (dat_type == "zipped")

    # Build sets of what exists in the folder (files and subfolders)
    folder_files:   set = set()
    folder_subdirs: set = set()
    try:
        for e in os.scandir(folder_path):
            if e.is_file(follow_symlinks=False):
                if is_zipped:
                    if e.name.lower().endswith(".zip"):
                        folder_files.add(e.name)
                else:
                    folder_files.add(e.name)
            elif e.is_dir(follow_symlinks=False):
                folder_subdirs.add(e.name)
    except Exception:
        pass

    matched:        set  = set()
    missing:        list = []
    seen_dat_files: set  = set()

    for gname, gdata in game_index.items():
        roms = gdata.get("roms", [])

        if is_zipped:
            expected = gname + ".zip"
            seen_dat_files.add(expected)
            if expected in folder_files:
                matched.add(expected)
            else:
                missing.append(gname)
        else:
            # Mixed — two layouts:
            # A) Folder-based (Structure 3/4): game_name = subfolder,
            #    roms are files inside that subfolder.  A game is fully
            #    matched only if the subfolder exists AND every listed rom
            #    file is present within it.
            # B) Flat Mixed: rom name is a file directly in folder_path.

            found_this = False

            if gname in folder_subdirs:
                # Folder-based Mixed — check every rom file inside the subfolder
                sub_path = os.path.join(folder_path, gname)
                try:
                    sub_files = {e.name for e in os.scandir(sub_path)
                                 if e.is_file(follow_symlinks=False)}
                except Exception:
                    sub_files = set()

                all_present = True
                for rom in roms:
                    rom_name = os.path.basename(rom.get("name", ""))
                    if rom_name and rom_name not in sub_files:
                        all_present = False
                        missing.append(f"{gname}/{rom_name}")

                seen_dat_files.add(gname)
                if all_present:
                    matched.add(gname)
                    found_this = True
                # Even if files missing, the folder existed — don't double-add to missing
                found_this = True   # folder presence = game presence for counting

                if not all_present:
                    # Already added individual missing rom entries above
                    pass

            else:
                # Flat Mixed — rom name is a direct file in folder_path
                first_rom_name = roms[0].get("name", "") if roms else ""
                bare_name = os.path.basename(first_rom_name) if first_rom_name else ""

                if first_rom_name in folder_files:
                    seen_dat_files.add(first_rom_name)
                    matched.add(first_rom_name)
                    found_this = True
                elif bare_name and bare_name in folder_files:
                    seen_dat_files.add(bare_name)
                    matched.add(bare_name)
                    found_this = True
                elif gname in folder_files:
                    seen_dat_files.add(gname)
                    matched.add(gname)
                    found_this = True
                else:
                    seen_dat_files.add(first_rom_name or gname)

            if not found_this:
                missing.append(gname)

    extra = sorted((folder_files | folder_subdirs) - seen_dat_files)

    # For folder-based Mixed, count is game-level (folder = game).
    # missing may contain both "GameName" (whole game absent) and
    # "GameName/file.ext" (individual file absent within a present game).
    # Report total games vs games fully matched.
    total         = len(game_index)
    found         = len(matched)
    pct           = (found / total * 100.0) if total > 0 else 0.0

    # Separate whole-game misses from within-game file misses for cleaner reporting
    game_missing  = [m for m in missing if "/" not in m and "\\" not in m]
    file_missing  = [m for m in missing if "/" in m or "\\" in m]

    return {
        "total_in_dat":    total,
        "found_in_folder": found,
        "missing":         game_missing,   # whole games absent
        "file_missing":    file_missing,   # individual files absent within games
        "extra":           extra,
        "match_pct":       pct,
    }


# ── Build incremental data dict ──────────────────────────────────────────────

def build_incremental_data(items: List[str], game_index: dict,
                           s: "Settings",
                           hard_stop: threading.Event,
                           ui_queue,
                           done_so_far: int,
                           throttle: Optional["_BandwidthThrottle"] = None) -> Tuple[dict, int, int, int, list]:
    """
    For each item in `items`:
      - If it matches an existing dat entry (filename+size+CRC for Zipped,
        filename+size for Mixed), carry forward all hashes from the dat.
      - Otherwise hash/analyze the item fresh.

    Returns:
        (data_dict, done_count, carried_count, hashed_count, errors)

    data_dict maps item_path → result tuple, same format as process() produces.
    """
    import subprocess

    is_zipped = (s.dat_type == "zipped")
    data: dict   = {}
    errors: list = []
    done_count    = done_so_far
    carried_count = 0
    hashed_count  = 0

    # Flatten game_index into a quick lookup by expected filename
    # For Zipped: key = "GameName.zip", value = sub-dict of internal_rom_name → rom_dict
    # For Mixed:  key = os.path.basename(rom_name), value = rom_dict
    #             ALL roms for every game are indexed — not just roms[0].
    #             Folder-based Mixed games have many files per game; indexing
    #             only the first rom meant every other file got rehashed.
    carry_lookup: dict = {}
    for gname, gdata in game_index.items():
        roms = gdata.get("roms", [])
        if not roms:
            continue
        if is_zipped:
            zip_key = gname + ".zip"
            carry_lookup[zip_key] = {r["name"]: r for r in roms}
        else:
            # Mixed: index every rom by its basename so any file in any
            # game subfolder can be looked up by filename alone.
            for r in roms:
                key = os.path.basename(r.get("name", ""))
                if key:
                    carry_lookup[key] = r

    def _try_carry_zipped(zip_path: str) -> Optional[list]:
        """
        Attempt to carry forward a zipped item.
        Returns rom tuple list if match confirmed, None if must rehash.
        """
        fname = os.path.basename(zip_path)
        rom_map = carry_lookup.get(fname)
        if rom_map is None:
            return None   # not in dat at all → new item

        # Quick CRC fingerprint from zip header
        folder_size, folder_crc = _zip_crc_fast(zip_path, s.sevenzip_path)
        if not folder_crc:
            return None   # couldn't read zip → rehash to be safe

        # Compare against stored CRC (XOR of all entries — same calculation)
        stored_crc_xor = 0
        stored_size    = 0
        for rd in rom_map.values():
            raw = rd.get("crc") or ""
            try:
                stored_crc_xor ^= int(raw, 16)
            except ValueError:
                pass
            try:
                stored_size += int(rd.get("size") or "0")
            except ValueError:
                pass

        if folder_size != stored_size or folder_crc != f"{stored_crc_xor & 0xFFFFFFFF:08x}":
            return None   # changed → rehash

        # Match — reconstruct rom tuples from stored values
        result_roms = []
        for rom_name, rd in sorted(rom_map.items()):
            try:
                sz = int(rd.get("size") or "0")
            except ValueError:
                sz = 0
            result_roms.append((
                rom_name,
                sz,
                rd.get("crc")    or "00000000",
                rd.get("sha1")   or "",
                rd.get("md5")    or None,
                rd.get("sha256") or None,
                rd.get("date")   or None,
            ))
        return result_roms

    def _try_carry_mixed(file_path: str) -> Optional[tuple]:
        """
        Attempt to carry forward a Mixed (fileonly) item.
        Returns hash tuple if filename+size match, None if must rehash.
        """
        fname = os.path.basename(file_path)
        rd    = carry_lookup.get(fname)
        if rd is None:
            return None   # new item

        stored_size = 0
        try:
            stored_size = int(rd.get("size") or "0")
        except ValueError:
            pass

        folder_size = 0
        try:
            folder_size = os.path.getsize(file_path)
        except OSError:
            return None

        if folder_size != stored_size:
            return None   # size changed → rehash

        # Match — reconstruct tuple
        try:
            sz = int(rd.get("size") or "0")
        except ValueError:
            sz = 0
        return (
            sz,
            rd.get("crc")    or "00000000",
            rd.get("sha1")   or "",
            rd.get("md5")    or None,
            rd.get("sha256") or None,
        )

    for item in items:
        if hard_stop.is_set():
            break

        fname = os.path.basename(item)
        ui_queue.put(("item", fname, done_count + 1))

        carried = False
        try:
            if is_zipped:
                carry_result = _try_carry_zipped(item)
                if carry_result is not None:
                    data[item] = carry_result
                    carried = True
                else:
                    # Full analyze
                    _res, _diag = analyze_zip(
                        item, s.include_md5, s.include_sha256,
                        s.incl_file_date, hard_stop, s.sevenzip_path,
                        throttle=throttle)
                    data[item] = _res
                    _hash_diag = _diag
            else:
                carry_result = _try_carry_mixed(item)
                if carry_result is not None:
                    data[item] = carry_result
                    carried = True
                else:
                    data[item] = hash_file(
                        item, s.include_md5, s.include_sha256, hard_stop,
                        throttle=throttle)
                    _hash_diag = ""

            if carried:
                carried_count += 1
                ui_queue.put(("item_carried", fname))
            else:
                hashed_count += 1
                ui_queue.put(("item_hashed", fname,
                               ("  (" + _hash_diag + ")") if _hash_diag else ""))

        except Exception as exc:
            msg = str(exc)
            if "CANCELLED" in msg:
                break
            err_detail = repr(exc)
            errors.append("ERROR: " + item + " :: " + err_detail)
            ui_queue.put(("item_error", fname, err_detail))

        done_count += 1
        ui_queue.put(("progress", done_count))

    return data, done_count, carried_count, hashed_count, errors


# ── .old rename helper ───────────────────────────────────────────────────────

def retire_old_dat(dat_path: str) -> Tuple[str, str]:
    """
    Rename dat_path to dat_path + ".old".
    If that name is already taken, appends "(1)", "(2)", etc.

    Returns (old_path_final, error_str).
    old_path_final is the actual name it was renamed to.
    error_str is non-empty if the rename failed.
    """
    base_old = dat_path + ".old"
    candidate = base_old
    counter   = 1
    while os.path.exists(candidate):
        candidate = dat_path + f"({counter}).old"
        counter  += 1
    try:
        os.rename(dat_path, candidate)
        return candidate, ""
    except Exception as e:
        return "", f"Could not rename {dat_path}: {e}"



# ═══════════════════════════════════════════════════════════════════════════
#  RUN PROGRESS WINDOW
# ═══════════════════════════════════════════════════════════════════════════

class RunProgressWindow(tk.Toplevel):
    """
    Detached floating window that appears when a dat run starts.
    Shows status, counts, progress bar, current item, and activity log.
    Stays open after the run so the user can review the log, save it,
    or click Preview Dats. Closing it is always safe.
    """

    def __init__(self, root_win, app):
        super().__init__(root_win)
        self.app  = app
        self._c   = app._c
        self._log_lines: List[str] = []
        self.total_items  = 0
        self.done_items   = 0
        self.total_jobs   = 0
        self.dats_written = 0
        self._warn_count  = 0            # errors/warnings encountered this run

        # Net speed polling state (psutil)
        self._net_after    = None
        self._net_last_rx  = 0
        self._net_last_tx  = 0
        self._net_last_t   = 0.0
        self._run_start_t  = 0.0   # monotonic time when run started (for elapsed)

        self.title("Eggman\'s Datfile Creator Suite — Run Progress")
        self.geometry("860x580+200+400")
        self.minsize(640, 420)
        self.configure(bg=self._c["progress"])
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        self._build_ui()

    def _build_ui(self):
        c   = self._c
        BG  = c["progress"]
        FG  = c["fg"]
        pad = 8

        top = ttk.Frame(self, style="Progress.TFrame", padding=(pad, pad, pad, 4))
        top.pack(fill="x")
        top.columnconfigure(0, weight=1)

        self.var_status = tk.StringVar(value="Starting...")
        ttk.Label(top, textvariable=self.var_status,
                  style="Progress.TLabel",
                  font=("Segoe UI", 9, "bold")).grid(
            row=0, column=0, sticky="w")

        self.var_counts = tk.StringVar(
            value="Items: 0/0  |  Folders: 0  |  Dats written: 0")
        ttk.Label(top, textvariable=self.var_counts,
                  style="Progress.TLabel").grid(row=1, column=0, sticky="w")

        self.progress = ttk.Progressbar(
            top, orient="horizontal", mode="determinate")
        self.progress.grid(row=2, column=0, sticky="ew", pady=(4, 2))

        # Network throughput display (updated once/sec via psutil)
        self.var_net_speed = tk.StringVar(value="Network: —")
        ttk.Label(top, textvariable=self.var_net_speed,
                  style="Progress.TLabel",
                  foreground="#5A5A5A",
                  font=("Consolas", 8)).grid(row=3, column=0, sticky="w")

        # Elapsed time display (updated once/sec alongside network)
        self.var_elapsed = tk.StringVar(value="Elapsed:  —")
        ttk.Label(top, textvariable=self.var_elapsed,
                  style="Progress.TLabel",
                  foreground="#5A5A5A",
                  font=("Consolas", 8)).grid(row=4, column=0, sticky="w")

        # Spinner — shown during Phase 1 discovery, hidden during Phase 2
        spin_row = ttk.Frame(top, style="Progress.TFrame")
        spin_row.grid(row=5, column=0, sticky="w", pady=(2, 0))
        self.var_spinner = tk.StringVar(value="")
        self._lbl_spinner = tk.Label(spin_row,
            textvariable=self.var_spinner,
            bg=self._c["progress"], fg="#7A5A9A",
            font=("Consolas", 9))
        self._lbl_spinner.pack(side="left")
        self._spinner_frames = [
            "⠋ Scanning...", "⠙ Scanning...", "⠹ Scanning...",
            "⠸ Scanning...", "⠼ Scanning...", "⠴ Scanning...",
            "⠦ Scanning...", "⠧ Scanning...", "⠇ Scanning...",
            "⠏ Scanning...",
        ]
        self._spinner_idx   = 0
        self._spinner_after = None   # holds the after() id

        # Log area
        log_outer = ttk.Frame(self, style="Progress.TFrame",
                              padding=(pad, 0, pad, 0))
        log_outer.pack(fill="both", expand=True)
        log_outer.columnconfigure(0, weight=1)
        log_outer.rowconfigure(1, weight=1)

        hdr = ttk.Frame(log_outer, style="Progress.TFrame")
        hdr.grid(row=0, column=0, sticky="ew", pady=(4, 2))
        ttk.Label(hdr, text="Activity log:",
                  style="Progress.TLabel").pack(side="left")

        self.lst = tk.Listbox(
            log_outer, height=16,
            bg=c["list"], fg=FG,
            selectbackground=c["sel"], selectforeground=FG,
            relief="flat", borderwidth=0, highlightthickness=0,
            font=("Segoe UI", 9))
        sb = ttk.Scrollbar(log_outer, orient="vertical",
                           command=self.lst.yview)
        self.lst.configure(yscrollcommand=sb.set)
        self.lst.grid(row=1, column=0, sticky="nsew")
        sb.grid(row=1, column=1, sticky="ns")

        # Bottom buttons
        bot = ttk.Frame(self, style="Progress.TFrame",
                        padding=(pad, 4, pad, pad))
        bot.pack(fill="x")

        self.btn_preview = ttk.Button(
            bot, text="\U0001f50d  Preview Dats",
            style="Preview.TButton",
            command=self.app.open_preview, state="disabled")
        self.btn_preview.pack(side="left")

        ttk.Button(bot, text="\U0001f4be  Save Activity Log",
                   style="Log.TButton",
                   command=self.save_log).pack(side="left", padx=(8, 0))

        # Stop buttons — mirror the main window controls from here
        self.btn_rp_soft = ttk.Button(
            bot, text="\u23f8  Soft Stop",
            style="SoftStop.TButton",
            command=self._soft_stop, state="disabled")
        self.btn_rp_soft.pack(side="left", padx=(24, 0))

        self.btn_rp_hard = ttk.Button(
            bot, text="\u23f9  Hard Stop",
            style="HardStop.TButton",
            command=self._hard_stop, state="disabled")
        self.btn_rp_hard.pack(side="left", padx=(8, 0))

        ttk.Button(bot, text="Close",
                   style="Browse.TButton",
                   command=self._on_close).pack(side="right")

    def reset(self):
        """Called at the start of each run to clear state."""
        self._spin_stop()
        self._net_stop()
        self.progress.stop()
        self.progress.configure(mode="determinate")
        self._log_lines.clear()
        self.lst.delete(0, tk.END)
        self.total_items = 0; self.done_items = 0
        self.total_jobs  = 0; self.dats_written = 0
        self._warn_count = 0
        self.progress["value"]   = 0
        self.progress["maximum"] = 1
        start_msg = getattr(self.app, "_start_msg", "Starting...")
        self.var_status.set(start_msg)
        self.var_counts.set("Items: 0/0  |  Folders: 0  |  Dats written: 0")
        self.var_net_speed.set("Network: —")
        self.var_elapsed.set("Elapsed:  —")
        self.btn_preview.configure(state="disabled")
        self.btn_rp_soft.configure(state="normal")
        self.btn_rp_hard.configure(state="normal")
        self._net_start()
        self.deiconify()
        self.lift()

    def _soft_stop(self):
        self.app.soft_stop_cmd()
        self.btn_rp_soft.configure(state="disabled")

    def _hard_stop(self):
        self.app.hard_stop_cmd()
        self.btn_rp_hard.configure(state="disabled")
        self.btn_rp_soft.configure(state="disabled")

    def set_status(self, text: str):
        self.var_status.set(text)
        self._log_entry(text)

    def handle(self, msg):
        """Process a message from the worker queue."""
        kind = msg[0]
        if kind == "status":
            self.var_status.set(msg[1])
            self._log_entry(msg[1])
        elif kind == "scan":
            folder_path = str(msg[1])
            folder_name = os.path.basename(folder_path)
            self._log_entry("  Scanning: " + folder_name, fg="#9A6A30")
            if self._spinner_after is None:
                if self.progress["mode"] == "determinate":
                    self.progress.configure(mode="indeterminate")
                    self.progress.start(12)
                self._spin_start()
        elif kind == "totals":
            self.total_jobs, self.total_items = msg[1], msg[2]
            self._spin_stop()
            self.progress.stop()
            self.progress.configure(mode="determinate")
            self.progress["value"]   = 0
            self.progress["maximum"] = max(1, self.total_items)
            self._update_counts()
        elif kind == "folder":
            self._log_entry(">> " + str(msg[1]) + "  (" + str(msg[2]) + " item(s))",
                            fg="#1e90ff")
        elif kind == "subfolder":
            rel = str(msg[1])
            if rel == "." or rel == "":
                label = "[root]"
            else:
                label = "[dir] " + rel
            self._log_entry("  " + label, fg="#9A6A30")
        elif kind == "item":
            pass   # no longer displayed — completion logged via item_hashed/item_error
        elif kind == "item_hashed":
            detail = str(msg[2]) if len(msg) > 2 else ""
            self._log_entry("    \u2713 " + str(msg[1]) + detail, fg="#208030")
        elif kind == "item_carried":
            self._log_entry("    ~ " + str(msg[1]) + "  (carried)", fg="#808080")
        elif kind == "item_error":
            self._warn_count += 1
            detail = str(msg[2]) if len(msg) > 2 else ""
            entry  = "    [ERROR] " + str(msg[1])
            if detail:
                entry += " :: " + detail
            self._log_entry(entry, fg="#CC2020")
        elif kind == "progress":
            self.done_items = msg[1]
            self.progress["value"] = self.done_items
            self._update_counts()
        elif kind == "dat_written":
            self.dats_written = msg[2]
            self._log_entry("++ Dat: " + str(msg[1]), fg="#00c040")
            self._update_counts()
        elif kind == "done":
            ok     = msg[1]
            errors = msg[2]
            done   = msg[3]
            total  = msg[4]
            written= msg[5]
            elapsed= msg[6]
            self.done_items   = done
            self.dats_written = written
            self._update_counts()
            self._net_stop()
            # Freeze elapsed at final time
            elapsed_s = time.monotonic() - self._run_start_t
            h  = int(elapsed_s // 3600)
            m  = int((elapsed_s % 3600) // 60)
            s  = int(elapsed_s % 60)
            if h:
                elapsed_str = f"{h}h {m:02d}m {s:02d}s"
            else:
                elapsed_str = f"{m}m {s:02d}s"
            self.var_elapsed.set("Elapsed:  " + elapsed_str + "  (finished)")
            result = "Done" if ok else "Stopped"
            status = (result + ". Wrote " + str(written) + " dat(s) / "
                      + str(done) + "/" + str(total) + " item(s) in "
                      + f"{elapsed:.1f}s.")
            self.var_status.set(status)
            self._log_entry(status)
            total_warns = self._warn_count + len(errors)
            if total_warns:
                self._log_entry("[SUMMARY] " + str(total_warns)
                                + " error(s)/warning(s) encountered.", fg="#CC2020")
                for ln in errors[:10]:
                    self._log_entry("  " + ln, fg="#CC2020")
                if len(errors) > 10:
                    self._log_entry(
                        "  ... and " + str(len(errors) - 10) + " more (save log to see all)",
                        fg="#CC2020")
            else:
                self._log_entry("[SUMMARY] No errors reported.", fg="#208030")
            if self.app._preview_results:
                self.btn_preview.configure(state="normal")
            self.btn_rp_soft.configure(state="disabled")
            self.btn_rp_hard.configure(state="disabled")

    def _spin_start(self):
        """Start the braille spinner animation during Phase 1."""
        self.var_spinner.set(self._spinner_frames[0])
        self._spinner_idx = 0
        self._tick_spinner()

    def _tick_spinner(self):
        """Advance spinner one frame and reschedule."""
        self._spinner_idx = (self._spinner_idx + 1) % len(self._spinner_frames)
        self.var_spinner.set(self._spinner_frames[self._spinner_idx])
        self._spinner_after = self.after(120, self._tick_spinner)

    def _spin_stop(self):
        """Stop the spinner and clear the label."""
        if self._spinner_after:
            self.after_cancel(self._spinner_after)
            self._spinner_after = None
        self.var_spinner.set("")

    # ── Network throughput display ────────────────────────────────────────────

    def _net_start(self):
        """Begin 1-second network throughput polling."""
        self._run_start_t = time.monotonic()
        if not _PSUTIL_OK or _psutil is None:
            self.var_net_speed.set("Network: (psutil not installed)")
            self._net_after = self.after(1000, self._tick_net_speed)
            return
        try:
            c = _psutil.net_io_counters()
            self._net_last_rx = c.bytes_recv
            self._net_last_tx = c.bytes_sent
        except Exception:
            self._net_last_rx = 0
            self._net_last_tx = 0
        self._net_last_t = time.monotonic()
        self._net_after  = self.after(1000, self._tick_net_speed)

    def _tick_net_speed(self):
        """Poll net_io_counters and update the Mbit/s and elapsed displays."""
        # Update elapsed regardless of psutil
        elapsed_s = time.monotonic() - self._run_start_t
        h  = int(elapsed_s // 3600)
        m  = int((elapsed_s % 3600) // 60)
        s  = int(elapsed_s % 60)
        if h:
            elapsed_str = f"{h}h {m:02d}m {s:02d}s"
        else:
            elapsed_str = f"{m}m {s:02d}s"
        self.var_elapsed.set("Elapsed:  " + elapsed_str)

        if _PSUTIL_OK and _psutil is not None:
            try:
                c   = _psutil.net_io_counters()
                now = time.monotonic()
                dt  = now - self._net_last_t
                if dt > 0:
                    rx_bps = (c.bytes_recv - self._net_last_rx) / dt
                    tx_bps = (c.bytes_sent - self._net_last_tx) / dt
                    rx_mb  = rx_bps * 8 / 1_000_000
                    tx_mb  = tx_bps * 8 / 1_000_000
                    self.var_net_speed.set(
                        "Network:  \u2193 " + f"{rx_mb:6.1f}" +
                        " Mbit/s   \u2191 " + f"{tx_mb:6.1f}" + " Mbit/s")
                self._net_last_rx = c.bytes_recv
                self._net_last_tx = c.bytes_sent
                self._net_last_t  = now
            except Exception:
                pass
        self._net_after = self.after(1000, self._tick_net_speed)

    def _net_stop(self):
        """Cancel the net speed polling loop."""
        if self._net_after:
            self.after_cancel(self._net_after)
            self._net_after = None

    def _update_counts(self):
        self.var_counts.set(
            "Items: " + str(self.done_items) + "/" + str(self.total_items)
            + "  |  Folders: " + str(self.total_jobs)
            + "  |  Dats written: " + str(self.dats_written))

    def _log_entry(self, line: str, fg: str = ""):
        self._log_lines.append(line)
        self.app._log_lines.append(line)   # keep copy in App too
        self.lst.insert(tk.END, line)
        if fg:
            self.lst.itemconfig(tk.END, fg=fg)
        self.lst.yview_moveto(1.0)

    def save_log(self):
        if not self._log_lines:
            messagebox.showinfo("Log", "Nothing to save.", parent=self)
            return
        ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        p    = filedialog.asksaveasfilename(
            parent=self,
            title="Save Activity Log",
            initialfile="dat_creator_log_" + ts + ".txt",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")])
        if not p:
            return
        try:
            with open(p, "w", encoding="utf-8", newline="\n") as f:
                f.write("\n".join(self._log_lines) + "\n")
            messagebox.showinfo("Saved", "Log saved to:\n" + p, parent=self)
        except Exception as e:
            messagebox.showerror("Error", repr(e), parent=self)

    def _on_close(self):
        # Only allow close if no worker is running
        if self.app.worker and self.app.worker.is_alive():
            messagebox.showwarning(
                "Run in progress",
                "A dat run is currently in progress.\n"
                "Use Soft Stop or Hard Stop before closing.",
                parent=self)
            return
        self._net_stop()
        self.withdraw()

# ═══════════════════════════════════════════════════════════════════════════
#  FOLDER STRUCTURE ANALYZER
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
#  FOLDER STRUCTURE ANALYZER
# ═══════════════════════════════════════════════════════════════════════════

def analyze_folder_structure(root_path, dat_type,
                              progress_cb=None, cancel_flag=None):
    """
    Fast folder structure analyzer.

    Key design decisions for performance:
    - DirEntry.is_file() / is_dir() use the cached stat from os.scandir —
      no extra syscall per entry.
    - is_hidden_or_system() (Windows API call) is skipped for files in Mixed
      mode. We only need to know "do files exist here" — we do not need to
      filter every individual file for hidden/system status for the purpose
      of structure analysis. Hidden-file filtering is left to the dat engine.
    - For Mixed mode, file count is an estimate (all non-directory entries in
      the directory, not filtered for extension). This is fast and accurate
      enough for structural analysis.
    - Subdirectory depth is discovered by recursion but only directories are
      recursed — we do not stat every file in every leaf directory.
    - progress_cb(folder_name) is called after each top-level folder completes.
    - cancel_flag is a threading.Event; scan exits cleanly if set.
    """
    is_zipped = (dat_type == "zipped")

    def fast_scan_node(dirpath, depth):
        """
        Characterize one directory node.
        Returns dict with direct_items, direct_subdirs, max_depth, total_items.
        Does NOT recurse into leaf directories unless they contain subdirs.
        """
        if cancel_flag and cancel_flag.is_set():
            return {"path": dirpath, "depth": depth, "direct_items": 0,
                    "direct_subdirs": 0, "max_depth": depth, "total_items": 0}
        node = {"path": dirpath, "depth": depth, "direct_items": 0,
                "direct_subdirs": 0, "max_depth": depth, "total_items": 0}
        try:
            with os.scandir(dirpath) as it:
                entries = list(it)
        except (PermissionError, OSError):
            return node

        subdirs = []
        for e in entries:
            try:
                if e.is_symlink():
                    continue
                if e.is_dir(follow_symlinks=False):
                    # Only skip hidden/system check on dirs (we recurse into them)
                    if is_hidden_or_system(e.path):
                        continue
                    node["direct_subdirs"] += 1
                    subdirs.append(e.path)
                elif e.is_file(follow_symlinks=False):
                    if is_zipped:
                        if e.name.lower().endswith(".zip"):
                            node["direct_items"] += 1
                    else:
                        # Mixed: count all non-hidden files without per-file
                        # Windows API calls — fast approximation sufficient
                        # for structural analysis.
                        if not e.name.startswith("."):
                            node["direct_items"] += 1
            except OSError:
                continue

        node["total_items"] = node["direct_items"]

        for sub_path in subdirs:
            if cancel_flag and cancel_flag.is_set():
                break
            child = fast_scan_node(sub_path, depth + 1)
            node["total_items"] += child["total_items"]
            node["max_depth"]    = max(node["max_depth"], child["max_depth"])

        return node

    findings = {
        "root_path": root_path, "dat_type": dat_type,
        "top_folders": 0, "total_items": 0, "max_depth": 0,
        "folders_with_direct_items": 0, "folders_as_containers": 0,
        "folders_with_nested_subdirs": 0, "folders_empty": 0,
        "depth_histogram": {}, "notes": [], "nodes": [],
    }

    try:
        with os.scandir(root_path) as it:
            raw = list(it)
        top_entries = sorted(
            [e for e in raw
             if not e.is_symlink()
             and e.is_dir(follow_symlinks=False)
             and not is_hidden_or_system(e.path)],
            key=lambda e: e.name.lower())
    except Exception as ex:
        findings["notes"].append("Error scanning root: " + str(ex))
        return _make_recommendation(findings)

    # Root-level items (fast count)
    root_items = sum(
        1 for e in raw
        if e.is_file(follow_symlinks=False)
        and (e.name.lower().endswith(".zip") if is_zipped
             else not e.name.startswith(".")))
    if root_items > 0:
        findings["notes"].append(
            str(root_items) + " item(s) found directly in the root folder — "
            "these will be datted separately as a root-level dat.")

    for entry in top_entries:
        if cancel_flag and cancel_flag.is_set():
            findings["notes"].append("Scan cancelled by user.")
            break

        node = fast_scan_node(entry.path, 1)
        findings["nodes"].append(node)
        findings["top_folders"]  += 1
        findings["total_items"]  += node["total_items"]
        findings["max_depth"]     = max(findings["max_depth"], node["max_depth"])
        d = node["max_depth"]
        findings["depth_histogram"][d] = findings["depth_histogram"].get(d, 0) + 1

        if node["total_items"] == 0:
            findings["folders_empty"] += 1
        elif node["direct_items"] > 0 and node["direct_subdirs"] > 0:
            findings["folders_with_direct_items"]   += 1
            findings["folders_with_nested_subdirs"] += 1
        elif node["direct_items"] > 0:
            findings["folders_with_direct_items"] += 1
        elif node["direct_subdirs"] > 0:
            findings["folders_as_containers"] += 1

        if progress_cb:
            progress_cb(entry.name, node["total_items"])

    findings["folders_flat_games"] = (
        findings["top_folders"]
        - findings["folders_empty"]
        - findings["folders_with_direct_items"]
        - findings["folders_as_containers"])

    return _make_recommendation(findings)


def _make_recommendation(findings):
    n          = findings["top_folders"]
    max_d      = findings["max_depth"]
    containers = findings["folders_as_containers"]
    nested     = findings["folders_with_nested_subdirs"]
    flat       = findings.get("folders_flat_games", 0)
    empty      = findings["folders_empty"]
    notes      = findings["notes"]
    cw         = "zip archives" if findings["dat_type"] == "zipped" else "files"

    rec = {"gen_mode": "per_root", "structure": "opt2", "dat_format": "modern",
           "incl_desc": True, "confidence": "high", "summary": "", "detail": []}
    detail = rec["detail"]

    if n == 0:
        rec["confidence"] = "none"
        rec["summary"] = "No subfolders found. Nothing to dat."
        findings["recommendation"] = rec
        return findings

    if findings["total_items"] == 0:
        rec["confidence"] = "none"
        rec["summary"] = "No content files found in any subfolder."
        findings["recommendation"] = rec
        return findings

    # Generation mode
    if n > 20 and max_d <= 2 and containers == 0:
        rec["gen_mode"] = "per_all"
        detail.append(
            str(n) + " shallow top-level folders (depth <= 2, no containers). "
            "'1 dat per root folder & all subfolders' works well here "
            "since each folder is independent.")
    else:
        rec["gen_mode"] = "per_root"
        detail.append(
            str(n) + " top-level folder(s) with content up to depth " + str(max_d) + ". "
            "'1 dat per root folder' recommended — each folder becomes "
            "one self-contained dat.")

    # Structure
    if max_d <= 2 and containers == 0 and nested == 0:
        rec["structure"] = "opt2"
        detail.append(
            "Content is flat or has at most one level of subdirectories. "
            "Structure 2 (Archives as Games) is the standard choice — "
            + cw + " become <game> entries, physical subdirs become <dir> entries.")
    elif max_d <= 2 and containers > 0:
        rec["structure"] = "opt2"
        rec["confidence"] = "medium"
        detail.append(
            str(containers) + " of " + str(n) + " top-level folder(s) act as containers "
            "(no " + cw + " directly, only subfolders). "
            "Structure 2 handles this correctly: game folders become <game> entries, "
            "container folders become <dir> entries.")
    elif max_d >= 3 and nested > (n // 2):
        rec["structure"] = "opt4"
        detail.append(
            "Deep structure detected (max depth " + str(max_d) + "). "
            + str(nested) + " folder(s) have both direct " + cw + " AND nested subdirectories. "
            "Structure 4 (First Level Dirs as Games + Merge Dirs) captures this most cleanly.")
    elif max_d >= 3 and containers > 0:
        rec["structure"] = "opt3"
        detail.append(
            "Deep structure detected (max depth " + str(max_d) + "). "
            + str(containers) + " container folder(s) found. "
            "Structure 3 (First Level Dirs as Games) maps each top-level "
            "folder to a game entry regardless of direct content.")
    else:
        rec["structure"] = "opt2"
        detail.append(
            "Moderate depth (max " + str(max_d) + " levels). "
            "Structure 2 (Archives as Games) is the standard choice.")

    # Confidence adjustments
    if containers > 0 and flat > 0 and nested > 0:
        rec["confidence"] = "medium"
        notes.append(
            "Mixed pattern: " + str(flat) + " flat game folder(s), "
            + str(containers) + " container folder(s), and "
            + str(nested) + " folder(s) with both direct content and subdirectories. "
            "Consider using the Preview window to compare structure options.")

    if empty > 0:
        notes.append(str(empty) + " empty top-level folder(s) found — these will be skipped.")

    if max_d >= 5:
        notes.append(
            "Very deep nesting detected (max " + str(max_d) + " levels). "
            "Structure 4 is recommended. If your top-level subfolders each represent "
            "independent sub-collections rather than a single game or title, consider "
            "switching Generation to '1 dat per root folder & all subfolders' so each "
            "subfolder gets its own separate dat file.")

    struct_labels = {
        "opt1": "Structure 1 (Dirs)",
        "opt2": "Structure 2 (Archives as Games)",
        "opt3": "Structure 3 (First Level Dirs as Games)",
        "opt4": "Structure 4 (First Level Dirs + Merge Dirs)",
    }
    mode_label = ("1 dat per root folder" if rec["gen_mode"] == "per_root"
                  else "1 dat per root folder & all subfolders")
    rec["summary"] = (mode_label + "  |  " + struct_labels[rec["structure"]]
                      + "  |  Modern  |  Confidence: " + rec["confidence"].upper())
    findings["recommendation"] = rec
    return findings


class AnalyzerWindow(tk.Toplevel):
    def __init__(self, parent, app):
        super().__init__(parent)
        self.app      = app
        self._result  = None
        self.title("Folder Structure Analyzer")
        self.geometry("740x580")
        self.minsize(640, 460)
        self.configure(bg=app._c["options"])
        self._c = app._c
        self._build_ui()
        self.grab_set()

    def _build_ui(self):
        c   = self._c
        BG  = c["options"]
        FG  = c["fg"]
        ENT = c["entry"]
        pad = 8

        # Paths + dat type
        top = tk.Frame(self, bg=c["paths"], padx=pad, pady=pad)
        top.pack(fill="x")
        top.columnconfigure(1, weight=1)

        tk.Label(top, text="Folder to analyze:", bg=c["paths"],
                 fg=FG).grid(row=0, column=0, sticky="w", pady=(0, 4))
        self.var_path = tk.StringVar()
        ent = ttk.Entry(top, textvariable=self.var_path)
        ent.grid(row=0, column=1, sticky="ew", padx=(6, 6))
        ttk.Button(top, text="Browse...", style="Browse.TButton",
                   command=self._browse).grid(row=0, column=2, sticky="ew")
        try:
            ent.drop_target_register(DND_FILES)
            ent.dnd_bind("<<Drop>>", self._on_drop)
        except Exception:
            pass

        tk.Label(top, text="Content type:", bg=c["paths"],
                 fg=FG).grid(row=1, column=0, sticky="w", pady=(6, 0))
        self.var_dtype = tk.StringVar(value=self.app.var_dat_type.get())
        rf = tk.Frame(top, bg=c["paths"])
        rf.grid(row=1, column=1, columnspan=2, sticky="w", pady=(6, 0))
        for txt, val in [("Mixed (Archive as File)", "mixed"),
                         ("Zipped (zip contents)",   "zipped")]:
            tk.Radiobutton(rf, text=txt, variable=self.var_dtype, value=val,
                           bg=c["paths"], fg=FG, activebackground=c["paths"],
                           selectcolor=ENT).pack(side="left", padx=(0, 20))

        btn_row = ttk.Frame(top, style="Paths.TFrame")
        btn_row.grid(row=2, column=0, columnspan=3, sticky="w", pady=(10, 0))
        self.btn_analyze = ttk.Button(btn_row, text="Analyze", style="Start.TButton",
                                      command=self._run)
        self.btn_analyze.pack(side="left")
        self.btn_analyze_stop = ttk.Button(btn_row, text="⏹  Stop",
                                            style="HardStop.TButton",
                                            command=self._cancel_analyze,
                                            state="disabled")
        self.btn_analyze_stop.pack(side="left", padx=(8, 0))

        # Findings text
        res = tk.Frame(self, bg=BG)
        res.pack(fill="both", expand=True, padx=pad, pady=(pad, 0))
        tk.Label(res, text="Findings:", bg=BG, fg=FG,
                 font=("Segoe UI", 9, "bold")).pack(anchor="w", pady=(0, 2))

        box = tk.Frame(res, bg=ENT, highlightbackground=c["sep"],
                       highlightthickness=1)
        box.pack(fill="both", expand=True)
        self.txt = tk.Text(box, height=12, wrap="word", bg=ENT, fg=FG,
                           insertbackground=FG, relief="flat", bd=0,
                           padx=6, pady=6, font=("Segoe UI", 9),
                           state="disabled")
        sb = ttk.Scrollbar(box, command=self.txt.yview)
        self.txt.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self.txt.pack(fill="both", expand=True)

        self.txt.tag_configure("h",    font=("Segoe UI", 9, "bold"), foreground="#1A4A7A")
        self.txt.tag_configure("good", foreground="#1A6A2A")
        self.txt.tag_configure("warn", foreground="#8A5A00")
        self.txt.tag_configure("stat", font=("Consolas", 9), foreground="#3A3A3A")
        self.txt.tag_configure("rech", font=("Segoe UI", 9, "bold"),
                               foreground="#1A5A1A", background="#D6EDD6")
        self.txt.tag_configure("recm", font=("Segoe UI", 9, "bold"),
                               foreground="#6A4A00", background="#F5F0D6")

        # Recommendation bar
        bar = tk.Frame(self, bg=c["header"], padx=pad, pady=6)
        bar.pack(fill="x")
        bar.columnconfigure(0, weight=1)
        self.var_rec = tk.StringVar(value="Run an analysis to see a recommendation.")
        tk.Label(bar, textvariable=self.var_rec, bg=c["header"], fg=FG,
                 font=("Segoe UI", 9, "italic"),
                 wraplength=480, justify="left").grid(row=0, column=0, sticky="w")
        self.btn_apply = ttk.Button(bar, text="Apply Recommended Settings",
                                    style="Start.TButton",
                                    command=self._apply, state="disabled")
        self.btn_apply.grid(row=0, column=1, sticky="e", padx=(12, 0))

        bot = tk.Frame(self, bg=BG, padx=pad, pady=6)
        bot.pack(fill="x")
        ttk.Button(bot, text="Close", style="Browse.TButton",
                   command=self.destroy).pack(side="right")

    def _browse(self):
        p = filedialog.askdirectory(title="Select folder to analyze", parent=self)
        if p:
            self.var_path.set(p)

    def _on_drop(self, event):
        p = clean_dnd_path(event.data)
        if p and os.path.isdir(p):
            self.var_path.set(p)
        return event.action

    def _wt(self, text, tag=""):
        self.txt.configure(state="normal")
        self.txt.insert(tk.END, text, tag) if tag else self.txt.insert(tk.END, text)
        self.txt.configure(state="disabled")

    def _clear(self):
        self.txt.configure(state="normal")
        self.txt.delete("1.0", tk.END)
        self.txt.configure(state="disabled")

    def _cancel_analyze(self):
        if hasattr(self, "_cancel_flag"):
            self._cancel_flag.set()
        self.btn_analyze_stop.configure(state="disabled")

    def _run(self):
        p = self.var_path.get().strip()
        if not p or not os.path.isdir(p):
            messagebox.showerror("Invalid folder",
                                 "Please select a valid folder to analyze.", parent=self)
            return

        # Disable controls during scan
        self._clear()
        self._wt("Scanning folder structure — please wait...\n")
        self._wt("(Folder names will appear below as each is completed.)\n\n")
        self.btn_analyze.configure(state="disabled")
        self.btn_analyze_stop.configure(state="normal")
        self._cancel_flag = threading.Event()
        self._scan_result = None

        dtype = self.var_dtype.get()

        def progress_cb(folder_name, item_count):
            """Called from worker thread after each top-level folder completes."""
            self.after(0, lambda fn=folder_name, n=item_count:
                       self._wt("  Scanned: " + fn +
                                " (" + str(n) + " items)\n"))

        def worker():
            try:
                result = analyze_folder_structure(
                    p, dtype,
                    progress_cb=progress_cb,
                    cancel_flag=self._cancel_flag)
                self.after(0, lambda r=result: self._on_scan_done(r))
            except Exception as exc:
                import traceback
                tb = traceback.format_exc()
                self.after(0, lambda t=tb: self._on_scan_error(t))

        self._scan_thread = threading.Thread(target=worker, daemon=True)
        self._scan_thread.start()

    def _on_scan_done(self, result):
        """Called on main thread when scan completes."""
        self._result = result
        self.btn_analyze.configure(state="normal")
        self.btn_analyze_stop.configure(state="disabled")
        self._wt("\nScan complete.\n\n", "h")
        self._display(result)

    def _on_scan_error(self, tb):
        """Called on main thread when scan raises."""
        self._clear()
        self._wt("ERROR during analysis:\n\n", "h")
        self._wt(tb)
        self.var_rec.set("Analysis failed — see error above.")
        self.btn_apply.configure(state="disabled")
        self.btn_analyze.configure(state="normal")
        self.btn_analyze_stop.configure(state="disabled")

    def _display(self, r):
        self._clear()
        rec = r.get("recommendation", {})
        cw  = "zip archives" if r["dat_type"] == "zipped" else "files"

        self._wt("FOLDER STRUCTURE ANALYSIS\n", "h")
        self._wt("Path : " + r["root_path"] + "\n")
        self._wt("Type : " + ("Zipped" if r["dat_type"] == "zipped"
                              else "Mixed (Archive as File)") + "\n\n")

        self._wt("STATISTICS\n", "h")
        self._wt(
            "  Top-level folders : " + str(r["top_folders"]) + "\n"
            "  Total " + cw[:16].ljust(16) + "  : " + str(r["total_items"]) + "\n"
            "  Max folder depth  : " + str(r["max_depth"]) + "\n", "stat")

        if r["depth_histogram"]:
            self._wt("  Depth distribution :\n", "stat")
            label_map = {1: "flat (items only)", 2: "one subdir level",
                         3: "two levels deep",  4: "three levels deep"}
            for d in sorted(r["depth_histogram"]):
                count = r["depth_histogram"][d]
                bar   = chr(9608) * min(count, 30)
                lbl   = label_map.get(d, str(d) + " levels deep")
                self._wt("    depth " + str(d) + "  " + lbl.ljust(22)
                        + str(count).rjust(4) + " folder(s)  " + bar + "\n", "stat")

        self._wt("\nPATTERN BREAKDOWN\n", "h")
        for label, count, desc, tag in [
            ("Flat game folders",      r.get("folders_flat_games", 0),
             "items directly, no subdirs", "good"),
            ("Games with subdirs",     r["folders_with_nested_subdirs"],
             "items directly + physical subdirs", "good"),
            ("Container folders",      r["folders_as_containers"],
             "no direct items, subdirs only", "warn"),
            ("Empty folders",          r["folders_empty"],
             "no items found", "warn"),
        ]:
            if count > 0:
                self._wt("  " + label.ljust(24) + str(count).rjust(4)
                        + "   (" + desc + ")\n", tag)

        nodes = r.get("nodes", [])
        if nodes:
            self._wt("\nSAMPLE FOLDERS (first 6)\n", "h")
            for node in nodes[:6]:
                name = os.path.basename(node["path"])
                self._wt("  " + name[:50].ljust(52)
                        + "items=" + str(node["direct_items"]).ljust(4)
                        + "subdirs=" + str(node["direct_subdirs"]).ljust(4)
                        + "depth=" + str(node["max_depth"]) + "\n", "stat")

        if r["notes"]:
            self._wt("\nNOTES\n", "h")
            for note in r["notes"]:
                self._wt("  " + note + "\n\n", "warn")

        if rec and rec.get("confidence") != "none":
            self._wt("\nRECOMMENDATION\n", "h")
            conf_tag = "rech" if rec["confidence"] == "high" else "recm"
            self._wt("  " + rec["summary"] + "\n\n", conf_tag)
            for line in rec.get("detail", []):
                self._wt("  " + line + "\n\n")
            self.var_rec.set(rec["summary"])
            self.btn_apply.configure(state="normal")
        else:
            self.var_rec.set(rec.get("summary", "Could not determine recommendation."))
            self.btn_apply.configure(state="disabled")

        self.txt.yview_moveto(0.0)

    def _apply(self):
        if not self._result:
            return
        rec = self._result.get("recommendation", {})
        if not rec or rec.get("confidence") == "none":
            return
        app = self.app
        app.var_dat_type.set(self._result["dat_type"])
        app.var_gen_mode.set(rec["gen_mode"])
        app.var_structure.set(rec["structure"])
        app.var_format.set("modern")
        app.var_inc_desc.set(True)
        app.var_input.set(self._result["root_path"])
        app._on_options_change()
        dtype_label = ("Mixed (Archive as File)"
                       if self._result["dat_type"] == "mixed" else "Zipped")
        mode_label  = ("1 dat per root folder"
                       if rec["gen_mode"] == "per_root"
                       else "1 dat per root folder & all subfolders")
        struct_map  = {"opt1": "1 - Dirs", "opt2": "2 - Archives as Games",
                       "opt3": "3 - First Level Dirs as Games",
                       "opt4": "4 - First Level Dirs + Merge Dirs"}
        messagebox.showinfo(
            "Settings Applied",
            "Recommended settings applied to the main window.\n\n"
            "Dat type    : " + dtype_label + "\n"
            "Generation  : " + mode_label + "\n"
            "Structure   : " + struct_map.get(rec["structure"], rec["structure"]) + "\n"
            "Format      : Modern\n\n"
            "Review the remaining settings (output folder, header fields, etc.) "
            "before clicking Start.",
            parent=self)
        self.destroy()



# ═══════════════════════════════════════════════════════════════════════════
#  INCREMENTAL UPDATE CONFIRMATION DIALOG
# ═══════════════════════════════════════════════════════════════════════════

class IncrementalConfirmDialog(tk.Toplevel):
    """
    Pre-run confirmation dialog for incremental update mode.
    Validates the dat-to-folder alignment, shows match stats,
    and lets the user set a new version string before proceeding.
    """
    THRESHOLD = 80.0   # warn if match % falls below this

    def __init__(self, root_win, app, s: "Settings"):
        super().__init__(root_win)
        self.title("Incremental Update — Confirm Before Starting")
        self.resizable(True, True)
        self.configure(bg=app._c["header"])
        self._s       = s
        self._c       = app._c
        self.proceed  = False
        self.new_version = ""
        self._inspection_log: list = []   # full detail for save-log
        try:
            self._build_ui()
            self._run_validation()
        except Exception as exc:
            import traceback
            # Show error in a simple label so the dialog is at least visible
            import tkinter as _tk
            _tk.Label(self, text="Dialog error:\n" + traceback.format_exc(),
                      bg=parent._c["header"], fg="#8A1A1A",
                      font=("Consolas", 8), justify="left",
                      wraplength=500).pack(padx=10, pady=10)
        self.grab_set()
        self.update_idletasks()
        self.geometry(f"{max(580, self.winfo_reqwidth()+20)}x"
                      f"{min(700, self.winfo_reqheight()+20)}+300+250")

    def _build_ui(self):
        c   = self._c
        BG  = c["header"]
        FG  = c["fg"]
        ENT = c["entry"]
        pad = 10

        # Title
        tk.Label(self, text="Incremental Update Pre-flight Check",
                 bg=BG, fg=FG,
                 font=("Segoe UI", 11, "bold")).pack(
            anchor="w", padx=pad, pady=(pad, 2))

        # Source info
        info_frm = tk.Frame(self, bg=BG, padx=pad)
        info_frm.pack(fill="x")
        for label, value in [
            ("Dat source:", self._s.incremental_dat_path),
            ("Input root:", self._s.input_root),
            ("Dat type:",   "Mixed (Archive as File)"
                            if self._s.dat_type == "mixed" else "Zipped"),
        ]:
            row = tk.Frame(info_frm, bg=BG)
            row.pack(fill="x", pady=1)
            tk.Label(row, text=label, bg=BG, fg=FG, width=14,
                     anchor="w", font=("Segoe UI", 9, "bold")).pack(side="left")
            tk.Label(row, text=value, bg=BG, fg=FG,
                     font=("Segoe UI", 9)).pack(side="left")

        ttk.Separator(self).pack(fill="x", padx=pad, pady=(8, 4))

        # Validation results
        tk.Label(self, text="Validation Results:", bg=BG, fg=FG,
                 font=("Segoe UI", 9, "bold")).pack(
            anchor="w", padx=pad, pady=(0, 4))

        res_outer = tk.Frame(self, bg=ENT, highlightbackground=c["sep"],
                             highlightthickness=1, padx=6, pady=6)
        res_outer.pack(fill="x", padx=pad)
        self.txt_val = tk.Text(res_outer, height=18, wrap="word",
                               bg=ENT, fg=FG, relief="flat", bd=0,
                               font=("Segoe UI", 9), state="disabled")
        self.txt_val.tag_configure("ok",   foreground="#1A6A2A")
        self.txt_val.tag_configure("warn", foreground="#8A4A00",
                                    font=("Segoe UI", 9, "bold"))
        self.txt_val.tag_configure("err",  foreground="#8A1A1A",
                                    font=("Segoe UI", 9, "bold"))
        self.txt_val.tag_configure("dim",  foreground="#5A5A5A")
        self.txt_val.pack(fill="x")

        ttk.Separator(self).pack(fill="x", padx=pad, pady=(8, 4))

        # New version field
        ver_frm = tk.Frame(self, bg=BG, padx=pad)
        ver_frm.pack(fill="x")
        tk.Label(ver_frm, text="Set a new <version> (optional, blank = no change):",
                 bg=BG, fg=FG, font=("Segoe UI", 9)).pack(side="left")
        self.var_version = tk.StringVar(value=self._s.version)
        ttk.Entry(ver_frm, textvariable=self.var_version,
                  width=22).pack(side="left", padx=(8, 0))

        # Mixed-mode warning
        if self._s.dat_type == "mixed":
            warn_frm = tk.Frame(self, bg="#F5F0D0", padx=pad, pady=6)
            warn_frm.pack(fill="x", padx=pad, pady=(8, 0))
            tk.Label(warn_frm,
                text="Mixed mode note: files are matched by filename and size only. "
                     "If a file was replaced with new content of the same name and "
                     "size, the change cannot be detected without a full rehash. "
                     "Use 'Rehash entire folder' below if you know replacements "
                     "of this kind have occurred.",
                bg="#F5F0D0", fg="#5A3A00",
                font=("Segoe UI", 8), wraplength=520, justify="left").pack(anchor="w")

        # Buttons
        btn_frm = tk.Frame(self, bg=BG, padx=pad, pady=pad)
        btn_frm.pack(fill="x")

        self.btn_proceed = ttk.Button(btn_frm, text="Proceed",
                                       style="Start.TButton",
                                       command=self._on_proceed,
                                       state="disabled")
        self.btn_proceed.pack(side="left")

        ttk.Button(btn_frm, text="Rehash entire folder",
                   style="SoftStop.TButton",
                   command=self._on_full_rehash).pack(side="left", padx=(8, 0))

        ttk.Button(btn_frm, text="🔄  Rescan Dats",
                   style="Browse.TButton",
                   command=self._rescan).pack(side="left", padx=(8, 0))

        self.btn_save_log = ttk.Button(btn_frm, text="💾  Save Pre-inspection Log",
                                        style="Log.TButton",
                                        command=self._save_inspection_log,
                                        state="disabled")
        self.btn_save_log.pack(side="left", padx=(8, 0))

        ttk.Button(btn_frm, text="Cancel",
                   style="Browse.TButton",
                   command=self.destroy).pack(side="right")

        # (geometry set in __init__ after full build)

    def _wv(self, text, tag=""):
        # Guard: widget may have been destroyed if user closed the dialog
        # while validation was still running in the background thread.
        try:
            if not self.winfo_exists():
                return
            self.txt_val.configure(state="normal")
            if tag:
                self.txt_val.insert(tk.END, text, tag)
            else:
                self.txt_val.insert(tk.END, text)
            self.txt_val.configure(state="disabled")
            # Auto-scroll to bottom so results are always visible
            self.txt_val.yview_moveto(1.0)
        except tk.TclError:
            pass   # widget already destroyed — silently ignore

    def _run_validation(self):
        """
        Kick off validation in a background thread so the dialog
        stays fully responsive while dats are being read and checked.
        All UI writes are dispatched via self.after(0, ...).
        """
        s        = self._s
        src_path = s.incremental_dat_path.strip()
        root     = s.input_root.strip()

        # Disable action buttons while validating
        self.btn_proceed.configure(state="disabled")

        def post(text, tag=""):
            """Thread-safe write to validation text box."""
            try:
                if self.winfo_exists():
                    self.after(0, lambda t=text, g=tag: self._wv(t, g))
            except tk.TclError:
                pass   # dialog destroyed while thread was still running

        # Clear existing log for fresh scan
        self._inspection_log.clear()
        self.after(0, lambda: self.btn_save_log.configure(state="disabled")
                   if self.winfo_exists() else None)

        def log(line):
            """Accumulate a line in the full inspection log (thread-safe list append)."""
            self._inspection_log.append(line)

        def worker():
            if not src_path:
                post("No dat source specified. Please set the dat path before starting.", "err")
                return
            if not root or not os.path.isdir(root):
                post("Input root folder not found.", "err")
                return

            dat_entries = []
            if os.path.isfile(src_path) and (src_path.lower().endswith(".xml")
                                              or src_path.lower().endswith(".dat")):
                dat_entries = [(src_path, ".")]
            elif os.path.isdir(src_path):
                for dirpath, _, filenames in os.walk(src_path):
                    for fn in sorted(filenames):
                        if ((fn.lower().endswith(".xml") or fn.lower().endswith(".dat"))
                                and not fn.lower().endswith(".old")):
                            full    = os.path.join(dirpath, fn)
                            rel_dir = os.path.relpath(dirpath, src_path)
                            dat_entries.append((full, rel_dir))
            else:
                post("Dat source not found or not a valid .xml/.dat file/folder:\n  "
                     + src_path + "\n", "err")
                return

            if not dat_entries:
                post("No dat files found in the specified folder.", "err")
                return

            ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log("=" * 72)
            log("Eggman's Datfile Creator Suite — Pre-inspection Log")
            log("Generated  : " + ts)
            log("Dat source : " + src_path)
            log("Input root : " + root)
            log("Dat type   : " + ("Mixed (Archive as File)" if s.dat_type == "mixed" else "Zipped"))
            log("=" * 72)
            log("")

            post("Found " + str(len(dat_entries)) + " dat file(s) to validate.\n\n")
            log("Found " + str(len(dat_entries)) + " dat file(s) to validate.")
            log("")

            all_ok         = True
            total_missing  = 0
            total_new      = 0
            total_fm       = 0

            for dat_path, rel_dir in sorted(dat_entries):
                gi, hd, err_s = _read_dat_index(dat_path)
                if err_s:
                    msg = "  ERROR reading " + os.path.basename(dat_path) + ": " + err_s
                    post(msg + "\n", "err")
                    log(msg)
                    all_ok = False
                    continue

                dat_name_hdr = hd.get("name", os.path.basename(dat_path))
                post("  " + dat_name_hdr + "\n", "dim")
                log("")
                log("DAT: " + dat_name_hdr)
                log("File: " + dat_path)

                if rel_dir and rel_dir != ".":
                    folder_candidate = os.path.join(root, rel_dir)
                else:
                    parts = [p.strip() for p in dat_name_hdr.split(" - ")]
                    folder_candidate = None
                    for p in reversed(parts):
                        candidate = os.path.join(root, p)
                        if os.path.isdir(candidate):
                            folder_candidate = candidate
                            break
                    if folder_candidate is None:
                        folder_candidate = root

                if not os.path.isdir(folder_candidate):
                    msg = "    [??] Source folder not found: " + folder_candidate
                    post(msg + "\n", "warn")
                    log(msg)
                    all_ok = False
                    continue

                log("Folder: " + folder_candidate)

                vr    = validate_dat_vs_folder(gi, folder_candidate, s.dat_type)
                pct   = vr["match_pct"]
                found = vr["found_in_folder"]
                total = vr["total_in_dat"]
                extra = vr["extra"]

                sym = "OK" if pct >= self.THRESHOLD else "!!"
                if pct < self.THRESHOLD:
                    all_ok = False

                # Screen: brief summary only
                tag = "ok" if sym == "OK" else "warn"
                post("    [" + sym + "] " + str(found) + "/" + str(total)
                     + " entries found in folder (" + f"{pct:.1f}%" + " match)", tag)
                if extra:
                    post("  |  " + str(len(extra)) + " new item(s) to add", "ok")
                post("\n")

                log("Result : [" + sym + "] " + str(found) + "/" + str(total)
                    + " entries (" + f"{pct:.1f}%" + " match)")

                # Screen: brief counts only
                gm = vr["missing"]
                fm = vr.get("file_missing", [])
                if gm:
                    miss_names = ", ".join(gm[:3]) + ("..." if len(gm) > 3 else "")
                    post("    Missing games/files: " + miss_names + "\n", "warn")
                if fm:
                    post("    Missing rom files  : " + str(len(fm))
                         + " file(s). First: " + fm[0] + "\n", "warn")

                # Log: FULL detail — every anomaly
                if gm:
                    log("  Missing game entries (" + str(len(gm)) + "):")
                    for g in gm:
                        log("    - " + g)
                    total_missing += len(gm)

                if fm:
                    log("  Missing rom files (" + str(len(fm)) + "):")
                    for f in fm:
                        log("    - " + f)
                    total_fm += len(fm)

                if extra:
                    log("  New items in folder (not in dat) (" + str(len(extra)) + "):")
                    for e in extra[:200]:   # cap at 200 to avoid huge logs
                        log("    + " + e)
                    if len(extra) > 200:
                        log("    ... (" + str(len(extra) - 200) + " more not shown)")
                    total_new += len(extra)

                if not gm and not fm and not extra:
                    log("  No anomalies — fully matched.")

            log("")
            log("=" * 72)
            log("SUMMARY")
            log("  Dat files scanned      : " + str(len(dat_entries)))
            log("  Missing game entries   : " + str(total_missing))
            log("  Missing rom files      : " + str(total_fm))
            log("  New items (to be added): " + str(total_new))
            log("  Overall status         : " + ("PASSED" if all_ok else "WARNINGS — review before proceeding"))
            log("=" * 72)

            post("\n")

            def finish(ok=all_ok):
                if ok:
                    self._wv("Validation passed. Ready to proceed.", "ok")
                else:
                    self._wv(
                        "Some dats have low match rates. Review before proceeding. "
                        "You can still proceed — entries not found in the folder "
                        "will be removed from the dat.", "warn")
                self.btn_proceed.configure(state="normal")
                if self._inspection_log:
                    self.btn_save_log.configure(state="normal")

            self.after(0, finish)

        threading.Thread(target=worker, daemon=True).start()

    def _save_inspection_log(self):
        """Save the full pre-inspection log to a timestamped file."""
        if not self._inspection_log:
            messagebox.showinfo("Log", "No inspection data to save.", parent=self)
            return
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        p  = filedialog.asksaveasfilename(
            parent=self,
            title="Save Pre-inspection Log",
            initialfile="dat_creator_pre-inspection_log_" + ts + ".txt",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")])
        if not p:
            return
        try:
            with open(p, "w", encoding="utf-8", newline="\n") as f:
                f.write("\n".join(self._inspection_log) + "\n")
            messagebox.showinfo("Saved", "Pre-inspection log saved to:\n" + p,
                                parent=self)
        except Exception as exc:
            messagebox.showerror("Error", repr(exc), parent=self)

    def _rescan(self):
        """Clear results and re-run validation — useful after changing settings."""
        self.btn_proceed.configure(state="disabled")
        self.btn_save_log.configure(state="disabled")
        self._inspection_log.clear()
        self.txt_val.configure(state="normal")
        self.txt_val.delete("1.0", tk.END)
        self.txt_val.configure(state="disabled")
        self._wv("Rescanning...\n")
        self._run_validation()

    def _on_proceed(self):
        self.new_version = self.var_version.get().strip()
        self.proceed     = True
        self.destroy()

    def _on_full_rehash(self):
        self.new_version = self.var_version.get().strip()
        self.proceed     = True
        # Signal to caller to disable incremental for this run
        self._s_override_incremental = False
        self.destroy()


import re as _re
from pathlib import Path as _Path
from datetime import datetime as _datetime

# ── Module-level logic (reused from standalone app) ──────────────────────────

_FILENAME_TOKEN_RE = re.compile(r"\(\d{4}-\d{2}-\d{2}_RomVault\)")
_DATE_TAG_RE       = re.compile(r"(<date>\s*)(\d{4}-\d{2}-\d{2})(\s*</date>)", re.IGNORECASE)
_DESCRIPTION_RE    = re.compile(r"(<description>)[^<]*(</description>)", re.IGNORECASE)
_CATEGORY_RE       = re.compile(r"(<category>)[^<]*(</category>)",       re.IGNORECASE)
_VERSION_RE        = re.compile(r"(<version>)[^<]*(</version>)",         re.IGNORECASE)
_AUTHOR_RE         = re.compile(r"(<author>)[^<]*(</author>)",           re.IGNORECASE)
_URL_RE            = re.compile(r"(<url>)[^<]*(</url>)",                 re.IGNORECASE)
_HOMEPAGE_RE       = re.compile(r"(<homepage>)[^<]*(</homepage>)",       re.IGNORECASE)
_COMMENT_RE        = re.compile(r"(<comment>)[^<]*(</comment>)",         re.IGNORECASE)
_FORCEPACKING_RE   = re.compile(r'<romvault\s+forcepacking\s*=\s*["\']fileonly["\']', re.IGNORECASE)
_HEADER_BLOCK_RE   = re.compile(r'(<header\b[^>]*>)(.*?)(</header>)',    re.IGNORECASE | re.DOTALL)
_HEADER_CLOSE_RE   = re.compile(r'(</header>)',                          re.IGNORECASE)
_DATE_STRICT_RE    = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_BHU_OPTIONAL_FIELDS = [
    ("description", _DESCRIPTION_RE),
    ("category",    _CATEGORY_RE),
    ("version",     _VERSION_RE),
    ("author",      _AUTHOR_RE),
    ("url",         _URL_RE),
    ("homepage",    _HOMEPAGE_RE),
    ("comment",     _COMMENT_RE),
]

_BHU_FIELD_LABELS = {
    "description": "Description",
    "category":    "Category",
    "version":     "Version",
    "author":      "Author",
    "url":         "URL",
    "homepage":    "Homepage",
    "comment":     "Comment",
}


def _bhu_validate_date(s: str) -> bool:
    if not _DATE_STRICT_RE.match(s.strip()):
        return False
    try:
        _datetime.strptime(s.strip(), "%Y-%m-%d"); return True
    except ValueError:
        return False


def _bhu_read_text(path) -> tuple:
    data = path.read_bytes()
    for enc in ("utf-8-sig", "utf-8", "utf-16", "utf-16le", "utf-16be", "cp1252"):
        try:
            return data.decode(enc), enc
        except Exception:
            pass
    raise UnicodeDecodeError("unknown", b"", 0, 1, f"Cannot decode {path}")


def _bhu_detect_indent(text: str) -> str:
    m = re.search(r"<header[^>]*>[ \t]*\r?\n([ \t]+)<", text, re.IGNORECASE)
    return m.group(1) if m else "\t\t"


def _bhu_iter_datfiles(root) -> list:
    root = _Path(root)
    if root.is_file():
        return [root]
    return [p for p in root.rglob("*")
            if p.is_file() and p.suffix.lower() in {".dat", ".xml"}]


def _bhu_update_file(path, new_date: str, field_values: dict,
                     add_forcepacking: bool) -> dict:
    details = {
        "path_before": str(path), "path_after": str(path),
        "fn_date_before": None, "fn_date_after": None,
        "hdr_date_before": None, "hdr_date_after": None,
        "fields_added": [], "fields_updated": [], "fields_cleared": [],
        "renamed": False, "content_updated": False, "warnings": [],
    }
    text, enc = _bhu_read_text(path)
    new_text = text

    hm = _HEADER_BLOCK_RE.search(new_text)
    if not hm:
        details["warnings"].append("No <header> block found — skipped.")
        return details

    h_open, h_inner, h_close = hm.group(1), hm.group(2), hm.group(3)
    working = h_inner + h_close
    indent  = _bhu_detect_indent(new_text)

    # Insert missing optional tags
    missing = ""
    for fname, pat in _BHU_OPTIONAL_FIELDS:
        if not pat.search(working):
            missing += f"{indent}<{fname}></{fname}>\n"
            details["fields_added"].append(fname)
    if missing:
        working = _HEADER_CLOSE_RE.sub(missing + r"\1", working, count=1)

    # Update date
    details["hdr_date_before"] = (m2 := _DATE_TAG_RE.search(working)) and m2.group(2) or None
    working, n = _DATE_TAG_RE.subn(rf"\g<1>{new_date}\g<3>", working)
    if n:
        details["hdr_date_after"] = new_date
        details["fields_updated"].append("date")
    else:
        details["warnings"].append("No <date> tag found.")

    # Optional fields
    for fname, pat in _BHU_OPTIONAL_FIELDS:
        val = field_values.get(fname)
        if val is None:
            continue
        working, n = pat.subn(rf"\g<1>{val}\g<2>", working)
        if n:
            (details["fields_cleared"] if val == "" else details["fields_updated"]).append(fname)
        else:
            details["warnings"].append(f"No <{fname}> tag found.")

    # forcepacking
    if add_forcepacking:
        if _FORCEPACKING_RE.search(working):
            details["warnings"].append("<romvault forcepacking> already present.")
        else:
            working = _HEADER_CLOSE_RE.sub(
                f'{indent}<romvault forcepacking="fileonly"/>\n\\1', working, count=1)
            details["fields_updated"].append("forcepacking")

    new_text = new_text[:hm.start()] + h_open + working + new_text[hm.end():]
    if new_text != text:
        _Path(path).write_text(new_text, encoding=enc, newline="")
        details["content_updated"] = True

    # Rename filename
    old_name = _Path(path).name
    details["fn_date_before"] = (m3 := _FILENAME_TOKEN_RE.search(old_name)) and \
        re.search(r"\d{4}-\d{2}-\d{2}", m3.group(0)).group(0) or None
    if _FILENAME_TOKEN_RE.search(old_name):
        new_name = _FILENAME_TOKEN_RE.sub(f"({new_date}_RomVault)", old_name, count=1)
        if new_name != old_name:
            target = _Path(path).with_name(new_name)
            if target.exists():
                details["warnings"].append(f"Rename skipped (exists): {target.name}")
            else:
                _Path(path).rename(target)
                details["renamed"] = True
                details["path_after"] = str(target)
                details["fn_date_after"] = new_date
    else:
        details["warnings"].append("No filename date token found to rename.")
    if not details["renamed"]:
        details["fn_date_after"] = details["fn_date_before"]
    return details


# ═══════════════════════════════════════════════════════════════════════════
#  BULK DATFILE HEADER UPDATER WINDOW
# ═══════════════════════════════════════════════════════════════════════════

class BulkHeaderUpdaterWindow(tk.Toplevel):
    """
    Bulk updates header fields and date-based filenames across all datfiles
    in a folder (or a single file). Accessible via Tools menu.

    Rules:
      - Leave a field blank  → leave existing dat content untouched
      - Enter a value        → overwrite that field in every dat
      - Tick 'Clear'         → erase the field content in every dat (write empty tags)
      - Date is always required and always updates the <date> tag and the
        (YYYY-MM-DD_RomVault) filename token
    """

    def __init__(self, root_win, app):
        super().__init__(root_win)
        self.app   = app
        self._c    = app._c
        self.title("Eggman's Datfile Creator Suite — Bulk Header Updater")
        self.geometry("780x780+150+150")
        self.minsize(680, 620)
        self.configure(bg="#EDE8E0")

        self._log_lines: list = []
        self._q = queue.Queue()
        self._bhu_cancel = threading.Event()

        # Variables
        self.var_path  = tk.StringVar()
        self.var_date  = tk.StringVar(value=_datetime.now().strftime("%Y-%m-%d"))
        self.var_fp    = tk.BooleanVar(value=False)
        self.field_vars: dict  = {f: tk.StringVar()          for f, _ in _BHU_OPTIONAL_FIELDS}
        self.clear_vars: dict  = {f: tk.BooleanVar(value=False) for f, _ in _BHU_OPTIONAL_FIELDS}
        self.field_entries: dict = {}

        self._build_ui()
        self._poll_queue()
        self.grab_set()

    def _build_ui(self):
        c   = self._c
        FG  = c["fg"]
        ENT = c["entry"]
        SEP = c["sep"]
        PAD = 6

        # ── Instructions (Paths section color) ────────────────────────────────
        inst_lf = ttk.LabelFrame(self, text="  Instructions  ",
                                  style="Paths.TLabelframe", padding=PAD)
        inst_lf.pack(fill="x", padx=PAD, pady=(PAD, 0))

        instructions = (
            "This tool updates the header fields of all datfiles found in a folder "
            "(or a single file) in bulk.\n\n"
            "• Date — always required. Updates the <date> header tag AND renames the "
            "(YYYY-MM-DD_RomVault) token in each filename to match.\n\n"
            "• Optional fields — leave blank to skip (existing content untouched). "
            "Enter a value to overwrite that field in every dat. "
            "Tick 'Clear' to erase the field content (writes empty tags).\n\n"
            "• forcepacking — adds <romvault forcepacking=\"fileonly\"/> to the header "
            "if not already present.\n\n"
            "Only .xml and .dat files are processed. Subfolders are searched recursively."
        )
        tk.Label(inst_lf, text=instructions, bg=c["paths"], fg=FG,
                 font=("Segoe UI", 9), wraplength=700, justify="left",
                 anchor="nw").pack(fill="x")

        # ── Header fields (Header section color) ──────────────────────────────
        hdr_lf = ttk.LabelFrame(self, text="  Header Fields  ",
                                  style="Header.TLabelframe", padding=PAD)
        hdr_lf.pack(fill="x", padx=PAD, pady=(4, 0))
        hdr_lf.columnconfigure(1, weight=1)

        # Date row
        ttk.Label(hdr_lf, text="New date (YYYY-MM-DD):",
                  style="Header.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 4))
        self._date_ent = ttk.Entry(hdr_lf, textvariable=self.var_date, width=16)
        self._date_ent.grid(row=0, column=1, sticky="w", padx=(4, 0), pady=(0, 4))
        ttk.Label(hdr_lf, text="(required — always updates <date> tag and filename)",
                  style="Header.TLabel", foreground="#7A6A50").grid(
            row=0, column=2, sticky="w", padx=(8, 0))

        # Optional field rows
        for r, (fname, _) in enumerate(_BHU_OPTIONAL_FIELDS, start=1):
            ttk.Label(hdr_lf, text=_BHU_FIELD_LABELS[fname] + ":",
                      style="Header.TLabel").grid(row=r, column=0, sticky="w",
                                                   padx=(0, 8), pady=(0, 3))
            ent = ttk.Entry(hdr_lf, textvariable=self.field_vars[fname])
            ent.grid(row=r, column=1, sticky="ew", padx=(4, 4), pady=(0, 3))
            self.field_entries[fname] = ent
            cb = ttk.Checkbutton(hdr_lf, text="Clear",
                                 style="Header.TCheckbutton",
                                 variable=self.clear_vars[fname],
                                 command=lambda fn=fname: self._on_clear(fn))
            cb.grid(row=r, column=2, sticky="w", padx=(4, 0), pady=(0, 3))

        # forcepacking checkbox
        ttk.Checkbutton(hdr_lf,
            text='Add <romvault forcepacking="fileonly"/> (skipped if already present)',
            style="Header.TCheckbutton",
            variable=self.var_fp).grid(
            row=len(_BHU_OPTIONAL_FIELDS)+1, column=0, columnspan=3,
            sticky="w", pady=(6, 0))

        # ── Source path (Options section color) ───────────────────────────────
        src_lf = ttk.LabelFrame(self, text="  Source  ",
                                  style="Options.TLabelframe", padding=PAD)
        src_lf.pack(fill="x", padx=PAD, pady=(4, 0))
        src_lf.columnconfigure(1, weight=1)

        ttk.Label(src_lf, text="Dat files or folder:",
                  style="Options.TLabel").grid(row=0, column=0, sticky="w")
        self._path_ent = ttk.Entry(src_lf, textvariable=self.var_path)
        self._path_ent.grid(row=0, column=1, sticky="ew", padx=(4, 4))

        btn_frm = ttk.Frame(src_lf, style="Options.TFrame")
        btn_frm.grid(row=0, column=2)
        ttk.Button(btn_frm, text="Browse File",
                   style="Browse.TButton",
                   command=self._browse_file).pack(side="left", padx=(0, 4))
        ttk.Button(btn_frm, text="Browse Folder",
                   style="Browse.TButton",
                   command=self._browse_folder).pack(side="left")

        try:
            self._path_ent.drop_target_register(DND_FILES)
            self._path_ent.dnd_bind("<<Drop>>", self._on_drop)
        except Exception:
            pass

        # ── Activity log (Progress section color) ─────────────────────────────
        log_lf = ttk.LabelFrame(self, text="  Activity Log  ",
                                  style="Progress.TLabelframe", padding=PAD)
        log_lf.pack(fill="both", expand=True, padx=PAD, pady=(4, 0))
        log_lf.columnconfigure(0, weight=1)
        log_lf.rowconfigure(0, weight=1)

        self._txt = tk.Text(log_lf, height=8, wrap="word",
                            bg=c["list"], fg=FG,
                            relief="flat", bd=0,
                            font=("Consolas", 9),
                            state="disabled")
        sb = ttk.Scrollbar(log_lf, orient="vertical", command=self._txt.yview)
        self._txt.configure(yscrollcommand=sb.set)
        self._txt.grid(row=0, column=0, sticky="nsew")
        sb.grid(row=0, column=1, sticky="ns")

        self._txt.tag_configure("ok",   foreground="#1A6A2A",
                                 font=("Consolas", 9, "bold"))
        self._txt.tag_configure("warn", foreground="#8A5A00")
        self._txt.tag_configure("err",  foreground="#8A1A1A",
                                 font=("Consolas", 9, "bold"))
        self._txt.tag_configure("dim",  foreground="#5A5A5A")

        # ── Buttons ───────────────────────────────────────────────────────────
        bot = ttk.Frame(self, style="Buttons.TFrame", padding=(PAD, 4))
        bot.pack(fill="x", padx=0, pady=(4, PAD))

        self._run_btn = ttk.Button(bot, text="▶  Run",
                                    style="Start.TButton",
                                    command=self._on_run)
        self._run_btn.pack(side="left")

        self._stop_btn = ttk.Button(bot, text="⏹  Stop",
                                     style="HardStop.TButton",
                                     command=self._on_stop,
                                     state="disabled")
        self._stop_btn.pack(side="left", padx=(8, 0))

        ttk.Button(bot, text="Clear Log",
                   style="Browse.TButton",
                   command=self._clear_log).pack(side="left", padx=(16, 0))

        ttk.Button(bot, text="💾  Save Log",
                   style="Log.TButton",
                   command=self._save_log).pack(side="left", padx=(8, 0))

        ttk.Button(bot, text="Close",
                   style="Browse.TButton",
                   command=self.destroy).pack(side="right")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _on_clear(self, fname):
        state = "disabled" if self.clear_vars[fname].get() else "normal"
        self.field_entries[fname].configure(state=state)

    def _browse_file(self):
        p = filedialog.askopenfilename(
            title="Select a dat/xml file", parent=self,
            filetypes=[("Dat files", "*.dat *.xml"), ("All files", "*.*")])
        if p:
            self.var_path.set(p)

    def _browse_folder(self):
        p = filedialog.askdirectory(title="Select folder containing dat files",
                                    parent=self)
        if p:
            self.var_path.set(p)

    def _on_drop(self, event):
        p = clean_dnd_path(event.data)
        if p:
            self.var_path.set(p)
        return event.action

    def _write(self, text, tag=""):
        self._txt.configure(state="normal")
        if tag:
            self._txt.insert(tk.END, text, tag)
        else:
            self._txt.insert(tk.END, text)
        self._txt.configure(state="disabled")
        self._txt.yview_moveto(1.0)

    def _clear_log(self):
        self._txt.configure(state="normal")
        self._txt.delete("1.0", tk.END)
        self._txt.configure(state="disabled")
        self._log_lines.clear()

    def _save_log(self):
        if not self._log_lines:
            messagebox.showinfo("Log", "Nothing to save yet.", parent=self)
            return
        ts = _datetime.now().strftime("%Y%m%d_%H%M%S")
        p  = filedialog.asksaveasfilename(
            parent=self, title="Save Log",
            initialfile=f"header_updater_log_{ts}.txt",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")])
        if not p:
            return
        try:
            _Path(p).write_text("\n".join(self._log_lines) + "\n", encoding="utf-8")
            messagebox.showinfo("Saved", f"Log saved to:\n{p}", parent=self)
        except Exception as e:
            messagebox.showerror("Error", repr(e), parent=self)

    def _poll_queue(self):
        try:
            while True:
                text, tag = self._q.get_nowait()
                self._write(text, tag)
        except queue.Empty:
            pass
        self.after(100, self._poll_queue)

    def _post(self, text, tag=""):
        self._q.put((text, tag))

    def _log(self, line):
        self._log_lines.append(line)

    def _collect_field_values(self) -> dict:
        result = {}
        for fname, _ in _BHU_OPTIONAL_FIELDS:
            if self.clear_vars[fname].get():
                result[fname] = ""
            else:
                val = self.field_vars[fname].get().strip()
                result[fname] = val if val else None
        return result

    # ── Run ───────────────────────────────────────────────────────────────────

    def _on_stop(self):
        self._bhu_cancel.set()
        self._stop_btn.configure(state="disabled")

    def _on_run(self):
        self._bhu_cancel.clear()
        new_date = self.var_date.get().strip()
        target   = self.var_path.get().strip().strip('"')

        if not _bhu_validate_date(new_date):
            messagebox.showerror("Invalid date",
                                 "Enter a valid date in YYYY-MM-DD format.",
                                 parent=self)
            return
        if not target:
            messagebox.showerror("Missing path",
                                 "Select a dat file or folder first.",
                                 parent=self)
            return

        root_path = _Path(target)
        if not root_path.exists():
            messagebox.showerror("Not found",
                                 f"Path does not exist:\n{root_path}",
                                 parent=self)
            return

        field_values = self._collect_field_values()
        add_fp       = self.var_fp.get()

        self._run_btn.configure(state="disabled")
        self._stop_btn.configure(state="normal")
        stamp = _datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._post(f"\n=== RUN {stamp} ===\n", "dim")
        self._post(f"Target : {root_path}\n", "dim")
        self._post(f"Date   : {new_date}\n\n", "dim")
        self._log(f"=== RUN {stamp} ===")
        self._log(f"Target: {root_path}  Date: {new_date}")

        def worker():
            try:
                files = _bhu_iter_datfiles(root_path)
                self._post(f"Found {len(files)} file(s) to process.\n\n")
                self._log(f"Found {len(files)} files")
                ok = err = warn = 0

                for f in files:
                    if self._bhu_cancel.is_set():
                        self._post("\n[STOPPED by user]\n", "warn")
                        self._log("[STOPPED by user]")
                        break
                    try:
                        d = _bhu_update_file(f, new_date, field_values, add_fp)
                        warn += len(d["warnings"])

                        lines  = f"[OK] {d['path_after']}\n"
                        lines += f"     filename : {d['fn_date_before']} → {d['fn_date_after']}\n"
                        lines += f"     header   : {d['hdr_date_before']} → {d['hdr_date_after']}\n"
                        if d["fields_added"]:
                            lines += f"     added    : {', '.join(d['fields_added'])}\n"
                        if d["fields_updated"]:
                            lines += f"     updated  : {', '.join(d['fields_updated'])}\n"
                        if d["fields_cleared"]:
                            lines += f"     cleared  : {', '.join(d['fields_cleared'])}\n"
                        if d["renamed"]:
                            lines += "     renamed  : yes\n"
                        self._post(lines, "ok")

                        for w in d["warnings"]:
                            self._post(f"     ⚠ {w}\n", "warn")
                        self._post("\n")

                        self._log(f"[OK] {d['path_after']}")
                        ok += 1

                    except Exception as e:
                        err += 1
                        self._post(f"[ERROR] {f}\n        {type(e).__name__}: {e}\n\n", "err")
                        self._log(f"[ERROR] {f}: {e}")

                summary = (f"=== COMPLETE — Success: {ok}  "
                           f"Warnings: {warn}  Errors: {err} ===\n")
                self._post(summary, "ok" if not err else "warn")
                self._log(summary.strip())

            finally:
                self.after(0, lambda: self._run_btn.configure(state="normal"))
                self.after(0, lambda: self._stop_btn.configure(state="disabled"))

        threading.Thread(target=worker, daemon=True).start()


#  GAME AND ROM COUNTER
# ═══════════════════════════════════════════════════════════════════════════


def _fmt_size(bytes_val: int) -> str:
    """Format bytes as human-readable decimal size (MB/GB/TB). Never uses bytes."""
    if bytes_val < 1_000_000:
        return f"{bytes_val/1_000_000:.2f} MB"
    elif bytes_val < 1_000_000_000:
        return f"{bytes_val/1_000_000:.1f} MB"
    elif bytes_val < 1_000_000_000_000:
        return f"{bytes_val/1_000_000_000:.2f} GB"
    else:
        return f"{bytes_val/1_000_000_000_000:.2f} TB"

def _scan_dat_counts(dat_path: str) -> Tuple[int, int, int, str, str]:
    """
    Parse a datfile and return (game_count, rom_count, total_bytes, dat_name, error).
    total_bytes is the sum of all rom size= attributes (uncompressed).
    """
    try:
        game_index, header, err = _read_dat_index(dat_path)
        if err:
            return 0, 0, 0, os.path.basename(dat_path), err
        dat_name    = header.get("name", "") or os.path.splitext(os.path.basename(dat_path))[0]
        game_count  = len(game_index)
        rom_count   = 0
        total_bytes = 0
        for gdata in game_index.values():
            for rom in gdata.get("roms", []):
                rom_count += 1
                try:
                    total_bytes += int(rom.get("size") or 0)
                except (ValueError, TypeError):
                    pass
        return game_count, rom_count, total_bytes, dat_name, ""
    except Exception as exc:
        return 0, 0, 0, os.path.basename(dat_path), str(exc)


class GameRomCounterWindow(tk.Toplevel):
    """
    Scans a folder of datfiles recursively and reports game and rom counts
    per dat, with multi-selection subtotals and a summary panel.

    Hierarchy: dats are shown indented by their relative folder depth.
    Multi-select via Ctrl+click, Shift+click, or clicking checkboxes.
    Selection subtotals update live.
    """

    COL_NAME  = 0
    COL_GAMES = 1
    COL_ROMS  = 2
    COL_PATH  = 3   # hidden, used for lookup

    def __init__(self, root_win, app):
        super().__init__(root_win)
        self.app   = app
        self._c    = app._c
        self.title("Eggman's Datfile Creator Suite — Game & ROM Counter")
        self.geometry("960x800+160+160")
        self.minsize(720, 600)
        self.configure(bg="#EDE8E0")

        self._results: list = []
        self._scan_thread = None
        self._grc_cancel  = threading.Event()
        self._view_mode   = "tree"  # "tree" | "flat"
        self._sort_col    = None    # last sorted column
        self._sort_asc    = True    # ascending?
        self._expanded    = True    # all expanded?

        self._build_ui()
        self.grab_set()

    def _build_ui(self):
        c   = self._c
        FG  = c["fg"]
        PAD = 6

        # ── Source path ───────────────────────────────────────────────────────
        src_lf = ttk.LabelFrame(self, text="  Dat Source Folder  ",
                                 style="Paths.TLabelframe", padding=PAD)
        src_lf.pack(fill="x", padx=PAD, pady=(PAD, 0))
        src_lf.columnconfigure(1, weight=1)

        ttk.Label(src_lf, text="Folder containing dat files:",
                  style="Paths.TLabel").grid(row=0, column=0, sticky="w")
        self.var_path = tk.StringVar()
        self._path_ent = ttk.Entry(src_lf, textvariable=self.var_path)
        self._path_ent.grid(row=0, column=1, sticky="ew", padx=(4, 4))

        btn_f = ttk.Frame(src_lf, style="Paths.TFrame")
        btn_f.grid(row=0, column=2)
        self._scan_btn = ttk.Button(btn_f, text="▶  Scan",
                                     style="Start.TButton",
                                     command=self._on_scan)
        self._scan_btn.pack(side="left")
        self._grc_stop_btn = ttk.Button(btn_f, text="⏹  Stop",
                                         style="HardStop.TButton",
                                         command=self._on_grc_stop,
                                         state="disabled")
        self._grc_stop_btn.pack(side="left", padx=(6, 0))
        ttk.Button(btn_f, text="Browse...",
                   style="Browse.TButton",
                   command=self._browse).pack(side="left", padx=(6, 0))

        try:
            self._path_ent.drop_target_register(DND_FILES)
            self._path_ent.dnd_bind("<<Drop>>", self._on_drop)
        except Exception:
            pass

        # ── Dat list (treeview) ───────────────────────────────────────────────
        list_lf = ttk.LabelFrame(self, text="  Dat Files  ",
                                  style="Options.TLabelframe", padding=PAD)
        list_lf.pack(fill="both", expand=True, padx=PAD, pady=(4, 0))
        list_lf.columnconfigure(0, weight=1)
        list_lf.rowconfigure(1, weight=1)

        # View-mode toggle bar
        tog = ttk.Frame(list_lf, style="Options.TFrame")
        tog.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 4))
        ttk.Label(tog, text="View:", style="Options.TLabel").pack(side="left", padx=(0, 6))
        self._view_btn_tree = ttk.Button(tog, text="🌳  Tree View",
                                          style="Preview.TButton",
                                          command=lambda: self._set_view("tree"))
        self._view_btn_tree.pack(side="left", padx=(0, 4))
        self._view_btn_flat = ttk.Button(tog, text="📋  Flat List",
                                          style="Browse.TButton",
                                          command=lambda: self._set_view("flat"))
        self._view_btn_flat.pack(side="left")

        cols = ("games", "roms", "size")
        self._tree = ttk.Treeview(list_lf, columns=cols,
                                   selectmode="extended", show="tree headings")
        self._tree.heading("#0",    text="Dat Name / Folder ↕",  anchor="w",
                           command=lambda: self._sort_by("name"))
        self._tree.heading("games", text="Games ↕",              anchor="e",
                           command=lambda: self._sort_by("games"))
        self._tree.heading("roms",  text="ROMs ↕",               anchor="e",
                           command=lambda: self._sort_by("roms"))
        self._tree.heading("size",  text="Uncompressed Size ↕",  anchor="e",
                           command=lambda: self._sort_by("size"))
        self._tree.column("#0",    width=460, stretch=True,  anchor="w", minwidth=200)
        self._tree.column("games", width=80,  stretch=False, anchor="e")
        self._tree.column("roms",  width=80,  stretch=False, anchor="e")
        self._tree.column("size",  width=130, stretch=False, anchor="e")

        vsb = ttk.Scrollbar(list_lf, orient="vertical",   command=self._tree.yview)
        hsb = ttk.Scrollbar(list_lf, orient="horizontal", command=self._tree.xview)
        self._tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self._tree.grid(row=1, column=0, sticky="nsew")
        vsb.grid(row=1, column=1, sticky="ns")
        hsb.grid(row=2, column=0, sticky="ew")

        # Tag styles for tree rows
        self._tree.tag_configure("folder",  font=("Segoe UI", 9, "bold"),
                                  foreground="#1A4A7A")
        self._tree.tag_configure("dat",     font=("Segoe UI", 9))
        self._tree.tag_configure("error",   foreground="#8A1A1A",
                                  font=("Segoe UI", 9, "italic"))
        self._tree.tag_configure("sel_dat", background=c["sel"],
                                  font=("Segoe UI", 9, "bold"))

        self._tree.bind("<<TreeviewSelect>>", self._on_select)

        # Context menu
        self._ctx = tk.Menu(self._tree, tearoff=0,
                             bg=c["entry"], fg=FG,
                             activebackground=c["sel"])
        self._ctx.add_command(label="Select All Dats",     command=self._select_all)
        self._ctx.add_command(label="Deselect All",        command=self._deselect_all)
        self._ctx.add_separator()
        self._ctx.add_command(label="Expand / Collapse All",
                              command=self._toggle_expand)
        self._ctx.add_separator()
        self._ctx.add_command(label="🌳  Switch to Tree View",
                              command=lambda: self._set_view("tree"))
        self._ctx.add_command(label="📋  Switch to Flat List",
                              command=lambda: self._set_view("flat"))
        self._tree.bind("<Button-3>", self._show_ctx)

        # ── Selection subtotal ────────────────────────────────────────────────
        sel_lf = ttk.LabelFrame(self, text="  Selection Subtotal  ",
                                  style="Header.TLabelframe", padding=PAD)
        sel_lf.pack(fill="x", padx=PAD, pady=(4, 0))

        self.var_sel_info = tk.StringVar(
            value="Select one or more dats to see a subtotal.")
        ttk.Label(sel_lf, textvariable=self.var_sel_info,
                  style="Header.TLabel",
                  font=("Segoe UI", 9, "bold")).pack(anchor="w")

        # ── Summary ───────────────────────────────────────────────────────────
        sum_lf = ttk.LabelFrame(self, text="  Collection Summary  ",
                                  style="Progress.TLabelframe", padding=PAD)
        sum_lf.pack(fill="x", padx=PAD, pady=(4, 0))
        sum_lf.columnconfigure(1, weight=1)
        sum_lf.columnconfigure(3, weight=1)

        def stat(parent, label, var, r, c):
            ttk.Label(parent, text=label,
                      style="Progress.TLabel",
                      foreground="#5A5A5A").grid(row=r, column=c,   sticky="w", padx=(0, 4))
            ttk.Label(parent, textvariable=var,
                      style="Progress.TLabel",
                      font=("Segoe UI", 9, "bold")).grid(row=r, column=c+1, sticky="w")

        self.var_total_dats  = tk.StringVar(value="—")
        self.var_total_games = tk.StringVar(value="—")
        self.var_total_roms  = tk.StringVar(value="—")
        self.var_total_size  = tk.StringVar(value="—")
        self.var_avg_games   = tk.StringVar(value="—")
        self.var_avg_roms    = tk.StringVar(value="—")
        self.var_max_games   = tk.StringVar(value="—")
        self.var_max_roms    = tk.StringVar(value="—")
        self.var_empty_dats  = tk.StringVar(value="—")
        self.var_errors      = tk.StringVar(value="—")
        self.var_folders     = tk.StringVar(value="—")

        stat(sum_lf, "Total dat files:",          self.var_total_dats,  0, 0)
        stat(sum_lf, "Total folders:",            self.var_folders,     0, 2)
        stat(sum_lf, "Total games:",              self.var_total_games, 1, 0)
        stat(sum_lf, "Total ROMs:",               self.var_total_roms,  1, 2)
        stat(sum_lf, "Total uncompressed size:",  self.var_total_size,  2, 0)
        stat(sum_lf, "Avg games / dat:",          self.var_avg_games,   3, 0)
        stat(sum_lf, "Avg ROMs / dat:",           self.var_avg_roms,    3, 2)
        stat(sum_lf, "Largest (games):",          self.var_max_games,   4, 0)
        stat(sum_lf, "Largest (ROMs):",           self.var_max_roms,    4, 2)
        stat(sum_lf, "Empty dats (0 games):",     self.var_empty_dats,  5, 0)
        stat(sum_lf, "Parse errors:",             self.var_errors,      5, 2)

        # ── Status + buttons ──────────────────────────────────────────────────
        bot = ttk.Frame(self, style="Buttons.TFrame", padding=(PAD, 4))
        bot.pack(fill="x", padx=0, pady=(4, PAD))

        self.var_status = tk.StringVar(value="Enter a folder path and click Scan.")
        ttk.Label(bot, textvariable=self.var_status,
                  background=c["buttons"],
                  foreground="#5A5A5A",
                  font=("Segoe UI", 8, "italic")).pack(side="left")

        ttk.Button(bot, text="Export CSV",
                   style="Log.TButton",
                   command=self._export_csv).pack(side="right", padx=(8, 0))
        ttk.Button(bot, text="Close",
                   style="Browse.TButton",
                   command=self.destroy).pack(side="right")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _on_grc_stop(self):
        self._grc_cancel.set()
        self._grc_stop_btn.configure(state="disabled")

    def _browse(self):
        p = filedialog.askdirectory(title="Select folder containing dat files",
                                    parent=self)
        if p:
            self.var_path.set(p)

    def _on_drop(self, event):
        p = clean_dnd_path(event.data)
        if p and os.path.isdir(p):
            self.var_path.set(p)
        return event.action

    def _on_scan(self):
        p = self.var_path.get().strip()
        if not p or not os.path.isdir(p):
            messagebox.showerror("Invalid folder",
                                 "Please select a valid folder.", parent=self)
            return
        self._grc_cancel.clear()
        self._scan_btn.configure(state="disabled")
        self._grc_stop_btn.configure(state="normal")
        self.var_status.set("Scanning...")
        self._tree.delete(*self._tree.get_children())
        self._results.clear()
        self._clear_summary()
        threading.Thread(target=self._worker, args=(p,), daemon=True).start()

    def _worker(self, root_path: str):
        """Scan all dats recursively, build results, post to UI via after()."""
        exts = {".xml", ".dat"}
        dat_files = []
        for dirpath, dirnames, filenames in os.walk(root_path):
            dirnames.sort(key=str.lower)
            for fn in sorted(filenames, key=str.lower):
                if os.path.splitext(fn)[1].lower() in exts:
                    dat_files.append(os.path.join(dirpath, fn))

        total = len(dat_files)
        self.after(0, lambda: self.var_status.set(
            f"Found {total} dat file(s) — parsing..."))

        results = []
        for i, fp in enumerate(dat_files):
            g, r, b, name, err = _scan_dat_counts(fp)
            rel = os.path.relpath(fp, root_path)
            results.append({
                "path":     fp,
                "rel":      rel,
                "name":     name,
                "games":    g,
                "roms":     r,
                "bytes":    b,
                "error":    err,
                "rel_dir":  os.path.dirname(rel),
            })
            if (i + 1) % 50 == 0 or (i + 1) == total:
                done = i + 1
                self.after(0, lambda d=done, t=total:
                           self.var_status.set(f"Parsed {d}/{t}..."))

        self._results = results
        self.after(0, self._populate_tree)

    def _populate_tree(self):
        self._tree.delete(*self._tree.get_children())

        if self._view_mode == "flat":
            self._populate_flat()
        else:
            self._populate_hier()

        self._update_summary()
        n = len(self._results)
        mode_hint = "Tree" if self._view_mode == "tree" else "Flat list"
        self.var_status.set(
            f"{mode_hint} — {n} dat(s). "
            f"Ctrl+click / Shift+click to select for subtotals.")
        self._scan_btn.configure(state="normal")
        self._grc_stop_btn.configure(state="disabled")

    def _populate_hier(self):
        """Build the hierarchical folder+dat tree."""
        folder_nodes: dict = {}

        for r in self._results:
            rel_dir = r["rel_dir"]
            if rel_dir and rel_dir not in folder_nodes:
                parts = rel_dir.replace("\\", "/").split("/")
                current_dir = ""
                for part in parts:
                    current_dir = os.path.join(current_dir, part) if current_dir else part
                    if current_dir not in folder_nodes:
                        parent_iid = folder_nodes.get(
                            os.path.dirname(current_dir), "") or ""
                        iid = self._tree.insert(
                            parent_iid, "end",
                            text="📁 " + part,
                            values=("", "", ""),
                            tags=("folder",),
                            open=self._expanded)
                        folder_nodes[current_dir] = iid

            parent_iid = folder_nodes.get(rel_dir, "") if rel_dir else ""
            self._insert_dat_row(parent_iid, r)

    def _populate_flat(self):
        """Insert all dat rows directly at root — no folder grouping."""
        for r in self._results:
            self._insert_dat_row("", r)

    def _insert_dat_row(self, parent_iid: str, r: dict):
        """Insert one dat or error row into the tree."""
        if r["error"]:
            self._tree.insert(parent_iid, "end",
                               text="⚠ " + os.path.basename(r["path"]),
                               values=("ERR", r["error"][:60], ""),
                               tags=("error",))
        else:
            self._tree.insert(parent_iid, "end",
                               text="  " + r["name"],
                               values=(f"{r['games']:,}",
                                       f"{r['roms']:,}",
                                       _fmt_size(r.get("bytes", 0))),
                               tags=("dat",))

    def _update_summary(self):
        data = [r for r in self._results if not r["error"]]
        if not data:
            return
        total_g = sum(r["games"] for r in data)
        total_r = sum(r["roms"]  for r in data)
        total_b = sum(r.get("bytes", 0) for r in data)
        n       = len(data)
        folders = len({r["rel_dir"] for r in self._results if r["rel_dir"]})
        max_g_r = max(data, key=lambda r: r["games"])
        max_r_r = max(data, key=lambda r: r["roms"])
        empty   = sum(1 for r in data if r["games"] == 0)
        errors  = sum(1 for r in self._results if r["error"])

        self.var_total_dats.set(f"{n:,}")
        self.var_folders.set(f"{folders:,}")
        self.var_total_games.set(f"{total_g:,}")
        self.var_total_roms.set(f"{total_r:,}")
        self.var_total_size.set(_fmt_size(total_b))
        self.var_avg_games.set(f"{total_g/n:,.1f}" if n else "—")
        self.var_avg_roms.set(f"{total_r/n:,.1f}"  if n else "—")
        self.var_max_games.set(
            f"{max_g_r['games']:,}  ({max_g_r['name'][:40]})")
        self.var_max_roms.set(
            f"{max_r_r['roms']:,}  ({max_r_r['name'][:40]})")
        self.var_empty_dats.set(f"{empty:,}")
        self.var_errors.set(f"{errors:,}")

    def _clear_summary(self):
        for v in (self.var_total_dats, self.var_total_games, self.var_total_roms,
                  self.var_total_size, self.var_avg_games, self.var_avg_roms,
                  self.var_max_games, self.var_max_roms, self.var_empty_dats,
                  self.var_errors, self.var_folders):
            v.set("—")

    def _on_select(self, _event=None):
        sel = self._tree.selection()
        games = roms = count = 0
        total_bytes = 0
        # Map displayed name → result dict for byte lookup
        name_to_bytes = {r["name"]: r.get("bytes", 0) for r in self._results}
        for iid in sel:
            tags = self._tree.item(iid, "tags")
            if "dat" in tags:
                vals = self._tree.item(iid, "values")
                try:
                    games += int(vals[0].replace(",", ""))
                    roms  += int(vals[1].replace(",", ""))
                    # Get bytes from results by matching name
                    iid_name = self._tree.item(iid, "text").lstrip()
                    total_bytes += name_to_bytes.get(iid_name, 0)
                    count += 1
                except (ValueError, IndexError):
                    pass
        if count:
            self.var_sel_info.set(
                f"{count} dat(s) selected  —  "
                f"Games: {games:,}  |  ROMs: {roms:,}  |  "
                f"Size: {_fmt_size(total_bytes)}  |  "
                f"Avg games/dat: {games/count:,.1f}")
        else:
            self.var_sel_info.set(
                "Select one or more dats (Ctrl+click / Shift+click) to see a subtotal.")

    def _select_all(self):
        """Select every dat row (non-folder)."""
        all_iids = []
        def collect(parent=""):
            for iid in self._tree.get_children(parent):
                tags = self._tree.item(iid, "tags")
                if "dat" in tags or "error" in tags:
                    all_iids.append(iid)
                collect(iid)
        collect()
        self._tree.selection_set(all_iids)

    def _deselect_all(self):
        self._tree.selection_set([])

    # ── View mode ─────────────────────────────────────────────────────────────

    def _set_view(self, mode: str):
        if mode == self._view_mode and self._tree.get_children():
            return
        self._view_mode = mode
        self._view_btn_tree.configure(
            style="Preview.TButton" if mode == "tree" else "Browse.TButton")
        self._view_btn_flat.configure(
            style="Preview.TButton" if mode == "flat" else "Browse.TButton")
        if self._results:
            self._populate_tree()

    # ── Expand / collapse all ──────────────────────────────────────────────────

    def _toggle_expand(self):
        self._expanded = not self._expanded
        def walk(parent=""):
            for iid in self._tree.get_children(parent):
                self._tree.item(iid, open=self._expanded)
                walk(iid)
        walk()

    # ── Column sort ───────────────────────────────────────────────────────────

    def _sort_by(self, col: str):
        if not self._results:
            return
        if self._sort_col == col:
            self._sort_asc = not self._sort_asc
        else:
            self._sort_col = col
            self._sort_asc = True

        key_fn = {
            "name":  lambda r: r["name"].lower(),
            "games": lambda r: r["games"],
            "roms":  lambda r: r["roms"],
            "size":  lambda r: r.get("bytes", 0),
        }[col]

        self._results.sort(key=key_fn, reverse=not self._sort_asc)

        arrows  = {"name": "Dat Name / Folder", "games": "Games",
                   "roms": "ROMs",              "size":  "Uncompressed Size"}
        dir_sym = " ▲" if self._sort_asc else " ▼"
        for c_id, base in [("#0", "name"), ("games", "games"),
                            ("roms", "roms"), ("size", "size")]:
            suffix = dir_sym if base == col else " ↕"
            self._tree.heading(c_id, text=arrows[base] + suffix)

        self._populate_tree()

    # ── Context menu ──────────────────────────────────────────────────────────

    def _show_ctx(self, event):
        try:
            self._ctx.tk_popup(event.x_root, event.y_root)
        finally:
            self._ctx.grab_release()

    def _export_csv(self):
        if not self._results:
            messagebox.showinfo("No data", "Run a scan first.", parent=self)
            return
        import csv, io
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        p  = filedialog.asksaveasfilename(
            parent=self, title="Export CSV",
            initialfile=f"dat_game_rom_counts_{ts}.csv",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
        if not p:
            return
        try:
            with open(p, "w", newline="", encoding="utf-8-sig") as fh:
                w = csv.writer(fh)
                w.writerow(["Dat Name", "Games", "ROMs",
                            "Uncompressed Size", "Bytes", "Relative Path", "Error"])
                for r in self._results:
                    w.writerow([r["name"], r["games"], r["roms"],
                                _fmt_size(r.get("bytes", 0)), r.get("bytes", 0),
                                r["rel"], r["error"]])
            messagebox.showinfo("Exported", f"CSV saved to:\n{p}", parent=self)
        except Exception as exc:
            messagebox.showerror("Error", repr(exc), parent=self)


# This file is spliced in before ENTRY POINT

# ── Optional: send2trash for Recycle Bin support in Archive Extractor ─────────
try:
    import send2trash as _au_send2trash
    _AU_TRASH_AVAILABLE = True
except ImportError:
    _au_send2trash = None
    _AU_TRASH_AVAILABLE = False

# ── Archive Utilities core library (prefixed _au_) ────────────────────────────

import zipfile as _au_zipfile
import shutil  as _au_shutil
import subprocess as _au_subprocess
import stat    as _au_stat
from pathlib import Path as _au_Path
from collections import deque as _au_deque
from datetime import timedelta as _au_timedelta

_AU_INVALID_WIN_CHARS = r'<>:"/\|?*'


def _au_run_7z(sevenzip_path: str, args: list):
    p = _au_subprocess.run(
        [sevenzip_path] + args,
        stdout=_au_subprocess.PIPE, stderr=_au_subprocess.PIPE,
        text=True, errors="replace")
    return p.returncode, p.stdout, p.stderr


def _au_sanitize(name: str) -> str:
    import re as _re2
    name = _re2.sub(f"[{_re2.escape(_AU_INVALID_WIN_CHARS)}]", "_", name)
    return name.rstrip(" .") or "extracted"


def _au_classify_zip_native(path):
    try:
        with _au_zipfile.ZipFile(path, "r") as zf:
            infos = zf.infolist()
    except Exception:
        return "bad", None
    files, has_dir = [], False
    for info in infos:
        n = info.filename
        if n.endswith("/"): has_dir = True; continue
        if "/" in n: has_dir = True
        files.append(n)
    if len(files) == 1 and not has_dir and "/" not in files[0]:
        return "single", files[0]
    return "folder", None


def _au_classify_via_7z(path, sevenzip_path: str):
    rc, _, _ = _au_run_7z(sevenzip_path, ["l", str(path)])
    return ("bad", None) if rc != 0 else ("folder", None)


def _au_classify(path, sevenzip_path: str):
    ext = _au_Path(path).suffix.lower()
    return _au_classify_zip_native(path) if ext == ".zip" \
        else _au_classify_via_7z(path, sevenzip_path)


def _au_merge_dir(src, dst):
    for item in src.iterdir():
        d = dst / item.name
        if d.exists():
            if d.is_dir() and item.is_dir():
                _au_merge_dir(item, d)
                _au_shutil.rmtree(item, ignore_errors=True)
            else:
                (d if d.is_dir() else d).unlink(missing_ok=True) \
                    if not d.is_dir() else _au_shutil.rmtree(d, ignore_errors=True)
                _au_shutil.move(str(item), str(d))
        else:
            _au_shutil.move(str(item), str(d))


def _au_flatten_double_nest(target):
    children = list(target.iterdir())
    if len(children) != 1: return
    only = children[0]
    if not only.is_dir() or only.name != target.name: return
    _au_merge_dir(only, target)
    _au_shutil.rmtree(only, ignore_errors=True)


def _au_extract_single(archive, out_dir, sevenzip_path: str):
    out_dir.mkdir(parents=True, exist_ok=True)
    rc, _, err = _au_run_7z(sevenzip_path, ["e", "-y", f"-o{out_dir}", str(archive)])
    return rc == 0, err.strip()


def _au_extract_to_folder(archive, target, sevenzip_path: str):
    target.mkdir(parents=True, exist_ok=True)
    tmp = target / "__tmp_extract__"
    if tmp.exists(): _au_shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True, exist_ok=True)
    rc, _, err = _au_run_7z(sevenzip_path, ["x", "-y", f"-o{tmp}", str(archive)])
    if rc != 0:
        _au_shutil.rmtree(tmp, ignore_errors=True)
        return False, err.strip()
    _au_merge_dir(tmp, target)
    _au_shutil.rmtree(tmp, ignore_errors=True)
    _au_flatten_double_nest(target)
    return True, ""


def _au_delete_archive(path, mode: str):
    if mode == "keep": return True, ""
    if mode == "recycle":
        if not _AU_TRASH_AVAILABLE: return False, "send2trash not installed"
        try: _au_send2trash.send2trash(str(path)); return True, ""
        except Exception as e: return False, str(e)
    try: _au_Path(path).unlink(); return True, ""
    except Exception as e: return False, str(e)


def _au_move_mirrored(archive, src_root, move_root):
    try:
        rel  = archive.relative_to(src_root)
        dest = move_root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        _au_shutil.move(str(archive), str(dest))
        return True, str(dest)
    except Exception as e: return False, str(e)


def _au_move_flat(archive, move_root):
    try:
        move_root.mkdir(parents=True, exist_ok=True)
        dest = move_root / archive.name
        stem, suffix, n = archive.stem, archive.suffix, 1
        while dest.exists():
            dest = move_root / f"{stem}({n}){suffix}"; n += 1
        _au_shutil.move(str(archive), str(dest))
        return True, str(dest)
    except Exception as e: return False, str(e)


def _au_scan_for_archives(folder, exts: set) -> list:
    found = []
    for ext in exts:
        found.extend(folder.rglob(f"*{ext}"))
    return sorted(found)


# ── Shared widgets (Suite-themed) ─────────────────────────────────────────────

class _ArchiveFolderPicker(tk.Frame):
    """Drop zone + entry + browse button, Suite-themed."""

    def __init__(self, parent, app, label="Drop folder here — or Browse →",
                 disabled=False, **kwargs):
        c = app._c
        super().__init__(parent, bg=c["options"], **kwargs)
        self._c = c
        self._disabled = disabled
        self._var = tk.StringVar()

        state   = "disabled" if disabled else "normal"
        zone_bg = c["entry"] if not disabled else c["options"]

        self.zone = tk.Label(self, text=label,
                             bg=zone_bg, fg="#5A5A5A",
                             font=("Segoe UI", 9),
                             pady=6, cursor="hand2" if not disabled else "",
                             relief="flat", anchor="w", padx=8)
        self.zone.pack(fill="x", pady=(0, 3))
        if not disabled:
            self.zone.bind("<Button-1>", lambda e: self._browse())

        row = tk.Frame(self, bg=c["options"])
        row.pack(fill="x")
        row.columnconfigure(0, weight=1)

        self.entry = ttk.Entry(row, textvariable=self._var, state=state)
        self.entry.grid(row=0, column=0, sticky="ew")

        self.btn = ttk.Button(row, text="Browse...", style="Browse.TButton",
                              command=self._browse, state=state)
        self.btn.grid(row=0, column=1, padx=(4, 0))

        if not disabled:
            try:
                self.zone.drop_target_register(DND_FILES)
                self.zone.dnd_bind("<<Drop>>", self._on_drop)
                self.entry.drop_target_register(DND_FILES)
                self.entry.dnd_bind("<<Drop>>", self._on_drop)
            except Exception:
                pass

    def _browse(self):
        d = filedialog.askdirectory()
        if d: self.set(d)

    def _on_drop(self, event):
        p = clean_dnd_path(event.data)
        if p and os.path.isdir(p): self.set(p)
        return event.action

    def set(self, path: str):
        self._var.set(path)
        name = _au_Path(path).name if path else ""
        self.zone.config(
            text=("📂  " + name) if name else "Drop folder here — or Browse →",
            fg="#1A4A7A" if name else "#5A5A5A")

    def enable(self, yes: bool):
        state = "normal" if yes else "disabled"
        self.entry.configure(state=state)
        self.btn.configure(state=state)
        self.zone.config(cursor="hand2" if yes else "")
        if yes: self.zone.bind("<Button-1>", lambda e: self._browse())
        else:   self.zone.unbind("<Button-1>")

    def get(self) -> str:
        return self._var.get().strip()


class _ArchiveLogPane(tk.Frame):
    """Scrollable log text widget, Suite-themed."""
    COLORS = {
        "ok":     "#1A6A2A",
        "fail":   "#8A1A1A",
        "warn":   "#8A5A00",
        "info":   "#1E90FF",
        "mute":   "#5A5A5A",
        "skip":   "#5A5A5A",
        "nested": "#7A5A9A",
    }

    def __init__(self, parent, app, **kwargs):
        c = app._c
        super().__init__(parent, bg=c["list"], **kwargs)
        self._app = app

        self.text = tk.Text(self, bg=c["list"], fg=c["fg"],
                            font=("Consolas", 9), relief="flat", bd=0,
                            state="disabled", wrap="none", height=8)
        vsb = ttk.Scrollbar(self, orient="vertical",   command=self.text.yview)
        hsb = ttk.Scrollbar(self, orient="horizontal", command=self.text.xview)
        self.text.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.text.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        for tag, color in self.COLORS.items():
            self.text.tag_configure(tag, foreground=color)
        self.text.tag_configure("nested", foreground="#7A5A9A",
                                font=("Consolas", 9, "bold"))

    def write(self, tag: str, msg: str):
        def _do():
            self.text.config(state="normal")
            self.text.insert("end", msg, tag)
            self.text.see("end")
            self.text.config(state="disabled")
        self.after(0, _do)

    def clear(self):
        self.text.config(state="normal")
        self.text.delete("1.0", "end")
        self.text.config(state="disabled")

    def save(self):
        content = self.text.get("1.0", "end").strip()
        if not content: return
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        p = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            initialfile=f"archive_utility_log_{ts}.txt")
        if p:
            _au_Path(p).write_text(content, encoding="utf-8")


# ═══════════════════════════════════════════════════════════════════════════
#  RECURSIVE ARCHIVE EXTRACTOR WINDOW
# ═══════════════════════════════════════════════════════════════════════════

class RecursiveArchiveExtractorWindow(tk.Toplevel):
    """
    Recursive ZIP / 7Z / RAR extraction tool.
    Adapted from Eggman's Archive Utilities v2.0, Suite-themed.
    """

    def __init__(self, root_win, app):
        super().__init__(root_win)
        self.app   = app
        self._c    = app._c
        self._stop = False
        self.title("Eggman's Datfile Creator Suite — Recursive Archive Extractor")
        self.geometry("920x860+175+175")
        self.minsize(760, 600)
        self.configure(bg=self._c["options"])
        self._build_ui()
        self.grab_set()

    def _build_ui(self):
        c   = self._c
        PAD = 8

        # ── Description ───────────────────────────────────────────────────────
        inst_lf = ttk.LabelFrame(self, text="  Recursive Archive Extractor  ",
                                  style="Paths.TLabelframe", padding=PAD)
        inst_lf.pack(fill="x", padx=PAD, pady=(PAD, 0))
        tk.Label(inst_lf,
                 text="Recursively extracts ZIP, 7Z, and RAR archives into their own named "
                      "subfolders. Detects archives nested inside extracted content and can "
                      "auto-extract them in the same pass. Extracted archives can be kept, "
                      "recycled, permanently deleted, or moved to a separate location.\n"
                      "Uses the 7-Zip-ZStandard path configured in the main Suite settings.",
                 bg=c["paths"], fg=c["fg"], font=("Segoe UI", 9),
                 wraplength=860, justify="left", anchor="w").pack(fill="x")

        if not _AU_TRASH_AVAILABLE:
            tk.Label(inst_lf,
                     text="⚠  Recycle Bin option unavailable — install send2trash: "
                          "pip install send2trash",
                     bg=c["paths"], fg="#8A5A00",
                     font=("Segoe UI", 8)).pack(anchor="w")

        # ── Source ────────────────────────────────────────────────────────────
        src_lf = ttk.LabelFrame(self, text="  Source Folder  ",
                                  style="Options.TLabelframe", padding=PAD)
        src_lf.pack(fill="x", padx=PAD, pady=(4, 0))
        self.src = _ArchiveFolderPicker(src_lf, self.app)
        self.src.pack(fill="x")

        # ── Destination ───────────────────────────────────────────────────────
        dst_lf = ttk.LabelFrame(self, text="  Destination  ",
                                  style="Header.TLabelframe", padding=PAD)
        dst_lf.pack(fill="x", padx=PAD, pady=(4, 0))

        mode_row = tk.Frame(dst_lf, bg=c["header"])
        mode_row.pack(fill="x", pady=(0, 4))
        self.dst_mode = tk.StringVar(value="same")
        for text, val in [("Same as source", "same"),
                           ("Mirror to custom destination", "custom")]:
            tk.Radiobutton(mode_row, text=text, variable=self.dst_mode, value=val,
                           bg=c["header"], fg=c["fg"], selectcolor=c["entry"],
                           activebackground=c["header"], font=("Segoe UI", 9),
                           command=self._on_dst_mode).pack(side="left", padx=(0, 14))

        self.dst = _ArchiveFolderPicker(dst_lf, self.app,
                                         label="Drop destination root here — or Browse →",
                                         disabled=True)
        self.dst.pack(fill="x")
        tk.Label(dst_lf,
                 text="  Mirror example:  D:\\source\\sub\\file.zip  →  E:\\dest\\source\\sub\\file\\",
                 bg=c["header"], fg="#5A5A5A", font=("Segoe UI", 8, "italic")).pack(anchor="w")

        # ── Options ───────────────────────────────────────────────────────────
        opt_lf = ttk.LabelFrame(self, text="  Options  ",
                                  style="Options.TLabelframe", padding=PAD)
        opt_lf.pack(fill="x", padx=PAD, pady=(4, 0))

        row1 = tk.Frame(opt_lf, bg=c["options"])
        row1.pack(fill="x", pady=(0, 2))
        tk.Label(row1, text="Formats:", bg=c["options"], fg=c["fg"],
                 font=("Segoe UI", 9)).pack(side="left")

        self.fmt_zip = tk.BooleanVar(value=True)
        self.fmt_7z  = tk.BooleanVar(value=True)
        self.fmt_rar = tk.BooleanVar(value=True)
        for var, lbl in [(self.fmt_zip, ".zip"), (self.fmt_7z, ".7z"),
                          (self.fmt_rar, ".rar")]:
            tk.Checkbutton(row1, text=lbl, variable=var,
                           bg=c["options"], fg=c["fg"], selectcolor=c["entry"],
                           activebackground=c["options"],
                           font=("Segoe UI", 9)).pack(side="left", padx=(8, 0))

        self.recurse_var = tk.BooleanVar(value=True)
        tk.Checkbutton(row1, text="Recursive", variable=self.recurse_var,
                       bg=c["options"], fg=c["fg"], selectcolor=c["entry"],
                       activebackground=c["options"],
                       font=("Segoe UI", 9)).pack(side="left", padx=(24, 0))

        self.nested_var = tk.BooleanVar(value=False)
        tk.Checkbutton(row1, text="Auto-extract nested archives",
                       variable=self.nested_var,
                       bg=c["options"], fg="#7A5A9A", selectcolor=c["entry"],
                       activebackground=c["options"],
                       font=("Segoe UI", 9, "bold")).pack(side="left", padx=(24, 0))

        row2a = tk.Frame(opt_lf, bg=c["options"])
        row2a.pack(fill="x", pady=(0, 2))
        tk.Label(row2a, text="After extraction:", bg=c["options"], fg=c["fg"],
                 font=("Segoe UI", 9)).pack(side="left")
        self.after_mode = tk.StringVar(value="keep")
        after_opts = [("Keep archive", "keep", c["fg"]),
                      ("→ Recycle Bin", "recycle", "#1E6A4A"),
                      ("→ Permanent delete", "permanent", "#8A5A00")]
        for txt, val, fg in after_opts:
            state = "normal"
            if val == "recycle" and not _AU_TRASH_AVAILABLE:
                state = "disabled"; txt += " (send2trash missing)"
            tk.Radiobutton(row2a, text=txt, variable=self.after_mode, value=val,
                           bg=c["options"], fg=fg, selectcolor=c["entry"],
                           activebackground=c["options"], font=("Segoe UI", 9),
                           state=state, command=self._on_after_mode).pack(
                               side="left", padx=(10, 0))

        row2b = tk.Frame(opt_lf, bg=c["options"])
        row2b.pack(fill="x")
        tk.Label(row2b, text=" " * 17, bg=c["options"]).pack(side="left")
        for txt, val in [("→ Move (mirror structure)", "move_mirror"),
                          ("→ Move (flat dump)", "move_flat")]:
            tk.Radiobutton(row2b, text=txt, variable=self.after_mode, value=val,
                           bg=c["options"], fg="#1A6A2A", selectcolor=c["entry"],
                           activebackground=c["options"], font=("Segoe UI", 9),
                           command=self._on_after_mode).pack(side="left", padx=(10, 0))

        self.move_dst_frame = tk.Frame(opt_lf, bg=c["options"])
        tk.Label(self.move_dst_frame, text="  Move destination:",
                 bg=c["options"], fg=c["fg"],
                 font=("Segoe UI", 9)).pack(anchor="w", pady=(4, 0))
        self.move_dst = _ArchiveFolderPicker(
            self.move_dst_frame, self.app,
            label="Drop move-destination folder here — or Browse →")
        self.move_dst.pack(fill="x")

        # ── Status + Progress ─────────────────────────────────────────────────
        stat_row = tk.Frame(self, bg=self._c["buttons"])
        stat_row.pack(fill="x", padx=PAD, pady=(4, 1))
        self.stat_var = tk.StringVar(value="Ready.")
        ttk.Label(stat_row, textvariable=self.stat_var,
                  background=self._c["buttons"],
                  foreground="#5A5A5A",
                  font=("Segoe UI", 8, "italic")).pack(side="left")

        self.progress = ttk.Progressbar(self, mode="determinate", maximum=100)
        self.progress.pack(fill="x", padx=PAD, pady=(1, 4))

        # ── Log ───────────────────────────────────────────────────────────────
        log_lf = ttk.LabelFrame(self, text="  Activity Log  ",
                                  style="Progress.TLabelframe", padding=4)
        log_lf.pack(fill="both", expand=True, padx=PAD, pady=(0, 4))
        self.log = _ArchiveLogPane(log_lf, self.app)
        self.log.pack(fill="both", expand=True)

        # ── Buttons ───────────────────────────────────────────────────────────
        bot = ttk.Frame(self, style="Buttons.TFrame", padding=(PAD, 4))
        bot.pack(fill="x", pady=(0, PAD))

        self.start_btn = ttk.Button(bot, text="▶  Extract",
                                     style="Start.TButton",
                                     command=self._start)
        self.start_btn.pack(side="left")

        self.stop_btn = ttk.Button(bot, text="⏹  Stop",
                                    style="HardStop.TButton",
                                    command=lambda: setattr(self, "_stop", True),
                                    state="disabled")
        self.stop_btn.pack(side="left", padx=(8, 0))

        ttk.Button(bot, text="💾  Save Log", style="Log.TButton",
                   command=self.log.save).pack(side="right")
        ttk.Button(bot, text="Clear Log", style="Browse.TButton",
                   command=self.log.clear).pack(side="right", padx=(0, 8))

    def _on_dst_mode(self):
        self.dst.enable(self.dst_mode.get() == "custom")

    def _on_after_mode(self):
        is_move = self.after_mode.get() in ("move_mirror", "move_flat")
        if is_move: self.move_dst_frame.pack(fill="x", padx=8, pady=(0, 4))
        else:       self.move_dst_frame.pack_forget()

    def _stat(self, msg):
        self.after(0, lambda: self.stat_var.set(msg))

    def _prog(self, val):
        self.after(0, lambda: self.progress.config(value=val))

    def _start(self):
        sevenzip = self.app._read_settings().sevenzip_path
        if not os.path.isfile(sevenzip):
            messagebox.showerror("7z not found",
                                 f"7z.exe not found at:\n{sevenzip}\n\n"
                                 "Check the 7-Zip-ZStandard path in Suite settings.",
                                 parent=self)
            return

        src_path = self.src.get()
        if not src_path or not _au_Path(src_path).is_dir():
            self.log.write("fail", "ERROR: Source folder not set or does not exist.\n")
            return

        custom_dst = None
        if self.dst_mode.get() == "custom":
            custom_dst = self.dst.get()
            if not custom_dst:
                self.log.write("fail", "ERROR: Custom destination not set.\n"); return

        after = self.after_mode.get()
        move_root = None
        if after in ("move_mirror", "move_flat"):
            move_root = self.move_dst.get()
            if not move_root:
                self.log.write("fail", "ERROR: Move destination not set.\n"); return
            move_root = _au_Path(move_root)

        exts = set()
        if self.fmt_zip.get(): exts.add(".zip")
        if self.fmt_7z.get():  exts.add(".7z")
        if self.fmt_rar.get(): exts.add(".rar")
        if not exts:
            self.log.write("fail", "ERROR: No formats selected.\n"); return

        self._stop = False
        self.start_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")

        threading.Thread(
            target=self._run,
            args=(sevenzip, _au_Path(src_path),
                  _au_Path(custom_dst) if custom_dst else None,
                  exts, after, move_root,
                  self.recurse_var.get(), self.nested_var.get()),
            daemon=True).start()

    def _run(self, sevenzip, src_root, dst_root, exts, after_mode,
             move_root, recurse, auto_nested):
        t0 = time.time()
        initial = []
        for ext in exts:
            initial.extend(src_root.rglob(f"*{ext}") if recurse
                           else src_root.glob(f"*{ext}"))
        initial = sorted(set(initial))

        if not initial:
            self.log.write("info", f"No archives found under: {src_root}\n")
            self._finish(); return

        queue      = _au_deque(initial)
        queued_set = set(initial)
        total_seen = len(initial)
        processed  = 0

        self.log.write("info", f"Found {total_seen} archive(s) under: {src_root}\n")
        if dst_root:   self.log.write("info", f"Extract destination: {dst_root}\n")
        if move_root:
            label = "mirror" if after_mode == "move_mirror" else "flat"
            self.log.write("info", f"Move destination ({label}): {move_root}\n")
        if auto_nested: self.log.write("info", "Auto-extract nested: ON\n")
        self.log.write("info", "─" * 64 + "\n")

        ok = fail = bad = nested_total = 0

        while queue and not self._stop:
            arc = queue.popleft()
            processed += 1
            elapsed = time.time() - t0
            rate    = processed / elapsed if elapsed > 0 else 0
            remain  = len(queue)
            eta     = remain / rate if rate > 0 else 0
            self._stat(
                f"{processed}/{total_seen}  (+{remain} queued)  |  "
                f"OK:{ok}  Fail:{fail}  |  "
                f"{_au_timedelta(seconds=int(elapsed))} elapsed  "
                f"ETA {_au_timedelta(seconds=int(eta))}")
            self._prog(min(99, 100 * processed / total_seen))

            mode, _ = _au_classify(arc, sevenzip)
            if mode == "bad":
                self.log.write("mute", f"[BAD]   {arc}\n"); bad += 1; continue

            try:
                rel_parent = arc.relative_to(src_root).parent
                under_src  = True
            except ValueError:
                rel_parent = _au_Path("."); under_src = False

            if dst_root is not None and under_src:
                out_dir = dst_root / src_root.name / rel_parent \
                    if mode == "single" \
                    else dst_root / src_root.name / rel_parent / _au_sanitize(arc.stem)
            else:
                out_dir = arc.parent if mode == "single" \
                    else arc.parent / _au_sanitize(arc.stem)

            if mode == "single":
                ok_ex, err = _au_extract_single(arc, out_dir, sevenzip)
            else:
                ok_ex, err = _au_extract_to_folder(arc, out_dir, sevenzip)

            if not ok_ex:
                self.log.write("fail", f"[FAIL]  {arc.name}\n        {err}\n")
                fail += 1; continue

            nested_found = _au_scan_for_archives(out_dir, exts)
            if nested_found:
                nested_total += len(nested_found)
                self.log.write("nested",
                    f"{'▼'*60}\n"
                    f"  ⚠  NESTED ARCHIVES in: {out_dir.name}\n"
                    f"  ↳  {len(nested_found)} archive(s) after extracting {arc.name}\n")
                for nf in nested_found:
                    if auto_nested and nf not in queued_set:
                        queued_set.add(nf); queue.append(nf); total_seen += 1
                        self.log.write("nested", f"       [QUEUED]  {nf.name}\n")
                    else:
                        action = "(already queued)" if nf in queued_set \
                            else "(not auto-extracting)"
                        self.log.write("nested", f"       [FOUND]   {nf.name}  {action}\n")
                self.log.write("nested", f"{'▼'*60}\n")

            if after_mode == "keep":
                suffix, tag = "", "ok"
            elif after_mode == "recycle":
                ok_d, ed = _au_delete_archive(arc, "recycle")
                suffix, tag = ("  [recycled]" if ok_d else f"  [recycle WARN: {ed}]",
                               "ok" if ok_d else "warn")
            elif after_mode == "permanent":
                ok_d, ed = _au_delete_archive(arc, "permanent")
                suffix, tag = ("  [deleted]" if ok_d else f"  [delete WARN: {ed}]",
                               "ok" if ok_d else "warn")
            elif after_mode == "move_mirror":
                ok_d, de = _au_move_mirrored(arc, src_root, move_root)
                suffix, tag = ((f"  [→ {de}]" if ok_d else f"  [move WARN: {de}]"),
                               "ok" if ok_d else "warn")
            elif after_mode == "move_flat":
                ok_d, de = _au_move_flat(arc, move_root)
                suffix, tag = ((f"  [→ {de}]" if ok_d else f"  [move WARN: {de}]"),
                               "ok" if ok_d else "warn")
            else:
                suffix, tag = "", "ok"

            self.log.write(tag, f"[OK]    {arc.name}  →  {out_dir}{suffix}\n")
            ok += 1

        if self._stop:
            self.log.write("warn", f"[STOPPED — {len(queue)} remaining]\n")

        elapsed = time.time() - t0
        self.log.write("info", "─" * 64 + "\n")
        nn = f"  |  Nested alerts: {nested_total}" if nested_total else ""
        self.log.write("info",
            f"Done.  OK: {ok}  Fail: {fail}  Bad: {bad}{nn}  |  "
            f"{_au_timedelta(seconds=int(elapsed))}\n")
        self._stat(f"Done — OK:{ok}  Fail:{fail}  Bad:{bad}")
        self._prog(100)
        self._finish()

    def _finish(self):
        self.after(0, lambda: self.start_btn.configure(state="normal"))
        self.after(0, lambda: self.stop_btn.configure(state="disabled"))


# ═══════════════════════════════════════════════════════════════════════════
#  ZIP STORE PACKER WINDOW
# ═══════════════════════════════════════════════════════════════════════════

class ZipStorePackerWindow(tk.Toplevel):
    """
    Wraps files in uncompressed ZIP_STORED containers.
    Adapted from Eggman's Archive Utilities v2.0, Suite-themed.
    """

    def __init__(self, root_win, app):
        super().__init__(root_win)
        self.app   = app
        self._c    = app._c
        self._stop = False
        self._exts: list = []
        self.title("Eggman's Datfile Creator Suite — ZIP Store Packer")
        self.geometry("780x720+200+200")
        self.minsize(640, 520)
        self.configure(bg=self._c["options"])
        self._build_ui()
        self.grab_set()

    def _build_ui(self):
        c   = self._c
        PAD = 8

        # ── Description ───────────────────────────────────────────────────────
        inst_lf = ttk.LabelFrame(self, text="  ZIP Store Packer  ",
                                  style="Paths.TLabelframe", padding=PAD)
        inst_lf.pack(fill="x", padx=PAD, pady=(PAD, 0))
        tk.Label(inst_lf,
                 text="Wraps files in uncompressed ZIP containers (ZIP_STORED — zero "
                      "compression) for use as a neutral byte-preserving wrapper before "
                      "downstream recompression by RomVault or other tools. Each source "
                      "file is verified inside its zip before the original is deleted. "
                      "Target extensions are configurable; existing zips are skipped by default.",
                 bg=c["paths"], fg=c["fg"], font=("Segoe UI", 9),
                 wraplength=720, justify="left", anchor="w").pack(fill="x")

        # ── Source ────────────────────────────────────────────────────────────
        src_lf = ttk.LabelFrame(self, text="  Target Folder  ",
                                  style="Options.TLabelframe", padding=PAD)
        src_lf.pack(fill="x", padx=PAD, pady=(4, 0))
        self.src = _ArchiveFolderPicker(src_lf, self.app)
        self.src.pack(fill="x")

        # ── Extensions ────────────────────────────────────────────────────────
        ext_lf = ttk.LabelFrame(self, text="  Target Extensions  ",
                                  style="Header.TLabelframe", padding=PAD)
        ext_lf.pack(fill="x", padx=PAD, pady=(4, 0))

        add_row = tk.Frame(ext_lf, bg=c["header"])
        add_row.pack(fill="x", pady=(0, 4))
        self.ext_entry = ttk.Entry(add_row, width=22)
        self.ext_entry.pack(side="left")
        self.ext_entry.insert(0, "exe")
        self.ext_entry.bind("<Return>", lambda e: self._add_ext())
        ttk.Button(add_row, text="Add", style="Browse.TButton",
                   command=self._add_ext).pack(side="left", padx=(6, 0))
        tk.Label(add_row, text="space/comma separated — e.g.  exe dll bin rom",
                 bg=c["header"], fg="#5A5A5A",
                 font=("Segoe UI", 8, "italic")).pack(side="left", padx=10)

        self.pill_frame = tk.Frame(ext_lf, bg=c["header"])
        self.pill_frame.pack(fill="x", pady=(0, 2))
        self._add_ext_internal(".exe")

        # ── Options ───────────────────────────────────────────────────────────
        opt_lf = ttk.LabelFrame(self, text="  Options  ",
                                  style="Options.TLabelframe", padding=PAD)
        opt_lf.pack(fill="x", padx=PAD, pady=(4, 0))
        row = tk.Frame(opt_lf, bg=c["options"])
        row.pack(fill="x")
        self.recurse_var = tk.BooleanVar(value=True)
        self.verify_var  = tk.BooleanVar(value=True)
        self.skip_var    = tk.BooleanVar(value=True)
        for var, lbl in [(self.recurse_var, "Recursive"),
                          (self.verify_var,  "Verify before delete"),
                          (self.skip_var,    "Skip if .zip already exists")]:
            tk.Checkbutton(row, text=lbl, variable=var,
                           bg=c["options"], fg=c["fg"], selectcolor=c["entry"],
                           activebackground=c["options"],
                           font=("Segoe UI", 9)).pack(side="left", padx=(0, 18))

        # ── Status + Progress ─────────────────────────────────────────────────
        stat_row = tk.Frame(self, bg=c["buttons"])
        stat_row.pack(fill="x", padx=PAD, pady=(6, 1))
        self.stat_var = tk.StringVar(value="Ready.")
        ttk.Label(stat_row, textvariable=self.stat_var,
                  background=c["buttons"], foreground="#5A5A5A",
                  font=("Segoe UI", 8, "italic")).pack(side="left")

        self.progress = ttk.Progressbar(self, mode="determinate", maximum=100)
        self.progress.pack(fill="x", padx=PAD, pady=(1, 4))

        # ── Log ───────────────────────────────────────────────────────────────
        log_lf = ttk.LabelFrame(self, text="  Activity Log  ",
                                  style="Progress.TLabelframe", padding=4)
        log_lf.pack(fill="both", expand=True, padx=PAD, pady=(0, 4))
        self.log = _ArchiveLogPane(log_lf, self.app)
        self.log.pack(fill="both", expand=True)

        # ── Buttons ───────────────────────────────────────────────────────────
        bot = ttk.Frame(self, style="Buttons.TFrame", padding=(PAD, 4))
        bot.pack(fill="x", pady=(0, PAD))
        self.start_btn = ttk.Button(bot, text="▶  Pack",
                                     style="Start.TButton", command=self._start)
        self.start_btn.pack(side="left")
        self.stop_btn = ttk.Button(bot, text="⏹  Stop",
                                    style="HardStop.TButton",
                                    command=lambda: setattr(self, "_stop", True),
                                    state="disabled")
        self.stop_btn.pack(side="left", padx=(8, 0))
        ttk.Button(bot, text="💾  Save Log", style="Log.TButton",
                   command=self.log.save).pack(side="right")
        ttk.Button(bot, text="Clear Log", style="Browse.TButton",
                   command=self.log.clear).pack(side="right", padx=(0, 8))

    def _add_ext(self):
        import re as _re3
        raw = self.ext_entry.get()
        for tok in _re3.split(r"[\s,]+", raw):
            tok = tok.strip().lstrip(".")
            if tok: self._add_ext_internal("." + tok.lower())
        self.ext_entry.delete(0, "end")

    def _add_ext_internal(self, ext: str):
        if ext in self._exts: return
        self._exts.append(ext)
        c = self._c
        pill = tk.Frame(self.pill_frame, bg=c["incr"], padx=6, pady=2)
        pill.pack(side="left", padx=(0, 5), pady=3)
        tk.Label(pill, text=ext, bg=c["incr"], fg=c["fg"],
                 font=("Segoe UI", 9, "bold")).pack(side="left")
        x = tk.Label(pill, text=" ✕", bg=c["incr"], fg=c["fg"],
                     font=("Segoe UI", 9), cursor="hand2")
        x.pack(side="left")
        x.bind("<Button-1>", lambda e, p=pill, ex=ext: self._remove_ext(p, ex))

    def _remove_ext(self, pill, ext: str):
        if ext in self._exts: self._exts.remove(ext)
        pill.destroy()

    def _stat(self, msg):
        self.after(0, lambda: self.stat_var.set(msg))

    def _prog(self, val):
        self.after(0, lambda: self.progress.config(value=val))

    def _start(self):
        src_path = self.src.get()
        if not src_path or not _au_Path(src_path).is_dir():
            self.log.write("fail", "ERROR: Target folder not set or does not exist.\n"); return
        if not self._exts:
            self.log.write("fail", "ERROR: No extensions configured.\n"); return
        self._stop = False
        self.start_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        threading.Thread(
            target=self._run,
            args=(_au_Path(src_path), list(self._exts),
                  self.recurse_var.get(), self.verify_var.get(), self.skip_var.get()),
            daemon=True).start()

    def _run(self, src, exts, recurse, verify, skip_existing):
        t0 = time.time()
        files = []
        for ext in exts:
            files.extend(src.rglob(f"*{ext}") if recurse else src.glob(f"*{ext}"))
        files = sorted(set(files))
        total = len(files)
        if total == 0:
            self.log.write("info", f"No matching files found under: {src}\n")
            self._finish(); return

        self.log.write("info",
            f"Found {total} file(s) under: {src}\nExtensions: {', '.join(exts)}\n")
        self.log.write("info", "─" * 64 + "\n")
        ok = fail = skipped = 0

        for i, fp in enumerate(files, 1):
            if self._stop:
                self.log.write("warn", f"[STOPPED at {i-1}/{total}]\n"); break
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta  = (total - i) / rate if rate > 0 else 0
            self._stat(
                f"{i}/{total}  |  OK:{ok}  Fail:{fail}  Skip:{skipped}  |  "
                f"{_au_timedelta(seconds=int(elapsed))} elapsed  "
                f"ETA {_au_timedelta(seconds=int(eta))}")
            self._prog(100 * i / total)

            zip_path = fp.with_suffix(".zip")
            if skip_existing and zip_path.exists():
                self.log.write("skip", f"[SKIP]  {fp.name}  (zip already exists)\n")
                skipped += 1; continue

            try:
                with _au_zipfile.ZipFile(zip_path, "w",
                                          compression=_au_zipfile.ZIP_STORED,
                                          allowZip64=True) as zf:
                    zf.write(fp, fp.name)
            except Exception as e:
                self.log.write("fail", f"[FAIL]  {fp.name}: create: {e}\n")
                zip_path.unlink(missing_ok=True); fail += 1; continue

            if verify:
                try:
                    with _au_zipfile.ZipFile(zip_path, "r") as zf:
                        bad = zf.testzip()
                        if bad: raise ValueError(f"corrupt entry: {bad}")
                        info = zf.getinfo(fp.name)
                        if info.file_size != fp.stat().st_size:
                            raise ValueError(
                                f"size mismatch ({info.file_size} vs {fp.stat().st_size})")
                except Exception as e:
                    self.log.write("fail", f"[FAIL]  {fp.name}: verify: {e}\n")
                    zip_path.unlink(missing_ok=True); fail += 1; continue

            try:
                fp.unlink()
                sz = zip_path.stat().st_size
                self.log.write("ok", f"[OK]    {fp.name}  ({sz:,} B)\n"); ok += 1
            except Exception as e:
                self.log.write("warn",
                    f"[WARN]  {fp.name}: packed OK, delete failed: {e}\n"); ok += 1

        elapsed = time.time() - t0
        self.log.write("info", "─" * 64 + "\n")
        self.log.write("info",
            f"Done.  OK: {ok}  Fail: {fail}  Skip: {skipped}  |  "
            f"{_au_timedelta(seconds=int(elapsed))}\n")
        self._stat(f"Done — OK:{ok}  Fail:{fail}  Skip:{skipped}")
        self._prog(100)
        self._finish()

    def _finish(self):
        self.after(0, lambda: self.start_btn.configure(state="normal"))
        self.after(0, lambda: self.stop_btn.configure(state="disabled"))


# ═══════════════════════════════════════════════════════════════════════════
#  REMOVE READONLY WINDOW
# ═══════════════════════════════════════════════════════════════════════════

class RemoveReadOnlyWindow(tk.Toplevel):
    """
    Clears the read-only file attribute (os.chmod) AND removes the
    Zone.Identifier NTFS alternate data stream (Windows 'Unblock File')
    via PowerShell Unblock-File, which may require administrator elevation.
    """

    def __init__(self, root_win, app):
        super().__init__(root_win)
        self.app  = app
        self._c   = app._c
        self.title("Eggman's Datfile Creator Suite — Remove ReadOnly Attribute")
        self.geometry("640x620+225+225")
        self.minsize(520, 500)
        self.configure(bg=self._c["options"])
        self._log_lines: list = []
        self._rro_cancel = threading.Event()
        self._build_ui()
        self.grab_set()

    def _build_ui(self):
        c   = self._c
        PAD = 8

        # ── Instructions ──────────────────────────────────────────────────────
        inst_lf = ttk.LabelFrame(self, text="  Remove ReadOnly Attribute  ",
                                  style="Paths.TLabelframe", padding=PAD)
        inst_lf.pack(fill="x", padx=PAD, pady=(PAD, 0))
        tk.Label(inst_lf,
                 text="This tool performs two operations on all files and folders recursively:\n\n"
                      "1.  Remove read-only file attribute — clears the Windows R flag on all "
                      "files and folders using standard file permissions.\n\n"
                      "2.  Unblock downloaded files — removes the Zone.Identifier NTFS alternate "
                      "data stream (the 'This file came from another computer' security flag) "
                      "using PowerShell's Unblock-File command.\n\n"
                      "⚠  The Unblock-File step may require the application to be running as "
                      "Administrator. If unblocking fails, re-run the Suite as Administrator "
                      "(right-click → Run as administrator).",
                 bg=c["paths"], fg=c["fg"], font=("Segoe UI", 9),
                 wraplength=580, justify="left", anchor="nw").pack(fill="x")

        # ── Source ────────────────────────────────────────────────────────────
        src_lf = ttk.LabelFrame(self, text="  Target  ",
                                  style="Options.TLabelframe", padding=PAD)
        src_lf.pack(fill="x", padx=PAD, pady=(4, 0))

        drop_label = tk.Label(src_lf,
                              text="Drop a file or folder here — or use the Browse buttons",
                              bg=c["entry"], fg="#5A5A5A",
                              font=("Segoe UI", 9), pady=8, anchor="w", padx=8,
                              relief="flat")
        drop_label.pack(fill="x", pady=(0, 4))

        self.var_path = tk.StringVar()
        path_row = tk.Frame(src_lf, bg=c["options"])
        path_row.pack(fill="x")
        path_row.columnconfigure(0, weight=1)
        self._path_ent = ttk.Entry(path_row, textvariable=self.var_path)
        self._path_ent.grid(row=0, column=0, sticky="ew")
        btn_row = tk.Frame(path_row, bg=c["options"])
        btn_row.grid(row=0, column=1, padx=(4, 0))
        ttk.Button(btn_row, text="Browse File",
                   style="Browse.TButton",
                   command=self._browse_file).pack(side="left", padx=(0, 4))
        ttk.Button(btn_row, text="Browse Folder",
                   style="Browse.TButton",
                   command=self._browse_folder).pack(side="left")

        # Wire DnD
        try:
            drop_label.drop_target_register(DND_FILES)
            drop_label.dnd_bind("<<Drop>>", self._on_drop)
            self._path_ent.drop_target_register(DND_FILES)
            self._path_ent.dnd_bind("<<Drop>>", self._on_drop)
        except Exception:
            pass

        # ── Log ───────────────────────────────────────────────────────────────
        log_lf = ttk.LabelFrame(self, text="  Activity Log  ",
                                  style="Progress.TLabelframe", padding=4)
        log_lf.pack(fill="both", expand=True, padx=PAD, pady=(4, 4))
        log_lf.columnconfigure(0, weight=1)
        log_lf.rowconfigure(0, weight=1)

        self._txt = tk.Text(log_lf, bg=c["list"], fg=c["fg"],
                            font=("Consolas", 9), relief="flat", bd=0,
                            state="disabled", wrap="word", height=8)
        vsb = ttk.Scrollbar(log_lf, orient="vertical", command=self._txt.yview)
        self._txt.configure(yscrollcommand=vsb.set)
        self._txt.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        for tag, fg in [("ok",   "#1A6A2A"), ("warn", "#8A5A00"),
                         ("err",  "#8A1A1A"), ("dim",  "#5A5A5A")]:
            self._txt.tag_configure(tag, foreground=fg)

        # ── Buttons ───────────────────────────────────────────────────────────
        bot = ttk.Frame(self, style="Buttons.TFrame", padding=(PAD, 4))
        bot.pack(fill="x", pady=(0, PAD))

        self._run_btn = ttk.Button(bot, text="▶  Run",
                                    style="Start.TButton",
                                    command=self._on_run)
        self._run_btn.pack(side="left")
        self._rro_stop = ttk.Button(bot, text="⏹  Stop",
                                     style="HardStop.TButton",
                                     command=self._on_rro_stop,
                                     state="disabled")
        self._rro_stop.pack(side="left", padx=(8, 0))
        ttk.Button(bot, text="Clear Log", style="Browse.TButton",
                   command=self._clear_log).pack(side="left", padx=(16, 0))
        ttk.Button(bot, text="💾  Save Log", style="Log.TButton",
                   command=self._save_log).pack(side="left", padx=(8, 0))
        ttk.Button(bot, text="Close", style="Browse.TButton",
                   command=self.destroy).pack(side="right")

    def _browse_file(self):
        p = filedialog.askopenfilename(parent=self, title="Select a file")
        if p: self.var_path.set(p)

    def _browse_folder(self):
        p = filedialog.askdirectory(parent=self, title="Select folder")
        if p: self.var_path.set(p)

    def _on_drop(self, event):
        p = clean_dnd_path(event.data)
        if p: self.var_path.set(p)
        return event.action

    def _write(self, text, tag=""):
        self._txt.configure(state="normal")
        if tag: self._txt.insert(tk.END, text, tag)
        else:   self._txt.insert(tk.END, text)
        self._txt.configure(state="disabled")
        self._txt.yview_moveto(1.0)
        self._log_lines.append(text)

    def _clear_log(self):
        self._txt.configure(state="normal")
        self._txt.delete("1.0", tk.END)
        self._txt.configure(state="disabled")
        self._log_lines.clear()

    def _save_log(self):
        if not self._log_lines:
            messagebox.showinfo("Log", "Nothing to save.", parent=self); return
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        p  = filedialog.asksaveasfilename(
            parent=self, title="Save Log",
            initialfile=f"remove_readonly_log_{ts}.txt",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")])
        if p:
            _au_Path(p).write_text("".join(self._log_lines), encoding="utf-8")

    def _on_rro_stop(self):
        self._rro_cancel.set()
        self._rro_stop.configure(state="disabled")

    def _on_run(self):
        self._rro_cancel.clear()
        target = self.var_path.get().strip().strip('"')
        if not target:
            messagebox.showerror("Missing path",
                                 "Please select a file or folder first.",
                                 parent=self); return
        if not os.path.exists(target):
            messagebox.showerror("Not found",
                                 f"Path does not exist:\n{target}",
                                 parent=self); return
        self._run_btn.configure(state="disabled")
        self._rro_stop.configure(state="normal")
        threading.Thread(target=self._worker, args=(target,), daemon=True).start()

    def _worker(self, target: str):
        def post(text, tag=""): self.after(0, lambda t=text, g=tag: self._write(t, g))

        post(f"Target: {target}\n", "dim")
        post("─" * 60 + "\n", "dim")

        # ── Step 1: os.chmod — clear read-only attribute ──────────────────────
        post("Step 1: Clearing read-only attribute...\n")
        chmod_ok = chmod_fail = 0
        paths_to_chmod = []

        if os.path.isfile(target):
            paths_to_chmod = [target]
        elif os.path.isdir(target):
            post(f"  Scanning: {target}\n", "dim")
            for root, dirs, files in os.walk(target):
                for d in dirs:
                    paths_to_chmod.append(os.path.join(root, d))
                for f in files:
                    paths_to_chmod.append(os.path.join(root, f))
            paths_to_chmod.append(target)

        for p in paths_to_chmod:
            try:
                os.chmod(p, _au_stat.S_IWRITE | _au_stat.S_IREAD)
                chmod_ok += 1
            except Exception as e:
                post(f"  [WARN] chmod failed: {p}\n        {e}\n", "warn")
                chmod_fail += 1

        post(f"  Done — {chmod_ok} path(s) updated"
             + (f", {chmod_fail} failed" if chmod_fail else "") + "\n", "ok")

        # ── Step 2: PowerShell Unblock-File — remove Zone.Identifier ADS ─────
        if self._rro_cancel.is_set():
            post("\n[STOPPED — Step 2 skipped]\n", "warn")
            self.after(0, lambda: self._run_btn.configure(state="normal"))
            self.after(0, lambda: self._rro_stop.configure(state="disabled"))
            return
        post("\nStep 2: Removing Zone.Identifier (Unblock-File)...\n")
        post("  Note: This step may silently require Administrator privileges.\n", "dim")

        if os.path.isfile(target):
            ps_cmd = f"Unblock-File -LiteralPath '{target}'"
        else:
            ps_cmd = (f"Get-ChildItem -LiteralPath '{target}' "
                      f"-Recurse -File | Unblock-File")

        try:
            result = _au_subprocess.run(
                ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass",
                 "-Command", ps_cmd],
                capture_output=True, text=True, timeout=300)
            if result.returncode == 0:
                post("  Unblock-File completed successfully.\n", "ok")
            else:
                post("  Unblock-File returned a non-zero exit code.\n", "warn")
                if result.stderr.strip():
                    post(f"  stderr: {result.stderr.strip()}\n", "warn")
                post("  If files remain blocked, try re-running the Suite as "
                     "Administrator.\n", "warn")
        except Exception as e:
            post(f"  [ERROR] PowerShell failed: {e}\n", "err")
            post("  The chmod step completed, but Zone.Identifier removal "
                 "was not performed.\n", "warn")

        post("─" * 60 + "\n", "dim")
        post("All operations complete.\n", "ok")
        self.after(0, lambda: self._run_btn.configure(state="normal"))
        self.after(0, lambda: self._rro_stop.configure(state="disabled"))


# ═══════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main():
    root = TkinterDnD.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
