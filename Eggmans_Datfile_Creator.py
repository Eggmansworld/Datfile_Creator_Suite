#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Eggman's Datfile Creator
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

import os, sys, json, re, time, queue, zlib
import ctypes, hashlib, threading, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinterdnd2 import DND_FILES, TkinterDnD
from xml.sax.saxutils import escape as xml_escape

CONFIG_FILENAME       = "Eggmans_Datfile_Creator_config.json"
FILE_ATTRIBUTE_HIDDEN = 0x2
FILE_ATTRIBUTE_SYSTEM = 0x4
MAX_SAFE_PATH         = 240


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
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


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
#  HASHING — MIXED (file as-is)
# ═══════════════════════════════════════════════════════════════════════════

def hash_file(path: str, include_md5: bool,
              include_sha256: bool,
              cancel: threading.Event, chunk: int = 8*1024*1024):
    """
    Hash a file with CRC32 + SHA1 (always) plus optional MD5 / SHA-256 / SHA-512.
    Returns (size, crc, sha1, md5, sha256) — optional fields may be None.
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

def analyze_zip(zip_path: str, include_md5: bool,
                include_sha256: bool,
                incl_date: bool,
                cancel: threading.Event,
                sevenzip_path: str = "") -> List[tuple]:
    """
    Analyze a zip archive using 7-Zip-ZStandard exclusively.

    Handles all compression methods including ZStandard (method 93 / RVZSTD),
    TorrentZip, standard Deflate, Stored, etc.

    Strategy:
      1. `7z l -slt -ba <zip>` — parse technical listing for name/size/crc/date
      2. `7z e -so <zip> <entry>` — pipe each file's decompressed bytes for SHA1/MD5

    CRC from the 7z listing is the CRC of the uncompressed data, which matches
    what RomVault expects in the dat (same as ZipInfo.CRC from Python zipfile).
    """
    import subprocess

    exe = sevenzip_path.strip() if sevenzip_path.strip() else \
          r"C:\Program Files\7-Zip-Zstandard\7z.exe"

    if not os.path.isfile(exe):
        raise RuntimeError(
            f"7-Zip-ZStandard not found at:\n  {exe}\n"
            f"Set the correct path in the '7-Zip-ZStandard (7z.exe)' field.")

    # ── Step 1: list contents ────────────────────────────────────────────────
    try:
        result = subprocess.run(
            [exe, "l", "-slt", "-ba", zip_path],
            capture_output=True, timeout=120)
    except FileNotFoundError:
        raise RuntimeError(
            f"7-Zip-ZStandard not found at:\n  {exe}\n"
            f"Set the correct path in the '7-Zip-ZStandard (7z.exe)' field.")

    if result.returncode not in (0, 1):
        raise RuntimeError(
            f"7z list failed (rc={result.returncode}): "
            f"{result.stderr.decode(errors='replace').strip()}")

    listing = result.stdout.decode("utf-8", errors="replace")

    # Parse key=value blocks separated by blank lines
    entries: List[dict] = []
    current: dict = {}
    for line in listing.splitlines():
        line = line.strip()
        if not line:
            if current:
                entries.append(current)
                current = {}
            continue
        if "=" in line:
            k, _, v = line.partition("=")
            current[k.strip().lower()] = v.strip()
    if current:
        entries.append(current)

    # Keep only file entries (skip directories — attributes start with 'D')
    file_entries = [
        e for e in entries
        if e.get("path") and
           not e.get("attributes", "").upper().startswith("D")
    ]
    file_entries.sort(key=lambda e: e.get("path", "").lower())

    # ── Step 2: hash each file via decompressed stdout stream ────────────────
    results = []
    for entry in file_entries:
        if cancel.is_set():
            raise RuntimeError("CANCELLED")

        rom_name  = entry.get("path", "").replace("\\", "/")

        try:
            file_size = int(entry.get("size", "0"))
        except ValueError:
            file_size = 0

        crc_raw   = entry.get("crc", "")
        crc32_hex = crc_raw.lower().zfill(8) if crc_raw else "00000000"

        # Date: 7z reports as "YYYY-MM-DD HH:MM:SS" → reformat to dat convention
        date_str = None
        if incl_date:
            raw = entry.get("modified", entry.get("created", ""))
            if raw and len(raw) >= 19:
                try:
                    yr, mo, dy = raw[:10].split("-")
                    h, mi, s   = raw[11:19].split(":")
                    date_str   = f"{yr}/{mo}/{dy} {h}-{mi}-{s}"
                except Exception:
                    pass

        # Extract single file to stdout, hash the decompressed stream
        sha1_obj   = hashlib.sha1()
        md5_obj    = hashlib.md5()    if include_md5    else None
        sha256_obj = hashlib.sha256() if include_sha256 else None
        try:
            proc = subprocess.Popen(
                [exe, "e", "-so", zip_path, rom_name],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            while True:
                if cancel.is_set():
                    proc.kill()
                    raise RuntimeError("CANCELLED")
                buf = proc.stdout.read(4 * 1024 * 1024)
                if not buf:
                    break
                sha1_obj.update(buf)
                if md5_obj:    md5_obj.update(buf)
                if sha256_obj: sha256_obj.update(buf)
            proc.wait(timeout=120)
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(
                f"7z extract failed for '{rom_name}' in "
                f"{os.path.basename(zip_path)}: {exc}")

        results.append((rom_name, file_size, crc32_hex,
                        sha1_obj.hexdigest(),
                        md5_obj.hexdigest()    if md5_obj    else None,
                        sha256_obj.hexdigest() if sha256_obj else None,
                        date_str))

    return results


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
    t = "\t" * depth
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
    t = "\t" * depth
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

        # ── Hash / analyze all items ──────────────────────────────────
        data: dict = {}   # item_path → hash result
        incomplete = False

        def do_mixed(fp):
            return fp, hash_file(fp, s.include_md5,
                                  s.include_sha256,
                                  hard_stop), None

        def do_zipped(zp):
            return zp, analyze_zip(zp, s.include_md5,
                                    s.include_sha256,
                                    s.incl_file_date,
                                    hard_stop, s.sevenzip_path), None

        work_fn = do_mixed if is_mixed else do_zipped

        def safe_work(item_path):
            try:
                p, result, _ = work_fn(item_path)
                return p, result, None
            except Exception as exc:
                msg = str(exc)
                return item_path, None, ("CANCELLED" if "CANCELLED" in msg else repr(exc))

        if max_workers == 1:
            for item in items:
                if hard_stop.is_set():
                    incomplete = True; break
                ui_queue.put(("item", os.path.basename(item), done_items + 1))
                _, result, err_s = safe_work(item)
                if err_s == "CANCELLED":
                    incomplete = True; break
                done_items += 1
                ui_queue.put(("progress", done_items))
                if err_s or result is None:
                    errors.append(f"ERROR: {item} :: {err_s}"); continue
                data[item] = result
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                fmap = {ex.submit(safe_work, it): it for it in items}
                for fut in as_completed(fmap):
                    if hard_stop.is_set():
                        incomplete = True
                        for ft in fmap: ft.cancel()
                        break
                    item = fmap[fut]
                    ui_queue.put(("item", os.path.basename(item), done_items + 1))
                    try:
                        _, result, err_s = fut.result()
                    except Exception as exc:
                        result, err_s = None, repr(exc)
                    if err_s == "CANCELLED":
                        incomplete = True; break
                    done_items += 1
                    ui_queue.put(("progress", done_items))
                    if err_s or result is None:
                        errors.append(f"ERROR: {item} :: {err_s}"); continue
                    data[item] = result

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
    ui_queue.put(("done", ok, errors, done_items, total_items,
                  written_dats, elapsed, ""))


# ═══════════════════════════════════════════════════════════════════════════
#  GUI
# ═══════════════════════════════════════════════════════════════════════════

class App:
    def __init__(self, root: TkinterDnD.Tk):
        self.root        = root
        self.root.title("Eggman's Datfile Creator")
        self.root.geometry("900x900")
        self.root.minsize(900, 900)

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
        """
        Warm off-white theme — tones down the harsh default white without
        going full dark mode. Uses a soft parchment base.
        """
        BG       = "#EDE8E0"   # warm light gray/parchment — main background
        BG_FRAME = "#EDE8E0"   # frame background
        BG_ENTRY = "#F7F4EF"   # slightly lighter for entry fields
        BG_LIST  = "#F2EEE8"   # listbox
        FG       = "#2A2A2A"   # near-black text
        FG_DIM   = "#6B6560"   # dimmed label text
        SEL_BG   = "#C8B89A"   # selection highlight
        BTN_BG   = "#D8D0C4"   # button face
        PROGRESS = "#8A7A6A"   # progress bar fill
        SEP      = "#C8C0B4"   # separator

        self.root.configure(bg=BG)

        style = ttk.Style(self.root)
        # Use 'clam' as base — it respects colour overrides better than 'vista'/'winnative'
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure(".",
            background=BG_FRAME, foreground=FG,
            fieldbackground=BG_ENTRY, selectbackground=SEL_BG,
            selectforeground=FG, troughcolor=BG_FRAME,
            bordercolor=SEP, darkcolor=SEP, lightcolor=BG_FRAME,
            focuscolor=SEL_BG)

        style.configure("TFrame",    background=BG_FRAME)
        style.configure("TLabel",    background=BG_FRAME, foreground=FG)
        style.configure("TCheckbutton", background=BG_FRAME, foreground=FG)
        style.configure("TRadiobutton", background=BG_FRAME, foreground=FG)

        style.configure("TEntry",
            fieldbackground=BG_ENTRY, foreground=FG,
            insertcolor=FG, selectbackground=SEL_BG)

        style.configure("TButton",
            background=BTN_BG, foreground=FG, bordercolor=SEP,
            relief="flat", padding=4)
        style.map("TButton",
            background=[("active", SEL_BG), ("pressed", "#B8A890")])

        style.configure("TSeparator", background=SEP)

        style.configure("Horizontal.TProgressbar",
            troughcolor=BG_FRAME, background=PROGRESS,
            bordercolor=SEP, darkcolor=PROGRESS, lightcolor=PROGRESS)

        style.configure("TSpinbox",
            fieldbackground=BG_ENTRY, foreground=FG,
            selectbackground=SEL_BG, arrowcolor=FG)

        # Store for manual widget coloring (Listbox is not a ttk widget)
        self._list_bg  = BG_LIST
        self._list_fg  = FG
        self._list_sel = SEL_BG

    # ── UI construction ─────────────────────────────────────────────────────

    def _build_ui(self):
        pad = {"padx": 10, "pady": 3}
        frm = ttk.Frame(self.root)
        frm.pack(fill="both", expand=True, **pad)
        frm.columnconfigure(1, weight=1)
        row = 0

        # ── Paths ────────────────────────────────────────────────────────
        ttk.Label(frm, text="Input top-level folder:").grid(
            row=row, column=0, sticky="w")
        self.var_input = tk.StringVar()
        self.ent_input = ttk.Entry(frm, textvariable=self.var_input)
        self.ent_input.grid(row=row, column=1, sticky="ew", padx=(0,6))
        ttk.Button(frm, text="Browse...",
                   command=self.browse_input).grid(row=row, column=2, sticky="ew")

        row += 1
        ttk.Label(frm, text="Output folder (dat root):").grid(
            row=row, column=0, sticky="w")
        self.var_output = tk.StringVar()
        self.ent_output = ttk.Entry(frm, textvariable=self.var_output)
        self.ent_output.grid(row=row, column=1, sticky="ew", padx=(0,6))
        ttk.Button(frm, text="Browse...",
                   command=self.browse_output).grid(row=row, column=2, sticky="ew")

        row += 1
        ttk.Label(frm, text="Parent name (optional prefix):").grid(
            row=row, column=0, sticky="w")
        self.var_parent = tk.StringVar()
        ttk.Entry(frm, textvariable=self.var_parent).grid(
            row=row, column=1, sticky="ew", padx=(0,6))
        ttk.Label(frm, text='e.g. "MyName" → "MyName - FolderName"',
                  foreground="gray50").grid(row=row, column=2, sticky="w")

        row += 1
        ttk.Label(frm, text="7-Zip-ZStandard (7z.exe):").grid(
            row=row, column=0, sticky="w")
        self.var_sevenzip = tk.StringVar()
        self.ent_sevenzip = ttk.Entry(frm, textvariable=self.var_sevenzip)
        self.ent_sevenzip.grid(row=row, column=1, sticky="ew", padx=(0,6))
        ttk.Button(frm, text="Browse...",
                   command=self.browse_sevenzip).grid(row=row, column=2, sticky="ew")

        # ── Header fields ─────────────────────────────────────────────────
        row += 1
        ttk.Separator(frm).grid(row=row, column=0, columnspan=3,
                                sticky="ew", pady=(6,4))

        def hrow(label, varname, hint="(blank = omit)"):
            nonlocal row
            row += 1
            setattr(self, varname, tk.StringVar())
            ttk.Label(frm, text=label).grid(row=row, column=0, sticky="w")
            ttk.Entry(frm, textvariable=getattr(self, varname)).grid(
                row=row, column=1, sticky="ew", padx=(0,6))
            ttk.Label(frm, text=hint, foreground="gray50").grid(
                row=row, column=2, sticky="w")

        hrow("Description:", "var_description", "")
        hrow("Category:",    "var_category")
        hrow("Version:",     "var_version")
        hrow("Date (yyyy-mm-dd):", "var_date", "")
        hrow("Author:",      "var_author", "")
        hrow("URL:",         "var_url")
        hrow("Homepage:",    "var_homepage")
        hrow("Comment:",     "var_comment")

        # ── Options ───────────────────────────────────────────────────────
        row += 1
        ttk.Separator(frm).grid(row=row, column=0, columnspan=3,
                                sticky="ew", pady=(6,4))
        row += 1
        opt = ttk.Frame(frm)
        opt.grid(row=row, column=0, columnspan=3, sticky="ew")

        # Dat Type
        self.var_dat_type = tk.StringVar(value="mixed")
        ttk.Label(opt, text="Dat type:").grid(
            row=0, column=0, sticky="w", padx=(0,8))
        rf = ttk.Frame(opt)
        rf.grid(row=0, column=1, sticky="w")
        ttk.Radiobutton(rf, text="Mixed (Archive as File)",
                        variable=self.var_dat_type, value="mixed",
                        command=self._on_options_change).pack(side="left", padx=(0,18))
        ttk.Radiobutton(rf, text="Zipped",
                        variable=self.var_dat_type, value="zipped",
                        command=self._on_options_change).pack(side="left")

        # Generation Mode
        self.var_gen_mode = tk.StringVar(value="per_root")
        ttk.Label(opt, text="Generation:").grid(
            row=1, column=0, sticky="w", padx=(0,8), pady=(4,0))
        gf = ttk.Frame(opt)
        gf.grid(row=1, column=1, sticky="w", pady=(4,0))
        ttk.Radiobutton(gf, text="1 dat per root folder",
                        variable=self.var_gen_mode, value="per_root",
                        command=self._on_options_change).pack(side="left", padx=(0,18))
        ttk.Radiobutton(gf, text="1 dat per root folder & all subfolders",
                        variable=self.var_gen_mode, value="per_all",
                        command=self._on_options_change).pack(side="left")

        # Structure
        self.var_structure = tk.StringVar(value="opt2")
        ttk.Label(opt, text="Structure:").grid(
            row=2, column=0, sticky="w", padx=(0,8), pady=(4,0))
        sf = ttk.Frame(opt)
        sf.grid(row=2, column=1, columnspan=2, sticky="w", pady=(4,0))
        self._struct_rbs = []
        for val, label in [
            ("opt1",  "1 — Dirs"),
            ("opt2",  "2 — Archives as Games"),
            ("opt3",  "3 — First Level Dirs as Games"),
            ("opt4",  "4 — First Level Dirs as Games + Merge Dirs in Games"),
        ]:
            rb = ttk.Radiobutton(sf, text=label,
                                 variable=self.var_structure, value=val)
            rb.pack(anchor="w")
            self._struct_rbs.append(rb)

        # Format
        self.var_format = tk.StringVar(value="modern")
        ttk.Label(opt, text="Format:").grid(
            row=3, column=0, sticky="w", padx=(0,8), pady=(4,0))
        ff = ttk.Frame(opt)
        ff.grid(row=3, column=1, sticky="w", pady=(4,0))
        ttk.Radiobutton(ff, text="Modern  (<game> / <dir>)",
                        variable=self.var_format, value="modern",
                        command=self._on_options_change).pack(side="left", padx=(0,18))
        ttk.Radiobutton(ff, text="Legacy  (all <dir>)",
                        variable=self.var_format, value="legacy",
                        command=self._on_options_change).pack(side="left")

        # Checkboxes row 1 — modern-only + type-specific
        row += 1
        ck1 = ttk.Frame(frm)
        ck1.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(4,0))

        self.var_machine  = tk.BooleanVar()
        self.var_inc_desc = tk.BooleanVar(value=True)
        self.var_forcepack = tk.BooleanVar(value=True)
        self.var_filedate  = tk.BooleanVar()

        self.cb_machine = ttk.Checkbutton(
            ck1, text="Use <machine> instead of <game>",
            variable=self.var_machine)
        self.cb_machine.pack(side="left", padx=(0,18))

        self.cb_inc_desc = ttk.Checkbutton(
            ck1, text="Include <description> in game entries",
            variable=self.var_inc_desc)
        self.cb_inc_desc.pack(side="left", padx=(0,18))

        self.cb_forcepack = ttk.Checkbutton(
            ck1, text='forcepacking="fileonly"',
            variable=self.var_forcepack)
        self.cb_forcepack.pack(side="left", padx=(0,18))

        self.cb_filedate = ttk.Checkbutton(
            ck1, text="File date & time  (yyyy/mm/dd hh-mm-ss)",
            variable=self.var_filedate)
        self.cb_filedate.pack(side="left")

        # Checkboxes row 2 — hashes (CRC + SHA-1 always; optional MD5 / SHA-256)
        row += 1
        ck2 = ttk.Frame(frm)
        ck2.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(4,0))

        self.var_md5    = tk.BooleanVar()
        self.var_sha256 = tk.BooleanVar()
        self.var_multithread = tk.BooleanVar(value=True)
        self.var_threads   = tk.IntVar(value=4)

        ttk.Label(ck2, text="Extra hashes:",
                  foreground="#6B6560").pack(side="left", padx=(0,6))
        ttk.Checkbutton(ck2, text="MD5",
                        variable=self.var_md5).pack(side="left", padx=(0,12))
        ttk.Checkbutton(ck2, text="SHA-256",
                        variable=self.var_sha256).pack(side="left", padx=(0,12))

        ttk.Separator(ck2, orient="vertical").pack(side="left",
            fill="y", padx=(0,12))

        ttk.Checkbutton(ck2, text="Multithread",
                        variable=self.var_multithread,
                        command=self._on_mt_toggle).pack(side="left", padx=(0,6))
        ttk.Label(ck2, text="Threads (1–8):").pack(side="left")
        self.spin_threads = ttk.Spinbox(ck2, from_=1, to=8,
                                        textvariable=self.var_threads, width=4)
        self.spin_threads.pack(side="left", padx=(4,0))

        # Extension filters — Mixed only
        row += 1
        ttk.Label(frm, text="Include only extensions:").grid(
            row=row, column=0, sticky="w")
        self.var_ext_include = tk.StringVar()
        self.ent_ext_include = ttk.Entry(frm, textvariable=self.var_ext_include)
        self.ent_ext_include.grid(row=row, column=1, sticky="ew", padx=(0,6))
        ttk.Label(frm, text='e.g. ".ima, .mfm"  (blank = all)',
                  foreground="gray50").grid(row=row, column=2, sticky="w")

        row += 1
        ttk.Label(frm, text="Exclude extensions / files:").grid(
            row=row, column=0, sticky="w")
        self.var_ext_exclude = tk.StringVar()
        self.ent_ext_exclude = ttk.Entry(frm, textvariable=self.var_ext_exclude)
        self.ent_ext_exclude.grid(row=row, column=1, sticky="ew", padx=(0,6))
        ttk.Label(frm, text='e.g. ".nfo, .sfv, thumbs.db"',
                  foreground="gray50").grid(row=row, column=2, sticky="w")

        # ── Buttons ───────────────────────────────────────────────────────
        row += 1
        ttk.Separator(frm).grid(row=row, column=0, columnspan=3,
                                sticky="ew", pady=(8,4))
        row += 1
        bf = ttk.Frame(frm)
        bf.grid(row=row, column=0, columnspan=3, sticky="ew")
        self.btn_start = ttk.Button(bf, text="Start", command=self.start)
        self.btn_start.pack(side="left")
        self.btn_soft = ttk.Button(bf, text="Soft Stop",
                                   command=self.soft_stop_cmd, state="disabled")
        self.btn_soft.pack(side="left", padx=(8,0))
        self.btn_hard = ttk.Button(bf, text="Hard Stop",
                                   command=self.hard_stop_cmd, state="disabled")
        self.btn_hard.pack(side="left", padx=(8,0))
        self.btn_preview = ttk.Button(bf, text="🔍  Preview Dats",
                                       command=self.open_preview,
                                       state="disabled")
        self.btn_preview.pack(side="right", padx=(0, 8))
        ttk.Button(bf, text="Save Settings",
                   command=self.save_settings_cmd).pack(side="right")

        # ── Progress ──────────────────────────────────────────────────────
        row += 1
        ttk.Separator(frm).grid(row=row, column=0, columnspan=3,
                                sticky="ew", pady=(8,6))
        row += 1
        self.var_status = tk.StringVar(value="Idle.")
        ttk.Label(frm, textvariable=self.var_status).grid(
            row=row, column=0, columnspan=3, sticky="w")
        row += 1
        self.var_counts = tk.StringVar(
            value="Items: 0/0 | Folders: 0 | Dats written: 0")
        ttk.Label(frm, textvariable=self.var_counts).grid(
            row=row, column=0, columnspan=3, sticky="w")
        row += 1
        self.progress = ttk.Progressbar(frm, orient="horizontal",
                                         mode="determinate")
        self.progress.grid(row=row, column=0, columnspan=3,
                          sticky="ew", pady=(4,0))
        row += 1
        self.var_current = tk.StringVar(value="Current item: (none)")
        ttk.Label(frm, textvariable=self.var_current).grid(
            row=row, column=0, columnspan=3, sticky="w")
        row += 1
        ach = ttk.Frame(frm)
        ach.grid(row=row, column=0, columnspan=3, sticky="ew")
        ttk.Label(ach, text="Recent activity:").pack(side="left")
        ttk.Button(ach, text="💾  Save Activity Log",
                   command=self.save_log).pack(side="right")
        row += 1
        self.lst = tk.Listbox(frm, height=10,
                               bg=self._list_bg, fg=self._list_fg,
                               selectbackground=self._list_sel,
                               selectforeground=self._list_fg,
                               relief="flat", borderwidth=1,
                               highlightthickness=0)
        self.lst.grid(row=row, column=0, columnspan=3, sticky="nsew")
        frm.rowconfigure(row, weight=1)

    # ── DnD ─────────────────────────────────────────────────────────────────

    def _setup_dnd(self):
        for ent, cb in [(self.ent_input,  self._drop_input),
                        (self.ent_output, self._drop_output),
                        (self.ent_sevenzip, self._drop_sevenzip)]:
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
        self.var_multithread.set(s.multithread)
        self.var_threads.set(s.threads)
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
            sevenzip_path  = self.var_sevenzip.get().strip(),
            multithread  = bool(self.var_multithread.get()),
            threads      = max(1, min(8, int(self.var_threads.get() or 1))),
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

    def save_settings_cmd(self):
        s = self._read_settings()
        try:
            save_settings(s)
            self.settings = s
            messagebox.showinfo("Saved",
                f"Settings saved to:\n{os.path.join(script_dir(), CONFIG_FILENAME)}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save:\n{repr(e)}")

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

        self.soft_stop.clear(); self.hard_stop.clear()
        self.total_items = 0; self.done_items = 0
        self.total_jobs  = 0; self.dats_written = 0
        self._preview_results.clear()
        self.btn_preview.configure(state="disabled")
        self.lst.delete(0, tk.END); self._log_lines.clear()
        self.progress["value"] = 0; self.progress["maximum"] = 1
        self.var_status.set("Starting...")
        self.var_counts.set("Items: 0/0 | Folders: 0 | Dats written: 0")
        self.var_current.set("Current item: (none)")
        self.btn_start.configure(state="disabled")
        self.btn_soft.configure(state="normal")
        self.btn_hard.configure(state="normal")

        self.worker = threading.Thread(
            target=process,
            args=(s, self.ui_queue, self.soft_stop, self.hard_stop,
                  self._preview_results),
            daemon=True)
        self.worker.start()

    def soft_stop_cmd(self):
        self.soft_stop.set()
        self.var_status.set("Soft stop: finishing current dat then stopping.")
        self.btn_soft.configure(state="disabled")

    def hard_stop_cmd(self):
        self.hard_stop.set()
        self.var_status.set("Hard stop: stopping ASAP.")
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
        kind = msg[0]
        if kind == "status":
            self.var_status.set(msg[1]); self._log(msg[1])
        elif kind == "scan":
            self.var_current.set(f"Phase 1 — Discovering: {msg[1]}")
        elif kind == "totals":
            self.total_jobs, self.total_items = msg[1], msg[2]
            self.progress["maximum"] = max(1, self.total_items)
            self._update_counts()
        elif kind == "folder":
            self._log(f">> {msg[1]}  ({msg[2]} item(s))", fg="#1e90ff")
        elif kind == "item":
            self.var_current.set(
                f"Current item: {msg[1]}  ({msg[2]}/{self.total_items})")
        elif kind == "progress":
            self.done_items = msg[1]
            self.progress["value"] = self.done_items
            self._update_counts()
        elif kind == "dat_written":
            self.dats_written = msg[2]
            self._log(f"++ Dat: {msg[1]}", fg="#00c040")
            self._update_counts()
        elif kind == "done":
            ok, errors, done, total, written, elapsed, _ = \
                msg[1], msg[2], msg[3], msg[4], msg[5], msg[6], msg[7]
            self.done_items = done; self.dats_written = written
            self._update_counts()
            status = (f"{'Done' if ok else 'Stopped'}. "
                      f"Wrote {written} dat(s) / {done}/{total} item(s) "
                      f"in {elapsed:.1f}s.")
            self.var_status.set(status); self._log(status)
            if errors:
                self._log(f"Errors/warnings: {len(errors)}")
                for ln in errors[:10]:
                    self._log(f"  {ln}")
                if len(errors) > 10:
                    self._log(f"  ... and {len(errors)-10} more")
            else:
                self._log("No errors reported.")
            self.btn_start.configure(state="normal")
            self.btn_soft.configure(state="disabled")
            self.btn_hard.configure(state="disabled")
            if self._preview_results:
                self.btn_preview.configure(state="normal")

    def _update_counts(self):
        self.var_counts.set(
            f"Items: {self.done_items}/{self.total_items} | "
            f"Folders: {self.total_jobs} | "
            f"Dats written: {self.dats_written}")

    def _log(self, line: str, fg: str = ""):
        self._log_lines.append(line)
        self.lst.insert(tk.END, line)
        if fg:
            self.lst.itemconfig(tk.END, fg=fg)
        self.lst.yview_moveto(1.0)

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
        self.title("Eggman\'s Datfile Creator — Dat Preview")
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
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main():
    root = TkinterDnD.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
