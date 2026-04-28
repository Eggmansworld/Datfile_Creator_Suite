# Eggman's Datfile Creator Suite

A bulk datfile generation tool for **RomVault**, built for collectors who manage large, structured archives and need consistent, reproducible DAT files across hundreds or thousands of folders.

Produces **Logiqx XML** datfiles compatible with RomVault, ClrMamePro, and RomCenter. Supports both **Mixed (Archive as File)** and **Zipped** collection types, with four structure options that replicate the datfile output styles that RomVault natively supports. Includes an incremental update engine for keeping existing datfiles current without rehashing unchanged content.

---

<img width="1619" height="971" alt="Eggmans_Datfile_Creator" src="https://github.com/user-attachments/assets/6d937f6f-ab83-4950-81ca-e817a057d0e1" />

---

If this tool saves you time, consider supporting the work:

<a href="https://buymeacoffee.com/eggmansworld">
  <img src="https://cdn.buymeacoffee.com/buttons/v2/default-orange.png" height="45" alt="Buy Me a Coffee">
</a>

---

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Interface Overview](#interface-overview)
  - [Paths](#paths)
  - [DAT Header Fields](#dat-header-fields)
  - [Options](#options)
- [Dat Types](#dat-types)
  - [Mixed — Archive as File](#mixed--archive-as-file)
  - [Zipped](#zipped)
- [Generation Modes](#generation-modes)
  - [1 Dat per Root Folder](#1-dat-per-root-folder)
  - [1 Dat per Root Folder and All Subfolders](#1-dat-per-root-folder-and-all-subfolders)
- [Structure Options](#structure-options)
  - [Structure 1 — Dirs](#structure-1--dirs)
  - [Structure 2 — Archives as Games](#structure-2--archives-as-games)
  - [Structure 3 — First Level Dirs as Games](#structure-3--first-level-dirs-as-games)
  - [Structure 4 — First Level Dirs as Games + Merge Dirs in Games](#structure-4--first-level-dirs-as-games--merge-dirs-in-games)
- [Format: Modern vs Legacy](#format-modern-vs-legacy)
- [Hash Options](#hash-options)
- [Network Cap](#network-cap)
- [Extension Filters](#extension-filters)
- [ZStandard Support](#zstandard-support)
- [Parent Name and Output Folder Structure](#parent-name-and-output-folder-structure)
- [Dat Preview Window](#dat-preview-window)
- [Run Progress Window](#run-progress-window)
- [Incremental Update — Skip Already-Hashed Files](#incremental-update--skip-already-hashed-files)
- [Folder Structure Analyzer](#folder-structure-analyzer)
- [Tools Menu](#tools-menu)
  - [Bulk Datfile Header Updater](#bulk-datfile-header-updater)
  - [Game and ROM Counter](#game-and-rom-counter)
  - [Recursive Archive Extractor](#recursive-archive-extractor)
  - [ZIP Store Packer](#zip-store-packer)
  - [Remove ReadOnly Attribute](#remove-readonly-attribute)
- [Settings and Config File](#settings-and-config-file)
- [Advanced: Datfile Landscape Analysis](#advanced-datfile-landscape-analysis)
- [Advanced: DAT Format Reference](#advanced-dat-format-reference)
- [Known Limitations](#known-limitations)
- [License](#license)

---

## Requirements

- **Python 3.10+**
- **tkinterdnd2** — drag-and-drop support
- **zstandard** — ZStandard decompression (hard required)
- **zipfile-zstd** — drop-in zipfile with method-93 support (hard required)
- **psutil** — NIC speed detection and live network monitoring (strongly recommended)
- **[7-Zip-ZStandard](https://github.com/mcmilk/7-Zip-zstd/releases)** — required only by the **Recursive Archive Extractor** tool (ZIP/7Z/RAR extraction). No longer needed for dat hashing.

```
pip install tkinterdnd2 zstandard zipfile-zstd psutil
```

The app will display an error dialog and refuse to start if `zstandard` or `zipfile-zstd` are not installed. `psutil` is optional — without it the network cap and live throughput display fall back gracefully.

---

## Python Installation

1. Install Python 3.10 or later from [python.org](https://python.org)
2. Install required packages:
   ```
   pip install tkinterdnd2 zstandard zipfile-zstd psutil
   ```
3. Download `Eggmans_Datfile_Creator_Suite.py` and place it anywhere convenient
4. Optionally place `Eggmans_Datfile_Creator_banner.png` in the same folder for the About window banner
5. Run it:
   ```
   python Eggmans_Datfile_Creator_Suite.py
   ```

The script saves its config file (`Eggmans_Datfile_Creator_Suite_config.json`) in the same directory as the script itself.

## Windows Installation

1. Visit the Releases section of this repository
2. Download the latest Windows exe
3. Place it somewhere on your computer
4. Run the exe

---

## Quick Start

1. Set your **Input top-level folder** — the folder that contains the game folders you want to dat
2. Set your **Output folder** — where the datfiles will be written (mirrors input folder structure)
3. Fill in the fields in the header section to suit your dat(s)
4. Choose **Dat Type**: Mixed or Zipped
5. Choose **Generation**: 1 Dat per root folder (most common)
6. Choose **Structure**: Structure 2 is the right choice for the majority of collections
7. Choose **Format**: Modern
8. Click **Start**

A detached **Run Progress** window opens automatically when the run begins, showing live status, progress bar, network throughput, elapsed time, and activity log.

Not sure which structure to use? Use **Tools → Analyze Folder Structure** before your first run.

---

## Interface Overview

### Paths

| Field | Purpose |
|---|---|
| **Input top-level folder** | The folder whose immediate subfolders become individual dat jobs |
| **Output folder (dat root)** | Root of the output structure. Datfiles are written into subfolders that mirror the input |
| **Parent name (optional prefix)** | Prepended to every dat name: `Parent - TopLevel - Subfolder` |
| **7-Zip-ZStandard (7z.exe)** | Full path to `7z.exe` — used only by the Recursive Archive Extractor tool |

All path fields support drag-and-drop.

### DAT Header Fields

These map directly to the `<header>` block in the output datfile. All fields are optional. Blank fields will write empty tags, which is valid and expected in Logiqx XML.

| Field | Header tag | Notes |
|---|---|---|
| Description | `<description>` | Free text. Typically matches the collection name |
| Category | `<category>` | e.g. `PC`, `Arcade`, `Commodore` |
| Version | `<version>` | Release version or date stamp |
| Date | `<date>` | Defaults to today's date at runtime |
| Author | `<author>` | Your name or handle |
| URL | `<url>` | Project or source URL |
| Homepage | `<homepage>` | Homepage URL |
| Comment | `<comment>` | Free text notes |

The `<name>` tag is populated automatically from the dat filename stem. Every dat also receives `<romvault/>` as the last header tag — this is the base RomVault recognition token, and expands to `<romvault forcepacking="fileonly"/>` when Mixed mode is active.

### Options

All options are described in detail in their own sections below. The UI greys out options that do not apply to the current combination of Dat Type and Generation mode — this prevents invalid combinations without hiding the controls. A **📖 link** to this README is provided directly beneath the Structure options for quick reference.

---

## Dat Types

### Mixed — Archive as File

The file **itself** is the ROM entry. The zip, 7z, or any other archive file is hashed as a single opaque file — its contents are not inspected.

Use this when RomVault is managing archives as **atomic units** — each archive is one logical game entry in your collection, and RomVault will not look inside it.

```xml
<game name="Lemmings (1991) (Psygnosis) [360K]">
    <description>Lemmings (1991) (Psygnosis) [360K]</description>
    <rom name="Lemmings (1991) (Psygnosis) [360K].zip"
         size="1423168" crc="a3f82b1c" sha1="d4e9c02a7f1b3e5d8c6a0f4b2e7d1a9c3f5b8e2d"/>
</game>
```

The header always contains `<romvault forcepacking="fileonly"/>`, which instructs RomVault to treat every matched file as a file-only entry regardless of extension.

**When to use Mixed:**
- PC floppy/CD image collections where each game is a discrete archive
- Any collection where the archive boundary is the logical game boundary
- Collections managed as scene-style releases where the zip is the delivery unit

### :warning: RomVault Mixed/fileonly Caveat:
- Dats created in **Mixed (Archive as File)** mode (aka "fileonly") have a fileonly header tag added to the dats to indicate to RomVault that it is dealing with a fileonly dat and to display it as such.
- The dat's `<rom>` entries need to be wrapped in a `<game>` block, which is named after its parent folder name. This `<game>` block must exist to support the ability for a user to switch the Dat Rule on the folder from fileonly to a compression format if they decide they want to compress the folder. Without the `<game>` block, RomVault will throw errors forever and require the user to kill the process.
- Adding the `<game>` block creates a new internal "set" folder for the fileonly roms to reside in, but this throws out the alignment of the dat folder vs. the file folder. Pre-existing roms will end up with a Cyan status because of the insertion of this folder. If you try to scan and rebuild, you'll end up with an extra root folder in your folder path, which is very likely undesirable.
- **To fix this, the user must set a DAT Rule on the parent folder of "Single Archive", and "Do not use subdirs for sets" to remove the internal `<game>` block and have the folder behave like a fileonly dat again.**

> <img width="473" height="296" alt="image" src="https://github.com/user-attachments/assets/e0df22ca-1c0d-446f-bd28-086ce720dc13" />

:arrow_right: **This anomaly occurs because of how RomVault is coded, not how the datfile is designed. Until RomVault can learn how to handle its users changing the Dat Rule settings on fileonly folders (which it is not expecting its users to do), this workaround must exist.**

Additional factoid:
- If you load a datfile and you get an **"Incompatible Compare Type"** error when trying to apply the compression setting to a folder, load the datfile into a text editor and check if the datfile's `<rom>` contents have a `<game>` block wrapped around the files. If not, one must be added.

---

### Zipped

The **contents** of each zip archive are hashed. Each zip becomes a game entry, and each file inside the zip becomes a `<rom>` entry within that game.

Use this when RomVault is expected to open, verify, and manage individual files within archives — the standard mode for ROM sets from No-Intro, Redump, TOSEC, and similar databases.

```xml
<game name="Lemmings (1991) (Psygnosis) [360K]">
    <description>Lemmings (1991) (Psygnosis) [360K]</description>
    <rom name="Disk 1.ima" size="368640" crc="3a9f12b4"
         sha1="e2c8a4f1b9d3e7c5a2f8b4d0e6c2a8f4b6d2e8c4"/>
    <rom name="Disk 2.ima" size="368640" crc="7b2e45c1"
         sha1="f3d9b5e2c1a4f8b6d2e0c6a3f9b5e1c7a3f9b5e1"/>
</game>
```

Internally zipped subfolders are always preserved in the rom name:

```xml
<rom name="original/Lemmings (1991) (Psygnosis) [360K] (Disk B).ima"
     size="368640" crc="9c4a21d7" sha1="..."/>
```

Empty folders inside a zip produce a zero-byte rom entry with canonical empty hashes, matching RomVault's behaviour:

```xml
<rom name="path/to/emptydir/" size="0" crc="00000000"
     sha1="da39a3ee5e6b4b0d3255bfef95601890afd80709"
     sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
     md5="d41d8cd98f00b204e9800998ecf8427e"/>
```

---

## Generation Modes

### 1 Dat per Root Folder

Each immediate subfolder of the input root becomes **one datfile**. All content within that subfolder — regardless of depth — is rolled into the single dat.

Given this input:
```
E:\Floppy\Access Software\
    Amazon (1992)\
        amazon.zip
        Docs\
            amazon_manual.zip
    Crime Wave (1990)\
        crime_wave.zip
```

The tool produces:
```
Output\Access Software\
    Parent - Floppy - Amazon (1992) (2026-04-24_RomVault).xml
    Parent - Floppy - Crime Wave (1990) (2026-04-24_RomVault).xml
```

The content of `Docs\` within `Amazon (1992)\` is captured inside the Amazon dat — how it appears depends on the chosen **Structure** option.

Use this mode when each top-level subfolder represents a single logical unit (one game title, one publisher, one platform).

### 1 Dat per Root Folder and All Subfolders

Every folder at every depth that contains relevant content gets its own datfile. The output folder structure mirrors the input exactly.

Given the same input above, this mode produces a separate dat for the `Docs` subfolder as well:

```
Output\Access Software\
    Parent - Floppy - Amazon (1992) (2026-04-24_RomVault).xml       ← amazon.zip only
    Parent - Floppy - Crime Wave (1990) (2026-04-24_RomVault).xml
    Amazon (1992)\Docs\
        Parent - Floppy - Amazon (1992) - Docs (2026-04-24_RomVault).xml
```

Each dat contains only the content sitting directly in that specific folder — no recursive capture.

Use this mode for large heterogeneous collections where subfolders are logically independent (different platforms, publishers, or release types sharing a common input root).

---

## Structure Options

Structure controls how the internal hierarchy of a datfile is expressed. It only applies to **1 Dat per Root Folder** mode — in **1 dat per root folder & all subfolders**, every dat is flat by definition.

The four structures replicate the output options from RomVault's dir2datUI tool. Use the **Preview window** to compare them side-by-side against your actual data before committing to a structure. Not sure where to start? Use **Tools → Analyze Folder Structure** first.

### Reference Folder Layout

All four examples below use this input:

```
Access Software PC Floppy Disk Image Collection\
    Crime Wave (1990)\                    ← folder with direct archives
        Crime Wave (1990) (Disk A).zip
        Crime Wave (1990) (Disk B).zip
        original\                         ← physical subfolder inside game folder
            Crime Wave (1990) (v1.0).zip
    Docs\                                 ← container folder, no direct archives
        Amazon Docs\
            Amazon - Manual.zip
        Crime Wave Docs\
            Crime Wave - Manual.zip
```

---

### Structure 1 — Dirs

Every folder at every depth becomes a `<dir>` tag. No `<game>` tags are used anywhere. Archives become `<dir>` entries containing their rom entries.

```xml
<dir name="Crime Wave (1990)">
    <dir name="Crime Wave (1990) (Disk A)">
        <rom name="Crime Wave (1990) (Disk A).ima" size="368640" crc="3a9f12b4" sha1="..."/>
    </dir>
    <dir name="original">
        <dir name="Crime Wave (1990) (v1.0)">
            <rom name="Crime Wave (1990) (v1.0).ima" .../>
        </dir>
    </dir>
</dir>
<dir name="Docs">
    <dir name="Amazon Docs">
        <dir name="Amazon - Manual">
            <rom name="Amazon - Manual.pdf" .../>
        </dir>
    </dir>
</dir>
```

**Use when:** Maximum structural compatibility is needed, or when your collection management tool treats all folder levels equivalently. Least common in practice — represents 0% of the 10,497 datfiles surveyed in a real-world collection of this scale.

---

### Structure 2 — Archives as Games

Archives become `<game>` entries. Physical filesystem folders become `<dir>` entries. Files inside archives that live in internal subfolders have their path preserved in the rom `name` attribute.

This is the **default and most widely used structure**. It matches the output format of No-Intro, Redump, TOSEC, and the majority of community-distributed dat files.

```xml
<game name="Crime Wave (1990) (Disk A)">
    <description>Crime Wave (1990) (Disk A)</description>
    <rom name="Crime Wave (1990) (Disk A).ima" size="368640" crc="3a9f12b4" sha1="..."/>
</game>
<dir name="original">
    <game name="Crime Wave (1990) (v1.0)">
        <description>Crime Wave (1990) (v1.0)</description>
        <rom name="Crime Wave (1990) (v1.0).ima" .../>
    </game>
</dir>
<dir name="Docs">
    <dir name="Amazon Docs">
        <game name="Amazon - Manual">
            <description>Amazon - Manual</description>
            <rom name="Amazon - Manual.pdf" .../>
        </game>
    </dir>
</dir>
```

**Use when:** Your collection uses standard zip-per-game organisation, with physical subfolders representing logical groupings (disc variants, regional versions, documentation).

> This structure represents **68.8% of all datfiles** in a survey of 10,497 dats across two large RomVault-managed collections (see [Advanced: Datfile Landscape Analysis](#advanced-datfile-landscape-analysis)).

---

### Structure 3 — First Level Dirs as Games

The first level of physical subfolders inside the dat root are always rendered as `<game>` entries, regardless of whether they contain archives directly or act as containers. Deeper physical subfolders become `<dir>` entries.

```xml
<game name="Crime Wave (1990)">
    <description>Crime Wave (1990)</description>
    <rom name="Crime Wave (1990) (Disk A).ima" .../>
    <rom name="Crime Wave (1990) (Disk B).ima" .../>
    <rom name="original/Crime Wave (1990) (v1.0).ima" .../>
</game>
<game name="Docs">
    <description>Docs</description>
    <dir name="Amazon Docs">
        <game name="Amazon - Manual">
            <rom name="Amazon - Manual.pdf" .../>
        </game>
    </dir>
</game>
```

**Use when:** Each first-level subfolder represents a complete game or release, and you want the folder itself — not its individual archives — to be the primary named entry in RomVault's database. Useful for multi-disc or multi-format releases where all variants live in one subfolder.

---

### Structure 4 — First Level Dirs as Games + Merge Dirs in Games

First-level subfolders become `<game>` entries. All deeper physical subfolders are merged flat into that game entry. Each merged subfolder gets an empty directory marker rom (`size="0" crc="00000000"`) followed by its files listed with path-prefixed rom names.

```xml
<game name="Crime Wave (1990)">
    <description>Crime Wave (1990)</description>
    <rom name="Crime Wave (1990) (Disk A).ima" .../>
    <rom name="Crime Wave (1990) (Disk B).ima" .../>
    <rom name="original/" size="0" crc="00000000"/>
    <rom name="original/Crime Wave (1990) (v1.0).ima" .../>
</game>
<game name="Docs">
    <description>Docs</description>
    <rom name="Amazon Docs/" size="0" crc="00000000"/>
    <rom name="Amazon Docs/Amazon - Manual.pdf" .../>
    <rom name="Crime Wave Docs/" size="0" crc="00000000"/>
    <rom name="Crime Wave Docs/Crime Wave - Manual.pdf" .../>
</game>
```

**Use when:** Collections have deep and variable subfolder hierarchies, and you want a single flat game entry to capture everything within a top-level folder including folder structure metadata. Well-suited to tape archives, rhythm game collections, and any collection where internal directory layout is part of the preservation data.

> In the survey, **2.0% of dats** used this structure — primarily complex arcade collections and large preservation projects with nesting depths between 4 and 9 levels.

---

## Format: Modern vs Legacy

| Setting | `<game>` / `<machine>` | `<dir>` | `<description>` |
|---|---|---|---|
| **Modern** | ✅ Used for archive entries | ✅ Used for folders | Optional (checkbox) |
| **Legacy** | ❌ | All entries use `<dir>` | Not emitted |

**Modern** is the correct choice for RomVault. The Legacy format (all `<dir>` tags, no `<game>`) is ClrMamePro's native format and is retained for compatibility with older toolchains.

When Modern is selected, an additional option appears: **Use `<machine>` instead of `<game>`**. This is the element name used by EmuMovies and some MAME-derived dat files. Unless you are producing dats specifically for an EmuMovies-style workflow, leave this set to `<game>`.

---

## Hash Options

CRC32 and SHA1 are always computed. Both are mandatory in Logiqx XML and are the primary integrity verification hashes used by RomVault.

| Option | Attribute | Notes |
|---|---|---|
| **MD5** | `md5=` | Optional. Adds meaningful overhead per file. Written after `sha1=` in the rom tag |
| **SHA-256** | `sha256=` | Optional. Written after `sha1=` and before `md5=`. Informational — RomVault displays it but does not use it for matching. A warning popup is shown when enabling this option. |

Attribute order in the output rom tag follows the RomVault DATReader source exactly:
`name` → `size` → `crc` → `sha1` → `sha256` → `md5` → `date`

---

## Network Cap

The hashing engine reads zip files from the network share at full available speed, which can saturate a 1 Gbps connection and impact other network users. A token-bucket rate limiter keeps throughput below a configurable ceiling.

**Auto mode (default, `Net cap = 0`):** `psutil` detects the fastest active NIC and caps reads at 85% of its link speed, leaving 15% free for other traffic. The detected cap is logged at the start of every run.

**Manual mode:** Enter a value in Mbit/s in the **Net cap Mbit/s** spinbox (in the Dat Settings row, to the right of the Threads spinbox). Setting `0` returns to auto mode.

> `psutil` must be installed for auto mode to work. Without it the cap is unlimited and the spinbox still accepts a manual value.

---

## Extension Filters

Available in **Mixed mode only** — in Zipped mode the unit is always the complete `.zip` file.

| Field | Behaviour |
|---|---|
| **Include only extensions** | If set, only files matching these extensions are hashed and included. Blank = include everything |
| **Exclude extensions / files** | Files matching these extensions or exact filenames are always skipped |

**Format:** Comma-separated. Leading dots are optional. Full filenames are supported.

```
.ima, .mfm, .86f, .td0        ← include only floppy image formats
.nfo, .sfv, .md5, thumbs.db   ← typical exclusion list
```

Filtering is applied during the folder scan phase, before any hashing begins. Excluded files never enter the work queue.

---

## ZStandard Support

Zip archives compressed with **RV-ZStandard** (zip comment beginning `RVZSTD-`) are handled natively by the `zipfile-zstd` and `zstandard` packages. No external tools are required for reading zip contents.

**Detection:** An RV-ZStandard archive can be identified by its zip comment, which begins with `RVZSTD-` followed by a CRC32 checksum (e.g. `RVZSTD-22DA5DD0`). TorrentZip archives use a similar deterministic recompression approach that also sets standardised internal timestamps (`1980/00/00 00-00-00`).

**Supported compression methods:**

| Method | Description | How handled |
|---|---|---|
| 0 | Stored (no compression) | Direct read |
| 8 | Deflate (standard zip) | `zlib.decompressobj(-15)` |
| 93 | ZStandard / RVZSTD | `zstandard.ZstdDecompressor` |

**File date and timestamps (Zipped mode):** When **File date & time** is enabled, the timestamp for each rom entry is read from the zip's internal metadata and written as `date="yyyy/mm/dd hh-mm-ss"`. For TorrentZip and RV-ZStandard archives this will always be `1980/00/00 00-00-00` — the standardised DOS epoch timestamp these tools write. This is intentional — it documents that the archive has been deterministically recompressed.

**7-Zip-ZStandard** (`7z.exe`) is still required if you use the **Recursive Archive Extractor** tool for ZIP/7Z/RAR extraction. It is no longer used for dat hashing.

---

## Parent Name and Output Folder Structure

### Dat Naming

Every datfile is named using the following pattern:

```
[ParentName - ] TopLevelFolderName - SubFolderName (YYYY-MM-DD_RomVault).xml
```

With **Parent name** set to `Digitoxin` and an input root of `Floppy`, processing the subfolder `Access Software PC Floppy Disk Image Collection` produces:

```
Digitoxin - Floppy - Access Software PC Floppy Disk Image Collection (2026-04-24_RomVault).xml
```

The `_RomVault` suffix is deliberate — it flags dats produced by this tool to downstream tools and distribution platforms, and prevents name collisions with dats produced by other sources covering the same content.

The **Parent name** field was added specifically to support external dat distribution platforms where generic or duplicated dat names cause indexing conflicts. By prepending a consistent identifying prefix, your entire dat output is uniquely namespaced.

### Output Folder Structure

Output always mirrors the input folder structure, rooted at a folder named after the input top-level folder:

```
Output root\
    TopLevelFolderName\          ← created automatically
        SubFolder1\
            Dat1.xml
        SubFolder2\
            Dat2.xml
```

This matches RomVault's expected DatRoot layout, where datfiles and their parent folders establish the scanning hierarchy. **Do not output flat** — RomVault uses the folder structure of DatRoot to define collection boundaries.

---

## Dat Preview Window

After a run completes, the **🔍 Preview Dats** button becomes active (in both the main window and the Run Progress window). It opens a preview window showing the XML of every dat produced during that run.

**Features:**
- Listbox of all completed dats — click any entry to switch
- Four **Structure** radio buttons, independent from the main window — switching any option instantly re-renders the selected dat from in-memory data, with no re-hashing
- Full XML syntax highlighting (angle brackets, tag names, attribute names, attribute values, text content)
- Selectable and copyable text — `Ctrl+A` selects all, `Ctrl+C` copies, right-click for context menu
- **Save Chosen Dat Structure As...** — writes the currently displayed XML to a file of your choosing, named with the structure label appended

The preview re-renders entirely from the hash data held in memory — switching structures is instantaneous even for large dats. This is the equivalent of dir2datUI's live preview mode, and is the recommended way to evaluate structure options against your real data before deciding which to use for a project.

---

## Run Progress Window

When **Start** is pressed, a detached **Run Progress** window opens automatically. This window is separate from the main window, allowing you to monitor the run while the main settings remain visible and accessible.

**Features:**
- Live status line, item counts, and progress bar
- Animated braille spinner during Phase 1 (folder discovery), switching to a determinate progress bar during Phase 2 (hashing)
- **Network throughput display** — live receive/send speed updated once per second:
  ```
  Network:  ↓  423.1 Mbit/s   ↑    1.5 Mbit/s
  ```
  Requires `psutil`. Shows `(psutil not installed)` if absent.
- **Elapsed time display** — running clock updated once per second:
  ```
  Elapsed:  7m 42s
  ```
  Freezes with `(finished)` appended when the run completes.
- Scrollable activity log — each folder and zip is logged in real time as it is processed, with colour-coded entries:
  - **Amber** — phase and scan events
  - **Blue** — folder boundaries (`>>`) and subdirectory markers (`[dir]`)
  - **Brown** — subfolder entries within a job folder
  - **Green** — successfully hashed zips with timing and throughput
  - **Grey** — carried items (incremental mode)
  - **Red** — errors
  - **Bright green** — completed dat files
- Per-zip diagnostics in the log, e.g.:
  ```
  ✓ GameName.zip  (648.4 MB in 6.2s = 104.6 MB/s (1333 entries, stream))
  ```
  `mem` = loaded to RAM (BytesIO path); `stream` = sequential read from network. Entries flagged `[SLOW]` if throughput falls below 5 MB/s. Individual slow entries within a zip are identified by name and uncompressed size.
- **📋 Show Progress** button in the main window re-opens the progress window if it has been closed — the activity log is preserved until the next run starts
- **🔍 Preview Dats** button enables in the progress window once a run completes
- **💾 Save Activity Log** — saves the full log to a text file. Search for `[ERROR]` to find any problem archives, or `[SUMMARY]` to jump to the final error count.
- The window cannot be closed while a run is in progress — use Soft Stop or Hard Stop first

The progress window opens at a fixed position on screen and can be freely moved and resized.

---

## Incremental Update — Skip Already-Hashed Files

For large collections — particularly those in the hundreds of GB or multi-TB range — rehashing every file on every run is impractical. The incremental update mode allows the tool to update an existing datfile by hashing only new or changed content, carrying forward hash data for everything that hasn't changed.

**Enable it:** Tick **Incremental update — skip already-hashed files** in the Incremental Update section and point the **Existing dat file or folder** field to the dat you want to update (or the folder containing your dats for bulk updates). Optionally tick **Rename superseded dat to .old** to invalidate the previous datfile in RomVault once the update is complete.

### How it works

When Start is pressed with incremental mode active, a **Pre-flight Check** dialog opens before any processing begins:

1. **Validation** — the tool scans the dat source folder recursively and cross-checks each dat's entries against the source file/zip folder. It reports a match percentage per dat, flags missing entries, and identifies new items to be added.
2. **New version** — optionally set a new `<version>` string for the updated dat header.
3. **Proceed / Rehash entire folder / Rescan Dats / Save Pre-inspection Log / Cancel** — choose how to continue.

### Pre-inspection Log

After validation completes, the **💾 Save Pre-inspection Log** button becomes available. This saves a full timestamped report (`dat_creator_pre-inspection_log_YYYYMMDD_HHMMSS.txt`) listing every anomaly found — every missing game entry, every missing individual rom file, and every new item detected in the source folder that is not yet in the dat. The on-screen dialog shows only summary counts; the log file contains the complete detail for every dat and every affected entry.

### Matching strategy

**Zipped mode:** Each zip is checked against the dat using filename + uncompressed size + CRC32 (read from the zip central directory — no decompression needed, takes milliseconds). If all three match, the existing SHA1, MD5, and SHA-256 values are carried forward directly from the dat. Only new or changed zips are fully analyzed.

**Mixed mode:** Files are matched by filename + size only. If both match, existing hash data is carried forward. This applies to all files across all game subfolders — every rom in every game is indexed, not just the first file per game. If a file was replaced with content of the same name and the same size, the change cannot be detected without a full rehash — see the warning in the Pre-flight Check dialog.

**Folder-based Mixed collections** (Structure 3/4 where each game is a subfolder containing multiple files): The incremental engine indexes every rom across every game subfolder. A game is considered fully matched only if its subfolder exists AND every listed rom file is present within it. Individual missing files within a present game folder are reported separately from whole games that are absent.

### After a successful update

- The new datfile is written with today's date in both the filename and the `<date>` header field.
- If **Rename superseded dat to .old** is ticked, the original datfile is renamed to `filename.xml.old` — this invalidates it from RomVault's perspective while preserving it for reference. If an `.old` file already exists, a numeric suffix is appended (`filename(1).old`, `filename(2).old`, etc.).
- There is never more than one active `.xml` datfile in a folder at a time.

### Validation and path alignment

The tool uses relative path mirroring to match dats to their source folders. A dat at `dats/Activision PC Floppy/foo.xml` is assumed to correspond to `input_root/Activision PC Floppy/`. This works correctly as long as your dat output folder structure mirrors your input folder structure — which it always will if dats were generated by this tool.

If the match percentage drops below 80%, the Pre-flight Check displays a warning. You can still proceed — entries not found in the source folder will be removed from the updated dat — but the warning gives you the opportunity to verify that the paths are correctly aligned before committing.

Use **🔄 Rescan Dats** inside the Pre-flight Check to re-run validation after making changes without closing and reopening the dialog.

---

## Folder Structure Analyzer

Available via **Tools → Analyze Folder Structure...** in the menu bar.

If you are unsure which Generation mode or Structure option is appropriate for a collection, the Analyzer can examine the folder layout and make a recommendation before any hashing takes place.

**How to use:**
1. Open the Analyzer from the Tools menu
2. Set the folder path (or drag-and-drop it onto the field)
3. Select whether the content is **Mixed** or **Zipped**
4. Click **Analyze**

The Analyzer walks the folder structure in a background thread (no hashing, completes in seconds even for very large collections with hundreds of thousands of files) and reports:

- Total folders and items found
- Depth distribution — how many levels of subfolders exist
- Pattern breakdown — flat game folders, container folders, folders with nested subdirs
- Sample folder names for spot-checking
- **Recommendation** — a suggested Generation mode and Structure option, colour-coded by confidence level (green = high, amber = medium)

Clicking **Apply Recommended Settings** fills the main window's Dat Type, Generation, Structure, Format, and Input folder fields automatically and closes the Analyzer.

---

## Tools Menu

### Bulk Datfile Header Updater

Available via **Tools → Bulk Datfile Header Updater...**.

Updates the header fields of all datfiles found in a folder (or a single file) in bulk. Particularly useful for updating dates across a folder of dats after content changes, or for correcting author, URL, or category fields across an entire collection.

**Rules:**
- Leave a field blank → existing content in each dat is left untouched
- Enter a value → that field is overwritten in every dat
- Tick **Clear** next to a field → the field is erased (written as an empty tag) in every dat
- **Date** is always required — updates both the `<date>` header tag and the `(YYYY-MM-DD_RomVault)` token in each filename

The updater runs in a background thread and streams results to the activity log in real time. A log can be saved on completion. Both `.xml` and `.dat` files are processed; subfolders are searched recursively.

### Game and ROM Counter

Available via **Tools → Game and ROM Counter...**.

Scans a folder of datfiles and reports the game count, ROM count, and total uncompressed file size for each dat. Designed to answer the question no ROM manager readily answers: **how many games (not just ROMs) are in each folder?**

**Features:**
- Recursive folder scan — finds all `.xml` and `.dat` files at any depth
- Results shown in a hierarchical tree view mirroring the folder structure, with clickable column headers for sorting
- **Tree View / Flat List** toggle — switch between folder hierarchy and a simple sorted list
- **Expand / Collapse All** via right-click context menu
- **Clickable column sort** for Dat Name, Games, ROMs, and Uncompressed Size (▲/▼ toggle, works in both views)
- **Multi-selection** with Ctrl+click and Shift+click — a live **Selection Subtotal** updates showing combined games, ROMs, size, and average games per dat for the selected rows
- **Collection Summary** panel — total dats, total folders, total games, total ROMs, total uncompressed size, averages, largest dat by games and by ROMs, empty dat count, and parse error count
- **Export CSV** — saves all results with raw byte counts for spreadsheet use
- Sizes reported in decimal MB/GB/TB (not powers-of-2)

### Recursive Archive Extractor

Available via **Tools → Recursive Archive Extractor...**.

Recursively extracts ZIP, 7Z, and RAR archives into their own named subfolders. Designed for processing large collections where archives may be nested inside other archives. Uses the 7-Zip-ZStandard path configured in the main Suite settings.

**Features:**
- Extracts `.zip`, `.7z`, and `.rar` formats — individually selectable
- **Same-as-source** or **mirror to custom destination** output modes
- **After extraction** options: Keep archive / Recycle Bin (requires `send2trash`) / Permanent delete / Move (mirror structure) / Move (flat dump)
- **Auto-extract nested archives** — archives found inside extracted content are automatically queued and extracted in the same pass
- Live ETA, elapsed time, and per-file status in the activity log
- Stop button halts processing cleanly after the current archive
- Save Log button

The extractor preserves the single-file-in-zip optimisation (flat extraction for single-file archives) and includes a double-nesting flattener that prevents tool-created duplicate parent folders.

> Install `send2trash` (`pip install send2trash`) to enable the Recycle Bin option. Without it, only Keep, Permanent delete, and Move modes are available.

### ZIP Store Packer

Available via **Tools → ZIP Store Packer...**.

Wraps files in uncompressed ZIP containers (`ZIP_STORED` — zero compression) for use as a neutral byte-preserving wrapper before downstream recompression by RomVault or other tools. Each source file is verified inside its zip before the original is deleted.

**Features:**
- Configurable target extensions via add/remove pill tags (default: `.exe`)
- Space or comma-separated batch entry
- **Verify before delete** — uses `zipfile.testzip()` and size comparison before removing the original
- **Skip if .zip already exists** — avoids re-packing files already wrapped
- Recursive or non-recursive folder scan
- Live ETA and per-file status log
- Stop button, Save Log button

### Remove ReadOnly Attribute

Available via **Tools → Remove ReadOnly Attribute...**.

Performs two distinct operations on all files and folders recursively:

1. **Clear read-only file attribute** — uses `os.chmod` to remove the Windows read-only (R) flag from all files and subfolders.

2. **Remove Zone.Identifier** — runs PowerShell's `Unblock-File` command to delete the `Zone.Identifier` NTFS alternate data stream. This is the actual "This file came from another computer" security flag that Windows applies to files downloaded from untrusted network locations and which requires checking the "Unblock" checkbox in file Properties to clear manually.

> ⚠  The Unblock-File step may require the Suite to be running as **Administrator**. If unblocking fails, close the Suite and re-launch it via right-click → **Run as administrator**, then try again.

Accepts individual files or folders via Browse or drag-and-drop. Results are logged with clear status for each step. Log can be saved on completion.

---

## Settings and Config File

All settings are saved to `Eggmans_Datfile_Creator_Suite_config.json` in the same folder as the script. Settings are written automatically when **Start** is pressed and can be explicitly saved at any time with the **Save Settings** button.

The config is plain JSON and can be edited by hand if needed. Unrecognised keys are silently ignored on load, so editing is safe.

---

# Advanced: Datfile Landscape Analysis

> **This section is for collectors and advanced users curious about the broader datfile ecosystem. None of this is required reading to use the tool.**

To understand how the datfiles produced by this tool relate to the wider landscape, a structural analysis was performed across two large RomVault-managed collections using a companion script (`analyze_datfiles.py`, not included with this project).

### Collections Surveyed

| Collection | Dats analysed | Parse failures | Games | ROMs |
|---|---|---|---|---|
| Core (general) | 10,136 | 4 | 3,473,346 | 20,594,118 |
| Arcade | 350 | 7 | 62,481 | 49,555,576 |
| **Combined** | **10,486** | **11** | **3,535,827** | **70,149,694** |

Parse failures were all `.txt` files present in the DatRoot folder — not actual datfiles.

### Structure Distribution

| Structure | Core | Arcade | Total | Share |
|---|---|---|---|---|
| Standard Logiqx `<game>→<rom>` (depth 3) | 7,006 | 219 | 7,225 | **68.8%** |
| EmuMovies `<machine>` flat | 2,909 | 0 | 2,909 | **27.7%** |
| Complex `<dir>+<game>` (depth 4–9) | 87 | 128 | 215 | **2.0%** |
| ClrMamePro text format | 132 | 2 | 134 | **1.3%** |
| Flat ROMs only (no `<game>` wrapper) | 2 | 1 | 3 | **~0%** |

**The standard Logiqx structure at depth 3 accounts for nearly 97.7% of all XML datfiles in the core collection.** This is the structure produced by No-Intro, Redump, TOSEC, GoodMerge, and the vast majority of community dat projects. It is what this tool produces in Zipped mode with Structure 2 (Archives as Games).

### The EmuMovies Ecosystem

2,909 dats (27.7% of the core collection) use `<machine>` entries with `forcepacking="unzip"`. These are artwork and media packs distributed by EmuMovies — snapshots, box art, manuals, videos — not ROM sets. They use a different structural convention (`<machine>` instead of `<game>`) because they originate from MAME-derived tooling. This tool can output `<machine>` tags via the Modern format option, but EmuMovies dats are not a target use case for a dat generator — they are produced by EmuMovies' own internal pipeline.

### The `forcepacking` Values in the Wild

| Value | Count | What it means |
|---|---|---|
| *(absent)* | 8,705 | RomVault defaults to Zip mode — contents verified against rom entries |
| `fileonly` | 1,582 | Files are managed as atomic units — no inspection of archive contents |
| `unzip` | 197 | RomVault extracts files before verification — EmuMovies-specific |
| `zip` | 2 | Explicitly request Zip mode — redundant with absent, almost never used |

This tool produces only `fileonly` (Mixed mode) and absent (Zipped mode). The `unzip` and `zip` values are not generated.

### Nesting Depth

The depth histogram reveals just how uniform real-world datfiles are:

| Depth | Description | Core count |
|---|---|---|
| 1 | ClrMamePro text (no XML nesting) | 132 |
| 2 | Flat ROMs only, no game wrapper | 2 |
| **3** | **`<datafile>→<game>→<rom>` — the universal standard** | **9,904** |
| 4 | One level of `<dir>` inside games | 50 |
| 5–9 | Deep preservation collections | 48 |

Dats at depth 5 and above were without exception large PC or arcade preservation projects with complex physical directory structures — the Digitoxin PC Floppy Disk Image Collection, DUSTBUNNiES, and Eggman's Arcade Repository. These collections are themselves produced by this tool or close predecessors of it. The deepest dat in the survey reached depth 9 (Funworld touchscreen collection: 324 physical dirs, 868 game entries).

### ClrMamePro Format

134 dats across both collections use ClrMamePro's text-based format rather than Logiqx XML. All of these originate from sources that pre-date the shift to XML as the standard (EMMA Italian Dumping Team, legacy firmware packs). RomVault reads both formats without issue, and for a long time ClrMamePro format was the primary exchange format for arcade dat files. This tool produces only Logiqx XML, which is the current standard and the format RomVault's own tools generate.

### SHA-256 in Datfiles

SHA-256 support was introduced to the Logiqx dat format by No-Intro's Dat-o-Matic database and is present in many No-Intro dats. RomVault will display the SHA-256 value when it exists in a dat, but does not use it as part of its hash matching or ROM verification workflow — CRC32 and SHA1 are the operative hashes. SHA-256 is available as an optional output field in this tool for completeness, but for most collections it adds computation time without practical benefit. A warning popup is shown when enabling SHA-256 to remind users of this.

---

# Advanced: DAT Format Reference

> **For users who want to understand exactly what the tool is writing. Skip this if you don't need to hand-edit datfiles.**

### Header Block

```xml
<?xml version="1.0"?>
<datafile>
    <header>
        <name>Digitoxin - Floppy - Access Software PC Floppy Disk Image Collection</name>
        <description>PC Floppy Disk Image Collection</description>
        <category>PC</category>
        <version>2026-04-24</version>
        <date>2026-04-24</date>
        <author>Digitoxin</author>
        <url>https://github.com/Eggmansworld</url>
        <homepage>https://github.com/Eggmansworld</homepage>
        <comment></comment>
        <romvault forcepacking="fileonly"/>   ← Mixed mode
        <!-- or: <romvault/>  ← Zipped mode -->
    </header>
    ...
</datafile>
```

### ROM Entry Attribute Order

Attribute order follows the RomVault DATReader source (`DatXMLWriter.cs`):

```xml
<rom name="filename.ima"
     size="368640"
     crc="3a9f12b4"
     sha1="d4e9c02a7f1b3e5d8c6a0f4b2e7d1a9c3f5b8e2d"
     sha256="b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
     md5="5eb63bbbe01eeed093cb22bb8f5acdc3"
     date="1980/00/00 00-00-00"/>
```

`sha256` and `md5` are only written when their respective checkboxes are enabled. `date` is only written when **File date & time** is enabled (Zipped mode only).

### `<game>` vs `<dir>` in RomVault

In RomVault's internal model (from `RVWorld` source):
- A `<game>` tag represents a `DatDir` with a `DGame` attached — it appears in RomVault's scanner as a named game entry with fixable ROM slots
- A `<dir>` tag represents a `DatDir` without a `DGame` — it appears as a structural folder in the tree, not a fixable entry

This distinction matters for RomVault's Fix engine: only `<game>` entries can be fixed (have files moved into them from the ToSort folder). `<dir>` entries are containers only.

---

## Known Limitations

- **Output format is Logiqx XML only.** ClrMamePro text format and RomCenter format are not supported.
- **Zipped mode only processes `.zip` files.** Archives in other formats (`.7z`, `.rar`, `.gz`) are ignored. If your collection uses these formats in Mixed mode, they are hashed as files (which is correct for Mixed/fileonly collections).
- **The `forcepacking="unzip"` and `forcepacking="zip"` values are not generated.** Only `fileonly` (Mixed) and absent (Zipped) are produced.
- **`<softwarelist>` and MAME XML formats are not produced.** These are specialised formats for MAME's internal database and are outside the scope of this tool.
- **Incremental update Mixed mode cannot detect same-name same-size file replacements.** If a file has been replaced with content of identical filename and size, the change will not be detected. The Pre-flight Check dialog warns of this when Mixed mode is active. A full rehash option is available in the dialog for this scenario.
- **Per All mode with very large collections may be slow during Phase 1.** The scanner must traverse every folder at every depth. The progress window shows a live spinner and logs each folder as it is discovered, so you can see that the scan is still running.
- **Heavily compressed Unity game archives (.resS resource streaming files)** with extreme compression ratios (e.g. 10 GB uncompressed in a 514 MB zip) will be slower than other archives regardless of network speed. The bottleneck is CPU decompression time, not network throughput. These entries are identified by name in the activity log.

---

## Licensing

Original source code, scripts, tooling, and hand-authored documentation and
metadata in this repository are licensed under the MIT License.

Archived game data, binaries, firmware, media assets, and other third-party
materials are **not** covered by the MIT License and remain the property of
their respective copyright holders.

See the `LICENSE` and `NOTICE` files for full details and scope clarification.

---

## CREDITS

Created for the preservation community by Eggman, with Claude's help turning ideas into code.

If you improve the script, feel free to share your changes back with the community.

*Made with ❤️ for the retro game preservation community.*
