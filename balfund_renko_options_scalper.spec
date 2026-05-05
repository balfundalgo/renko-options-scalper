# -*- mode: python ; coding: utf-8 -*-
# ─────────────────────────────────────────────────────────────────
# PyInstaller spec — Balfund Renko Scalper v2.0
# Produces a single-file Windows EXE with CustomTkinter bundled.
# ─────────────────────────────────────────────────────────────────

import os
import sys
import importlib

block_cipher = None

# ── Locate CustomTkinter package for data bundling ──────────────
ctk_path = os.path.dirname(importlib.import_module("customtkinter").__file__)

a = Analysis(
    ["balfund_renko_options_scalper.py"],
    pathex=[],
    binaries=[],
    datas=[
        (ctk_path, "customtkinter"),
    ],
    hiddenimports=[
        "customtkinter",
        "darkdetect",
        "requests",
        "pandas",
        "websocket",
        "websocket._abnf",
        "websocket._app",
        "websocket._core",
        "websocket._exceptions",
        "websocket._handshake",
        "websocket._http",
        "websocket._logging",
        "websocket._socket",
        "websocket._ssl_compat",
        "websocket._url",
        "websocket._utils",
        "dotenv",
        "PIL",
        "PIL._tkinter_finder",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        "matplotlib",
        "scipy",
        "numpy.tests",
        "pandas.tests",
        "IPython",
        "notebook",
        "pytest",
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="BalfundRenkoScalper",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,          # No console window — GUI only
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,              # Add icon path here if desired
)
