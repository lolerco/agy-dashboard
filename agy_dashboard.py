#!/usr/bin/env python3
"""AGY CLI Usage Tracker & Dashboard — Standalone Backend & Server.

Zero-dependency Python 3.9+ stdlib implementation providing:
- Cross-platform AGY brain directory auto-discovery
- Streaming JSONL transcript parsing with `transcript_full.jsonl` truncation recovery
- Calibrated token estimation (optional offline tiktoken fallback to stdlib estimator)
- Dual token accounting: Unique Turn Tokens vs. Billed API Context Tokens
- Incremental SQLite caching (`agy_usage.db`) using mtime/size/offset watermarks
- Realistic 45-day synthetic demo dataset generator (`--demo` or empty-dir fallback)
- Thread-safe HTTP server (`ThreadingHTTPServer`) + CLI (`--summary`, `--export-json`)
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import math
import os
import random
import re
import socket
import sqlite3
import sys
import threading
import time
import urllib.parse
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

__version__ = "1.0.0"

DASHBOARD_HTML = json.loads('"<!DOCTYPE html>\\n<html lang=\\"en\\" data-theme=\\"dark\\">\\n<head>\\n  <meta charset=\\"UTF-8\\" />\\n  <meta name=\\"viewport\\" content=\\"width=device-width, initial-scale=1.0\\" />\\n  <meta name=\\"color-scheme\\" content=\\"dark light\\" />\\n  <title>agy-telemetry :: usage &amp; footprint monitor [gruvbox]</title>\\n  <link rel=\\"preconnect\\" href=\\"https://cdn.jsdelivr.net\\" crossorigin />\\n  <link rel=\\"preconnect\\" href=\\"https://fonts.googleapis.com\\" />\\n  <link rel=\\"preconnect\\" href=\\"https://fonts.gstatic.com\\" crossorigin />\\n  <link rel=\\"stylesheet\\" href=\\"https://cdn.jsdelivr.net/gh/mshaugh/nerdfont-webfonts@v3.3.0/build/jetbrainsmono-nerd-font.css\\" />\\n  <link rel=\\"stylesheet\\" href=\\"https://cdn.jsdelivr.net/gh/mshaugh/nerdfont-webfonts@v3.3.0/build/symbols-nerd-font.css\\" />\\n  <link href=\\"https://fonts.googleapis.com/css2?family=JetBrains+Mono:ital,wght@0,400;0,500;0,600;0,700;1,400&display=swap\\" rel=\\"stylesheet\\" />\\n  <script src=\\"https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js\\" onerror=\\"window.__chartJsLoadFailed=true\\"></script>\\n  <style>\\n    @font-face {\\n      font-family: \'Symbols Nerd Font Mono\';\\n      src: local(\'Symbols Nerd Font Mono\'),\\n           local(\'Symbols Nerd Font\'),\\n           local(\'SymbolsNerdFontMono-Regular\'),\\n           url(\'https://cdn.jsdelivr.net/gh/ryanoasis/nerd-fonts@v3.3.0/patched-fonts/NerdFontsSymbolsOnly/SymbolsNerdFontMono-Regular.ttf\') format(\'truetype\');\\n      font-weight: normal;\\n      font-style: normal;\\n      font-display: swap;\\n    }\\n\\n    :root {\\n      /* Canonical Gruvbox Dark (morhetz/gruvbox) */\\n      --gb-bg0-h: #1d2021;\\n      --gb-bg0: #282828;\\n      --gb-bg0-s: #32302f;\\n      --gb-bg1: #3c3836;\\n      --gb-bg2: #504945;\\n      --gb-bg3: #665c54;\\n      --gb-bg4: #7c6f64;\\n      --gb-gray: #928374;\\n\\n      --gb-fg0: #fbf1c7;\\n      --gb-fg1: #ebdbb2;\\n      --gb-fg2: #d5c4a1;\\n      --gb-fg3: #bdae93;\\n      --gb-fg4: #a89984;\\n\\n      --gb-red: #fb4934;\\n      --gb-red-dim: #cc241d;\\n      --gb-green: #b8bb26;\\n      --gb-green-dim: #98971a;\\n      --gb-yellow: #fabd2f;\\n      --gb-yellow-dim: #d79921;\\n      --gb-blue: #83a598;\\n      --gb-blue-dim: #458588;\\n      --gb-purple: #d3869b;\\n      --gb-purple-dim: #b16286;\\n      --gb-aqua: #8ec07c;\\n      --gb-aqua-dim: #689d6a;\\n      --gb-orange: #fe8019;\\n      --gb-orange-dim: #d65d0e;\\n\\n      --bg-canvas: var(--gb-bg0-h);\\n      --bg-header: rgba(40, 40, 40, 0.94);\\n      --bg-panel: var(--gb-bg0);\\n      --bg-elevated: var(--gb-bg0-s);\\n      --bg-popover: var(--gb-bg0-s);\\n      --bg-card: var(--gb-bg0);\\n      --bg-card-hover: var(--gb-bg0-s);\\n      --bg-subtle: var(--gb-bg0-h);\\n      --bg-control: var(--gb-bg1);\\n      --bg-control-hover: var(--gb-bg2);\\n      --bg-active: rgba(254, 128, 25, 0.18);\\n\\n      --border-header: var(--gb-bg2);\\n      --border-subtle: var(--gb-bg1);\\n      --border-card: var(--gb-bg2);\\n      --border-strong: var(--gb-bg3);\\n      --border-active: var(--gb-orange);\\n\\n      --text-bright: var(--gb-fg0);\\n      --text-primary: var(--gb-fg1);\\n      --text-secondary: var(--gb-fg2);\\n      --text-muted: var(--gb-fg4);\\n      --text-subtle: var(--gb-gray);\\n\\n      --indigo-brand: var(--gb-blue);\\n      --indigo-accent: var(--gb-orange);\\n      --indigo-hover: var(--gb-yellow);\\n      --cyan-output: var(--gb-aqua);\\n      --violet-thinking: var(--gb-purple);\\n      --emerald-live: var(--gb-green);\\n      --amber-warn: var(--gb-yellow);\\n      --danger: var(--gb-red);\\n\\n      --hm-0: #32302f;\\n      --hm-1: rgba(152, 151, 26, 0.34);\\n      --hm-2: rgba(152, 151, 26, 0.62);\\n      --hm-3: #98971a;\\n      --hm-4: #b8bb26;\\n\\n      --radius-card: 3px;\\n      --radius-control: 2px;\\n      --radius-sm: 2px;\\n\\n      --font-nf: \'JetBrainsMono Nerd Font\', \'JetBrainsMonoNL Nerd Font\', \'JetBrains Mono NF\',\\n                 \'FiraCode Nerd Font\', \'Iosevka Nerd Font\', \'CaskaydiaCove Nerd Font\',\\n                 \'MesloLGS NF\', \'Hack Nerd Font\', \'SauceCodePro Nerd Font\', \'UbuntuMono Nerd Font\',\\n                 \'JetBrains Mono\', \'Symbols Nerd Font Mono\', \'Symbols Nerd Font\',\\n                 ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, \'Liberation Mono\', monospace;\\n      --font-sans: var(--font-nf);\\n      --font-mono: var(--font-nf);\\n    }\\n\\n    html[data-theme=\\"light\\"] {\\n      --gb-bg0-h: #f9f5d7;\\n      --gb-bg0: #fbf1c7;\\n      --gb-bg0-s: #f2e5bc;\\n      --gb-bg1: #ebdbb2;\\n      --gb-bg2: #d5c4a1;\\n      --gb-bg3: #bdae93;\\n      --gb-bg4: #a89984;\\n      --gb-gray: #7c6f64;\\n\\n      --gb-fg0: #282828;\\n      --gb-fg1: #3c3836;\\n      --gb-fg2: #504945;\\n      --gb-fg3: #665c54;\\n      --gb-fg4: #7c6f64;\\n\\n      --gb-red: #9d0006;\\n      --gb-red-dim: #cc241d;\\n      --gb-green: #79740e;\\n      --gb-green-dim: #98971a;\\n      --gb-yellow: #b57614;\\n      --gb-yellow-dim: #d79921;\\n      --gb-blue: #076678;\\n      --gb-blue-dim: #458588;\\n      --gb-purple: #8f3f71;\\n      --gb-purple-dim: #b16286;\\n      --gb-aqua: #427b58;\\n      --gb-aqua-dim: #689d6a;\\n      --gb-orange: #af3a03;\\n      --gb-orange-dim: #d65d0e;\\n\\n      --bg-canvas: var(--gb-bg0-h);\\n      --bg-header: rgba(251, 241, 199, 0.94);\\n      --bg-panel: var(--gb-bg0);\\n      --bg-elevated: var(--gb-bg0-s);\\n      --bg-popover: var(--gb-bg0);\\n      --bg-card: var(--gb-bg0);\\n      --bg-card-hover: var(--gb-bg0-s);\\n      --bg-subtle: var(--gb-bg0-s);\\n      --bg-control: var(--gb-bg1);\\n      --bg-control-hover: var(--gb-bg2);\\n      --bg-active: rgba(175, 58, 3, 0.14);\\n\\n      --hm-0: #ebdbb2;\\n      --hm-1: rgba(121, 116, 14, 0.25);\\n      --hm-2: rgba(121, 116, 14, 0.50);\\n      --hm-3: #98971a;\\n      --hm-4: #79740e;\\n    }\\n\\n    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }\\n\\n    html, body {\\n      background-color: var(--bg-canvas);\\n      color: var(--text-primary);\\n      font-family: var(--font-nf);\\n      font-variant-ligatures: contextual;\\n      -webkit-font-smoothing: antialiased;\\n      -moz-osx-font-smoothing: grayscale;\\n      font-size: 12.5px;\\n      line-height: 1.5;\\n      min-height: 100vh;\\n      overflow-x: hidden;\\n    }\\n\\n    .mono, [data-mono] {\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n    }\\n\\n    .nf-icon {\\n      font-family: \'Symbols Nerd Font Mono\', \'Symbols Nerd Font\', var(--font-nf);\\n      font-style: normal;\\n      font-weight: normal;\\n      line-height: 1;\\n      display: inline-flex;\\n      align-items: center;\\n      justify-content: center;\\n      flex-shrink: 0;\\n      user-select: none;\\n    }\\n\\n    button, input, select { font-family: inherit; color: inherit; }\\n    a { color: inherit; text-decoration: none; }\\n\\n    ::-webkit-scrollbar { width: 8px; height: 8px; }\\n    ::-webkit-scrollbar-track { background: var(--gb-bg0-h); }\\n    ::-webkit-scrollbar-thumb { background: var(--gb-bg2); border-radius: 2px; border: 1px solid var(--gb-bg0-h); }\\n    ::-webkit-scrollbar-thumb:hover { background: var(--gb-bg3); }\\n\\n    /* Sticky Top Header (Lualine / Tmux Bar) */\\n    .topbar {\\n      position: sticky; top: 0; z-index: 40;\\n      background: var(--bg-header);\\n      backdrop-filter: blur(8px); -webkit-backdrop-filter: blur(8px);\\n      border-bottom: 2px solid var(--border-header);\\n    }\\n    .topbar-inner {\\n      max-width: 1460px; margin: 0 auto; padding: 9px 20px;\\n      display: flex; align-items: center; justify-content: space-between;\\n      gap: 14px; flex-wrap: wrap;\\n    }\\n    .brand-cluster { display: flex; align-items: center; gap: 12px; min-width: 0; }\\n\\n    .pl-ribbon {\\n      display: inline-flex; align-items: stretch; height: 26px;\\n      font-size: 12px; font-weight: 700; line-height: 1; flex-shrink: 0; user-select: none;\\n    }\\n    .pl-seg-mode {\\n      background: var(--gb-orange); color: #1d2021; padding: 0 9px;\\n      display: inline-flex; align-items: center; gap: 6px;\\n      border-radius: 2px 0 0 2px; letter-spacing: 0.03em;\\n    }\\n    html[data-theme=\\"light\\"] .pl-seg-mode { color: #fbf1c7; }\\n    .pl-sep {\\n      font-family: \'Symbols Nerd Font Mono\', \'Symbols Nerd Font\', var(--font-nf);\\n      font-size: 18px; line-height: 26px; display: inline-flex; align-items: center; overflow: hidden;\\n    }\\n    .pl-sep-mode-to-title { background: var(--gb-bg1); color: var(--gb-orange); }\\n    .pl-seg-title {\\n      background: var(--gb-bg1); color: var(--text-bright);\\n      padding: 0 10px 0 6px; display: inline-flex; align-items: center; gap: 6px; font-weight: 600;\\n    }\\n    .pl-sep-title-end { background: transparent; color: var(--gb-bg1); }\\n\\n    .brand-titles { display: flex; flex-direction: column; min-width: 0; gap: 2px; }\\n    .brand-row { display: flex; align-items: center; gap: 8px; }\\n    .brand-subtitle-wrap { display: inline-flex; align-items: center; gap: 6px; min-width: 0; color: var(--text-muted); font-size: 11px; }\\n    .brand-subtitle {\\n      font-family: var(--font-mono); font-variant-numeric: tabular-nums; font-size: 11px;\\n      color: var(--text-muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis;\\n      max-width: 330px; cursor: default;\\n    }\\n\\n    .demo-badge {\\n      display: none; align-items: center; gap: 5px; padding: 2px 7px; border-radius: 2px;\\n      background: rgba(250, 189, 47, 0.15); border: 1px solid var(--gb-yellow-dim);\\n      color: var(--gb-yellow); font-family: var(--font-mono); font-size: 10px; font-weight: 700;\\n      letter-spacing: 0.04em; text-transform: uppercase; cursor: help; white-space: nowrap;\\n    }\\n    .demo-badge.is-visible { display: inline-flex; }\\n    .demo-badge-dot { width: 6px; height: 6px; border-radius: 1px; background: var(--gb-yellow); }\\n\\n    .controls-cluster { display: flex; align-items: center; gap: 7px; flex-wrap: wrap; }\\n\\n    .segmented {\\n      display: inline-flex; align-items: center; background: var(--bg-subtle);\\n      border: 1px solid var(--border-card); border-radius: var(--radius-control); padding: 2px; gap: 2px;\\n    }\\n    .seg-btn {\\n      appearance: none; background: transparent; border: 1px solid transparent;\\n      color: var(--text-muted); font-size: 11px; font-weight: 500; padding: 4px 8px;\\n      border-radius: var(--radius-sm); cursor: pointer;\\n      transition: color 0.12s ease, background 0.12s ease, border-color 0.12s ease;\\n      white-space: nowrap; line-height: 1.2; display: inline-flex; align-items: center; gap: 5px;\\n    }\\n    .seg-btn.mono-label { font-family: var(--font-mono); font-variant-numeric: tabular-nums; font-size: 11px; }\\n    .seg-btn:hover { color: var(--text-bright); background: var(--bg-control); }\\n    .seg-btn.is-active {\\n      background: var(--bg-active); border-color: var(--border-active);\\n      color: var(--gb-orange); font-weight: 600;\\n    }\\n\\n    .control-with-help { display: inline-flex; align-items: center; gap: 4px; }\\n    .help-trigger {\\n      width: 24px; height: 26px; border-radius: var(--radius-sm);\\n      border: 1px solid var(--border-card); background: var(--bg-control);\\n      color: var(--text-muted); font-family: var(--font-mono); font-size: 11px; font-weight: 700;\\n      display: inline-flex; align-items: center; justify-content: center; cursor: help;\\n      transition: border-color 0.12s ease, color 0.12s ease, background 0.12s ease;\\n    }\\n    .help-trigger:hover { border-color: var(--gb-orange); color: var(--gb-orange); background: var(--bg-control-hover); }\\n\\n    .select-wrap { position: relative; display: inline-flex; align-items: center; }\\n    .linear-select {\\n      appearance: none; -webkit-appearance: none; background: var(--bg-control);\\n      border: 1px solid var(--border-card); border-radius: var(--radius-control);\\n      color: var(--text-primary); font-size: 11.5px; font-weight: 500;\\n      padding: 4px 26px 4px 9px; height: 28px; cursor: pointer;\\n      transition: background 0.12s ease, border-color 0.12s ease; max-width: 225px; text-overflow: ellipsis;\\n    }\\n    .linear-select:hover { background: var(--bg-control-hover); border-color: var(--border-strong); color: var(--text-bright); }\\n    .linear-select:focus { outline: none; border-color: var(--gb-orange); }\\n    .linear-select option { background: var(--bg-panel); color: var(--text-primary); }\\n    .select-chevron { position: absolute; right: 8px; pointer-events: none; color: var(--gb-yellow); font-size: 11px; display: flex; }\\n\\n    .btn-ghost {\\n      appearance: none; display: inline-flex; align-items: center; gap: 6px;\\n      height: 28px; padding: 0 9px; border-radius: var(--radius-control);\\n      background: var(--bg-control); border: 1px solid var(--border-card);\\n      color: var(--text-primary); font-size: 11.5px; font-weight: 500; cursor: pointer;\\n      transition: background 0.12s ease, border-color 0.12s ease, color 0.12s ease; white-space: nowrap;\\n    }\\n    .btn-ghost:hover:not(:disabled) { background: var(--bg-control-hover); border-color: var(--gb-orange); color: var(--text-bright); }\\n    .btn-ghost:disabled { opacity: 0.55; cursor: not-allowed; }\\n    .btn-ghost.is-live { border-color: var(--gb-green-dim); background: rgba(184, 187, 38, 0.14); color: var(--gb-green); }\\n\\n    .pulse-dot { width: 7px; height: 7px; border-radius: 1px; background: var(--text-subtle); transition: background 0.2s ease; flex-shrink: 0; }\\n    .btn-ghost.is-live .pulse-dot { background: var(--gb-green); box-shadow: 0 0 0 0 rgba(184, 187, 38, 0.6); animation: pulseGruvboxGreen 2s infinite; }\\n    @keyframes pulseGruvboxGreen {\\n      0% { box-shadow: 0 0 0 0 rgba(184, 187, 38, 0.55); }\\n      70% { box-shadow: 0 0 0 5px rgba(184, 187, 38, 0); }\\n      100% { box-shadow: 0 0 0 0 rgba(184, 187, 38, 0); }\\n    }\\n    @keyframes spin { to { transform: rotate(360deg); } }\\n    .is-spinning svg, .is-spinning .nf-icon { animation: spin 0.75s linear infinite; }\\n\\n    .banner-strip { display: none; border-bottom: 1px solid var(--border-card); font-size: 12px; }\\n    .banner-strip.is-visible { display: block; }\\n    .banner-strip.banner-demo { background: rgba(215, 153, 33, 0.12); border-bottom-color: var(--gb-yellow-dim); color: var(--gb-yellow); }\\n    .banner-strip.banner-error { background: rgba(204, 36, 29, 0.15); border-bottom-color: var(--gb-red-dim); color: var(--gb-red); }\\n    .banner-inner { max-width: 1460px; margin: 0 auto; padding: 7px 20px; display: flex; align-items: center; justify-content: space-between; gap: 12px; }\\n    .banner-msg { display: flex; align-items: center; gap: 8px; min-width: 0; }\\n    .banner-actions { display: flex; align-items: center; gap: 8px; flex-shrink: 0; }\\n    .banner-btn { appearance: none; background: var(--bg-control); border: 1px solid var(--border-strong); color: var(--text-bright); border-radius: var(--radius-sm); padding: 2px 8px; font-size: 11px; font-weight: 600; cursor: pointer; }\\n    .banner-btn:hover { background: var(--bg-control-hover); border-color: var(--gb-orange); }\\n\\n    .main-container { max-width: 1460px; margin: 0 auto; padding: 16px 20px 44px; display: flex; flex-direction: column; gap: 14px; }\\n\\n    /* Neovim / Tmux Statusline */\\n    .status-strip {\\n      display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap;\\n      background: var(--bg-panel); border: 1px solid var(--border-card); border-radius: var(--radius-card);\\n      color: var(--text-muted); font-size: 11px; padding: 5px 10px;\\n    }\\n    .status-meta-group { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }\\n    .status-mode-pill { background: var(--gb-aqua-dim); color: #fbf1c7; font-weight: 700; font-size: 10px; padding: 1px 6px; border-radius: 2px; letter-spacing: 0.05em; }\\n    .status-item { display: inline-flex; align-items: center; gap: 5px; }\\n    .status-sep { color: var(--gb-bg3); font-family: \'Symbols Nerd Font Mono\', var(--font-nf); font-size: 11px; }\\n\\n    .surface-card {\\n      background: var(--bg-card); border: 1px solid var(--border-card); border-radius: var(--radius-card);\\n      box-shadow: 0 2px 0 rgba(0, 0, 0, 0.18); transition: background 0.14s ease, border-color 0.14s ease;\\n    }\\n    html[data-theme=\\"light\\"] .surface-card { box-shadow: 0 2px 0 rgba(60, 56, 54, 0.06); }\\n    .surface-card.interactive-card:hover { background: var(--bg-card-hover); border-color: var(--border-strong); }\\n\\n    .kpi-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }\\n    @media (max-width: 1080px) { .kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }\\n    @media (max-width: 640px) {\\n      .kpi-grid { grid-template-columns: 1fr; }\\n      .topbar-inner, .main-container { padding-left: 12px; padding-right: 12px; }\\n    }\\n\\n    .kpi-card { padding: 14px 16px; display: flex; flex-direction: column; justify-content: space-between; gap: 12px; min-height: 138px; position: relative; }\\n    .kpi-card[data-accent=\\"blue\\"]   { border-top: 2px solid var(--gb-blue); }\\n    .kpi-card[data-accent=\\"orange\\"] { border-top: 2px solid var(--gb-orange); }\\n    .kpi-card[data-accent=\\"yellow\\"] { border-top: 2px solid var(--gb-yellow); }\\n    .kpi-card[data-accent=\\"aqua\\"]   { border-top: 2px solid var(--gb-aqua); }\\n    .kpi-card[data-accent=\\"green\\"]  { border-top: 2px solid var(--gb-green); }\\n    .kpi-card[data-accent=\\"purple\\"] { border-top: 2px solid var(--gb-purple); }\\n\\n    .kpi-top { display: flex; align-items: center; justify-content: space-between; gap: 8px; }\\n    .kpi-label { font-size: 11.5px; font-weight: 600; color: var(--text-secondary); letter-spacing: 0.01em; display: inline-flex; align-items: center; gap: 7px; }\\n    .kpi-glyph { width: 22px; height: 22px; border-radius: 2px; background: var(--bg-subtle); border: 1px solid var(--border-subtle); font-size: 12px; }\\n    .kpi-tag { font-family: var(--font-mono); font-variant-numeric: tabular-nums; font-size: 10.5px; color: var(--text-muted); padding: 1px 6px; border-radius: 2px; background: var(--bg-subtle); border: 1px solid var(--border-card); white-space: nowrap; }\\n    .kpi-tag.tag-emerald { color: var(--gb-green); border-color: var(--gb-green-dim); background: rgba(152, 151, 26, 0.14); }\\n    .kpi-tag.tag-violet { color: var(--gb-purple); border-color: var(--gb-purple-dim); background: rgba(177, 98, 134, 0.15); }\\n    .kpi-tag.tag-yellow { color: var(--gb-yellow); border-color: var(--gb-yellow-dim); background: rgba(215, 153, 33, 0.14); }\\n\\n    .kpi-value-row { display: flex; align-items: baseline; gap: 8px; flex-wrap: wrap; }\\n    .kpi-value { font-family: var(--font-mono); font-variant-numeric: tabular-nums; font-size: 23px; font-weight: 700; letter-spacing: -0.02em; line-height: 1.15; color: var(--text-bright); }\\n    .kpi-value-sub { font-family: var(--font-mono); font-variant-numeric: tabular-nums; font-size: 11px; color: var(--text-subtle); }\\n\\n    .seg-bar { width: 100%; height: 6px; border-radius: 2px; background: var(--bg-subtle); border: 1px solid var(--border-subtle); display: flex; overflow: hidden; gap: 1px; }\\n    .seg-bar-piece { height: 100%; transition: width 0.28s ease; min-width: 0; }\\n    .kpi-breakdown { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 6px 12px; font-size: 11px; color: var(--text-muted); }\\n    .legend-dot { width: 7px; height: 7px; border-radius: 1px; display: inline-block; flex-shrink: 0; }\\n    .bd-item { display: inline-flex; align-items: center; gap: 5px; white-space: nowrap; }\\n    .bd-val { font-family: var(--font-mono); font-variant-numeric: tabular-nums; color: var(--text-primary); font-weight: 600; font-size: 11px; }\\n\\n    .eco-pills { display: flex; flex-wrap: wrap; gap: 6px; }\\n    .eco-pill { display: inline-flex; align-items: center; gap: 5px; padding: 2px 7px; border-radius: 2px; background: rgba(104, 157, 106, 0.14); border: 1px solid var(--gb-aqua-dim); color: var(--text-primary); font-size: 10.5px; }\\n    .eco-pill .eco-dot { width: 5px; height: 5px; border-radius: 1px; background: var(--gb-aqua); }\\n\\n    .card-header { padding: 11px 16px; border-bottom: 1px dashed var(--border-card); background: rgba(0, 0, 0, 0.08); display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; }\\n    html[data-theme=\\"light\\"] .card-header { background: rgba(60, 56, 54, 0.03); }\\n    .card-title-group { display: flex; flex-direction: column; gap: 2px; }\\n    .card-title-row { display: flex; align-items: center; gap: 8px; }\\n    .card-title { font-size: 13px; font-weight: 700; color: var(--text-bright); display: inline-flex; align-items: center; gap: 7px; }\\n    .tui-bracket { color: var(--gb-gray); font-weight: 400; }\\n    .card-subtitle { font-size: 11px; color: var(--text-muted); }\\n    .card-controls { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }\\n    .chart-legend { display: flex; align-items: center; gap: 12px; font-size: 11px; color: var(--text-muted); }\\n    .chart-legend-item { display: inline-flex; align-items: center; gap: 5px; }\\n\\n    .chart-body { padding: 16px 16px 14px; position: relative; height: 310px; }\\n    .chart-canvas-wrap { width: 100%; height: 100%; position: relative; }\\n    .chart-empty { position: absolute; inset: 0; display: none; align-items: center; justify-content: center; color: var(--text-subtle); font-family: var(--font-mono); font-size: 12px; }\\n    .chart-empty.is-visible { display: flex; }\\n    .svg-fallback-container { width: 100%; height: 100%; display: none; }\\n    .svg-fallback-container.is-visible { display: block; }\\n\\n    /* 365-Day Heatmap */\\n    .heatmap-body { padding: 16px 16px 12px; overflow-x: auto; }\\n    .heatmap-scroll-inner { min-width: 740px; }\\n    .heatmap-months { position: relative; height: 16px; margin-left: 32px; margin-bottom: 4px; font-size: 10.5px; color: var(--text-muted); }\\n    .heatmap-month-label { position: absolute; top: 0; font-size: 10.5px; color: var(--text-muted); white-space: nowrap; }\\n    .heatmap-grid-row { display: flex; align-items: flex-start; gap: 6px; }\\n    .heatmap-weekdays { width: 26px; display: grid; grid-template-rows: repeat(7, 12px); gap: 3px; font-size: 10px; color: var(--text-subtle); line-height: 12px; flex-shrink: 0; }\\n    .heatmap-weeks { flex: 1; display: grid; grid-template-columns: repeat(53, minmax(11px, 1fr)); gap: 3px; }\\n    .heatmap-week-col { display: grid; grid-template-rows: repeat(7, 12px); gap: 3px; }\\n    .hm-cell { width: 100%; height: 12px; border-radius: 1.5px; border: 1px solid rgba(146, 131, 116, 0.14); transition: transform 0.1s ease, border-color 0.1s ease; cursor: crosshair; }\\n    .hm-cell.hm-pad { background: transparent; border-color: transparent; pointer-events: none; }\\n    .hm-cell.hm-lvl-0 { background: var(--hm-0); }\\n    .hm-cell.hm-lvl-1 { background: var(--hm-1); border-color: rgba(184, 187, 38, 0.25); }\\n    .hm-cell.hm-lvl-2 { background: var(--hm-2); border-color: rgba(184, 187, 38, 0.42); }\\n    .hm-cell.hm-lvl-3 { background: var(--hm-3); border-color: var(--gb-green); }\\n    .hm-cell.hm-lvl-4 { background: var(--hm-4); border-color: var(--gb-yellow); }\\n    .hm-cell:not(.hm-pad):hover { border-color: var(--gb-orange); transform: scale(1.16); z-index: 2; }\\n    .heatmap-footer { padding: 10px 16px 12px; border-top: 1px dashed var(--border-subtle); display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; font-size: 11px; color: var(--text-muted); }\\n    .heatmap-stats { display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }\\n    .heatmap-legend { display: inline-flex; align-items: center; gap: 4px; font-size: 10.5px; color: var(--text-subtle); }\\n    .heatmap-legend .hm-swatch { width: 11px; height: 11px; border-radius: 1.5px; border: 1px solid rgba(146, 131, 116, 0.22); }\\n    .hm-swatch.hm-lvl-0 { background: var(--hm-0); }\\n    .hm-swatch.hm-lvl-1 { background: var(--hm-1); }\\n    .hm-swatch.hm-lvl-2 { background: var(--hm-2); }\\n    .hm-swatch.hm-lvl-3 { background: var(--hm-3); }\\n    .hm-swatch.hm-lvl-4 { background: var(--hm-4); }\\n\\n    .insights-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }\\n    @media (max-width: 960px) { .insights-grid { grid-template-columns: 1fr; } }\\n\\n    .hourly-body { padding: 16px 16px 14px; display: flex; flex-direction: column; justify-content: space-between; gap: 12px; min-height: 245px; }\\n    .hourly-bars { display: grid; grid-template-columns: repeat(24, minmax(0, 1fr)); gap: 4px; height: 162px; align-items: end; padding-top: 8px; border-bottom: 1px solid var(--border-card); }\\n    .hour-col { height: 100%; display: flex; flex-direction: column; justify-content: flex-end; align-items: center; cursor: crosshair; position: relative; padding-bottom: 1px; }\\n    .hour-col:hover .hour-bar { filter: brightness(1.2); }\\n    .hour-bar { width: 100%; min-height: 2px; border-radius: 2px 2px 0 0; background: var(--gb-blue-dim); border-top: 2px solid var(--gb-blue); transition: height 0.25s ease, background 0.15s ease; }\\n    .hour-bar.is-zero { background: var(--bg-control); border-top-color: transparent; }\\n    .hour-bar.is-peak { background: linear-gradient(180deg, var(--gb-orange) 0%, var(--gb-orange-dim) 100%); border-top-color: var(--gb-yellow); }\\n    .hourly-axis { display: grid; grid-template-columns: repeat(24, minmax(0, 1fr)); gap: 4px; font-family: var(--font-mono); font-variant-numeric: tabular-nums; font-size: 10px; color: var(--text-subtle); text-align: center; }\\n    .hourly-footer { display: flex; align-items: center; justify-content: space-between; gap: 8px; font-size: 11px; color: var(--text-muted); }\\n\\n    .tools-body { padding: 12px 16px 16px; max-height: 265px; overflow-y: auto; display: flex; flex-direction: column; gap: 10px; }\\n    .tool-row { display: flex; flex-direction: column; gap: 4px; }\\n    .tool-row-header { display: flex; align-items: center; justify-content: space-between; gap: 10px; }\\n    .tool-name-wrap { display: inline-flex; align-items: center; gap: 7px; min-width: 0; }\\n    .tool-rank { font-family: var(--font-mono); font-variant-numeric: tabular-nums; font-size: 10.5px; color: var(--text-subtle); width: 20px; flex-shrink: 0; }\\n    .tool-glyph { color: var(--gb-aqua); font-size: 12px; width: 14px; }\\n    .tool-row:first-child .tool-glyph { color: var(--gb-orange); }\\n    .tool-name { font-family: var(--font-mono); font-size: 12px; color: var(--text-bright); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }\\n    .tool-metrics { display: inline-flex; align-items: center; gap: 10px; font-family: var(--font-mono); font-variant-numeric: tabular-nums; font-size: 11.5px; flex-shrink: 0; }\\n    .tool-count { color: var(--text-primary); font-weight: 600; }\\n    .tool-share { color: var(--text-muted); min-width: 44px; text-align: right; }\\n    .tool-track { width: 100%; height: 6px; background: var(--bg-subtle); border: 1px solid var(--border-subtle); border-radius: 2px; overflow: hidden; }\\n    .tool-fill { height: 100%; border-radius: 1px; background: var(--gb-aqua-dim); transition: width 0.3s ease; }\\n    .tool-row:first-child .tool-fill { background: var(--gb-orange); }\\n\\n    .search-box { position: relative; display: inline-flex; align-items: center; }\\n    .search-icon { position: absolute; left: 9px; color: var(--gb-yellow); pointer-events: none; display: flex; font-size: 11.5px; }\\n    .search-input { background: var(--bg-subtle); border: 1px solid var(--border-card); border-radius: var(--radius-control); color: var(--text-bright); font-size: 11.5px; height: 29px; width: 270px; padding: 0 50px 0 28px; transition: border-color 0.12s ease, background 0.12s ease; }\\n    @media (max-width: 560px) { .search-input { width: 100%; } }\\n    .search-input::placeholder { color: var(--text-subtle); }\\n    .search-input:focus { outline: none; border-color: var(--gb-orange); background: var(--bg-panel); }\\n    .search-kbd { position: absolute; right: 7px; font-family: var(--font-mono); font-size: 10px; color: var(--gb-yellow); border: 1px solid var(--border-card); background: var(--bg-control); padding: 1px 5px; border-radius: 2px; pointer-events: none; }\\n    .search-clear { position: absolute; right: 7px; display: none; appearance: none; border: 1px solid var(--border-card); background: var(--bg-control); color: var(--text-primary); width: 17px; height: 17px; border-radius: 2px; font-size: 11px; line-height: 1; cursor: pointer; align-items: center; justify-content: center; }\\n    .search-clear.is-visible { display: inline-flex; }\\n\\n    .table-wrap { width: 100%; overflow-x: auto; }\\n    .conv-table { width: 100%; border-collapse: collapse; font-size: 12px; min-width: 940px; }\\n    .conv-table thead th { text-align: left; padding: 9px 14px; font-size: 10.5px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; color: var(--text-muted); border-bottom: 1px solid var(--border-card); background: var(--bg-subtle); user-select: none; white-space: nowrap; }\\n    .conv-table thead th.num-col { text-align: right; }\\n    .conv-table thead th.sortable { cursor: pointer; transition: color 0.12s ease, background 0.12s ease; }\\n    .conv-table thead th.sortable:hover { color: var(--text-bright); background: var(--bg-control); }\\n    .conv-table thead th.is-sorted { color: var(--gb-orange); }\\n    .th-inner { display: inline-flex; align-items: center; gap: 4px; }\\n    .sort-arrow { font-family: var(--font-mono); font-size: 10px; color: var(--gb-orange); opacity: 0; width: 10px; display: inline-block; text-align: center; }\\n    th.sortable:hover .sort-arrow { opacity: 0.55; color: var(--text-muted); }\\n    th.is-sorted .sort-arrow { opacity: 1 !important; color: var(--gb-orange) !important; }\\n    .conv-table tbody tr { border-bottom: 1px solid var(--border-subtle); transition: background 0.1s ease; cursor: pointer; }\\n    .conv-table tbody tr:last-child { border-bottom: none; }\\n    .conv-table tbody tr:hover { background: var(--bg-card-hover); }\\n    .conv-table tbody tr.is-selected { background: var(--bg-active); }\\n    .conv-table td { padding: 9px 14px; vertical-align: middle; color: var(--text-secondary); white-space: nowrap; }\\n    .conv-table td.num-col { text-align: right; font-family: var(--font-mono); font-variant-numeric: tabular-nums; font-size: 11.5px; }\\n    .conv-title-cell { max-width: 320px; display: flex; flex-direction: column; gap: 2px; }\\n    .conv-title-text { color: var(--text-bright); font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }\\n    .conv-id-short { font-family: var(--font-mono); font-variant-numeric: tabular-nums; font-size: 10.5px; color: var(--text-subtle); }\\n    .tok-input { color: var(--gb-blue); }\\n    .tok-output { color: var(--gb-aqua); }\\n    .tok-thinking { color: var(--gb-purple); }\\n    .tok-total { color: var(--text-bright); font-weight: 600; }\\n    .cost-cell { color: var(--gb-yellow); font-weight: 600; }\\n    .table-footer { padding: 10px 16px; border-top: 1px dashed var(--border-card); display: flex; align-items: center; justify-content: space-between; gap: 12px; font-size: 11px; color: var(--text-muted); }\\n    .empty-state { padding: 36px 18px; text-align: center; color: var(--text-subtle); font-size: 12px; }\\n\\n    .floating-tooltip { position: fixed; z-index: 90; pointer-events: none; background: var(--bg-popover); border: 1px solid var(--border-strong); border-radius: 3px; padding: 8px 11px; box-shadow: 0 8px 24px rgba(0, 0, 0, 0.55); font-size: 11px; color: var(--text-secondary); max-width: 310px; opacity: 0; transform: translateY(3px); transition: opacity 0.1s ease, transform 0.1s ease; }\\n    .floating-tooltip.is-visible { opacity: 1; transform: translateY(0); }\\n    .tt-title { font-weight: 700; color: var(--gb-yellow); margin-bottom: 4px; font-size: 11.5px; border-bottom: 1px dashed var(--border-card); padding-bottom: 3px; }\\n    .tt-row { display: flex; align-items: center; justify-content: space-between; gap: 14px; line-height: 1.55; }\\n    .tt-label { color: var(--text-muted); display: inline-flex; align-items: center; gap: 5px; }\\n    .tt-val { font-family: var(--font-mono); font-variant-numeric: tabular-nums; color: var(--text-bright); font-weight: 600; }\\n\\n    .drawer-backdrop { position: fixed; inset: 0; z-index: 60; background: rgba(29, 32, 33, 0.76); backdrop-filter: blur(2px); -webkit-backdrop-filter: blur(2px); opacity: 0; pointer-events: none; transition: opacity 0.18s ease; }\\n    .drawer-backdrop.is-open { opacity: 1; pointer-events: auto; }\\n    .drawer-panel { position: fixed; top: 0; right: 0; bottom: 0; z-index: 70; width: min(530px, 100vw); background: var(--bg-panel); border-left: 2px solid var(--border-strong); box-shadow: none; display: flex; flex-direction: column; transform: translateX(100%); visibility: hidden; pointer-events: none; transition: transform 0.22s cubic-bezier(0.16, 1, 0.3, 1), visibility 0.22s; }\\n    .drawer-panel.is-open { transform: translateX(0); visibility: visible; pointer-events: auto; box-shadow: -16px 0 48px rgba(0, 0, 0, 0.65); }\\n    .drawer-header { padding: 15px 18px; border-bottom: 1px dashed var(--border-card); background: var(--bg-subtle); display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; }\\n    .drawer-title { font-size: 14px; font-weight: 700; color: var(--text-bright); line-height: 1.35; word-break: break-word; }\\n    .drawer-header-actions { display: inline-flex; align-items: center; gap: 6px; flex-shrink: 0; }\\n    .icon-btn { appearance: none; width: 28px; height: 28px; border-radius: var(--radius-control); background: var(--bg-control); border: 1px solid var(--border-card); color: var(--text-primary); display: inline-flex; align-items: center; justify-content: center; cursor: pointer; transition: background 0.12s ease, color 0.12s ease, border-color 0.12s ease; }\\n    .icon-btn:hover:not(:disabled) { background: var(--bg-control-hover); color: var(--gb-orange); border-color: var(--gb-orange); }\\n    .icon-btn:disabled { opacity: 0.35; cursor: not-allowed; }\\n    .drawer-content { padding: 16px 18px 28px; overflow-y: auto; display: flex; flex-direction: column; gap: 16px; flex: 1; }\\n    .id-box { display: flex; align-items: center; justify-content: space-between; gap: 10px; padding: 8px 11px; border-radius: var(--radius-control); background: var(--bg-subtle); border: 1px solid var(--border-card); }\\n    .id-box-code { font-family: var(--font-mono); font-variant-numeric: tabular-nums; font-size: 11px; color: var(--text-secondary); word-break: break-all; }\\n    .copy-btn { appearance: none; display: inline-flex; align-items: center; gap: 5px; padding: 4px 9px; border-radius: var(--radius-sm); background: var(--bg-active); border: 1px solid var(--gb-orange); color: var(--gb-orange); font-size: 11px; font-weight: 600; cursor: pointer; flex-shrink: 0; transition: background 0.12s ease, border-color 0.12s ease; }\\n    .copy-btn:hover { background: rgba(254, 128, 25, 0.30); }\\n    .copy-btn.is-copied { background: rgba(184, 187, 38, 0.18); border-color: var(--gb-green); color: var(--gb-green); }\\n    .drawer-section { display: flex; flex-direction: column; gap: 8px; }\\n    .drawer-section-title { font-size: 10.5px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; color: var(--gb-yellow); }\\n    .detail-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }\\n    .detail-tile { padding: 10px 12px; border-radius: var(--radius-control); background: var(--bg-subtle); border: 1px solid var(--border-subtle); display: flex; flex-direction: column; gap: 3px; }\\n    .detail-tile-label { font-size: 10.5px; color: var(--text-muted); }\\n    .detail-tile-val { font-family: var(--font-mono); font-variant-numeric: tabular-nums; font-size: 13.5px; font-weight: 700; color: var(--text-bright); }\\n    .detail-tile-sub { font-family: var(--font-mono); font-variant-numeric: tabular-nums; font-size: 10.5px; color: var(--text-subtle); }\\n    .comp-list { border: 1px solid var(--border-card); border-radius: var(--radius-control); overflow: hidden; }\\n    .comp-row { display: flex; align-items: center; justify-content: space-between; padding: 8px 12px; border-bottom: 1px solid var(--border-subtle); font-size: 11.5px; }\\n    .comp-row:last-child { border-bottom: none; }\\n    .comp-row.comp-total { background: var(--bg-subtle); font-weight: 700; color: var(--text-bright); }\\n\\n    .toast { position: fixed; bottom: 20px; right: 20px; z-index: 95; background: var(--bg-elevated); border: 1px solid var(--gb-orange); border-radius: 3px; padding: 8px 13px; font-size: 11.5px; color: var(--text-bright); box-shadow: 0 10px 28px rgba(0, 0, 0, 0.55); display: flex; align-items: center; gap: 8px; opacity: 0; transform: translateY(8px); pointer-events: none; transition: opacity 0.18s ease, transform 0.18s ease; }\\n    .toast.is-visible { opacity: 1; transform: translateY(0); }\\n  </style>\\n</head>\\n<body>\\n  <!-- Sticky Top Bar (Gruvbox Lualine / Tmux Status Ribbon) -->\\n  <header class=\\"topbar\\">\\n    <div class=\\"topbar-inner\\">\\n      <div class=\\"brand-cluster\\">\\n        <div class=\\"pl-ribbon\\" aria-hidden=\\"true\\">\\n          <span class=\\"pl-seg-mode\\"><span class=\\"nf-icon\\">\\uf489</span><span>AGY</span></span><span class=\\"pl-sep pl-sep-mode-to-title\\">\\ue0b0</span><span class=\\"pl-seg-title\\"><span>telemetry.nvim</span></span><span class=\\"pl-sep pl-sep-title-end\\">\\ue0b0</span>\\n        </div>\\n        <div class=\\"brand-titles\\">\\n          <div class=\\"brand-row\\">\\n            <span id=\\"demoBadge\\" class=\\"demo-badge\\" role=\\"status\\">\\n              <span class=\\"demo-badge-dot\\"></span>\\n              <span>DEMO PREVIEW</span>\\n            </span>\\n          </div>\\n          <div class=\\"brand-subtitle-wrap\\">\\n            <span class=\\"nf-icon\\" style=\\"color:var(--gb-yellow);font-size:11px;\\">\\uf07b</span>\\n            <div id=\\"agyPathSubtitle\\" class=\\"brand-subtitle\\">Initializing telemetry workspace\\u2026</div>\\n          </div>\\n        </div>\\n      </div>\\n\\n      <div class=\\"controls-cluster\\">\\n        <!-- Segmented Date Filter -->\\n        <div class=\\"segmented\\" id=\\"dateFilterGroup\\" role=\\"group\\" aria-label=\\"Date range filter\\">\\n          <button type=\\"button\\" class=\\"seg-btn mono-label\\" data-days=\\"7\\">7D</button>\\n          <button type=\\"button\\" class=\\"seg-btn mono-label is-active\\" data-days=\\"30\\">30D</button>\\n          <button type=\\"button\\" class=\\"seg-btn mono-label\\" data-days=\\"90\\">90D</button>\\n          <button type=\\"button\\" class=\\"seg-btn mono-label\\" data-days=\\"365\\">365D</button>\\n          <button type=\\"button\\" class=\\"seg-btn mono-label\\" data-days=\\"all\\">ALL</button>\\n        </div>\\n\\n        <!-- Billing Mode Toggle -->\\n        <div class=\\"control-with-help\\">\\n          <div class=\\"segmented\\" id=\\"billingModeGroup\\" role=\\"group\\" aria-label=\\"Token billing mode\\">\\n            <button type=\\"button\\" class=\\"seg-btn is-active\\" data-billing=\\"context\\">Context</button>\\n            <button type=\\"button\\" class=\\"seg-btn\\" data-billing=\\"turn\\">Turn</button>\\n          </div>\\n          <button type=\\"button\\" id=\\"billingHelpBtn\\" class=\\"help-trigger\\" aria-label=\\"Explain billing modes\\">?</button>\\n        </div>\\n\\n                <!-- Model Pricing Simulator Selector -->\\n        <div class=\\"control-with-help\\">\\n          <div class=\\"select-wrap\\">\\n            <select id=\\"pricingSelect\\" class=\\"linear-select\\" aria-label=\\"Theoretical pricing profile\\">\\n              <option value=\\"\\">Loading pricing\\u2026</option>\\n            </select>\\n            <span class=\\"select-chevron\\"><span class=\\"nf-icon\\"></span></span>\\n          </div>\\n          <button type=\\"button\\" id=\\"pricingHelpBtn\\" class=\\"help-trigger\\" aria-label=\\"Explain pricing simulator profiles\\">?</button>\\n        </div>\\n\\n        <!-- Auto-refresh 10s toggle -->\\n        <button type=\\"button\\" id=\\"autoRefreshBtn\\" class=\\"btn-ghost\\" title=\\"Poll /api/stats every 10 seconds\\">\\n          <span class=\\"pulse-dot\\"></span>\\n          <span>Live</span>\\n          <span class=\\"mono\\" style=\\"font-size:10.5px;color:var(--text-muted);\\">10s</span>\\n        </button>\\n\\n        <!-- Rescan Button -->\\n        <button type=\\"button\\" id=\\"rescanBtn\\" class=\\"btn-ghost\\" title=\\"Trigger workspace telemetry rescan\\">\\n          <span class=\\"nf-icon\\" style=\\"color:var(--gb-aqua);\\">\\uf021</span>\\n          <span>Rescan</span>\\n        </button>\\n\\n        <!-- Export CSV Link -->\\n        <a id=\\"exportCsvLink\\" href=\\"/api/export.csv?days=30&billing_mode=context\\" class=\\"btn-ghost\\" download title=\\"Export filtered telemetry as CSV\\">\\n          <span class=\\"nf-icon\\" style=\\"color:var(--gb-yellow);\\">\\uf019</span>\\n          <span>CSV</span>\\n        </a>\\n\\n        <!-- Gruvbox Dark / Light Theme Toggle -->\\n        <button type=\\"button\\" id=\\"themeToggleBtn\\" class=\\"btn-ghost\\" title=\\"Toggle Gruvbox Dark / Light palette\\" aria-label=\\"Toggle Gruvbox Dark or Light theme\\">\\n          <span id=\\"themeToggleGlyph\\" class=\\"nf-icon\\" style=\\"color:var(--gb-orange);\\">\\uf186</span>\\n          <span id=\\"themeToggleLabel\\" class=\\"mono\\" style=\\"font-size:11px;\\">dark</span>\\n        </button>\\n\\n        <!-- Glossar / Reference Modal Button -->\\n        <button type=\\"button\\" id=\\"glossaryBtn\\" class=\\"btn-ghost\\" title=\\"Open Metrics &amp; Buzzwords Glossary\\" aria-label=\\"Open Metrics &amp; Buzzwords Glossary\\" style=\\"border-color:rgba(211,134,155,0.4);background:rgba(211,134,155,0.08);\\">\\n          <span class=\\"nf-icon\\" style=\\"color:var(--gb-purple);font-size:12px;\\">\\uf02d</span>\\n          <span style=\\"font-weight:600;color:var(--gb-purple);\\">Glossar</span>\\n        </button>\\n      </div>\\n    </div>\\n  </header>\\n\\n  <!-- Demo Preview Info Banner -->\\n  <div id=\\"demoBanner\\" class=\\"banner-strip banner-demo\\">\\n    <div class=\\"banner-inner\\">\\n      <div class=\\"banner-msg\\">\\n        <span class=\\"nf-icon\\">\\uf071</span>\\n        <span class=\\"mono\\" style=\\"font-weight:700;\\">[DEMO]</span>\\n        <span id=\\"demoBannerText\\">Synthetic telemetry preview is active.</span>\\n      </div>\\n      <div class=\\"banner-actions\\">\\n        <button type=\\"button\\" id=\\"dismissDemoBanner\\" class=\\"banner-btn\\">:q dismiss</button>\\n      </div>\\n    </div>\\n  </div>\\n\\n  <!-- API Error Banner -->\\n  <div id=\\"errorBanner\\" class=\\"banner-strip banner-error\\">\\n    <div class=\\"banner-inner\\">\\n      <div class=\\"banner-msg\\">\\n        <span class=\\"nf-icon\\">\\uf06a</span>\\n        <span class=\\"mono\\" style=\\"font-weight:700;\\">[OFFLINE]</span>\\n        <span id=\\"errorBannerText\\">Unable to reach /api/stats.</span>\\n      </div>\\n      <div class=\\"banner-actions\\">\\n        <button type=\\"button\\" id=\\"retryApiBtn\\" class=\\"banner-btn\\">retry</button>\\n        <button type=\\"button\\" id=\\"loadFallbackDemoBtn\\" class=\\"banner-btn\\">preview demo</button>\\n      </div>\\n    </div>\\n  </div>\\n\\n  <!-- Main Telemetry Surface -->\\n  <main class=\\"main-container\\">\\n    <!-- Neovim / Tmux Metadata Statusline -->\\n    <div class=\\"status-strip\\">\\n      <div class=\\"status-meta-group\\">\\n        <span class=\\"status-mode-pill\\">STATUS</span>\\n        <span class=\\"status-item\\"><span class=\\"nf-icon\\" style=\\"color:var(--gb-blue);\\">\\uf1c0</span> db: <span id=\\"metaDbPath\\" class=\\"mono\\" style=\\"color:var(--text-secondary);\\">\\u2014</span></span>\\n        <span class=\\"status-sep\\">\\ue0b1</span>\\n        <span class=\\"status-item\\"><span class=\\"nf-icon\\" style=\\"color:var(--gb-aqua);\\">\\uf017</span> scanned: <span id=\\"metaLastScanned\\" class=\\"mono\\" style=\\"color:var(--text-secondary);\\">\\u2014</span></span>\\n        <span class=\\"status-sep\\">\\ue0b1</span>\\n        <span class=\\"status-item\\"><span class=\\"nf-icon\\" style=\\"color:var(--gb-purple);\\">\\uf080</span> renderer: <span id=\\"metaChartEngine\\" class=\\"mono\\" style=\\"color:var(--text-secondary);\\">\\u2014</span></span>\\n      </div>\\n      <div class=\\"status-meta-group\\">\\n        <span class=\\"status-item\\"><span class=\\"nf-icon\\" style=\\"color:var(--gb-orange);\\">\\uf155</span> mode: <span id=\\"metaBillingBadge\\" class=\\"mono\\" style=\\"color:var(--gb-yellow);font-weight:600;\\">context</span></span>\\n      </div>\\n    </div>\\n\\n    <!-- Section 2: 6 KPI Summary Cards -->\\n    <section class=\\"kpi-grid\\" aria-label=\\"Key performance indicators\\">\\n      <!-- Card 1: Total Tokens -->\\n      <article class=\\"surface-card interactive-card kpi-card\\" data-accent=\\"blue\\">\\n        <div class=\\"kpi-top\\">\\n          <span class=\\"kpi-label\\"><span class=\\"nf-icon kpi-glyph\\" style=\\"color:var(--gb-blue);\\">\\uf2db</span>Total Tokens</span>\\n          <span id=\\"kpiTokenModeTag\\" class=\\"kpi-tag\\">Context</span>\\n        </div>\\n        <div class=\\"kpi-value-row\\">\\n          <span id=\\"kpiTotalTokens\\" class=\\"kpi-value\\">\\u2014</span>\\n          <span id=\\"kpiTotalTokensExact\\" class=\\"kpi-value-sub\\"></span>\\n        </div>\\n        <div style=\\"display:flex;flex-direction:column;gap:7px;\\">\\n          <div class=\\"seg-bar\\" aria-label=\\"Token composition bar\\">\\n            <div id=\\"barInputShare\\" class=\\"seg-bar-piece\\" style=\\"width:0%;background:var(--gb-blue);\\"></div>\\n            <div id=\\"barOutputShare\\" class=\\"seg-bar-piece\\" style=\\"width:0%;background:var(--gb-aqua);\\"></div>\\n            <div id=\\"barThinkingShare\\" class=\\"seg-bar-piece\\" style=\\"width:0%;background:var(--gb-purple);\\"></div>\\n          </div>\\n          <div class=\\"kpi-breakdown\\">\\n            <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--gb-blue);\\"></span>In <span id=\\"kpiInputTokens\\" class=\\"bd-val\\">\\u2014</span></span>\\n            <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--gb-aqua);\\"></span>Out <span id=\\"kpiOutputTokens\\" class=\\"bd-val\\">\\u2014</span></span>\\n            <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--gb-purple);\\"></span>Think <span id=\\"kpiThinkingTokens\\" class=\\"bd-val\\">\\u2014</span></span>\\n          </div>\\n        </div>\\n      </article>\\n\\n      <!-- Card 2: API Requests -->\\n      <article class=\\"surface-card interactive-card kpi-card\\" data-accent=\\"orange\\">\\n        <div class=\\"kpi-top\\">\\n          <span class=\\"kpi-label\\"><span class=\\"nf-icon kpi-glyph\\" style=\\"color:var(--gb-orange);\\">\\uf0e7</span>API Requests</span>\\n          <span id=\\"kpiToolCallsTag\\" class=\\"kpi-tag\\">\\u2014 tools</span>\\n        </div>\\n        <div class=\\"kpi-value-row\\">\\n          <span id=\\"kpiApiRequests\\" class=\\"kpi-value\\">\\u2014</span>\\n          <span id=\\"kpiAvgTokPerReq\\" class=\\"kpi-value-sub\\">\\u2014 tok/req</span>\\n        </div>\\n        <div class=\\"kpi-breakdown\\">\\n          <span class=\\"bd-item\\"><span class=\\"nf-icon\\" style=\\"color:var(--gb-aqua);font-size:11px;\\">\\uf086</span> Sessions <span id=\\"kpiConversations\\" class=\\"bd-val\\">\\u2014</span></span>\\n          <span class=\\"bd-item\\"><span class=\\"nf-icon\\" style=\\"color:var(--gb-purple);font-size:11px;\\">\\uf0e8</span> Subagents <span id=\\"kpiSubagents\\" class=\\"bd-val\\">\\u2014</span></span>\\n        </div>\\n      </article>\\n\\n      <!-- Card 3: Est. API Value / Cost -->\\n      <article class=\\"surface-card interactive-card kpi-card\\" data-accent=\\"yellow\\">\\n        <div class=\\"kpi-top\\">\\n          <span class=\\"kpi-label\\"><span class=\\"nf-icon kpi-glyph\\" style=\\"color:var(--gb-yellow);\\">\\uf155</span>Est. API Value / Cost</span>\\n          <span id=\\"kpiPricingTag\\" class=\\"kpi-tag tag-yellow\\">\\u2014</span>\\n        </div>\\n        <div class=\\"kpi-value-row\\">\\n          <span id=\\"kpiCostUsd\\" class=\\"kpi-value\\" style=\\"color:var(--gb-yellow);\\">\\u2014</span>\\n          <span id=\\"kpiCostPerReq\\" class=\\"kpi-value-sub\\"></span>\\n        </div>\\n        <div style=\\"display:flex;flex-direction:column;gap:7px;\\">\\n          <div class=\\"seg-bar\\">\\n            <div id=\\"barCostInput\\" class=\\"seg-bar-piece\\" style=\\"width:50%;background:var(--gb-blue);\\"></div>\\n            <div id=\\"barCostOutput\\" class=\\"seg-bar-piece\\" style=\\"width:50%;background:var(--gb-orange);\\"></div>\\n          </div>\\n          <div class=\\"kpi-breakdown\\">\\n            <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--gb-blue);\\"></span>Input <span id=\\"kpiInputCost\\" class=\\"bd-val\\">\\u2014</span></span>\\n            <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--gb-orange);\\"></span>Out+Think <span id=\\"kpiOutThinkCost\\" class=\\"bd-val\\">\\u2014</span></span>\\n          </div>\\n        </div>\\n      </article>\\n\\n      <!-- Card 4: Electricity Used -->\\n      <article id=\\"kpiCardElectricity\\" class=\\"surface-card interactive-card kpi-card\\" data-accent=\\"aqua\\" style=\\"cursor:pointer;\\" title=\\"Click or hover to inspect real-world electricity comparisons\\">\\n        <div class=\\"kpi-top\\">\\n          <span class=\\"kpi-label\\"><span class=\\"nf-icon kpi-glyph\\" style=\\"color:var(--gb-aqua);\\">\\uf1e6</span>Electricity Used</span>\\n          <div style=\\"display:inline-flex;align-items:center;gap:6px;\\">\\n            <span id=\\"kpiEnergyAlt\\" class=\\"kpi-tag tag-emerald\\">\\u2014</span>\\n          </div>\\n        </div>\\n        <div class=\\"kpi-value-row\\">\\n          <span id=\\"kpiElectricity\\" class=\\"kpi-value\\">\\u2014</span>\\n        </div>\\n        <div class=\\"eco-pills\\">\\n          <span class=\\"eco-pill\\"><span class=\\"eco-dot\\"></span><span id=\\"kpiPhoneCharges\\" class=\\"mono\\">~0.0 phone charges</span></span>\\n          <span class=\\"eco-pill\\"><span class=\\"eco-dot\\"></span><span id=\\"kpiLedHours\\" class=\\"mono\\">~0.0 hrs 10W LED</span></span>\\n          <span class=\\"eco-pill\\" style=\\"border-color:rgba(142,192,124,0.3);background:rgba(142,192,124,0.08);\\" title=\\"Scope 1 direct + Scope 2 indirect datacenter cooling water (Li et al., 2023)\\"><span class=\\"eco-dot\\" style=\\"background:var(--gb-aqua);\\"></span><span id=\\"kpiWaterPill\\" class=\\"mono\\" style=\\"color:var(--gb-aqua);font-weight:600;\\">~0.0 ml water</span></span>\\n        </div>\\n      </article>\\n\\n      <!-- Card 5: Carbon Footprint -->\\n      <article id=\\"kpiCardCarbon\\" class=\\"surface-card interactive-card kpi-card\\" data-accent=\\"green\\" style=\\"cursor:pointer;\\" title=\\"Click or hover to inspect real-world carbon emission comparisons\\">\\n        <div class=\\"kpi-top\\">\\n          <span class=\\"kpi-label\\"><span class=\\"nf-icon kpi-glyph\\" style=\\"color:var(--gb-green);\\">\\uf06c</span>Carbon Footprint</span>\\n          <div style=\\"display:inline-flex;align-items:center;gap:6px;\\">\\n            <span class=\\"kpi-tag tag-emerald\\">@ 385g/kWh grid avg</span>\\n          </div>\\n        </div>\\n        <div class=\\"kpi-value-row\\">\\n          <span id=\\"kpiCarbon\\" class=\\"kpi-value\\">\\u2014</span>\\n          <span id=\\"kpiCarbonSub\\" class=\\"kpi-value-sub\\"></span>\\n        </div>\\n        <div class=\\"kpi-breakdown\\">\\n          <span class=\\"bd-item\\">Per request <span id=\\"kpiCo2PerReq\\" class=\\"bd-val\\">\\u2014</span></span>\\n          <span class=\\"bd-item\\">Per 1M tok <span id=\\"kpiCo2PerM\\" class=\\"bd-val\\">\\u2014</span></span>\\n        </div>\\n      </article>\\n\\n      <!-- Card 6: Reasoning & Peak Velocity -->\\n      <article class=\\"surface-card interactive-card kpi-card\\" data-accent=\\"purple\\">\\n        <div class=\\"kpi-top\\">\\n          <span class=\\"kpi-label\\"><span class=\\"nf-icon kpi-glyph\\" style=\\"color:var(--gb-purple);\\">\\uf06d</span>Reasoning &amp; Peak Velocity</span>\\n          <span id=\\"kpiActiveDaysTag\\" class=\\"kpi-tag tag-violet\\">\\u2014 active days</span>\\n        </div>\\n        <div class=\\"kpi-value-row\\">\\n          <span id=\\"kpiThinkingShare\\" class=\\"kpi-value\\">\\u2014</span>\\n          <span class=\\"kpi-value-sub\\">thinking share</span>\\n        </div>\\n        <div style=\\"display:flex;flex-direction:column;gap:7px;\\">\\n          <div class=\\"seg-bar\\">\\n            <div id=\\"barReasoningGauge\\" class=\\"seg-bar-piece\\" style=\\"width:0%;background:var(--gb-purple);\\"></div>\\n          </div>\\n          <div class=\\"kpi-breakdown\\">\\n            <span class=\\"bd-item\\">Peak day <span id=\\"kpiPeakDate\\" class=\\"bd-val\\">\\u2014</span></span>\\n            <span class=\\"bd-item\\"><span id=\\"kpiPeakTokens\\" class=\\"bd-val\\">\\u2014</span></span>\\n          </div>\\n        </div>\\n      </article>\\n    </section>\\n\\n    <!-- Section 3: Main Daily Chart Card -->\\n    <section class=\\"surface-card\\" aria-label=\\"Daily telemetry chart\\">\\n      <div class=\\"card-header\\">\\n        <div class=\\"card-title-group\\">\\n          <div class=\\"card-title-row\\">\\n            <h2 class=\\"card-title\\"><span class=\\"tui-bracket\\">[</span><span class=\\"nf-icon\\" style=\\"color:var(--gb-orange);\\">\\uf201</span><span>Daily Telemetry</span><span class=\\"tui-bracket\\">]</span></h2>\\n            <span id=\\"dailyRangeBadge\\" class=\\"kpi-tag\\">30D</span>\\n          </div>\\n          <span id=\\"dailySubtitle\\" class=\\"card-subtitle\\">Stacked token composition by UTC day</span>\\n        </div>\\n        <div class=\\"card-controls\\">\\n          <div id=\\"dailyChartLegend\\" class=\\"chart-legend\\"></div>\\n          <div class=\\"segmented\\" id=\\"chartTabGroup\\" role=\\"tablist\\" aria-label=\\"Daily chart metric\\">\\n            <button type=\\"button\\" class=\\"seg-btn is-active\\" role=\\"tab\\" aria-selected=\\"true\\" data-chart-tab=\\"tokens\\">Token Usage</button>\\n            <button type=\\"button\\" class=\\"seg-btn\\" role=\\"tab\\" aria-selected=\\"false\\" data-chart-tab=\\"cost\\">Est. Value ($)</button>\\n            <button type=\\"button\\" class=\\"seg-btn\\" role=\\"tab\\" aria-selected=\\"false\\" data-chart-tab=\\"requests\\">API Requests &amp; Tools</button>\\n          </div>\\n        </div>\\n      </div>\\n      <div class=\\"chart-body\\">\\n        <div class=\\"chart-canvas-wrap\\">\\n          <canvas id=\\"dailyChartCanvas\\"></canvas>\\n          <div id=\\"dailySvgFallback\\" class=\\"svg-fallback-container\\"></div>\\n          <div id=\\"dailyChartEmpty\\" class=\\"chart-empty\\">No daily telemetry recorded in this window</div>\\n        </div>\\n      </div>\\n    </section>\\n\\n    <!-- Section 4: 365-Day Activity Heatmap Card -->\\n    <section class=\\"surface-card\\" aria-label=\\"365-day activity heatmap\\">\\n      <div class=\\"card-header\\">\\n        <div class=\\"card-title-group\\">\\n          <div class=\\"card-title-row\\">\\n            <h2 class=\\"card-title\\"><span class=\\"tui-bracket\\">[</span><span class=\\"nf-icon\\" style=\\"color:var(--gb-green);\\">\\uf073</span><span>365-Day Activity Matrix</span><span class=\\"tui-bracket\\">]</span></h2>\\n            <span id=\\"heatmapYearSpan\\" class=\\"kpi-tag\\">53 weeks</span>\\n          </div>\\n          <span class=\\"card-subtitle\\">Daily token throughput intensity over the trailing year</span>\\n        </div>\\n      </div>\\n      <div class=\\"heatmap-body\\">\\n        <div class=\\"heatmap-scroll-inner\\">\\n          <div id=\\"heatmapMonths\\" class=\\"heatmap-months\\"></div>\\n          <div class=\\"heatmap-grid-row\\">\\n            <div class=\\"heatmap-weekdays\\" aria-hidden=\\"true\\">\\n              <span></span>\\n              <span>Mon</span>\\n              <span></span>\\n              <span>Wed</span>\\n              <span></span>\\n              <span>Fri</span>\\n              <span></span>\\n            </div>\\n            <div id=\\"heatmapWeeks\\" class=\\"heatmap-weeks\\" role=\\"grid\\" aria-label=\\"365 day token activity grid\\"></div>\\n          </div>\\n        </div>\\n      </div>\\n      <div class=\\"heatmap-footer\\">\\n        <div class=\\"heatmap-stats\\">\\n          <span><strong id=\\"hmActiveDays\\" class=\\"mono\\" style=\\"color:var(--gb-green);font-weight:700;\\">0</strong> active days</span>\\n          <span>365d Volume: <span id=\\"hmYearTokens\\" class=\\"mono\\" style=\\"color:var(--text-primary);\\">0</span></span>\\n          <span>Longest Streak: <span id=\\"hmLongestStreak\\" class=\\"mono\\" style=\\"color:var(--gb-orange);font-weight:600;\\">0d</span></span>\\n        </div>\\n        <div class=\\"heatmap-legend\\" aria-label=\\"Intensity legend\\">\\n          <span>Less</span>\\n          <span class=\\"hm-swatch hm-lvl-0\\"></span>\\n          <span class=\\"hm-swatch hm-lvl-1\\"></span>\\n          <span class=\\"hm-swatch hm-lvl-2\\"></span>\\n          <span class=\\"hm-swatch hm-lvl-3\\"></span>\\n          <span class=\\"hm-swatch hm-lvl-4\\"></span>\\n          <span>More</span>\\n        </div>\\n      </div>\\n    </section>\\n\\n    <!-- Section 5: Secondary Insights Row (Hourly + Tools) -->\\n    <section class=\\"insights-grid\\" aria-label=\\"Hourly distribution and tool breakdown\\">\\n      <!-- Left: Hour-of-Day Activity Distribution -->\\n      <article class=\\"surface-card\\">\\n        <div class=\\"card-header\\">\\n          <div class=\\"card-title-group\\">\\n            <div class=\\"card-title-row\\">\\n              <h2 class=\\"card-title\\"><span class=\\"tui-bracket\\">[</span><span class=\\"nf-icon\\" style=\\"color:var(--gb-blue);\\">\\uf017</span><span>Hour-of-Day Activity</span><span class=\\"tui-bracket\\">]</span></h2>\\n              <span id=\\"hourlyPeakTag\\" class=\\"kpi-tag\\">Peak \\u2014</span>\\n            </div>\\n            <span id=\\"hourlyTzSubtitle\\" class=\\"card-subtitle\\">24-hour request &amp; token cadence</span>\\n          </div>\\n          <div class=\\"segmented\\" id=\\"hourlyMetricGroup\\" role=\\"group\\" aria-label=\\"Hourly metric\\">\\n            <button type=\\"button\\" class=\\"seg-btn is-active\\" data-hourly-mode=\\"tokens\\">Tokens</button>\\n            <button type=\\"button\\" class=\\"seg-btn\\" data-hourly-mode=\\"requests\\">Requests</button>\\n          </div>\\n        </div>\\n        <div class=\\"hourly-body\\">\\n          <div>\\n            <div id=\\"hourlyBars\\" class=\\"hourly-bars\\"></div>\\n            <div id=\\"hourlyAxis\\" class=\\"hourly-axis\\" style=\\"margin-top:6px;\\"></div>\\n          </div>\\n          <div class=\\"hourly-footer\\">\\n            <span id=\\"hourlyTzNote\\">Hours in recorded session clock (00\\u201323)</span>\\n            <span id=\\"hourlyPeakShare\\" class=\\"mono\\" style=\\"color:var(--gb-orange);font-weight:600;\\">\\u2014</span>\\n          </div>\\n        </div>\\n      </article>\\n\\n      <!-- Right: Tool Invocations Breakdown -->\\n      <article class=\\"surface-card\\">\\n        <div class=\\"card-header\\">\\n          <div class=\\"card-title-group\\">\\n            <div class=\\"card-title-row\\">\\n              <h2 class=\\"card-title\\"><span class=\\"tui-bracket\\">[</span><span class=\\"nf-icon\\" style=\\"color:var(--gb-aqua);\\">\\uf0ad</span><span>Tool Invocations</span><span class=\\"tui-bracket\\">]</span></h2>\\n              <span id=\\"toolsTotalBadge\\" class=\\"kpi-tag\\">0 calls</span>\\n            </div>\\n            <span class=\\"card-subtitle\\">Share of executed agent tool calls in window</span>\\n          </div>\\n        </div>\\n        <div id=\\"toolsListBody\\" class=\\"tools-body\\">\\n          <div class=\\"empty-state\\">No tool invocations recorded</div>\\n        </div>\\n      </article>\\n    </section>\\n\\n    <!-- Section 6: Recent Conversations Table -->\\n    <section class=\\"surface-card\\" aria-label=\\"Recent conversations\\">\\n      <div class=\\"card-header\\">\\n        <div class=\\"card-title-group\\">\\n          <div class=\\"card-title-row\\">\\n            <h2 class=\\"card-title\\"><span class=\\"tui-bracket\\">[</span><span class=\\"nf-icon\\" style=\\"color:var(--gb-yellow);\\">\\uf086</span><span>Recent Conversations</span><span class=\\"tui-bracket\\">]</span></h2>\\n            <span id=\\"convCountBadge\\" class=\\"kpi-tag\\">0</span>\\n          </div>\\n          <span class=\\"card-subtitle\\">Click any session row to inspect token composition, footprint &amp; metadata</span>\\n        </div>\\n        <div class=\\"card-controls\\">\\n          <div class=\\"search-box\\">\\n            <span class=\\"search-icon\\"><span class=\\"nf-icon\\">\\uf002</span></span>\\n            <input type=\\"text\\" id=\\"convSearchInput\\" class=\\"search-input\\" placeholder=\\"/filter title or session id\\u2026\\" autocomplete=\\"off\\" spellcheck=\\"false\\" />\\n            <span id=\\"searchKbd\\" class=\\"search-kbd\\">/</span>\\n            <button type=\\"button\\" id=\\"searchClearBtn\\" class=\\"search-clear\\" aria-label=\\"Clear search\\">&times;</button>\\n          </div>\\n        </div>\\n      </div>\\n\\n      <div class=\\"table-wrap\\">\\n        <table class=\\"conv-table\\">\\n          <thead>\\n            <tr>\\n              <th class=\\"sortable\\" data-sort=\\"title\\"><span class=\\"th-inner\\">Conversation <span class=\\"sort-arrow\\">\\u2193</span></span></th>\\n              <th class=\\"sortable is-sorted\\" data-sort=\\"updated_at\\"><span class=\\"th-inner\\">Updated <span class=\\"sort-arrow\\">\\u2193</span></span></th>\\n              <th class=\\"sortable num-col\\" data-sort=\\"api_requests\\"><span class=\\"th-inner\\"><span class=\\"sort-arrow\\">\\u2193</span> Requests</span></th>\\n              <th class=\\"sortable num-col\\" data-sort=\\"input_tokens\\"><span class=\\"th-inner\\"><span class=\\"sort-arrow\\">\\u2193</span> Input</span></th>\\n              <th class=\\"sortable num-col\\" data-sort=\\"output_tokens\\"><span class=\\"th-inner\\"><span class=\\"sort-arrow\\">\\u2193</span> Output</span></th>\\n              <th class=\\"sortable num-col\\" data-sort=\\"thinking_tokens\\"><span class=\\"th-inner\\"><span class=\\"sort-arrow\\">\\u2193</span> Thinking</span></th>\\n              <th class=\\"sortable num-col\\" data-sort=\\"total_tokens\\"><span class=\\"th-inner\\"><span class=\\"sort-arrow\\">\\u2193</span> Total Tokens</span></th>\\n              <th class=\\"sortable num-col\\" data-sort=\\"cost_usd\\"><span class=\\"th-inner\\"><span class=\\"sort-arrow\\">\\u2193</span> Est. Cost</span></th>\\n              <th class=\\"sortable num-col\\" data-sort=\\"electricity_wh\\"><span class=\\"th-inner\\"><span class=\\"sort-arrow\\">\\u2193</span> Energy</span></th>\\n            </tr>\\n          </thead>\\n          <tbody id=\\"convTableBody\\">\\n            <tr><td colspan=\\"9\\" class=\\"empty-state\\">Loading conversations\\u2026</td></tr>\\n          </tbody>\\n        </table>\\n      </div>\\n\\n      <div class=\\"table-footer\\">\\n        <span id=\\"convFooterSummary\\">Showing 0 conversations</span>\\n        <div style=\\"display:flex;align-items:center;gap:8px;\\">\\n          <button type=\\"button\\" id=\\"convShowMoreBtn\\" class=\\"btn-ghost\\" style=\\"display:none;height:24px;font-size:11px;\\">Show more</button>\\n        </div>\\n      </div>\\n    </section>\\n  </main>\\n\\n  <!-- Glossar Drawer Dialog -->\\n  <div id=\\"glossaryModalBackdrop\\" class=\\"drawer-backdrop\\" aria-hidden=\\"true\\"></div>\\n  <aside id=\\"glossaryModal\\" class=\\"drawer-panel\\" role=\\"dialog\\" aria-modal=\\"true\\" aria-labelledby=\\"glossaryTitle\\" aria-hidden=\\"true\\" style=\\"width:min(680px, 100vw);\\">\\n    <div class=\\"drawer-header\\">\\n      <div style=\\"min-width:0;\\">\\n        <div style=\\"font-size:10.5px;color:var(--gb-purple);margin-bottom:3px;font-weight:700;\\" class=\\"mono\\"><span class=\\"nf-icon\\">\\uf02d</span> DASHBOARD REFERENCE &amp; SPECIFICATION</div>\\n        <h3 id=\\"glossaryTitle\\" class=\\"drawer-title\\">Glossar: Metrics &amp; Buzzwords</h3>\\n      </div>\\n      <div class=\\"drawer-header-actions\\">\\n        <button type=\\"button\\" id=\\"glossaryCloseBtn\\" class=\\"icon-btn\\" title=\\"Close glossar (Esc)\\" aria-label=\\"Close glossar\\">\\n          <span class=\\"nf-icon\\">\\u2715</span>\\n        </button>\\n      </div>\\n    </div>\\n    <div class=\\"drawer-content\\" style=\\"padding:16px 20px;overflow-y:auto;display:flex;flex-direction:column;gap:14px;\\">\\n      <div style=\\"display:flex;flex-direction:column;gap:8px;\\">\\n        <div style=\\"position:relative;width:100%;\\">\\n          <span class=\\"nf-icon\\" style=\\"position:absolute;left:10px;top:50%;transform:translateY(-50%);color:var(--text-muted);font-size:12px;\\">\\uf002</span>\\n          <input type=\\"text\\" id=\\"glossarySearchInput\\" class=\\"search-input\\" style=\\"width:100%;padding-left:30px;height:32px;font-size:12px;\\" placeholder=\\"/filter terms (e.g. context, thinking, PUE, CoT, subagent)\\u2026\\" autocomplete=\\"off\\" spellcheck=\\"false\\" />\\n        </div>\\n        <div id=\\"glossaryCategoryPills\\" style=\\"display:flex;flex-wrap:wrap;gap:6px;font-size:11px;\\"></div>\\n      </div>\\n      <div id=\\"glossaryList\\" style=\\"display:flex;flex-direction:column;gap:10px;padding-bottom:12px;\\"></div>\\n    </div>\\n  </aside>\\n\\n  <!-- Real-World Perspective Modal Dialog -->\\n  <div id=\\"perspectiveModalBackdrop\\" class=\\"drawer-backdrop\\" aria-hidden=\\"true\\"></div>\\n  <aside id=\\"perspectiveModal\\" class=\\"drawer-panel\\" role=\\"dialog\\" aria-modal=\\"true\\" aria-labelledby=\\"perspectiveTitle\\" aria-hidden=\\"true\\" style=\\"width:min(500px, 100vw);\\">\\n    <div class=\\"drawer-header\\">\\n      <div style=\\"min-width:0;\\">\\n        <div id=\\"perspectiveCategory\\" style=\\"font-size:10.5px;color:var(--gb-yellow);margin-bottom:3px;font-weight:700;\\" class=\\"mono\\">REAL-WORLD PERSPECTIVE</div>\\n        <h3 id=\\"perspectiveTitle\\" class=\\"drawer-title\\">Environmental Context</h3>\\n      </div>\\n      <div class=\\"drawer-header-actions\\">\\n        <button type=\\"button\\" id=\\"perspectiveCloseBtn\\" class=\\"icon-btn\\" title=\\"Close dialog (Esc)\\" aria-label=\\"Close dialog\\">\\n          <span class=\\"nf-icon\\">\\u2715</span>\\n        </button>\\n      </div>\\n    </div>\\n    <div id=\\"perspectiveBody\\" class=\\"drawer-content\\"></div>\\n  </aside>\\n\\n  <!-- Slide-Over Conversation Detail Drawer -->\\n  <div id=\\"drawerBackdrop\\" class=\\"drawer-backdrop\\" aria-hidden=\\"true\\"></div>\\n  <aside id=\\"drawerPanel\\" class=\\"drawer-panel\\" role=\\"dialog\\" aria-modal=\\"true\\" aria-labelledby=\\"drawerConvTitle\\" aria-hidden=\\"true\\">\\n    <div class=\\"drawer-header\\">\\n      <div style=\\"min-width:0;\\">\\n        <div style=\\"font-size:10.5px;color:var(--gb-yellow);margin-bottom:3px;font-weight:700;\\" class=\\"mono\\"><span class=\\"nf-icon\\">\\uf489</span> SESSION INSPECTOR</div>\\n        <h3 id=\\"drawerConvTitle\\" class=\\"drawer-title\\">Conversation Details</h3>\\n      </div>\\n      <div class=\\"drawer-header-actions\\">\\n        <button type=\\"button\\" id=\\"drawerPrevBtn\\" class=\\"icon-btn\\" title=\\"Previous conversation (\\u2191)\\" aria-label=\\"Previous conversation\\">\\n          <span class=\\"nf-icon\\">\\uf077</span>\\n        </button>\\n        <button type=\\"button\\" id=\\"drawerNextBtn\\" class=\\"icon-btn\\" title=\\"Next conversation (\\u2193)\\" aria-label=\\"Next conversation\\">\\n          <span class=\\"nf-icon\\">\\uf078</span>\\n        </button>\\n        <button type=\\"button\\" id=\\"drawerCloseBtn\\" class=\\"icon-btn\\" title=\\"Close inspector (Esc)\\" aria-label=\\"Close inspector\\">\\n          <span class=\\"nf-icon\\">\\uf00d</span>\\n        </button>\\n      </div>\\n    </div>\\n    <div id=\\"drawerBody\\" class=\\"drawer-content\\"></div>\\n  </aside>\\n\\n  <!-- Shared Floating Tooltip -->\\n  <div id=\\"floatingTooltip\\" class=\\"floating-tooltip\\" role=\\"tooltip\\"></div>\\n\\n  <!-- Toast -->\\n  <div id=\\"toast\\" class=\\"toast\\" role=\\"status\\" aria-live=\\"polite\\">\\n    <span class=\\"pulse-dot\\" style=\\"background:var(--emerald-live);\\"></span>\\n    <span id=\\"toastText\\">Updated</span>\\n  </div>\\n  <script>\\n    (function () {\\n      \'use strict\';\\n\\n      const NF_FONT_STACK = \\"\'JetBrainsMono Nerd Font\', \'JetBrainsMonoNL Nerd Font\', \'FiraCode Nerd Font\', \'Iosevka Nerd Font\', \'CaskaydiaCove Nerd Font\', \'MesloLGS NF\', \'Hack Nerd Font\', \'JetBrains Mono\', \'Symbols Nerd Font Mono\', ui-monospace, monospace\\";\\n\\n      function getGruvboxPalette() {\\n        const isLight = document.documentElement.getAttribute(\'data-theme\') === \'light\';\\n        if (isLight) {\\n          return {\\n            input: \'#076678\',\\n            inputHover: \'#458588\',\\n            inputBarRgba: \'rgba(7, 102, 120, 0.78)\',\\n            output: \'#427b58\',\\n            outputHover: \'#689d6a\',\\n            thinking: \'#8f3f71\',\\n            thinkingHover: \'#b16286\',\\n            cost: \'#b57614\',\\n            costFillTop: \'rgba(181, 118, 20, 0.25)\',\\n            costFillBottom: \'rgba(181, 118, 20, 0.0)\',\\n            tools: \'#af3a03\',\\n            pointBorder: \'#fbf1c7\',\\n            axisBorder: \'rgba(124, 111, 100, 0.30)\',\\n            grid: \'rgba(124, 111, 100, 0.15)\',\\n            tick: \'#7c6f64\',\\n            tooltipBg: \'#f2e5bc\',\\n            tooltipTitle: \'#af3a03\',\\n            tooltipBody: \'#3c3836\',\\n            tooltipBorder: \'#bdae93\'\\n          };\\n        }\\n        return {\\n          input: \'#83a598\',\\n          inputHover: \'#9dc2b5\',\\n          inputBarRgba: \'rgba(131, 165, 152, 0.78)\',\\n          output: \'#8ec07c\',\\n          outputHover: \'#b8bb26\',\\n          thinking: \'#d3869b\',\\n          thinkingHover: \'#e39cb0\',\\n          cost: \'#fabd2f\',\\n          costFillTop: \'rgba(250, 189, 47, 0.26)\',\\n          costFillBottom: \'rgba(250, 189, 47, 0.0)\',\\n          tools: \'#fe8019\',\\n          pointBorder: \'#1d2021\',\\n          axisBorder: \'rgba(168, 153, 132, 0.22)\',\\n          grid: \'rgba(168, 153, 132, 0.12)\',\\n          tick: \'#a89984\',\\n          tooltipBg: \'#32302f\',\\n          tooltipTitle: \'#fabd2f\',\\n          tooltipBody: \'#ebdbb2\',\\n          tooltipBorder: \'#665c54\'\\n        };\\n      }\\n\\n      function toolNerdGlyph(name) {\\n        const n = String(name || \'\').toLowerCase();\\n        if (n.includes(\'terminal\') || n.includes(\'command\') || n.includes(\'exec\') || n.includes(\'bash\')) return \'\\uf489\';\\n        if (n.includes(\'read\') || n.includes(\'view\')) return \'\\uf15c\';\\n        if (n.includes(\'write\') || n.includes(\'patch\') || n.includes(\'edit\')) return \'\\uf044\';\\n        if (n.includes(\'search\') || n.includes(\'grep\') || n.includes(\'find\') || n.includes(\'web\')) return \'\\uf002\';\\n        if (n.includes(\'delegate\') || n.includes(\'subagent\') || n.includes(\'agent\')) return \'\\uf0e8\';\\n        if (n.includes(\'browser\')) return \'\\uf268\';\\n        if (n.includes(\'skill\')) return \'\\uf02d\';\\n        return \'\\uf0ad\';\\n      }\\n\\n      function applySavedTheme(mode) {\\n        const next = mode === \'light\' ? \'light\' : \'dark\';\\n        document.documentElement.setAttribute(\'data-theme\', next);\\n        try { localStorage.setItem(\'agy_gruvbox_theme\', next); } catch (_) {}\\n        const glyphEl = document.getElementById(\'themeToggleGlyph\');\\n        const labelEl = document.getElementById(\'themeToggleLabel\');\\n        if (glyphEl) glyphEl.textContent = next === \'light\' ? \'\\uf185\' : \'\\uf186\';\\n        if (labelEl) labelEl.textContent = next;\\n      }\\n      (function initThemeEarly() {\\n        let saved = \'dark\';\\n        try { saved = localStorage.getItem(\'agy_gruvbox_theme\') || \'dark\'; } catch (_) {}\\n        applySavedTheme(saved);\\n      })();\\n\\n      // --- State ---\\n      const urlParams = new URLSearchParams(window.location.search);\\n      const state = {\\n        days: urlParams.get(\'days\') || \'30\',\\n        pricing: urlParams.get(\'pricing\') || \'\',\\n        billingMode: urlParams.get(\'billing_mode\') || \'context\',\\n        chartTab: \'tokens\',\\n        hourlyMode: \'tokens\',\\n        autoRefresh: false,\\n        autoRefreshTimer: null,\\n        isRescanning: false,\\n        isFetching: false,\\n        demoBannerDismissed: false,\\n        searchQuery: \'\',\\n        sortKey: \'updated_at\',\\n        sortDir: \'desc\',\\n        visibleLimit: 50,\\n        selectedConvId: null,\\n        data: null,\\n        normalizedProfiles: [],\\n        chartInstance: null\\n      };\\n\\n      // --- Formatting & Escape Utilities ---\\n      const intFmt = new Intl.NumberFormat(\'en-US\', { maximumFractionDigits: 0 });\\n      const dec1Fmt = new Intl.NumberFormat(\'en-US\', { minimumFractionDigits: 1, maximumFractionDigits: 1 });\\n      const dec2Fmt = new Intl.NumberFormat(\'en-US\', { minimumFractionDigits: 2, maximumFractionDigits: 2 });\\n\\n      function escapeHtml(val) {\\n        if (val === null || val === undefined) return \'\';\\n        return String(val)\\n          .replace(/&/g, \'&amp;\')\\n          .replace(/</g, \'&lt;\')\\n          .replace(/>/g, \'&gt;\')\\n          .replace(/\\"/g, \'&quot;\')\\n          .replace(/\'/g, \'&#39;\');\\n      }\\n\\n      function num(v, fallback = 0) {\\n        const n = Number(v);\\n        return Number.isFinite(n) ? n : fallback;\\n      }\\n\\n      function fmtInt(v) {\\n        return intFmt.format(Math.round(num(v)));\\n      }\\n\\n      function fmtCompact(v) {\\n        const n = num(v);\\n        const abs = Math.abs(n);\\n        if (abs >= 1e9) return (n / 1e9).toFixed(2) + \'B\';\\n        if (abs >= 1e6) return (n / 1e6).toFixed(2) + \'M\';\\n        if (abs >= 1e4) return (n / 1e3).toFixed(1) + \'K\';\\n        return intFmt.format(Math.round(n));\\n      }\\n\\n      function fmtUsd(v) {\\n        const n = num(v);\\n        if (n === 0) return \'$0.00\';\\n        if (n > 0 && n < 0.01) return \'$\' + n.toFixed(4);\\n        if (n >= 1000) return \'$\' + intFmt.format(Math.round(n));\\n        return \'$\' + dec2Fmt.format(n);\\n      }\\n\\n      function fmtEnergy(whVal, kwhVal) {\\n        const wh = num(whVal);\\n        const kwh = kwhVal !== undefined && kwhVal !== null ? num(kwhVal) : wh / 1000;\\n        if (wh >= 1000) {\\n          return { primary: dec2Fmt.format(kwh) + \' kWh\', secondary: fmtInt(wh) + \' Wh\' };\\n        }\\n        if (wh > 0 && wh < 0.1) {\\n          return { primary: wh.toFixed(3) + \' Wh\', secondary: (kwh * 1000).toFixed(3) + \' Wh\' };\\n        }\\n        return { primary: dec1Fmt.format(wh) + \' Wh\', secondary: kwh.toFixed(3) + \' kWh\' };\\n      }\\n\\n      function fmtEnergyShort(whVal) {\\n        const wh = num(whVal);\\n        if (wh >= 1000) return (wh / 1000).toFixed(2) + \' kWh\';\\n        if (wh === 0) return \'0.0 Wh\';\\n        if (wh < 0.1) return wh.toFixed(2) + \' Wh\';\\n        return dec1Fmt.format(wh) + \' Wh\';\\n      }\\n\\n      function fmtCarbon(gramsVal) {\\n        const g = num(gramsVal);\\n        if (g >= 1000) {\\n          return { primary: dec2Fmt.format(g / 1000) + \' kg CO\\u2082e\', secondary: fmtInt(g) + \' g CO\\u2082e\' };\\n        }\\n        if (g > 0 && g < 0.1) {\\n          return { primary: g.toFixed(3) + \' g CO\\u2082e\', secondary: \'\' };\\n        }\\n        return { primary: dec1Fmt.format(g) + \' g CO\\u2082e\', secondary: (g / 1000).toFixed(3) + \' kg\' };\\n      }\\n\\n      function parseTimestamp(raw) {\\n        if (!raw && raw !== 0) return null;\\n        if (typeof raw === \'number\') {\\n          const ms = raw < 1e12 ? raw * 1000 : raw;\\n          const d = new Date(ms);\\n          return Number.isNaN(d.getTime()) ? null : d;\\n        }\\n        const s = String(raw).trim();\\n        if (!s) return null;\\n        if (/^\\\\d+(\\\\.\\\\d+)?$/.test(s)) {\\n          const v = parseFloat(s);\\n          const d = new Date(v < 1e12 ? v * 1000 : v);\\n          return Number.isNaN(d.getTime()) ? null : d;\\n        }\\n        const isoCandidate = s.includes(\'T\') ? s : s.replace(\' \', \'T\');\\n        let d = new Date(isoCandidate);\\n        if (Number.isNaN(d.getTime())) d = new Date(s);\\n        return Number.isNaN(d.getTime()) ? null : d;\\n      }\\n\\n      function fmtTimestamp(raw) {\\n        const d = parseTimestamp(raw);\\n        if (!d) return raw ? String(raw) : \'\\u2014\';\\n        const pad = (n) => String(n).padStart(2, \'0\');\\n        return d.getFullYear() + \'-\' + pad(d.getMonth() + 1) + \'-\' + pad(d.getDate()) + \' \' + pad(d.getHours()) + \':\' + pad(d.getMinutes());\\n      }\\n\\n      function fmtDatePretty(dateStr) {\\n        if (!dateStr) return \'\\u2014\';\\n        const parts = String(dateStr).split(\'-\');\\n        if (parts.length === 3) {\\n          const d = new Date(Number(parts[0]), Number(parts[1]) - 1, Number(parts[2]));\\n          if (!Number.isNaN(d.getTime())) {\\n            return d.toLocaleDateString(\'en-US\', { weekday: \'short\', month: \'short\', day: \'numeric\', year: \'numeric\' });\\n          }\\n        }\\n        return String(dateStr);\\n      }\\n\\n      function fmtShortDate(dateStr) {\\n        if (!dateStr) return \'\';\\n        const parts = String(dateStr).split(\'-\');\\n        if (parts.length === 3) {\\n          const months = [\'Jan\', \'Feb\', \'Mar\', \'Apr\', \'May\', \'Jun\', \'Jul\', \'Aug\', \'Sep\', \'Oct\', \'Nov\', \'Dec\'];\\n          const m = months[Number(parts[1]) - 1] || parts[1];\\n          return m + \' \' + Number(parts[2]);\\n        }\\n        return String(dateStr);\\n      }\\n\\n      // --- Pricing Profile Normalization ---\\n      function normalizePricingProfiles(rawProfiles) {\\n        if (!rawProfiles) return [];\\n        const result = [];\\n        const extractRates = (obj) => {\\n          if (!obj || typeof obj !== \'object\') return { inRate: null, outRate: null, thinkRate: null };\\n          const inRate = obj.input_per_1m ?? obj.input_cost_per_1m ?? obj.input_per_m ?? obj.input_cost_per_million ?? obj.input_usd_per_1m ?? obj.input_rate ?? obj.input ?? null;\\n          const outRate = obj.output_per_1m ?? obj.output_cost_per_1m ?? obj.output_per_m ?? obj.output_cost_per_million ?? obj.output_usd_per_1m ?? obj.output_rate ?? obj.output ?? null;\\n          const thinkRate = obj.thinking_per_1m ?? obj.thinking_cost_per_1m ?? obj.thinking_per_m ?? obj.thinking_rate ?? obj.thinking ?? outRate;\\n          return {\\n            inRate: inRate !== null ? num(inRate, null) : null,\\n            outRate: outRate !== null ? num(outRate, null) : null,\\n            thinkRate: thinkRate !== null ? num(thinkRate, null) : null\\n          };\\n        };\\n\\n        if (Array.isArray(rawProfiles)) {\\n          rawProfiles.forEach((item) => {\\n            if (typeof item === \'string\') {\\n              result.push({ id: item, label: item, inRate: null, outRate: null, thinkRate: null });\\n            } else if (item && typeof item === \'object\') {\\n              const id = String(item.id ?? item.key ?? item.slug ?? item.profile ?? item.name ?? \'\');\\n              if (!id) return;\\n              const baseName = String(item.label ?? item.name ?? item.title ?? id);\\n              const rates = extractRates(item);\\n              const rateSuffix = (rates.inRate !== null && rates.outRate !== null && !baseName.includes(\'$\'))\\n                ? ` ($${rates.inRate}/$${rates.outRate})`\\n                : \'\';\\n              result.push({ id, label: baseName + rateSuffix, shortName: baseName, ...rates });\\n            }\\n          });\\n        } else if (typeof rawProfiles === \'object\') {\\n          Object.entries(rawProfiles).forEach(([key, val]) => {\\n            if (typeof val === \'string\') {\\n              result.push({ id: key, label: val, shortName: val, inRate: null, outRate: null, thinkRate: null });\\n            } else if (val && typeof val === \'object\') {\\n              const baseName = String(val.label ?? val.name ?? val.title ?? key);\\n              const rates = extractRates(val);\\n              const rateSuffix = (rates.inRate !== null && rates.outRate !== null && !baseName.includes(\'$\'))\\n                ? ` ($${rates.inRate}/$${rates.outRate})`\\n                : \'\';\\n              result.push({ id: key, label: baseName + rateSuffix, shortName: baseName, ...rates });\\n            }\\n          });\\n        }\\n        return result;\\n      }\\n\\n      function isTurnBilling(mode) {\\n        const m = String(mode || \'\').toLowerCase();\\n        return m === \'turn\' || m === \'unique\' || m === \'turn_tokens\' || m === \'unique_input\';\\n      }\\n\\n      function resolveConvInputTokens(conv) {\\n        if (conv.input_tokens !== undefined && conv.input_tokens !== null) return num(conv.input_tokens);\\n        return isTurnBilling(state.billingMode)\\n          ? num(conv.unique_input_tokens)\\n          : num(conv.context_input_tokens);\\n      }\\n\\n      // --- Tooltip Controller ---\\n      const tooltipEl = document.getElementById(\'floatingTooltip\');\\n      function showTooltip(html, clientX, clientY) {\\n        tooltipEl.innerHTML = html;\\n        tooltipEl.classList.add(\'is-visible\');\\n        positionTooltip(clientX, clientY);\\n      }\\n\\n      function positionTooltip(clientX, clientY) {\\n        const pad = 12;\\n        const rect = tooltipEl.getBoundingClientRect();\\n        let x = clientX + 14;\\n        let y = clientY - rect.height - 10;\\n        if (x + rect.width + pad > window.innerWidth) {\\n          x = Math.max(pad, clientX - rect.width - 14);\\n        }\\n        if (y < pad) {\\n          y = clientY + 18;\\n        }\\n        tooltipEl.style.left = x + \'px\';\\n        tooltipEl.style.top = y + \'px\';\\n      }\\n\\n      function hideTooltip() {\\n        tooltipEl.classList.remove(\'is-visible\');\\n      }\\n\\n      // --- Toast Notification ---\\n      let toastTimer = null;\\n      function showToast(msg) {\\n        const toast = document.getElementById(\'toast\');\\n        document.getElementById(\'toastText\').textContent = msg;\\n        toast.classList.add(\'is-visible\');\\n        if (toastTimer) clearTimeout(toastTimer);\\n        toastTimer = setTimeout(() => toast.classList.remove(\'is-visible\'), 2600);\\n      }\\n\\n      // --- Export Link Updater ---\\n      function buildQueryParams() {\\n        const p = new URLSearchParams();\\n        p.set(\'days\', state.days);\\n        if (state.pricing) p.set(\'pricing\', state.pricing);\\n        if (state.billingMode) p.set(\'billing_mode\', state.billingMode);\\n        return p.toString();\\n      }\\n\\n      function syncExportLink() {\\n        const link = document.getElementById(\'exportCsvLink\');\\n        link.href = \'/api/export.csv?\' + buildQueryParams();\\n      }\\n\\n      // --- Synthetic Fallback Dataset (for file:// offline preview or explicit demo button) ---\\n      function generateOfflineFallbackData() {\\n        const today = new Date();\\n        const profiles = {\\n          \'claude-sonnet-4-6\': { name: \'Claude Sonnet 4.6\', input_per_1m: 3.0, output_per_1m: 15.0, thinking_per_1m: 15.0 },\\n          \'claude-opus-4-6\': { name: \'Claude Opus 4.6\', input_per_1m: 15.0, output_per_1m: 75.0, thinking_per_1m: 75.0 },\\n          \'gemini-2-5-pro\': { name: \'Gemini 2.5 Pro\', input_per_1m: 1.25, output_per_1m: 10.0, thinking_per_1m: 10.0 },\\n          \'gpt-5\': { name: \'GPT-5\', input_per_1m: 2.5, output_per_1m: 10.0, thinking_per_1m: 10.0 }\\n        };\\n        const activeProfKey = state.pricing && profiles[state.pricing] ? state.pricing : \'claude-sonnet-4-6\';\\n        const prof = profiles[activeProfKey];\\n        const turnMode = isTurnBilling(state.billingMode);\\n\\n        const heatmap = [];\\n        const allDaily = [];\\n        for (let i = 364; i >= 0; i--) {\\n          const d = new Date(today.getFullYear(), today.getMonth(), today.getDate() - i);\\n          const iso = d.toISOString().slice(0, 10);\\n          const seed = (i * 73 + 19) % 97;\\n          const active = seed > 28;\\n          const reqs = active ? Math.max(4, Math.round((seed - 25) * 2.2)) : 0;\\n          const ctxIn = reqs * (16500 + (seed % 9) * 1800);\\n          const uniqIn = Math.round(ctxIn * 0.19);\\n          const effIn = turnMode ? uniqIn : ctxIn;\\n          const outTok = reqs * (1350 + (seed % 5) * 240);\\n          const thinkTok = reqs * (920 + (seed % 7) * 310);\\n          const tot = effIn + outTok + thinkTok;\\n          const cost = (effIn * prof.input_per_1m + (outTok + thinkTok) * prof.output_per_1m) / 1e6;\\n          const wh = (tot / 1e6) * 42.5;\\n          const co2 = (wh / 1000) * 385;\\n          let lvl = 0;\\n          if (tot > 0) lvl = tot > 2600000 ? 4 : tot > 1600000 ? 3 : tot > 800000 ? 2 : 1;\\n          heatmap.push({ date: iso, total_tokens: tot, api_requests: reqs, cost_usd: cost, level: lvl });\\n          allDaily.push({\\n            date: iso,\\n            input_tokens: effIn,\\n            unique_input_tokens: uniqIn,\\n            context_input_tokens: ctxIn,\\n            output_tokens: outTok,\\n            thinking_tokens: thinkTok,\\n            total_tokens: tot,\\n            api_requests: reqs,\\n            tool_calls: Math.round(reqs * 1.45),\\n            conversations: active ? Math.max(1, Math.round(reqs / 14)) : 0,\\n            cost_usd: cost,\\n            electricity_wh: wh,\\n            co2_grams: co2\\n          });\\n        }\\n\\n        const dayCount = state.days === \'all\' ? 365 : Math.min(365, Math.max(1, parseInt(state.days, 10) || 30));\\n        const daily = allDaily.slice(-dayCount);\\n\\n        let sumEffIn = 0, sumUniqIn = 0, sumCtxIn = 0, sumOut = 0, sumThink = 0, sumReqs = 0, sumTools = 0, sumConvs = 0, sumCost = 0, sumWh = 0;\\n        let peakDay = { date: daily[0]?.date || \'\', tokens: 0 };\\n        daily.forEach((r) => {\\n          sumEffIn += r.input_tokens;\\n          sumUniqIn += r.unique_input_tokens;\\n          sumCtxIn += r.context_input_tokens;\\n          sumOut += r.output_tokens;\\n          sumThink += r.thinking_tokens;\\n          sumReqs += r.api_requests;\\n          sumTools += r.tool_calls;\\n          sumConvs += r.conversations;\\n          sumCost += r.cost_usd;\\n          sumWh += r.electricity_wh;\\n          if (r.total_tokens > peakDay.tokens) peakDay = { date: r.date, tokens: r.total_tokens };\\n        });\\n        const sumTotal = sumEffIn + sumOut + sumThink;\\n        const kwh = sumWh / 1000;\\n        const co2 = kwh * 385;\\n        const activeDays365 = heatmap.filter((h) => h.total_tokens > 0).length;\\n\\n        const hourlyWeights = [2, 1, 0, 0, 1, 2, 4, 9, 18, 28, 36, 33, 24, 38, 44, 41, 35, 29, 21, 17, 15, 12, 7, 4];\\n        const weightSum = hourlyWeights.reduce((a, b) => a + b, 0);\\n        const hourly = hourlyWeights.map((w, hour) => ({\\n          hour,\\n          total_tokens: Math.round((sumTotal * w) / weightSum),\\n          api_requests: Math.round((sumReqs * w) / weightSum)\\n        }));\\n\\n        const toolSpec = [\\n          [\'read_file\', 0.312],\\n          [\'edit_file\', 0.214],\\n          [\'run_command\', 0.186],\\n          [\'search_files\', 0.142],\\n          [\'write_file\', 0.078],\\n          [\'invoke_subagent\', 0.041],\\n          [\'search_web\', 0.027]\\n        ];\\n        const tools = toolSpec.map(([name, ratio]) => ({\\n          name,\\n          count: Math.round(sumTools * ratio),\\n          share_pct: +(ratio * 100).toFixed(1)\\n        }));\\n\\n        const sampleTitles = [\\n          \'Implement SQLite WAL indexer and delta scanner for AGY logs\',\\n          \'Refactor Linear dark-mode token palette and tabular metrics\',\\n          \'Debug streaming SSE parser chunk boundary edge case\',\\n          \'Add OpenRouter cumulative context vs turn token billing toggle\',\\n          \'Optimize 365-day activity matrix query latency under 8ms\',\\n          \'Build zero-dependency CSV streaming exporter endpoint\',\\n          \'Audit subagent spawn tree attribution and thinking tokens\',\\n          \'Fix timezone bucketing in hourly spark histogram aggregation\',\\n          \'Benchmark token electricity and 385g/kWh CO2e footprint model\',\\n          \'Verify offline SVG fallback chart renderer on headless Linux\'\\n        ];\\n        const conversations = sampleTitles.map((title, idx) => {\\n          const reqs = 14 + ((idx * 17) % 48);\\n          const cIn = reqs * (21000 + idx * 2400);\\n          const uIn = Math.round(cIn * 0.18);\\n          const eIn = turnMode ? uIn : cIn;\\n          const out = reqs * (1520 + idx * 140);\\n          const thk = reqs * (1100 + (idx % 4) * 580);\\n          const tot = eIn + out + thk;\\n          const cCost = (eIn * prof.input_per_1m + (out + thk) * prof.output_per_1m) / 1e6;\\n          const cWh = (tot / 1e6) * 42.5;\\n          const upd = new Date(today.getTime() - idx * 7.5 * 3600 * 1000);\\n          const st = new Date(upd.getTime() - (22 + idx * 9) * 60 * 1000);\\n          const hex = ((idx + 1) * 0x19a7f3b).toString(16).padStart(8, \'0\');\\n          return {\\n            id: `${hex}-4c2b-49e1-9a1${idx}-7f8e2b4d90${idx}${idx}`,\\n            title,\\n            started_at: st.toISOString(),\\n            updated_at: upd.toISOString(),\\n            api_requests: reqs,\\n            tool_calls: Math.round(reqs * 1.5),\\n            input_tokens: eIn,\\n            unique_input_tokens: uIn,\\n            context_input_tokens: cIn,\\n            output_tokens: out,\\n            thinking_tokens: thk,\\n            total_tokens: tot,\\n            cost_usd: cCost,\\n            electricity_wh: cWh\\n          };\\n        });\\n\\n        return {\\n          meta: {\\n            agy_dir: \'~/.gemini/antigravity\',\\n            db_path: \'~/.agy-usage-tracker/telemetry.sqlite3\',\\n            is_demo: true,\\n            demo_reason: \'Local preview mode \\u2014 backend /api/stats unreachable, displaying synthetic telemetry dataset.\',\\n            last_scanned_at: today.toISOString(),\\n            days_filter: state.days,\\n            pricing_profile: activeProfKey,\\n            billing_mode: state.billingMode,\\n            pricing_profiles: profiles\\n          },\\n          totals: {\\n            total_tokens: sumTotal,\\n            effective_input_tokens: sumEffIn,\\n            unique_input_tokens: sumUniqIn,\\n            context_input_tokens: sumCtxIn,\\n            output_tokens: sumOut,\\n            thinking_tokens: sumThink,\\n            api_requests: sumReqs,\\n            conversations: sumConvs,\\n            subagent_spawns: Math.max(2, Math.round(sumConvs * 0.34)),\\n            tool_calls: sumTools,\\n            cost_usd: sumCost,\\n            electricity_wh: sumWh,\\n            electricity_kwh: kwh,\\n            co2_grams: co2,\\n            phone_charges_equiv: +(sumWh / 12.0).toFixed(1),\\n            led_bulb_hours_equiv: +(sumWh / 10.0).toFixed(1),\\n            avg_tokens_per_request: sumReqs > 0 ? Math.round(sumTotal / sumReqs) : 0,\\n            thinking_share_pct: sumTotal > 0 ? +((sumThink / sumTotal) * 100).toFixed(1) : 0,\\n            active_days: activeDays365,\\n            peak_day: peakDay\\n          },\\n          daily,\\n          heatmap,\\n          hourly,\\n          tools,\\n          conversations\\n        };\\n      }\\n\\n      // --- API Fetching ---\\n      async function fetchStats(options = {}) {\\n        if (state.isFetching && !options.force) return;\\n        state.isFetching = true;\\n        syncExportLink();\\n\\n        if (window.location.protocol === \'file:\') {\\n          state.data = generateOfflineFallbackData();\\n          state.isFetching = false;\\n          renderAll();\\n          return;\\n        }\\n\\n        try {\\n          const res = await fetch(\'/api/stats?\' + buildQueryParams(), {\\n            headers: { \'Accept\': \'application/json\' },\\n            cache: \'no-store\'\\n          });\\n          if (!res.ok) {\\n            throw new Error(`HTTP ${res.status} ${res.statusText}`);\\n          }\\n          const payload = await res.json();\\n          document.getElementById(\'errorBanner\').classList.remove(\'is-visible\');\\n          state.data = payload;\\n          syncControlsFromMeta(payload.meta || {});\\n          renderAll();\\n        } catch (err) {\\n          const errBanner = document.getElementById(\'errorBanner\');\\n          document.getElementById(\'errorBannerText\').textContent =\\n            `Could not fetch /api/stats (${err.message || \'network error\'}).`;\\n          errBanner.classList.add(\'is-visible\');\\n          if (!state.data) {\\n            state.data = generateOfflineFallbackData();\\n            renderAll();\\n          }\\n        } finally {\\n          state.isFetching = false;\\n        }\\n      }\\n\\n      async function triggerRescan() {\\n        if (state.isRescanning) return;\\n        state.isRescanning = true;\\n        const btn = document.getElementById(\'rescanBtn\');\\n        btn.disabled = true;\\n        btn.classList.add(\'is-spinning\');\\n\\n        try {\\n          if (window.location.protocol === \'file:\') {\\n            await new Promise((r) => setTimeout(r, 420));\\n            state.data = generateOfflineFallbackData();\\n            renderAll();\\n            showToast(\'Demo telemetry rescanned\');\\n            return;\\n          }\\n          const res = await fetch(\'/api/rescan\', { method: \'POST\', headers: { \'Accept\': \'application/json\' } });\\n          if (!res.ok) throw new Error(`HTTP ${res.status}`);\\n          let body = null;\\n          try { body = await res.json(); } catch (_) { body = null; }\\n          if (body && body.totals && body.meta) {\\n            state.data = body;\\n            syncControlsFromMeta(body.meta);\\n            renderAll();\\n          } else {\\n            await fetchStats({ force: true });\\n          }\\n          showToast(\'Rescan complete \\u2014 telemetry updated\');\\n        } catch (err) {\\n          showToast(\'Rescan failed: \' + (err.message || \'error\'));\\n        } finally {\\n          state.isRescanning = false;\\n          btn.disabled = false;\\n          btn.classList.remove(\'is-spinning\');\\n        }\\n      }\\n\\n      function syncControlsFromMeta(meta) {\\n        if (meta.days_filter !== undefined && meta.days_filter !== null) {\\n          const rawDays = String(meta.days_filter).toLowerCase();\\n          state.days = (rawDays === \'0\' || rawDays === \'all\' || rawDays === \'max\') ? \'all\' : rawDays;\\n        }\\n        if (meta.billing_mode) {\\n          state.billingMode = String(meta.billing_mode);\\n        }\\n        if (meta.pricing_profile) {\\n          state.pricing = String(meta.pricing_profile);\\n        }\\n        syncExportLink();\\n      }\\n\\n      // --- Master Render ---\\n      function renderAll() {\\n        if (!state.data) return;\\n        const data = state.data;\\n        const meta = data.meta || {};\\n        const totals = data.totals || {};\\n\\n        renderHeaderAndMeta(meta);\\n        renderKpis(meta, totals);\\n        renderDailyChart(data.daily || [], meta);\\n        renderHeatmap(data.heatmap || [], totals);\\n        renderHourly(data.hourly || []);\\n        renderTools(data.tools || [], totals);\\n        renderConversationsTable(data.conversations || []);\\n\\n        if (state.selectedConvId) {\\n          const found = (data.conversations || []).find((c) => String(c.id) === String(state.selectedConvId));\\n          if (found) renderDrawerContent(found);\\n        }\\n      }\\n\\n      // --- Section 1: Header & Meta ---\\n      function renderHeaderAndMeta(meta) {\\n        const pathEl = document.getElementById(\'agyPathSubtitle\');\\n        const agyDir = meta.agy_dir || \'~/.gemini/antigravity\';\\n        pathEl.textContent = agyDir;\\n        pathEl.dataset.fullInfo = `AGY Dir: ${agyDir}\\nSQLite DB: ${meta.db_path || \'\\u2014\'}\\nLast Scan: ${fmtTimestamp(meta.last_scanned_at)}`;\\n\\n        document.getElementById(\'metaDbPath\').textContent = meta.db_path || \'\\u2014\';\\n        document.getElementById(\'metaLastScanned\').textContent = fmtTimestamp(meta.last_scanned_at);\\n        document.getElementById(\'metaBillingBadge\').textContent = isTurnBilling(state.billingMode) ? \'turn (new tokens)\' : \'context (cumulative)\';\\n\\n        const hasChartJs = typeof window.Chart !== \'undefined\' && !window.__chartJsLoadFailed;\\n        document.getElementById(\'metaChartEngine\').textContent = hasChartJs ? \'Chart.js 4\' : \'SVG Fallback\';\\n\\n        // Demo badge + banner\\n        const demoBadge = document.getElementById(\'demoBadge\');\\n        const demoBanner = document.getElementById(\'demoBanner\');\\n        const demoReason = meta.demo_reason || \'Synthetic demonstration telemetry is active.\';\\n        if (meta.is_demo) {\\n          demoBadge.classList.add(\'is-visible\');\\n          demoBadge.dataset.reason = demoReason;\\n          document.getElementById(\'demoBannerText\').textContent = demoReason;\\n          if (!state.demoBannerDismissed) {\\n            demoBanner.classList.add(\'is-visible\');\\n          }\\n        } else {\\n          demoBadge.classList.remove(\'is-visible\');\\n          demoBanner.classList.remove(\'is-visible\');\\n        }\\n\\n        // Active date button\\n        document.querySelectorAll(\'#dateFilterGroup .seg-btn\').forEach((btn) => {\\n          btn.classList.toggle(\'is-active\', btn.dataset.days === String(state.days));\\n        });\\n\\n        // Active billing button\\n        const turnActive = isTurnBilling(state.billingMode);\\n        document.querySelectorAll(\'#billingModeGroup .seg-btn\').forEach((btn) => {\\n          const btnIsTurn = btn.dataset.billing === \'turn\';\\n          btn.classList.toggle(\'is-active\', btnIsTurn === turnActive);\\n        });\\n\\n        // Pricing dropdown\\n        state.normalizedProfiles = normalizePricingProfiles(meta.pricing_profiles);\\n        const select = document.getElementById(\'pricingSelect\');\\n        if (state.normalizedProfiles.length > 0) {\\n          select.innerHTML = state.normalizedProfiles\\n            .map((p) => `<option value=\\"${escapeHtml(p.id)}\\">${escapeHtml(p.label)}</option>`)\\n            .join(\'\');\\n          if (state.pricing && state.normalizedProfiles.some((p) => p.id === state.pricing)) {\\n            select.value = state.pricing;\\n          } else {\\n            state.pricing = state.normalizedProfiles[0].id;\\n            select.value = state.pricing;\\n          }\\n        } else {\\n          const fallbackId = meta.pricing_profile || state.pricing || \'default\';\\n          select.innerHTML = `<option value=\\"${escapeHtml(fallbackId)}\\">${escapeHtml(fallbackId)}</option>`;\\n        }\\n      }\\n\\n      // --- Section 2: KPI Cards ---\\n      function renderKpis(meta, totals) {\\n        const effInput = num(totals.effective_input_tokens ?? totals.input_tokens ?? (isTurnBilling(state.billingMode) ? totals.unique_input_tokens : totals.context_input_tokens));\\n        const outTok = num(totals.output_tokens);\\n        const thinkTok = num(totals.thinking_tokens);\\n        const totalTok = num(totals.total_tokens, effInput + outTok + thinkTok);\\n\\n        // Card 1: Total Tokens\\n        document.getElementById(\'kpiTokenModeTag\').textContent = isTurnBilling(state.billingMode) ? \'Turn Mode\' : \'Context Mode\';\\n        document.getElementById(\'kpiTotalTokens\').textContent = fmtCompact(totalTok);\\n        document.getElementById(\'kpiTotalTokensExact\').textContent = totalTok >= 10000 ? fmtInt(totalTok) : \'\';\\n        document.getElementById(\'kpiInputTokens\').textContent = fmtCompact(effInput);\\n        document.getElementById(\'kpiOutputTokens\').textContent = fmtCompact(outTok);\\n        document.getElementById(\'kpiThinkingTokens\').textContent = fmtCompact(thinkTok);\\n\\n        const denom = Math.max(1, effInput + outTok + thinkTok);\\n        document.getElementById(\'barInputShare\').style.width = ((effInput / denom) * 100).toFixed(2) + \'%\';\\n        document.getElementById(\'barOutputShare\').style.width = ((outTok / denom) * 100).toFixed(2) + \'%\';\\n        document.getElementById(\'barThinkingShare\').style.width = ((thinkTok / denom) * 100).toFixed(2) + \'%\';\\n\\n        // Card 2: API Requests\\n        const reqs = num(totals.api_requests);\\n        const avgTok = num(totals.avg_tokens_per_request, reqs > 0 ? totalTok / reqs : 0);\\n        document.getElementById(\'kpiApiRequests\').textContent = fmtInt(reqs);\\n        document.getElementById(\'kpiAvgTokPerReq\').textContent = fmtCompact(avgTok) + \' tok/req\';\\n        document.getElementById(\'kpiConversations\').textContent = fmtInt(totals.conversations);\\n        document.getElementById(\'kpiSubagents\').textContent = fmtInt(totals.subagent_spawns);\\n        document.getElementById(\'kpiToolCallsTag\').textContent = fmtInt(totals.tool_calls) + \' tools\';\\n\\n        // Card 3: Est. API Value / Cost\\n        const totalCost = num(totals.cost_usd);\\n        const activeProfile = state.normalizedProfiles.find((p) => p.id === state.pricing);\\n        document.getElementById(\'kpiPricingTag\').textContent = activeProfile ? activeProfile.shortName : (meta.pricing_profile || \'Standard\');\\n        document.getElementById(\'kpiCostUsd\').textContent = fmtUsd(totalCost);\\n        document.getElementById(\'kpiCostPerReq\').textContent = reqs > 0 ? fmtUsd(totalCost / reqs) + \'/req\' : \'\';\\n\\n        let inCost = totals.input_cost_usd !== undefined ? num(totals.input_cost_usd) : null;\\n        let outThinkCost = (totals.output_cost_usd !== undefined || totals.thinking_cost_usd !== undefined)\\n          ? num(totals.output_cost_usd) + num(totals.thinking_cost_usd)\\n          : null;\\n\\n        if (inCost === null || outThinkCost === null) {\\n          if (activeProfile && activeProfile.inRate !== null && activeProfile.outRate !== null) {\\n            const tRate = activeProfile.thinkRate !== null ? activeProfile.thinkRate : activeProfile.outRate;\\n            const rawIn = (effInput * activeProfile.inRate) / 1e6;\\n            const rawOut = (outTok * activeProfile.outRate + thinkTok * tRate) / 1e6;\\n            const rawSum = rawIn + rawOut;\\n            if (rawSum > 0) {\\n              inCost = totalCost * (rawIn / rawSum);\\n              outThinkCost = totalCost - inCost;\\n            }\\n          }\\n        }\\n        if (inCost === null || outThinkCost === null) {\\n          const weightedIn = effInput;\\n          const weightedOut = (outTok + thinkTok) * 5;\\n          const wSum = Math.max(1, weightedIn + weightedOut);\\n          inCost = totalCost * (weightedIn / wSum);\\n          outThinkCost = totalCost - inCost;\\n        }\\n\\n        const costSum = Math.max(1e-9, inCost + outThinkCost);\\n        document.getElementById(\'barCostInput\').style.width = (totalCost > 0 ? (inCost / costSum) * 100 : 50).toFixed(1) + \'%\';\\n        document.getElementById(\'barCostOutput\').style.width = (totalCost > 0 ? (outThinkCost / costSum) * 100 : 50).toFixed(1) + \'%\';\\n        document.getElementById(\'kpiInputCost\').textContent = fmtUsd(inCost);\\n        document.getElementById(\'kpiOutThinkCost\').textContent = fmtUsd(outThinkCost);\\n\\n        // Card 4: Electricity Used\\n        const energy = fmtEnergy(totals.electricity_wh, totals.electricity_kwh);\\n        document.getElementById(\'kpiElectricity\').textContent = energy.primary;\\n        document.getElementById(\'kpiEnergyAlt\').textContent = energy.secondary;\\n        document.getElementById(\'kpiPhoneCharges\').textContent = `~${dec1Fmt.format(num(totals.phone_charges_equiv))} phone charges`;\\n        document.getElementById(\'kpiLedHours\').textContent = `~${dec1Fmt.format(num(totals.led_bulb_hours_equiv))} hrs 10W LED`;\\n        const wEl = document.getElementById(\'kpiWaterPill\');\\n        if (wEl) {\\n          const wMl = totals.water_ml !== undefined ? num(totals.water_ml) : (num(totals.electricity_wh) * 1.80);\\n          wEl.textContent = wMl >= 1000 ? `~${(wMl/1000).toFixed(2)} L water` : `~${wMl.toFixed(1)} ml water`;\\n        }\\n\\n        // Card 5: Carbon Footprint\\n        const co2g = num(totals.co2_grams);\\n        const carbon = fmtCarbon(co2g);\\n        document.getElementById(\'kpiCarbon\').textContent = carbon.primary;\\n        document.getElementById(\'kpiCarbonSub\').textContent = carbon.secondary;\\n        document.getElementById(\'kpiCo2PerReq\').textContent = reqs > 0 ? (co2g / reqs).toFixed(2) + \' g\' : \'0.00 g\';\\n        document.getElementById(\'kpiCo2PerM\').textContent = totalTok > 0 ? ((co2g / totalTok) * 1e6).toFixed(1) + \' g\' : \'0.0 g\';\\n\\n        // Card 6: Reasoning & Peak Velocity\\n        const thinkPct = num(totals.thinking_share_pct, totalTok > 0 ? (thinkTok / totalTok) * 100 : 0);\\n        document.getElementById(\'kpiThinkingShare\').textContent = dec1Fmt.format(thinkPct) + \'%\';\\n        document.getElementById(\'barReasoningGauge\').style.width = Math.min(100, Math.max(0, thinkPct)).toFixed(1) + \'%\';\\n        document.getElementById(\'kpiActiveDaysTag\').textContent = `${fmtInt(totals.active_days)} active days`;\\n\\n        if (totals.peak_day && totals.peak_day.date && num(totals.peak_day.tokens) > 0) {\\n          document.getElementById(\'kpiPeakDate\').textContent = totals.peak_day.date;\\n          document.getElementById(\'kpiPeakTokens\').textContent = fmtCompact(totals.peak_day.tokens) + \' tok\';\\n        } else {\\n          document.getElementById(\'kpiPeakDate\').textContent = \'None\';\\n          document.getElementById(\'kpiPeakTokens\').textContent = \'0 tok\';\\n        }\\n      }\\n\\n      // --- Section 3: Main Daily Chart (Chart.js 4 + Offline SVG Fallback) ---\\n      function renderDailyChart(dailyRaw) {\\n        const daily = Array.isArray(dailyRaw) ? [...dailyRaw].sort((a, b) => String(a.date).localeCompare(String(b.date))) : [];\\n        const canvas = document.getElementById(\'dailyChartCanvas\');\\n        const svgWrap = document.getElementById(\'dailySvgFallback\');\\n        const emptyEl = document.getElementById(\'dailyChartEmpty\');\\n\\n        const rangeLabel = state.days === \'all\' ? \'ALL TIME\' : `${state.days}D`;\\n        document.getElementById(\'dailyRangeBadge\').textContent = rangeLabel;\\n\\n        const subtitles = {\\n          tokens: \'Stacked daily token volume \\u2014 Input, Output, and Reasoning tokens\',\\n          cost: \'Estimated daily API equivalent spend in USD\',\\n          requests: \'Daily API request volume vs executed tool calls\'\\n        };\\n        document.getElementById(\'dailySubtitle\').textContent = subtitles[state.chartTab] || subtitles.tokens;\\n\\n        // Legend\\n        const pal = getGruvboxPalette();\\n        const legendEl = document.getElementById(\'dailyChartLegend\');\\n        if (state.chartTab === \'tokens\') {\\n          legendEl.innerHTML = `\\n            <span class=\\"chart-legend-item\\"><span class=\\"legend-dot\\" style=\\"background:${pal.input};\\"></span>Input</span>\\n            <span class=\\"chart-legend-item\\"><span class=\\"legend-dot\\" style=\\"background:${pal.output};\\"></span>Output</span>\\n            <span class=\\"chart-legend-item\\"><span class=\\"legend-dot\\" style=\\"background:${pal.thinking};\\"></span>Thinking</span>\\n          `;\\n        } else if (state.chartTab === \'cost\') {\\n          legendEl.innerHTML = `<span class=\\"chart-legend-item\\"><span class=\\"legend-dot\\" style=\\"background:${pal.cost};\\"></span>Est. Value ($)</span>`;\\n        } else {\\n          legendEl.innerHTML = `\\n            <span class=\\"chart-legend-item\\"><span class=\\"legend-dot\\" style=\\"background:${pal.input};\\"></span>API Requests</span>\\n            <span class=\\"chart-legend-item\\"><span class=\\"legend-dot\\" style=\\"background:${pal.tools};\\"></span>Tool Calls</span>\\n          `;\\n        }\\n\\n        if (daily.length === 0) {\\n          if (state.chartInstance) {\\n            state.chartInstance.destroy();\\n            state.chartInstance = null;\\n          }\\n          canvas.style.display = \'none\';\\n          svgWrap.classList.remove(\'is-visible\');\\n          emptyEl.classList.add(\'is-visible\');\\n          return;\\n        }\\n        emptyEl.classList.remove(\'is-visible\');\\n\\n        const hasChartJs = typeof window.Chart !== \'undefined\' && !window.__chartJsLoadFailed;\\n        if (hasChartJs) {\\n          svgWrap.classList.remove(\'is-visible\');\\n          canvas.style.display = \'block\';\\n          renderDailyWithChartJs(canvas, daily);\\n        } else {\\n          canvas.style.display = \'none\';\\n          svgWrap.classList.add(\'is-visible\');\\n          renderDailyWithSvgFallback(svgWrap, daily);\\n        }\\n      }\\n\\n      function getDailyInput(row) {\\n        if (row.input_tokens !== undefined && row.input_tokens !== null) return num(row.input_tokens);\\n        return isTurnBilling(state.billingMode) ? num(row.unique_input_tokens) : num(row.context_input_tokens);\\n      }\\n\\n      function renderDailyWithChartJs(canvas, daily) {\\n        if (state.chartInstance) {\\n          state.chartInstance.destroy();\\n          state.chartInstance = null;\\n        }\\n        const ctx = canvas.getContext(\'2d\');\\n        const pal = getGruvboxPalette();\\n        const labels = daily.map((d) => fmtShortDate(d.date));\\n        const monoFont = { family: NF_FONT_STACK, size: 11 };\\n\\n        let datasets = [];\\n        let scales = {};\\n\\n        if (state.chartTab === \'tokens\') {\\n          datasets = [\\n            {\\n              type: \'bar\',\\n              label: \'Input Tokens\',\\n              data: daily.map((d) => getDailyInput(d)),\\n              backgroundColor: pal.input,\\n              hoverBackgroundColor: pal.inputHover,\\n              borderRadius: 1,\\n              stack: \'tok\'\\n            },\\n            {\\n              type: \'bar\',\\n              label: \'Output Tokens\',\\n              data: daily.map((d) => num(d.output_tokens)),\\n              backgroundColor: pal.output,\\n              hoverBackgroundColor: pal.outputHover,\\n              borderRadius: 1,\\n              stack: \'tok\'\\n            },\\n            {\\n              type: \'bar\',\\n              label: \'Thinking Tokens\',\\n              data: daily.map((d) => num(d.thinking_tokens)),\\n              backgroundColor: pal.thinking,\\n              hoverBackgroundColor: pal.thinkingHover,\\n              borderRadius: { topLeft: 2, topRight: 2, bottomLeft: 0, bottomRight: 0 },\\n              stack: \'tok\'\\n            }\\n          ];\\n          scales = {\\n            x: {\\n              stacked: true,\\n              grid: { display: false },\\n              border: { color: pal.axisBorder },\\n              ticks: { color: pal.tick, font: monoFont, maxTicksLimit: 12, maxRotation: 0 }\\n            },\\n            y: {\\n              stacked: true,\\n              beginAtZero: true,\\n              border: { display: false },\\n              grid: { color: pal.grid },\\n              ticks: { color: pal.tick, font: monoFont, callback: (v) => fmtCompact(v) }\\n            }\\n          };\\n        } else if (state.chartTab === \'cost\') {\\n          const grad = ctx.createLinearGradient(0, 0, 0, 270);\\n          grad.addColorStop(0, pal.costFillTop);\\n          grad.addColorStop(1, pal.costFillBottom);\\n          datasets = [\\n            {\\n              type: \'line\',\\n              label: \'Est. Value ($)\',\\n              data: daily.map((d) => num(d.cost_usd)),\\n              borderColor: pal.cost,\\n              backgroundColor: grad,\\n              borderWidth: 2,\\n              fill: true,\\n              tension: 0.18,\\n              pointRadius: daily.length <= 35 ? 2.5 : 0,\\n              pointHoverRadius: 4.5,\\n              pointBackgroundColor: pal.cost,\\n              pointBorderColor: pal.pointBorder,\\n              pointBorderWidth: 1.5\\n            }\\n          ];\\n          scales = {\\n            x: {\\n              grid: { display: false },\\n              border: { color: pal.axisBorder },\\n              ticks: { color: pal.tick, font: monoFont, maxTicksLimit: 12, maxRotation: 0 }\\n            },\\n            y: {\\n              beginAtZero: true,\\n              border: { display: false },\\n              grid: { color: pal.grid },\\n              ticks: { color: pal.tick, font: monoFont, callback: (v) => fmtUsd(v) }\\n            }\\n          };\\n        } else {\\n          datasets = [\\n            {\\n              type: \'bar\',\\n              label: \'API Requests\',\\n              data: daily.map((d) => num(d.api_requests)),\\n              backgroundColor: pal.inputBarRgba,\\n              hoverBackgroundColor: pal.inputHover,\\n              borderRadius: 2,\\n              order: 2\\n            },\\n            {\\n              type: \'line\',\\n              label: \'Tool Calls\',\\n              data: daily.map((d) => num(d.tool_calls)),\\n              borderColor: pal.tools,\\n              backgroundColor: pal.tools,\\n              borderWidth: 2,\\n              tension: 0.18,\\n              pointRadius: daily.length <= 45 ? 2.5 : 0,\\n              pointHoverRadius: 4,\\n              order: 1\\n            }\\n          ];\\n          scales = {\\n            x: {\\n              grid: { display: false },\\n              border: { color: pal.axisBorder },\\n              ticks: { color: pal.tick, font: monoFont, maxTicksLimit: 12, maxRotation: 0 }\\n            },\\n            y: {\\n              beginAtZero: true,\\n              border: { display: false },\\n              grid: { color: pal.grid },\\n              ticks: { color: pal.tick, font: monoFont, callback: (v) => fmtCompact(v) }\\n            }\\n          };\\n        }\\n\\n        state.chartInstance = new window.Chart(ctx, {\\n          data: { labels, datasets },\\n          options: {\\n            responsive: true,\\n            maintainAspectRatio: false,\\n            animation: { duration: 220 },\\n            interaction: { mode: \'index\', intersect: false },\\n            plugins: {\\n              legend: { display: false },\\n              tooltip: {\\n                backgroundColor: pal.tooltipBg,\\n                titleColor: pal.tooltipTitle,\\n                bodyColor: pal.tooltipBody,\\n                borderColor: pal.tooltipBorder,\\n                borderWidth: 1,\\n                padding: 10,\\n                cornerRadius: 3,\\n                titleFont: { family: NF_FONT_STACK, size: 11.5, weight: \'700\' },\\n                bodyFont: { family: NF_FONT_STACK, size: 11 },\\n                callbacks: {\\n                  title: (items) => {\\n                    const idx = items[0]?.dataIndex ?? 0;\\n                    return fmtDatePretty(daily[idx]?.date);\\n                  },\\n                  label: (item) => {\\n                    const val = item.raw;\\n                    if (state.chartTab === \'cost\') return ` ${item.dataset.label}: ${fmtUsd(val)}`;\\n                    return ` ${item.dataset.label}: ${fmtInt(val)}`;\\n                  },\\n                  afterBody: (items) => {\\n                    const idx = items[0]?.dataIndex ?? 0;\\n                    const row = daily[idx];\\n                    if (!row) return [];\\n                    if (state.chartTab === \'tokens\') {\\n                      return [` Total: ${fmtInt(row.total_tokens)}  (${fmtUsd(row.cost_usd)})`];\\n                    }\\n                    return [];\\n                  }\\n                }\\n              }\\n            },\\n            scales\\n          }\\n        });\\n      }\\n\\n      // --- Custom Responsive SVG Chart Renderer (Offline Fallback) ---\\n      function renderDailyWithSvgFallback(container, daily) {\\n        const rect = container.getBoundingClientRect();\\n        const width = Math.max(360, Math.floor(rect.width || 920));\\n        const height = Math.max(220, Math.floor(rect.height || 278));\\n        const padL = 56, padR = 16, padT = 14, padB = 28;\\n        const plotW = width - padL - padR;\\n        const plotH = height - padT - padB;\\n        const n = daily.length;\\n\\n        let maxVal = 0;\\n        daily.forEach((d) => {\\n          if (state.chartTab === \'tokens\') {\\n            maxVal = Math.max(maxVal, getDailyInput(d) + num(d.output_tokens) + num(d.thinking_tokens));\\n          } else if (state.chartTab === \'cost\') {\\n            maxVal = Math.max(maxVal, num(d.cost_usd));\\n          } else {\\n            maxVal = Math.max(maxVal, num(d.api_requests), num(d.tool_calls));\\n          }\\n        });\\n        if (maxVal <= 0) maxVal = 1;\\n        const yMax = maxVal * 1.08;\\n\\n        const pal = getGruvboxPalette();\\n        const yTicks = 4;\\n        let gridSvg = \'\';\\n        for (let i = 0; i <= yTicks; i++) {\\n          const ratio = i / yTicks;\\n          const y = (padT + plotH - ratio * plotH).toFixed(1);\\n          const tickVal = yMax * ratio;\\n          const label = state.chartTab === \'cost\' ? fmtUsd(tickVal) : fmtCompact(tickVal);\\n          gridSvg += `<line x1=\\"${padL}\\" y1=\\"${y}\\" x2=\\"${width - padR}\\" y2=\\"${y}\\" stroke=\\"${pal.grid}\\" stroke-width=\\"1\\" />`;\\n          gridSvg += `<text x=\\"${padL - 8}\\" y=\\"${Number(y) + 3.5}\\" text-anchor=\\"end\\" fill=\\"${pal.tick}\\" font-family=\\"JetBrainsMono Nerd Font, JetBrains Mono, monospace\\" font-size=\\"10.5\\">${escapeHtml(label)}</text>`;\\n        }\\n\\n        const stepX = plotW / Math.max(1, n);\\n        const labelStride = Math.max(1, Math.ceil(n / 10));\\n        let xLabelsSvg = \'\';\\n        daily.forEach((d, idx) => {\\n          if (idx % labelStride === 0 || idx === n - 1) {\\n            const cx = (padL + idx * stepX + stepX / 2).toFixed(1);\\n            xLabelsSvg += `<text x=\\"${cx}\\" y=\\"${height - 7}\\" text-anchor=\\"middle\\" fill=\\"${pal.tick}\\" font-family=\\"JetBrainsMono Nerd Font, JetBrains Mono, monospace\\" font-size=\\"10\\">${escapeHtml(fmtShortDate(d.date))}</text>`;\\n          }\\n        });\\n\\n        let seriesSvg = \'\';\\n        if (state.chartTab === \'tokens\') {\\n          const barW = Math.max(2, Math.min(28, stepX * 0.72));\\n          daily.forEach((d, idx) => {\\n            const x = padL + idx * stepX + (stepX - barW) / 2;\\n            const vIn = getDailyInput(d);\\n            const vOut = num(d.output_tokens);\\n            const vThk = num(d.thinking_tokens);\\n            const hIn = (vIn / yMax) * plotH;\\n            const hOut = (vOut / yMax) * plotH;\\n            const hThk = (vThk / yMax) * plotH;\\n            const yIn = padT + plotH - hIn;\\n            const yOut = yIn - hOut;\\n            const yThk = yOut - hThk;\\n            if (hIn > 0) seriesSvg += `<rect x=\\"${x.toFixed(1)}\\" y=\\"${yIn.toFixed(1)}\\" width=\\"${barW.toFixed(1)}\\" height=\\"${hIn.toFixed(1)}\\" fill=\\"${pal.input}\\" />`;\\n            if (hOut > 0) seriesSvg += `<rect x=\\"${x.toFixed(1)}\\" y=\\"${yOut.toFixed(1)}\\" width=\\"${barW.toFixed(1)}\\" height=\\"${hOut.toFixed(1)}\\" fill=\\"${pal.output}\\" />`;\\n            if (hThk > 0) seriesSvg += `<rect x=\\"${x.toFixed(1)}\\" y=\\"${yThk.toFixed(1)}\\" width=\\"${barW.toFixed(1)}\\" height=\\"${hThk.toFixed(1)}\\" fill=\\"${pal.thinking}\\" rx=\\"1\\" />`;\\n          });\\n        } else if (state.chartTab === \'cost\') {\\n          const pts = daily.map((d, idx) => {\\n            const cx = padL + (n === 1 ? plotW / 2 : (idx / (n - 1)) * plotW);\\n            const cy = padT + plotH - (num(d.cost_usd) / yMax) * plotH;\\n            return [cx, cy];\\n          });\\n          const linePath = pts.map((p, i) => (i === 0 ? \'M\' : \'L\') + p[0].toFixed(1) + \',\' + p[1].toFixed(1)).join(\' \');\\n          const areaPath = `${linePath} L${pts[pts.length - 1][0].toFixed(1)},${(padT + plotH).toFixed(1)} L${pts[0][0].toFixed(1)},${(padT + plotH).toFixed(1)} Z`;\\n          seriesSvg += `\\n            <defs>\\n              <linearGradient id=\\"svgCostGrad\\" x1=\\"0\\" y1=\\"0\\" x2=\\"0\\" y2=\\"1\\">\\n                <stop offset=\\"0%\\" stop-color=\\"${pal.cost}\\" stop-opacity=\\"0.26\\" />\\n                <stop offset=\\"100%\\" stop-color=\\"${pal.cost}\\" stop-opacity=\\"0.0\\" />\\n              </linearGradient>\\n            </defs>\\n            <path d=\\"${areaPath}\\" fill=\\"url(#svgCostGrad)\\" />\\n            <path d=\\"${linePath}\\" fill=\\"none\\" stroke=\\"${pal.cost}\\" stroke-width=\\"2\\" />\\n          `;\\n          if (n <= 40) {\\n            pts.forEach((p) => {\\n              seriesSvg += `<circle cx=\\"${p[0].toFixed(1)}\\" cy=\\"${p[1].toFixed(1)}\\" r=\\"2.6\\" fill=\\"${pal.cost}\\" stroke=\\"${pal.pointBorder}\\" stroke-width=\\"1.2\\" />`;\\n            });\\n          }\\n        } else {\\n          const barW = Math.max(2, Math.min(26, stepX * 0.68));\\n          const toolPts = [];\\n          daily.forEach((d, idx) => {\\n            const cx = padL + idx * stepX + stepX / 2;\\n            const x = cx - barW / 2;\\n            const hReq = (num(d.api_requests) / yMax) * plotH;\\n            const yReq = padT + plotH - hReq;\\n            if (hReq > 0) {\\n              seriesSvg += `<rect x=\\"${x.toFixed(1)}\\" y=\\"${yReq.toFixed(1)}\\" width=\\"${barW.toFixed(1)}\\" height=\\"${hReq.toFixed(1)}\\" fill=\\"${pal.inputBarRgba}\\" rx=\\"1.5\\" />`;\\n            }\\n            const cyTool = padT + plotH - (num(d.tool_calls) / yMax) * plotH;\\n            toolPts.push([cx, cyTool]);\\n          });\\n          if (toolPts.length > 1) {\\n            const tPath = toolPts.map((p, i) => (i === 0 ? \'M\' : \'L\') + p[0].toFixed(1) + \',\' + p[1].toFixed(1)).join(\' \');\\n            seriesSvg += `<path d=\\"${tPath}\\" fill=\\"none\\" stroke=\\"${pal.tools}\\" stroke-width=\\"2\\" />`;\\n          }\\n          if (n <= 45) {\\n            toolPts.forEach((p) => {\\n              seriesSvg += `<circle cx=\\"${p[0].toFixed(1)}\\" cy=\\"${p[1].toFixed(1)}\\" r=\\"2.5\\" fill=\\"${pal.tools}\\" />`;\\n            });\\n          }\\n        }\\n\\n        let hitSvg = \'\';\\n        daily.forEach((_, idx) => {\\n          const x = padL + idx * stepX;\\n          hitSvg += `<rect class=\\"svg-day-hit\\" data-idx=\\"${idx}\\" x=\\"${x.toFixed(1)}\\" y=\\"${padT}\\" width=\\"${stepX.toFixed(1)}\\" height=\\"${plotH}\\" fill=\\"transparent\\" style=\\"cursor:crosshair;\\" />`;\\n        });\\n\\n        container.innerHTML = `\\n          <svg width=\\"100%\\" height=\\"100%\\" viewBox=\\"0 0 ${width} ${height}\\" preserveAspectRatio=\\"none\\">\\n            ${gridSvg}\\n            <line x1=\\"${padL}\\" y1=\\"${padT + plotH}\\" x2=\\"${width - padR}\\" y2=\\"${padT + plotH}\\" stroke=\\"${pal.axisBorder}\\" stroke-width=\\"1\\" />\\n            ${seriesSvg}\\n            ${xLabelsSvg}\\n            ${hitSvg}\\n          </svg>\\n        `;\\n\\n        container.querySelectorAll(\'.svg-day-hit\').forEach((el) => {\\n          el.addEventListener(\'mouseenter\', (ev) => {\\n            const d = daily[Number(el.dataset.idx)];\\n            if (!d) return;\\n            let rows = \'\';\\n            if (state.chartTab === \'tokens\') {\\n              rows = `\\n                <div class=\\"tt-row\\"><span class=\\"tt-label\\"><span class=\\"legend-dot\\" style=\\"background:${pal.input};\\"></span>Input</span><span class=\\"tt-val\\">${fmtInt(getDailyInput(d))}</span></div>\\n                <div class=\\"tt-row\\"><span class=\\"tt-label\\"><span class=\\"legend-dot\\" style=\\"background:${pal.output};\\"></span>Output</span><span class=\\"tt-val\\">${fmtInt(d.output_tokens)}</span></div>\\n                <div class=\\"tt-row\\"><span class=\\"tt-label\\"><span class=\\"legend-dot\\" style=\\"background:${pal.thinking};\\"></span>Thinking</span><span class=\\"tt-val\\">${fmtInt(d.thinking_tokens)}</span></div>\\n                <div class=\\"tt-row\\" style=\\"border-top:1px dashed var(--border-card);margin-top:3px;padding-top:3px;\\"><span class=\\"tt-label\\">Total</span><span class=\\"tt-val\\">${fmtInt(d.total_tokens)}</span></div>\\n              `;\\n            } else if (state.chartTab === \'cost\') {\\n              rows = `<div class=\\"tt-row\\"><span class=\\"tt-label\\">Est. Value</span><span class=\\"tt-val\\">${fmtUsd(d.cost_usd)}</span></div>`;\\n            } else {\\n              rows = `\\n                <div class=\\"tt-row\\"><span class=\\"tt-label\\">API Requests</span><span class=\\"tt-val\\">${fmtInt(d.api_requests)}</span></div>\\n                <div class=\\"tt-row\\"><span class=\\"tt-label\\">Tool Calls</span><span class=\\"tt-val\\">${fmtInt(d.tool_calls)}</span></div>\\n              `;\\n            }\\n            showTooltip(`<div class=\\"tt-title\\">${escapeHtml(fmtDatePretty(d.date))}</div>${rows}`, ev.clientX, ev.clientY);\\n          });\\n          el.addEventListener(\'mousemove\', (ev) => positionTooltip(ev.clientX, ev.clientY));\\n          el.addEventListener(\'mouseleave\', hideTooltip);\\n        });\\n      }\\n\\n      // --- Section 4: 365-Day Activity Heatmap ---\\n      function renderHeatmap(heatmapRaw, totals) {\\n        let items = Array.isArray(heatmapRaw) ? [...heatmapRaw] : [];\\n        if (items.length === 0) {\\n          const now = new Date();\\n          for (let i = 364; i >= 0; i--) {\\n            const d = new Date(now.getFullYear(), now.getMonth(), now.getDate() - i);\\n            items.push({ date: d.toISOString().slice(0, 10), total_tokens: 0, api_requests: 0, cost_usd: 0, level: 0 });\\n          }\\n        }\\n        items.sort((a, b) => String(a.date).localeCompare(String(b.date)));\\n        if (items.length > 371) items = items.slice(-371);\\n\\n        const firstParts = String(items[0].date).split(\'-\');\\n        const firstDate = firstParts.length === 3\\n          ? new Date(Number(firstParts[0]), Number(firstParts[1]) - 1, Number(firstParts[2]))\\n          : new Date();\\n        const startDow = Number.isNaN(firstDate.getTime()) ? 0 : firstDate.getDay(); // 0=Sun..6=Sat\\n\\n        const padded = [];\\n        for (let p = 0; p < startDow; p++) padded.push(null);\\n        items.forEach((it) => padded.push(it));\\n        while (padded.length < 53 * 7) padded.push(null);\\n        const totalWeeks = Math.ceil(padded.length / 7);\\n\\n        const weeksEl = document.getElementById(\'heatmapWeeks\');\\n        weeksEl.style.gridTemplateColumns = `repeat(${totalWeeks}, minmax(11px, 1fr))`;\\n\\n        const monthNames = [\'Jan\', \'Feb\', \'Mar\', \'Apr\', \'May\', \'Jun\', \'Jul\', \'Aug\', \'Sep\', \'Oct\', \'Nov\', \'Dec\'];\\n        const monthPositions = [];\\n        let lastMonth = -1;\\n\\n        let weeksHtml = \'\';\\n        for (let w = 0; w < totalWeeks; w++) {\\n          let colHtml = \'<div class=\\"heatmap-week-col\\">\';\\n          for (let dow = 0; dow < 7; dow++) {\\n            const cell = padded[w * 7 + dow];\\n            if (!cell) {\\n              colHtml += \'<div class=\\"hm-cell hm-pad\\"></div>\';\\n            } else {\\n              const parts = String(cell.date).split(\'-\');\\n              if (parts.length === 3) {\\n                const mIdx = Number(parts[1]) - 1;\\n                const dayNum = Number(parts[2]);\\n                if (mIdx !== lastMonth && (dayNum <= 7 || w === 0)) {\\n                  if (monthPositions.length === 0 || w - monthPositions[monthPositions.length - 1].week >= 3) {\\n                    monthPositions.push({ week: w, label: monthNames[mIdx] || \'\' });\\n                  }\\n                  lastMonth = mIdx;\\n                }\\n              }\\n              const lvl = Math.min(4, Math.max(0, Math.round(num(cell.level))));\\n              colHtml += `<div class=\\"hm-cell hm-lvl-${lvl}\\" data-date=\\"${escapeHtml(cell.date)}\\" data-tokens=\\"${num(cell.total_tokens)}\\" data-reqs=\\"${num(cell.api_requests)}\\" data-cost=\\"${num(cell.cost_usd)}\\"></div>`;\\n            }\\n          }\\n          colHtml += \'</div>\';\\n          weeksHtml += colHtml;\\n        }\\n        weeksEl.innerHTML = weeksHtml;\\n\\n        const monthsEl = document.getElementById(\'heatmapMonths\');\\n        monthsEl.innerHTML = monthPositions\\n          .map((mp) => `<span class=\\"heatmap-month-label\\" style=\\"left:${((mp.week / totalWeeks) * 100).toFixed(2)}%;\\">${escapeHtml(mp.label)}</span>`)\\n          .join(\'\');\\n\\n        // Stats & Streak\\n        let yearTokens = 0;\\n        let activeCount = 0;\\n        let curStreak = 0;\\n        let maxStreak = 0;\\n        items.forEach((it) => {\\n          const t = num(it.total_tokens);\\n          yearTokens += t;\\n          if (t > 0 || num(it.api_requests) > 0) {\\n            activeCount++;\\n            curStreak++;\\n            if (curStreak > maxStreak) maxStreak = curStreak;\\n          } else {\\n            curStreak = 0;\\n          }\\n        });\\n\\n        const shownActive = totals.active_days !== undefined ? num(totals.active_days) : activeCount;\\n        document.getElementById(\'hmActiveDays\').textContent = fmtInt(shownActive);\\n        document.getElementById(\'hmYearTokens\').textContent = fmtCompact(yearTokens);\\n        document.getElementById(\'hmLongestStreak\').textContent = `${maxStreak}d`;\\n        if (items.length > 0) {\\n          document.getElementById(\'heatmapYearSpan\').textContent = `${items[0].date} \\u2192 ${items[items.length - 1].date}`;\\n        }\\n      }\\n\\n      // --- Section 5A: Hour-of-Day Activity ---\\n      function renderHourly(hourlyRaw) {\\n        const byHour = new Array(24).fill(null).map((_, h) => ({ hour: h, total_tokens: 0, api_requests: 0 }));\\n        if (Array.isArray(hourlyRaw)) {\\n          hourlyRaw.forEach((item) => {\\n            const h = Number(item.hour);\\n            if (h >= 0 && h < 24) {\\n              byHour[h] = {\\n                hour: h,\\n                total_tokens: num(item.total_tokens),\\n                api_requests: num(item.api_requests)\\n              };\\n            }\\n          });\\n        }\\n\\n        const metricKey = state.hourlyMode === \'requests\' ? \'api_requests\' : \'total_tokens\';\\n        let maxVal = 0;\\n        let sumVal = 0;\\n        let peakHour = 0;\\n        byHour.forEach((h) => {\\n          const v = h[metricKey];\\n          sumVal += v;\\n          if (v > maxVal) {\\n            maxVal = v;\\n            peakHour = h.hour;\\n          }\\n        });\\n\\n        const pad2 = (n) => String(n).padStart(2, \'0\');\\n        document.getElementById(\'hourlyPeakTag\').textContent =\\n          maxVal > 0 ? `Peak ${pad2(peakHour)}:00` : \'No activity\';\\n        document.getElementById(\'hourlyPeakShare\').textContent =\\n          sumVal > 0 ? `Peak share: ${((maxVal / sumVal) * 100).toFixed(1)}%` : \'0%\';\\n\\n        const barsEl = document.getElementById(\'hourlyBars\');\\n        barsEl.innerHTML = byHour\\n          .map((h) => {\\n            const v = h[metricKey];\\n            const pct = maxVal > 0 ? Math.max(v > 0 ? 4 : 1.5, (v / maxVal) * 100) : 1.5;\\n            const isPeak = maxVal > 0 && v === maxVal;\\n            const cls = v === 0 ? \'hour-bar is-zero\' : isPeak ? \'hour-bar is-peak\' : \'hour-bar\';\\n            return `\\n              <div class=\\"hour-col\\" data-hour=\\"${h.hour}\\" data-tokens=\\"${h.total_tokens}\\" data-reqs=\\"${h.api_requests}\\">\\n                <div class=\\"${cls}\\" style=\\"height:${pct.toFixed(1)}%;\\"></div>\\n              </div>\\n            `;\\n          })\\n          .join(\'\');\\n\\n        const axisEl = document.getElementById(\'hourlyAxis\');\\n        axisEl.innerHTML = byHour\\n          .map((h) => `<span>${h.hour % 3 === 0 ? pad2(h.hour) : \'\'}</span>`)\\n          .join(\'\');\\n      }\\n\\n      // --- Section 5B: Tool Invocations Breakdown ---\\n      function renderTools(toolsRaw, totals) {\\n        const tools = Array.isArray(toolsRaw) ? [...toolsRaw].sort((a, b) => num(b.count) - num(a.count)) : [];\\n        const totalCalls = totals.tool_calls !== undefined\\n          ? num(totals.tool_calls)\\n          : tools.reduce((acc, t) => acc + num(t.count), 0);\\n        document.getElementById(\'toolsTotalBadge\').textContent = `${fmtInt(totalCalls)} calls`;\\n\\n        const body = document.getElementById(\'toolsListBody\');\\n        if (tools.length === 0) {\\n          body.innerHTML = \'<div class=\\"empty-state\\">No tool invocations recorded in selected window</div>\';\\n          return;\\n        }\\n\\n        const maxShare = Math.max(1, ...tools.map((t) => num(t.share_pct)));\\n        body.innerHTML = tools\\n          .map((t, idx) => {\\n            const share = num(t.share_pct, totalCalls > 0 ? (num(t.count) / totalCalls) * 100 : 0);\\n            const barWidth = Math.min(100, Math.max(2, (share / maxShare) * 100));\\n            return `\\n              <div class=\\"tool-row\\">\\n                <div class=\\"tool-row-header\\">\\n                  <div class=\\"tool-name-wrap\\">\\n                    <span class=\\"tool-rank\\">${String(idx + 1).padStart(2, \'0\')}</span>\\n                    <span class=\\"nf-icon tool-glyph\\">${toolNerdGlyph(t.name)}</span>\\n                    <span class=\\"tool-name\\" title=\\"${escapeHtml(t.name)}\\">${escapeHtml(t.name)}</span>\\n                  </div>\\n                  <div class=\\"tool-metrics\\">\\n                    <span class=\\"tool-count\\">${fmtInt(t.count)}</span>\\n                    <span class=\\"tool-share\\">${dec1Fmt.format(share)}%</span>\\n                  </div>\\n                </div>\\n                <div class=\\"tool-track\\">\\n                  <div class=\\"tool-fill\\" style=\\"width:${barWidth.toFixed(1)}%;\\"></div>\\n                </div>\\n              </div>\\n            `;\\n          })\\n          .join(\'\');\\n      }\\n\\n      // --- Section 6: Recent Conversations Table ---\\n      function getFilteredSortedConversations() {\\n        const raw = (state.data && Array.isArray(state.data.conversations)) ? state.data.conversations : [];\\n        const q = state.searchQuery.trim().toLowerCase();\\n        const filtered = q\\n          ? raw.filter((c) =>\\n              String(c.title || \'\').toLowerCase().includes(q) ||\\n              String(c.id || \'\').toLowerCase().includes(q)\\n            )\\n          : [...raw];\\n\\n        const dir = state.sortDir === \'asc\' ? 1 : -1;\\n        const key = state.sortKey;\\n\\n        filtered.sort((a, b) => {\\n          if (key === \'title\') {\\n            return String(a.title || a.id || \'\').localeCompare(String(b.title || b.id || \'\')) * dir;\\n          }\\n          if (key === \'updated_at\') {\\n            const ta = parseTimestamp(a.updated_at)?.getTime() ?? 0;\\n            const tb = parseTimestamp(b.updated_at)?.getTime() ?? 0;\\n            return (ta - tb) * dir;\\n          }\\n          if (key === \'input_tokens\') {\\n            return (resolveConvInputTokens(a) - resolveConvInputTokens(b)) * dir;\\n          }\\n          return (num(a[key]) - num(b[key])) * dir;\\n        });\\n\\n        return { filtered, total: raw.length };\\n      }\\n\\n      function renderConversationsTable() {\\n        const { filtered, total } = getFilteredSortedConversations();\\n        document.getElementById(\'convCountBadge\').textContent = fmtInt(total);\\n\\n        // Update header sort indicators\\n        document.querySelectorAll(\'.conv-table thead th.sortable\').forEach((th) => {\\n          const isSorted = th.dataset.sort === state.sortKey;\\n          th.classList.toggle(\'is-sorted\', isSorted);\\n          const arrow = th.querySelector(\'.sort-arrow\');\\n          if (arrow) arrow.textContent = isSorted && state.sortDir === \'asc\' ? \'\\u2191\' : \'\\u2193\';\\n        });\\n\\n        const tbody = document.getElementById(\'convTableBody\');\\n        const showMoreBtn = document.getElementById(\'convShowMoreBtn\');\\n\\n        if (filtered.length === 0) {\\n          tbody.innerHTML = `<tr><td colspan=\\"9\\" class=\\"empty-state\\">${state.searchQuery ? \'No conversations match your filter.\' : \'No conversations recorded in this window.\'}</td></tr>`;\\n          document.getElementById(\'convFooterSummary\').textContent = `Showing 0 of ${fmtInt(total)} conversations`;\\n          showMoreBtn.style.display = \'none\';\\n          return;\\n        }\\n\\n        const slice = filtered.slice(0, state.visibleLimit);\\n        tbody.innerHTML = slice\\n          .map((c) => {\\n            const idStr = String(c.id || \'\');\\n            const shortId = idStr.length > 14 ? idStr.slice(0, 8) + \'\\u2026\' + idStr.slice(-4) : idStr;\\n            const title = c.title && String(c.title).trim() ? c.title : \'Untitled Session\';\\n            const inTok = resolveConvInputTokens(c);\\n            const selectedCls = String(state.selectedConvId) === idStr ? \' class=\\"is-selected\\"\' : \'\';\\n            return `\\n              <tr data-conv-id=\\"${escapeHtml(idStr)}\\" tabindex=\\"0\\"${selectedCls}>\\n                <td>\\n                  <div class=\\"conv-title-cell\\">\\n                    <span class=\\"conv-title-text\\" title=\\"${escapeHtml(title)}\\">${escapeHtml(title)}</span>\\n                    <span class=\\"conv-id-short\\">${escapeHtml(shortId)}${num(c.tool_calls) > 0 ? \' \\u00b7 \' + fmtInt(c.tool_calls) + \' tools\' : \'\'}</span>\\n                  </div>\\n                </td>\\n                <td class=\\"mono\\" style=\\"font-size:11.5px;color:var(--text-muted);\\">${escapeHtml(fmtTimestamp(c.updated_at))}</td>\\n                <td class=\\"num-col\\">${fmtInt(c.api_requests)}</td>\\n                <td class=\\"num-col tok-input\\">${fmtCompact(inTok)}</td>\\n                <td class=\\"num-col tok-output\\">${fmtCompact(c.output_tokens)}</td>\\n                <td class=\\"num-col tok-thinking\\">${fmtCompact(c.thinking_tokens)}</td>\\n                <td class=\\"num-col tok-total\\">${fmtCompact(c.total_tokens)}</td>\\n                <td class=\\"num-col cost-cell\\">${fmtUsd(c.cost_usd)}</td>\\n                <td class=\\"num-col\\">${escapeHtml(fmtEnergyShort(c.electricity_wh))}</td>\\n              </tr>\\n            `;\\n          })\\n          .join(\'\');\\n\\n        document.getElementById(\'convFooterSummary\').textContent =\\n          `Showing ${fmtInt(slice.length)} of ${fmtInt(filtered.length)} matching (${fmtInt(total)} total)`;\\n\\n        if (filtered.length > state.visibleLimit) {\\n          showMoreBtn.style.display = \'inline-flex\';\\n          showMoreBtn.textContent = `Show more (${fmtInt(filtered.length - state.visibleLimit)} remaining)`;\\n        } else {\\n          showMoreBtn.style.display = \'none\';\\n        }\\n      }\\n\\n      // --- Conversation Detail Slide-over Drawer ---\\n      function openDrawer(convId) {\\n        const list = getFilteredSortedConversations().filtered;\\n        const conv = list.find((c) => String(c.id) === String(convId)) ||\\n          (state.data?.conversations || []).find((c) => String(c.id) === String(convId));\\n        if (!conv) return;\\n\\n        state.selectedConvId = String(conv.id);\\n        renderConversationsTable();\\n        renderDrawerContent(conv);\\n\\n        document.getElementById(\'drawerBackdrop\').classList.add(\'is-open\');\\n        const panel = document.getElementById(\'drawerPanel\');\\n        panel.classList.add(\'is-open\');\\n        panel.setAttribute(\'aria-hidden\', \'false\');\\n      }\\n\\n      function closeDrawer() {\\n        state.selectedConvId = null;\\n        document.getElementById(\'drawerBackdrop\').classList.remove(\'is-open\');\\n        const panel = document.getElementById(\'drawerPanel\');\\n        panel.classList.remove(\'is-open\');\\n        panel.setAttribute(\'aria-hidden\', \'true\');\\n        renderConversationsTable();\\n      }\\n\\n      function stepDrawer(delta) {\\n        if (!state.selectedConvId) return;\\n        const list = getFilteredSortedConversations().filtered;\\n        const idx = list.findIndex((c) => String(c.id) === String(state.selectedConvId));\\n        if (idx === -1) return;\\n        const next = list[idx + delta];\\n        if (next) openDrawer(next.id);\\n      }\\n\\n      function renderDrawerContent(conv) {\\n        const list = getFilteredSortedConversations().filtered;\\n        const idx = list.findIndex((c) => String(c.id) === String(conv.id));\\n        document.getElementById(\'drawerPrevBtn\').disabled = idx <= 0;\\n        document.getElementById(\'drawerNextBtn\').disabled = idx === -1 || idx >= list.length - 1;\\n\\n        const title = conv.title && String(conv.title).trim() ? conv.title : \'Untitled Session\';\\n        document.getElementById(\'drawerConvTitle\').textContent = title;\\n\\n        const inTok = resolveConvInputTokens(conv);\\n        const ctxTok = num(conv.context_input_tokens, inTok);\\n        const uniqTok = num(conv.unique_input_tokens, inTok);\\n        const outTok = num(conv.output_tokens);\\n        const thinkTok = num(conv.thinking_tokens);\\n        const totTok = num(conv.total_tokens, inTok + outTok + thinkTok);\\n        const denom = Math.max(1, inTok + outTok + thinkTok);\\n\\n        const reqs = num(conv.api_requests);\\n        const tools = num(conv.tool_calls);\\n        const wh = num(conv.electricity_wh);\\n        const co2g = conv.co2_grams !== undefined ? num(conv.co2_grams) : (wh / 1000) * 385;\\n\\n        const startDt = parseTimestamp(conv.started_at);\\n        const endDt = parseTimestamp(conv.updated_at);\\n        let durationStr = \'\\u2014\';\\n        if (startDt && endDt && endDt >= startDt) {\\n          const mins = Math.round((endDt - startDt) / 60000);\\n          durationStr = mins < 1 ? \'< 1 min\' : mins < 60 ? `${mins}m` : `${Math.floor(mins / 60)}h ${mins % 60}m`;\\n        }\\n\\n        const body = document.getElementById(\'drawerBody\');\\n        body.innerHTML = `\\n          <div class=\\"drawer-section\\">\\n            <span class=\\"drawer-section-title\\">Conversation ID</span>\\n            <div class=\\"id-box\\">\\n              <span class=\\"id-box-code\\">${escapeHtml(conv.id)}</span>\\n              <button type=\\"button\\" id=\\"copyConvIdBtn\\" class=\\"copy-btn\\" data-copy-id=\\"${escapeHtml(conv.id)}\\">\\n                <span>Copy ID</span>\\n              </button>\\n            </div>\\n          </div>\\n\\n          <div class=\\"drawer-section\\">\\n            <span class=\\"drawer-section-title\\">Session Timeline</span>\\n            <div class=\\"detail-grid\\">\\n              <div class=\\"detail-tile\\">\\n                <span class=\\"detail-tile-label\\">Started At</span>\\n                <span class=\\"detail-tile-val\\" style=\\"font-size:12.5px;\\">${escapeHtml(fmtTimestamp(conv.started_at))}</span>\\n                <span class=\\"detail-tile-sub\\">Duration: ${escapeHtml(durationStr)}</span>\\n              </div>\\n              <div class=\\"detail-tile\\">\\n                <span class=\\"detail-tile-label\\">Last Updated</span>\\n                <span class=\\"detail-tile-val\\" style=\\"font-size:12.5px;\\">${escapeHtml(fmtTimestamp(conv.updated_at))}</span>\\n                <span class=\\"detail-tile-sub\\">${fmtInt(reqs)} turns \\u00b7 ${fmtInt(tools)} tool calls</span>\\n              </div>\\n            </div>\\n          </div>\\n\\n          <div class=\\"drawer-section\\">\\n            <span class=\\"drawer-section-title\\">Token Composition</span>\\n            <div class=\\"seg-bar\\" style=\\"height:8px;\\">\\n              <div class=\\"seg-bar-piece\\" style=\\"width:${((inTok / denom) * 100).toFixed(2)}%;background:var(--indigo-brand);\\"></div>\\n              <div class=\\"seg-bar-piece\\" style=\\"width:${((outTok / denom) * 100).toFixed(2)}%;background:var(--cyan-output);\\"></div>\\n              <div class=\\"seg-bar-piece\\" style=\\"width:${((thinkTok / denom) * 100).toFixed(2)}%;background:var(--violet-thinking);\\"></div>\\n            </div>\\n            <div class=\\"comp-list\\">\\n              <div class=\\"comp-row\\">\\n                <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--indigo-brand);\\"></span>Effective Input (${isTurnBilling(state.billingMode) ? \'Turn\' : \'Context\'})</span>\\n                <span class=\\"mono\\">${fmtInt(inTok)} <span style=\\"color:var(--text-subtle);\\">(${((inTok / denom) * 100).toFixed(1)}%)</span></span>\\n              </div>\\n              <div class=\\"comp-row\\" style=\\"color:var(--text-muted);font-size:11.5px;\\">\\n                <span style=\\"padding-left:11px;\\">\\u21b3 Cumulative Context Input</span>\\n                <span class=\\"mono\\">${fmtInt(ctxTok)}</span>\\n              </div>\\n              <div class=\\"comp-row\\" style=\\"color:var(--text-muted);font-size:11.5px;\\">\\n                <span style=\\"padding-left:11px;\\">\\u21b3 Unique Turn Input</span>\\n                <span class=\\"mono\\">${fmtInt(uniqTok)}</span>\\n              </div>\\n              <div class=\\"comp-row\\">\\n                <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--cyan-output);\\"></span>Output Tokens</span>\\n                <span class=\\"mono\\">${fmtInt(outTok)} <span style=\\"color:var(--text-subtle);\\">(${((outTok / denom) * 100).toFixed(1)}%)</span></span>\\n              </div>\\n              <div class=\\"comp-row\\">\\n                <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--violet-thinking);\\"></span>Thinking / Reasoning</span>\\n                <span class=\\"mono\\">${fmtInt(thinkTok)} <span style=\\"color:var(--text-subtle);\\">(${((thinkTok / denom) * 100).toFixed(1)}%)</span></span>\\n              </div>\\n              <div class=\\"comp-row comp-total\\">\\n                <span>Total Session Tokens</span>\\n                <span class=\\"mono\\">${fmtInt(totTok)}</span>\\n              </div>\\n            </div>\\n          </div>\\n\\n          <div class=\\"drawer-section\\">\\n            <span class=\\"drawer-section-title\\">Cost, Throughput &amp; Environmental Footprint</span>\\n            <div class=\\"detail-grid\\">\\n              <div class=\\"detail-tile\\">\\n                <span class=\\"detail-tile-label\\">Est. API Cost</span>\\n                <span class=\\"detail-tile-val\\" style=\\"color:var(--gb-yellow);\\">${fmtUsd(conv.cost_usd)}</span>\\n                <span class=\\"detail-tile-sub\\">${reqs > 0 ? fmtUsd(num(conv.cost_usd) / reqs) + \' avg / req\' : \'\\u2014\'}</span>\\n              </div>\\n              <div class=\\"detail-tile\\">\\n                <span class=\\"detail-tile-label\\">API Requests &amp; Tools</span>\\n                <span class=\\"detail-tile-val\\">${fmtInt(reqs)} reqs</span>\\n                <span class=\\"detail-tile-sub\\">${fmtInt(tools)} tool calls \\u00b7 ${reqs > 0 ? fmtCompact(totTok / reqs) : \'0\'} tok/req</span>\\n              </div>\\n              <div class=\\"detail-tile\\">\\n                <span class=\\"detail-tile-label\\">Electricity Used</span>\\n                <span class=\\"detail-tile-val\\" style=\\"color:var(--gb-green);\\">${escapeHtml(fmtEnergyShort(wh))}</span>\\n                <span class=\\"detail-tile-sub\\">${(wh / 1000).toFixed(4)} kWh</span>\\n              </div>\\n              <div class=\\"detail-tile\\">\\n                <span class=\\"detail-tile-label\\">Carbon Footprint</span>\\n                <span class=\\"detail-tile-val\\" style=\\"color:var(--gb-green);\\">${escapeHtml(fmtCarbon(co2g).primary)}</span>\\n                <span class=\\"detail-tile-sub\\">@ 385g CO\\u2082e / kWh</span>\\n              </div>\\n            </div>\\n          </div>\\n        `;\\n\\n        const copyBtn = document.getElementById(\'copyConvIdBtn\');\\n        if (copyBtn) {\\n          copyBtn.addEventListener(\'click\', () => copyTextToClipboard(String(conv.id), copyBtn));\\n        }\\n      }\\n\\n      function copyTextToClipboard(text, btnEl) {\\n        const onDone = () => {\\n          if (!btnEl) return;\\n          btnEl.classList.add(\'is-copied\');\\n          btnEl.innerHTML = \'<span>\\u2713 Copied</span>\';\\n          setTimeout(() => {\\n            btnEl.classList.remove(\'is-copied\');\\n            btnEl.innerHTML = \'<span>Copy ID</span>\';\\n          }, 1600);\\n        };\\n\\n        if (navigator.clipboard && navigator.clipboard.writeText) {\\n          navigator.clipboard.writeText(text).then(onDone).catch(() => fallbackCopy(text, onDone));\\n        } else {\\n          fallbackCopy(text, onDone);\\n        }\\n      }\\n\\n      function fallbackCopy(text, cb) {\\n        const ta = document.createElement(\'textarea\');\\n        ta.value = text;\\n        ta.style.position = \'fixed\';\\n        ta.style.opacity = \'0\';\\n        document.body.appendChild(ta);\\n        ta.select();\\n        try { document.execCommand(\'copy\'); cb(); } catch (_) {}\\n        document.body.removeChild(ta);\\n      }\\n\\n      // --- Event Wiring ---\\n      function bindEvents() {\\n        // Date range buttons\\n        document.getElementById(\'dateFilterGroup\').addEventListener(\'click\', (e) => {\\n          const btn = e.target.closest(\'.seg-btn\');\\n          if (!btn || btn.dataset.days === state.days) return;\\n          state.days = btn.dataset.days;\\n          document.querySelectorAll(\'#dateFilterGroup .seg-btn\').forEach((b) => b.classList.toggle(\'is-active\', b === btn));\\n          fetchStats({ force: true });\\n        });\\n\\n        // Billing mode toggle\\n        document.getElementById(\'billingModeGroup\').addEventListener(\'click\', (e) => {\\n          const btn = e.target.closest(\'.seg-btn\');\\n          if (!btn) return;\\n          const targetMode = btn.dataset.billing;\\n          if ((targetMode === \'turn\') === isTurnBilling(state.billingMode)) return;\\n          state.billingMode = targetMode;\\n          document.querySelectorAll(\'#billingModeGroup .seg-btn\').forEach((b) => b.classList.toggle(\'is-active\', b === btn));\\n          fetchStats({ force: true });\\n        });\\n\\n        // Billing mode help tooltip\\n        const billHelp = document.getElementById(\'billingHelpBtn\');\\n        const showBillingHelp = (ev) => {\\n          const rect = billHelp.getBoundingClientRect();\\n          const cx = ev.clientX || (rect.left + rect.width / 2);\\n          const cy = ev.clientY || rect.bottom;\\n          showTooltip(\\n            `<div class=\\"tt-title\\">Token Billing Modes</div>\\n             <div style=\\"color:var(--text-secondary);line-height:1.45;margin-bottom:6px;\\"><strong style=\\"color:#9aa5ff;\\">Context Tokens:</strong> OpenRouter-style cumulative prompt billing \\u2014 counts the full conversation context window sent on every API turn.</div>\\n             <div style=\\"color:var(--text-secondary);line-height:1.45;\\"><strong style=\\"color:#5eead4;\\">Turn Tokens:</strong> Incremental turn billing \\u2014 counts only newly added message characters/tokens per turn, excluding repeated history.</div>`,\\n            cx,\\n            cy\\n          );\\n        };\\n        billHelp.addEventListener(\'mouseenter\', showBillingHelp);\\n        billHelp.addEventListener(\'focus\', showBillingHelp);\\n        billHelp.addEventListener(\'mouseleave\', hideTooltip);\\n        billHelp.addEventListener(\'blur\', hideTooltip);\\n\\n        // Pricing Simulator Profile Help Tooltip\\n        const priceHelp = document.getElementById(\'pricingHelpBtn\');\\n        const showPricingHelp = (ev) => {\\n          const rect = priceHelp.getBoundingClientRect();\\n          const cx = ev.clientX || (rect.left + rect.width / 2);\\n          const cy = ev.clientY || rect.bottom;\\n          showTooltip(\\n            `<div class=\\"tt-title\\"><span class=\\"nf-icon\\" style=\\"color:var(--gb-yellow);margin-right:5px;\\">\\uf155</span>Pricing Simulator Tiers</div>\\n             <div style=\\"color:var(--text-secondary);line-height:1.45;margin-bottom:6px;\\">\\n               Google Antigravity runs on flat subscriptions or pooled quota (not per-token metered invoices). These tiers simulate what your consumed tokens would cost under comparable API rates:\\n             </div>\\n             <div style=\\"color:var(--text-secondary);line-height:1.45;margin-bottom:4px;\\">\\n               <strong style=\\"color:var(--gb-blue);\\">AGY Blended:</strong> $0.50 / $2.50 per 1M tok (standard mixed baseline).\\n             </div>\\n             <div style=\\"color:var(--text-secondary);line-height:1.45;margin-bottom:4px;\\">\\n               <strong style=\\"color:var(--gb-purple);\\">AGY Pro Tier:</strong> $1.25 / $10.00 per 1M tok (Gemini 2.5/3 Pro level).\\n             </div>\\n             <div style=\\"color:var(--text-secondary);line-height:1.45;margin-bottom:4px;\\">\\n               <strong style=\\"color:var(--gb-aqua);\\">Gemini Flash Tier (recommended):</strong> $0.15 / $0.60 per 1M tok (Gemini 3.8 Flash medium baseline).\\n             </div>\\n             <div style=\\"margin-top:7px;padding:6px 9px;background:var(--bg-subtle);border-radius:2px;border-left:3px solid var(--gb-aqua);font-size:11px;line-height:1.45;color:var(--text-secondary);\\">\\n               <strong style=\\"color:var(--gb-aqua);display:flex;align-items:center;gap:4px;margin-bottom:2px;\\"><span class=\\"nf-icon\\">\\uf42e</span> What does (recommended) mean?</strong>\\n               This profile represents <strong>Gemini 3.8 Flash</strong> (the primary medium/fast model powering Google Antigravity and Hermes Agent inference). Selecting it yields the most realistic pay-as-you-go valuation of your actual transcript tokens at standard commercial rates ($0.15/1M input, $0.60/1M output &amp; thinking).\\n             </div>\\n             <div style=\\"color:var(--text-secondary);line-height:1.45;\\">\\n               <strong style=\\"color:var(--gb-orange);\\">AGY Opus Tier:</strong> $5.00 / $25.00 per 1M tok (high-tier reasoning models).\\n             </div>\\n             <div style=\\"margin-top:6px;padding-top:5px;border-top:1px dashed var(--border-card);color:var(--text-muted);font-size:10.5px;\\">\\n               <em>Note:</em> To filter telemetry by the actual model used (e.g. Gemini 3.8 vs Claude), use the Model Separation filter.\\n             </div>`,\\n            cx,\\n            cy\\n          );\\n        };\\n        priceHelp.addEventListener(\'mouseenter\', showPricingHelp);\\n        priceHelp.addEventListener(\'focus\', showPricingHelp);\\n        priceHelp.addEventListener(\'mouseleave\', () => hideTooltip());\\n        priceHelp.addEventListener(\'blur\', () => hideTooltip());\\n\\n        // --- Environmental & Real-World Perspective Reference Logic ---\\n        function getEnergyPerspectiveData(whVal) {\\n          const wh = num(whVal);\\n          const kwh = wh / 1000.0;\\n          const wMl = wh * 1.80;\\n          const wL = wMl / 1000.0;\\n          return {\\n            wh: wh,\\n            kwh: kwh,\\n            // Electricity benchmarks\\n            phoneCharges: wh / 12.0,                  // ~12 Wh per smartphone charge\\n            ledHours: wh / 10.0,                      // 10W LED household bulb\\n            laptopHours: wh / 45.0,                   // 45W active laptop workload\\n            googleSearches: wh / 0.3,                 // ~0.3 Wh per standard Google query\\n            kettleBoils: wh / 15.0,                   // ~15 Wh per cup boiled in kettle\\n            refrigeratorHours: wh / 40.0,             // ~40W avg household fridge compressor (0.96 kWh/day)\\n            wifiRouterHours: wh / 6.0,                // ~6W home Wi-Fi router continuous\\n            // Impactful Macro Benchmarks: 2-person apartment & household energy\\n            // Eurostat / Destatis: 2-person apartment uses ~2,500 kWh/yr = ~6.85 kWh/day = ~285.4 Wh/hour\\n            apt2PersonHours: wh / (2500000.0 / 8760.0), // ~285.4 Wh per hour for entire 2-person apartment\\n            apt2PersonDays: kwh / (2500.0 / 365.0),     // ~6.85 kWh per day\\n            // US EIA average household (~10,500 kWh/yr = ~1.20 kW avg load)\\n            usHomeHours: wh / (10500000.0 / 8760.0),\\n            // Water benchmarks (Scope 1 direct evaporative + Scope 2 power generation cooling)\\n            waterMl: wMl,\\n            waterLiters: wL,\\n            waterBottles: wMl / 500.0,                // 500ml standard PET drinking bottles\\n            espressoCups: wMl / 30.0,                 // 30ml espresso cups\\n            toiletFlushes: wL / 6.0,                  // standard 6L dual-flush toilet\\n            showerMinutes: wL / 9.0,                  // ~9 L/min water-saving showerhead\\n            // 2-person household water (WHO / EEA: ~250 L/day for 2 people = ~10.4 L/hour)\\n            apt2WaterHours: wL / (250.0 / 24.0),      // hours of baseline 2-person apartment domestic water\\n            apt2WaterDays: wL / 250.0,\\n          };\\n        }\\n\\n        function getCarbonPerspectiveData(co2GramsVal, whVal) {\\n          const g = num(co2GramsVal, (num(whVal) / 1000.0) * 385.0);\\n          const kg = g / 1000.0;\\n          return {\\n            grams: g,\\n            kg: kg,\\n            gasCarKm: g / 170.0,                      // ~170g CO2e per km for typical petrol car (WLTP Europe)\\n            gasCarMiles: (g / 170.0) * 0.621371,\\n            evKm: g / 45.0,                           // ~45g CO2e per km for EV on grid mix\\n            treeAbsorbHours: (g / (22000.0 / 8760.0)), // Mature tree absorbs ~22 kg/yr (~2.51g / hr)\\n            treeAbsorbDays: g / (22000.0 / 365.0),    // Mature tree absorbs ~60.27g / day\\n            mailSends: g / 4.0,                       // ~4g CO2e per standard email with attachment\\n            plasticBottles: g / 82.8,                 // ~82.8g CO2e to produce 500ml PET bottle\\n            // Macro benchmark: European 2-person apartment daily emissions\\n            // ~2,500 kWh/yr * 385g/kWh = ~962.5 kg CO2e/yr = ~2.64 kg/day\\n            apt2EmissionsHours: g / (962500.0 / 8760.0), // ~109.9g CO2e per hour\\n            apt2EmissionsDays: kg / 2.637,\\n            beefSteakGrams: g / 60.0,                 // ~60kg CO2e per 1kg beef = 60g CO2e per gram of beef\\n          };\\n        }\\n\\n        function generateEnergyPerspectiveHTML(whVal, title) {\\n          const p = getEnergyPerspectiveData(whVal);\\n          return `\\n            <div class=\\"tt-title\\" style=\\"color:var(--gb-aqua);\\"><span class=\\"nf-icon\\" style=\\"margin-right:6px;\\">\\u26a1</span>${escapeHtml(title || \'Electricity in Perspective\')}</div>\\n            <div style=\\"font-size:12px;color:var(--text-bright);margin-bottom:8px;font-weight:700;\\">\\n              Total: <span style=\\"color:var(--gb-aqua);\\" class=\\"mono\\">${dec2Fmt.format(p.wh)} Wh</span> <span style=\\"color:var(--text-muted);font-weight:400;\\">(${p.kwh.toFixed(4)} kWh)</span>\\n            </div>\\n            <div style=\\"display:flex;flex-direction:column;gap:5px;font-size:11px;\\">\\n              <div class=\\"tt-row\\"><span class=\\"tt-label\\" style=\\"color:var(--gb-aqua);font-weight:600;\\">\\ud83d\\udca7 Datacenter water footprint (~1.8ml/Wh)</span><span class=\\"tt-val mono\\" style=\\"color:var(--gb-aqua);font-weight:700;\\">${p.waterMl >= 1000 ? dec2Fmt.format(p.waterLiters) + \' L\' : dec1Fmt.format(p.waterMl) + \' ml\'} (${dec2Fmt.format(p.waterBottles)} bottles)</span></div>\\n              <div class=\\"tt-row\\"><span class=\\"tt-label\\" style=\\"color:var(--gb-yellow);font-weight:600;\\">\\ud83c\\udfe0 2-Person Apartment Electricity</span><span class=\\"tt-val mono\\" style=\\"color:var(--gb-yellow);font-weight:700;\\">${p.apt2PersonHours >= 24 ? dec2Fmt.format(p.apt2PersonDays) + \' days\' : (p.apt2PersonHours >= 1 ? dec1Fmt.format(p.apt2PersonHours) + \' hrs\' : (p.apt2PersonHours * 60).toFixed(0) + \' min\')}</span></div>\\n              <div class=\\"tt-row\\"><span class=\\"tt-label\\">\\ud83d\\udcf1 Smartphone full charges (~12Wh)</span><span class=\\"tt-val mono\\" style=\\"color:var(--gb-blue);\\">${dec1Fmt.format(p.phoneCharges)} cycles</span></div>\\n              <div class=\\"tt-row\\"><span class=\\"tt-label\\">\\ud83d\\udca1 10W household LED lightbulb</span><span class=\\"tt-val mono\\" style=\\"color:var(--gb-yellow);\\">${dec1Fmt.format(p.ledHours)} hours</span></div>\\n              <div class=\\"tt-row\\"><span class=\\"tt-label\\">\\ud83d\\udcbb Laptop working battery life (~45W)</span><span class=\\"tt-val mono\\" style=\\"color:var(--gb-purple);\\">${dec1Fmt.format(p.laptopHours)} hours</span></div>\\n              <div class=\\"tt-row\\"><span class=\\"tt-label\\">\\ud83d\\udcf6 Wi-Fi router running (~6W)</span><span class=\\"tt-val mono\\" style=\\"color:var(--gb-orange);\\">${dec1Fmt.format(p.wifiRouterHours)} hours</span></div>\\n              <div class=\\"tt-row\\"><span class=\\"tt-label\\">\\u2615 Electric kettle boiling water</span><span class=\\"tt-val mono\\" style=\\"color:var(--gb-red);\\">${dec1Fmt.format(p.kettleBoils)} cups</span></div>\\n              <div class=\\"tt-row\\"><span class=\\"tt-label\\">\\ud83d\\udd0d Standard web searches (~0.3Wh)</span><span class=\\"tt-val mono\\" style=\\"color:var(--gb-aqua);\\">${fmtInt(p.googleSearches)} queries</span></div>\\n            </div>\\n            <div style=\\"margin-top:8px;padding-top:6px;border-top:1px dashed var(--border-card);color:var(--text-muted);font-size:10px;line-height:1.4;\\">\\n              <em>Basis:</em> 0.08 Wh/1k in \\u00b7 0.45 Wh/1k out/think \\u00b7 Water: ~1.80 L/kWh (Li et al., UC Riverside/ACM 2023 <em>\\"Making AI Less Thirsty\\"</em>).\\n            </div>\\n          `;\\n        }\\n\\n        function generateCarbonPerspectiveHTML(co2gVal, whVal, title) {\\n          const p = getCarbonPerspectiveData(co2gVal, whVal);\\n          return `\\n            <div class=\\"tt-title\\" style=\\"color:var(--gb-green);\\"><span class=\\"nf-icon\\" style=\\"margin-right:6px;\\">\\ud83c\\udf31</span>${escapeHtml(title || \'Carbon Footprint in Perspective\')}</div>\\n            <div style=\\"font-size:12px;color:var(--text-bright);margin-bottom:8px;font-weight:700;\\">\\n              Total: <span style=\\"color:var(--gb-green);\\" class=\\"mono\\">${dec2Fmt.format(p.grams)} g CO\\u2082e</span> <span style=\\"color:var(--text-muted);font-weight:400;\\">(${p.kg >= 1 ? (p.kg.toFixed(3) + \' kg\') : ((p.grams).toFixed(1) + \' g\')})</span>\\n            </div>\\n            <div style=\\"display:flex;flex-direction:column;gap:5px;font-size:11px;\\">\\n              <div class=\\"tt-row\\"><span class=\\"tt-label\\" style=\\"color:var(--gb-yellow);font-weight:600;\\">\\ud83c\\udfe0 2-Person Home Carbon Equiv</span><span class=\\"tt-val mono\\" style=\\"color:var(--gb-yellow);font-weight:700;\\">${p.apt2EmissionsHours >= 24 ? dec2Fmt.format(p.apt2EmissionsDays) + \' days\' : (p.apt2EmissionsHours >= 1 ? dec1Fmt.format(p.apt2EmissionsHours) + \' hrs\' : (p.apt2EmissionsHours * 60).toFixed(0) + \' min\')}</span></div>\\n              <div class=\\"tt-row\\"><span class=\\"tt-label\\">\\ud83d\\ude97 Petrol car driving (170g/km)</span><span class=\\"tt-val mono\\" style=\\"color:var(--gb-red);\\">${dec2Fmt.format(p.gasCarKm)} km (${dec2Fmt.format(p.gasCarMiles)} mi)</span></div>\\n              <div class=\\"tt-row\\"><span class=\\"tt-label\\">\\ud83d\\udd0b Electric vehicle driving (45g/km)</span><span class=\\"tt-val mono\\" style=\\"color:var(--gb-aqua);\\">${dec2Fmt.format(p.evKm)} km</span></div>\\n              <div class=\\"tt-row\\"><span class=\\"tt-label\\">\\ud83c\\udf32 Mature tree CO\\u2082 absorption</span><span class=\\"tt-val mono\\" style=\\"color:var(--gb-green);\\">${p.treeAbsorbDays >= 1 ? dec1Fmt.format(p.treeAbsorbDays) + \' days\' : dec1Fmt.format(p.treeAbsorbHours) + \' hours\'}</span></div>\\n              <div class=\\"tt-row\\"><span class=\\"tt-label\\">\\ud83d\\udce7 Emails with attachments (~4g)</span><span class=\\"tt-val mono\\" style=\\"color:var(--gb-blue);\\">${fmtInt(p.mailSends)} emails</span></div>\\n              <div class=\\"tt-row\\"><span class=\\"tt-label\\">\\ud83c\\udf76 500ml plastic PET bottles made</span><span class=\\"tt-val mono\\" style=\\"color:var(--gb-yellow);\\">${dec2Fmt.format(p.plasticBottles)} bottles</span></div>\\n            </div>\\n            <div style=\\"margin-top:8px;padding-top:6px;border-top:1px dashed var(--border-card);color:var(--text-muted);font-size:10px;line-height:1.4;\\">\\n              <em>Basis:</em> Datacenter grid emission factor @ 385g CO\\u2082e / kWh (IEA blended global baseline).\\n            </div>\\n          `;\\n        }\\n\\n        // Perspective Modal dialog controller\\n        const perspModal = document.getElementById(\'perspectiveModal\');\\n        const perspBackdrop = document.getElementById(\'perspectiveModalBackdrop\');\\n        const perspCloseBtn = document.getElementById(\'perspectiveCloseBtn\');\\n\\n        function openPerspectiveModal(title, categoryText, contentHtml) {\\n          if (!perspModal || !perspBackdrop) return;\\n          hideTooltip();\\n          document.getElementById(\'perspectiveTitle\').textContent = title;\\n          document.getElementById(\'perspectiveCategory\').textContent = categoryText;\\n          document.getElementById(\'perspectiveBody\').innerHTML = contentHtml;\\n          perspBackdrop.classList.add(\'is-open\');\\n          perspModal.classList.add(\'is-open\');\\n          perspModal.setAttribute(\'aria-hidden\', \'false\');\\n        }\\n\\n        function closePerspectiveModal() {\\n          if (!perspModal || !perspBackdrop) return;\\n          perspModal.classList.remove(\'is-open\');\\n          perspBackdrop.classList.remove(\'is-open\');\\n          perspModal.setAttribute(\'aria-hidden\', \'true\');\\n        }\\n\\n        if (perspCloseBtn) perspCloseBtn.addEventListener(\'click\', closePerspectiveModal);\\n        if (perspBackdrop) perspBackdrop.addEventListener(\'click\', closePerspectiveModal);\\n        window.addEventListener(\'keydown\', (ev) => {\\n          if (ev.key === \'Escape\' && perspModal && perspModal.classList.contains(\'is-open\')) {\\n            closePerspectiveModal();\\n          }\\n        });\\n\\n        function showEnergyPerspectiveModal(whVal) {\\n          const p = getEnergyPerspectiveData(whVal);\\n          const bodyHtml = `\\n            <div class=\\"drawer-section\\">\\n              <span class=\\"drawer-section-title\\">Aggregated Electricity Telemetry</span>\\n              <div class=\\"detail-grid\\">\\n                <div class=\\"detail-tile\\">\\n                  <span class=\\"detail-tile-label\\">Total Consumption</span>\\n                  <span class=\\"detail-tile-val\\" style=\\"color:var(--gb-aqua);\\">${dec2Fmt.format(p.wh)} Wh</span>\\n                  <span class=\\"detail-tile-sub\\">${p.kwh.toFixed(4)} kWh</span>\\n                </div>\\n                <div class=\\"detail-tile\\">\\n                  <span class=\\"detail-tile-label\\">Equivalent Phone Charges</span>\\n                  <span class=\\"detail-tile-val\\" style=\\"color:var(--gb-yellow);\\">${dec1Fmt.format(p.phoneCharges)}</span>\\n                  <span class=\\"detail-tile-sub\\">~12Wh smartphone batteries</span>\\n                </div>\\n              </div>\\n            </div>\\n\\n            <div class=\\"drawer-section\\">\\n              <span class=\\"drawer-section-title\\">Real-World Reference Equivalents</span>\\n              <div class=\\"comp-list\\" style=\\"display:flex;flex-direction:column;gap:6px;\\">\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:7px 10px;background:var(--bg-subtle);border-radius:var(--radius-control);border:1px solid var(--border-card);\\">\\n                  <span class=\\"tt-label\\">\\ud83d\\udcf1 Smartphone Full Charges (~12Wh)</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-blue);font-weight:700;\\">${dec1Fmt.format(p.phoneCharges)} cycles</span>\\n                </div>\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:7px 10px;background:var(--bg-subtle);border-radius:var(--radius-control);border:1px solid var(--border-card);\\">\\n                  <span class=\\"tt-label\\">\\ud83d\\udca1 10W Household LED Lightbulb</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-yellow);font-weight:700;\\">${dec1Fmt.format(p.ledHours)} hours runtime</span>\\n                </div>\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:7px 10px;background:var(--bg-subtle);border-radius:var(--radius-control);border:1px solid var(--border-card);\\">\\n                  <span class=\\"tt-label\\">\\ud83d\\udcbb Modern Laptop Working (~45W)</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-purple);font-weight:700;\\">${dec1Fmt.format(p.laptopHours)} hours runtime</span>\\n                </div>\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:7px 10px;background:var(--bg-subtle);border-radius:var(--radius-control);border:1px solid var(--border-card);\\">\\n                  <span class=\\"tt-label\\">\\ud83d\\udcf6 Home Wi-Fi Router (~6W)</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-orange);font-weight:700;\\">${dec1Fmt.format(p.wifiRouterHours)} hours</span>\\n                </div>\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:7px 10px;background:var(--bg-subtle);border-radius:var(--radius-control);border:1px solid var(--border-card);\\">\\n                  <span class=\\"tt-label\\">\\u2615 Electric Kettle Water Boils (~15Wh/cup)</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-red);font-weight:700;\\">${dec1Fmt.format(p.kettleBoils)} cups</span>\\n                </div>\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:8px 10px;background:rgba(250,189,47,0.07);border-radius:var(--radius-control);border:1px solid rgba(250,189,47,0.3);\\">\\n                  <span class=\\"tt-label\\" style=\\"color:var(--gb-yellow);font-weight:700;\\">\\ud83c\\udfe0 2-Person Apartment Sustained (Electricity)</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-yellow);font-weight:700;\\">${p.apt2PersonHours >= 24 ? dec2Fmt.format(p.apt2PersonDays) + \' days\' : (p.apt2PersonHours >= 1 ? dec1Fmt.format(p.apt2PersonHours) + \' hours\' : (p.apt2PersonHours * 60).toFixed(1) + \' minutes\')} (~285W avg load)</span>\\n                </div>\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:8px 10px;background:rgba(142,192,124,0.07);border-radius:var(--radius-control);border:1px solid rgba(142,192,124,0.3);\\">\\n                  <span class=\\"tt-label\\" style=\\"color:var(--gb-aqua);font-weight:700;\\">\\ud83d\\udca7 Datacenter Water Consumed (Scope 1+2)</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-aqua);font-weight:700;\\">${p.waterMl >= 1000 ? dec2Fmt.format(p.waterLiters) + \' L\' : dec1Fmt.format(p.waterMl) + \' ml\'} (~${dec2Fmt.format(p.waterBottles)} bottles)</span>\\n                </div>\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:7px 10px;background:rgba(142,192,124,0.04);border-radius:var(--radius-control);border:1px solid var(--border-card);\\">\\n                  <span class=\\"tt-label\\">\\ud83d\\udebf Water-Saving Showerhead Equivalent (~9L/min)</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-aqua);font-weight:700;\\">${p.showerMinutes >= 1 ? dec1Fmt.format(p.showerMinutes) + \' min\' : (p.showerMinutes * 60).toFixed(0) + \' sec\'} runtime</span>\\n                </div>\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:7px 10px;background:rgba(142,192,124,0.04);border-radius:var(--radius-control);border:1px solid var(--border-card);\\">\\n                  <span class=\\"tt-label\\">\\ud83c\\udfe0 2-Person Apartment Daily Water Share (~250L/day)</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-aqua);font-weight:700;\\">${p.apt2WaterHours >= 1 ? dec1Fmt.format(p.apt2WaterHours) + \' hours\' : (p.apt2WaterHours * 60).toFixed(1) + \' min\'}</span>\\n                </div>\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:7px 10px;background:var(--bg-subtle);border-radius:var(--radius-control);border:1px solid var(--border-card);\\">\\n                  <span class=\\"tt-label\\">\\ud83d\\udd0d Standard Web Searches (~0.3Wh)</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-aqua);font-weight:700;\\">${fmtInt(p.googleSearches)} queries</span>\\n                </div>\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:7px 10px;background:var(--bg-subtle);border-radius:var(--radius-control);border:1px solid var(--border-card);\\">\\n                  <span class=\\"tt-label\\">\\u2744\\ufe0f Refrigerator Compressor Runtime</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-green);font-weight:700;\\">${dec1Fmt.format(p.refrigeratorHours)} hours</span>\\n                </div>\\n              </div>\\n            </div>\\n\\n            <div class=\\"drawer-section\\">\\n              <span class=\\"drawer-section-title\\">Calculation Methodology &amp; Scientific Sources</span>\\n              <div style=\\"font-size:11px;color:var(--text-secondary);line-height:1.55;background:var(--bg-subtle);padding:10px 12px;border-radius:var(--radius-control);border:1px solid var(--border-card);display:flex;flex-direction:column;gap:8px;\\">\\n                <div><strong>1. Token Compute Energy (Wh/tok):</strong> Input tokens ~0.08 Wh / 1k tokens; output &amp; thinking tokens ~0.45 Wh / 1k tokens based on empirical GPU accelerator measurements (NVIDIA H100 / Google TPU v5e) and modern hyperscale datacenter PUE ~1.15 (Luccioni et al., 2023; Patterson et al. Google Research).</div>\\n                <div><strong>2. Datacenter Water Footprint (~1.80 ml/Wh):</strong> Modeled from peer-reviewed research by <em>Li, Peng et al. (UC Riverside / ACM 2023, \\"Making AI Less Thirsty\\")</em>. Covers both <strong>Scope 1 on-site cooling tower evaporation</strong> (~0.72 L/kWh) and <strong>Scope 2 off-site thermoelectric power plant cooling water</strong> (~1.08 L/kWh), totaling ~1.80 L/kWh (1.80 ml/Wh).</div>\\n                <div><strong>3. 2-Person Apartment Sustained (~285W):</strong> Derived from <em>Eurostat &amp; German Federal Statistical Office (Destatis)</em> household energy statistics for a 2-person flat: ~2,500 kWh/year = 6.85 kWh/day = ~285.4 Watts continuous average draw.</div>\\n                <div><strong>4. Household Water Baseline:</strong> Derived from <em>European Environment Agency (EEA) / WHO</em> baseline of ~125 L/person/day (~250 L/day for 2 people). Water-saving showerhead rated at ~9.0 L/min (EPA WaterSense).</div>\\n              </div>\\n            </div>\\n          `;\\n          openPerspectiveModal(\'Electricity Consumption in Perspective\', \'\\u26a1 ELECTRICITY PERSPECTIVE\', bodyHtml);\\n        }\\n\\n        function showCarbonPerspectiveModal(co2gVal, whVal) {\\n          const p = getCarbonPerspectiveData(co2gVal, whVal);\\n          const bodyHtml = `\\n            <div class=\\"drawer-section\\">\\n              <span class=\\"drawer-section-title\\">Aggregated Carbon Footprint Telemetry</span>\\n              <div class=\\"detail-grid\\">\\n                <div class=\\"detail-tile\\">\\n                  <span class=\\"detail-tile-label\\">Estimated Emissions</span>\\n                  <span class=\\"detail-tile-val\\" style=\\"color:var(--gb-green);\\">${dec2Fmt.format(p.grams)} g CO\\u2082e</span>\\n                  <span class=\\"detail-tile-sub\\">${p.kg.toFixed(4)} kg CO\\u2082e</span>\\n                </div>\\n                <div class=\\"detail-tile\\">\\n                  <span class=\\"detail-tile-label\\">Combustion Car Equiv</span>\\n                  <span class=\\"detail-tile-val\\" style=\\"color:var(--gb-red);\\">${dec2Fmt.format(p.gasCarKm)} km</span>\\n                  <span class=\\"detail-tile-sub\\">${dec2Fmt.format(p.gasCarMiles)} miles driven</span>\\n                </div>\\n              </div>\\n            </div>\\n\\n            <div class=\\"drawer-section\\">\\n              <span class=\\"drawer-section-title\\">Real-World Environmental Equivalents</span>\\n              <div class=\\"comp-list\\" style=\\"display:flex;flex-direction:column;gap:6px;\\">\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:8px 10px;background:rgba(250,189,47,0.07);border-radius:var(--radius-control);border:1px solid rgba(250,189,47,0.3);\\">\\n                  <span class=\\"tt-label\\" style=\\"color:var(--gb-yellow);font-weight:700;\\">\\ud83c\\udfe0 2-Person Apartment Daily Carbon Share</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-yellow);font-weight:700;\\">${p.apt2EmissionsHours >= 24 ? dec2Fmt.format(p.apt2EmissionsDays) + \' days\' : (p.apt2EmissionsHours >= 1 ? dec1Fmt.format(p.apt2EmissionsHours) + \' hours\' : (p.apt2EmissionsHours * 60).toFixed(1) + \' minutes\')} (~110g CO\\u2082e/hr)</span>\\n                </div>\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:7px 10px;background:var(--bg-subtle);border-radius:var(--radius-control);border:1px solid var(--border-card);\\">\\n                  <span class=\\"tt-label\\">\\ud83d\\ude97 Average Petrol Passenger Car (170g/km)</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-red);font-weight:700;\\">${dec2Fmt.format(p.gasCarKm)} km (${dec2Fmt.format(p.gasCarMiles)} mi)</span>\\n                </div>\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:7px 10px;background:var(--bg-subtle);border-radius:var(--radius-control);border:1px solid var(--border-card);\\">\\n                  <span class=\\"tt-label\\">\\ud83d\\udd0b Electric Vehicle on Grid Mix (45g/km)</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-aqua);font-weight:700;\\">${dec2Fmt.format(p.evKm)} km</span>\\n                </div>\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:7px 10px;background:var(--bg-subtle);border-radius:var(--radius-control);border:1px solid var(--border-card);\\">\\n                  <span class=\\"tt-label\\">\\ud83c\\udf32 Mature Tree CO\\u2082 Absorption Work</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-green);font-weight:700;\\">${p.treeAbsorbDays >= 1 ? dec1Fmt.format(p.treeAbsorbDays) + \' days\' : dec1Fmt.format(p.treeAbsorbHours) + \' hours\'}</span>\\n                </div>\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:7px 10px;background:var(--bg-subtle);border-radius:var(--radius-control);border:1px solid var(--border-card);\\">\\n                  <span class=\\"tt-label\\">\\ud83d\\udce7 Emails with PDF Attachment (~4g)</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-blue);font-weight:700;\\">${fmtInt(p.mailSends)} emails sent</span>\\n                </div>\\n                <div class=\\"comp-row\\" style=\\"display:flex;justify-content:space-between;padding:7px 10px;background:var(--bg-subtle);border-radius:var(--radius-control);border:1px solid var(--border-card);\\">\\n                  <span class=\\"tt-label\\">\\ud83c\\udf76 Manufacturing 500ml Plastic Bottles</span>\\n                  <span class=\\"tt-val mono\\" style=\\"color:var(--gb-yellow);font-weight:700;\\">${dec2Fmt.format(p.plasticBottles)} bottles (~82.8g ea)</span>\\n                </div>\\n              </div>\\n            </div>\\n\\n            <div class=\\"drawer-section\\">\\n              <span class=\\"drawer-section-title\\">Calculation Methodology &amp; Scientific Sources</span>\\n              <div style=\\"font-size:11px;color:var(--text-secondary);line-height:1.55;background:var(--bg-subtle);padding:10px 12px;border-radius:var(--radius-control);border:1px solid var(--border-card);display:flex;flex-direction:column;gap:8px;\\">\\n                <div><strong>1. Grid Carbon Intensity:</strong> Global datacenter blended grid factor of <strong>385 g CO\\u2082e / kWh</strong> from the <em>International Energy Agency (IEA World Energy Outlook)</em>. Datacenters running on hydro/nuclear (e.g. Quebec, Nordic) have lower emissions (~20-50g/kWh), while fossil-heavy grids exceed 500g/kWh.</div>\\n                <div><strong>2. 2-Person Home Carbon Baseline:</strong> ~2,500 kWh annual power @ 385g/kWh = ~962.5 kg CO\\u2082e / year = ~2.64 kg/day (~109.9 g/hour) based on <em>Eurostat</em> domestic residential datasets.</div>\\n                <div><strong>3. Tree Sequestration Baseline:</strong> A mature broadleaf tree absorbs approximately <strong>22 kg of CO\\u2082 annually</strong> (~60.3 g per day) based on <em>European Environment Agency (EEA) / USDA Forest Service</em> forestry carbon tables.</div>\\n                <div><strong>4. Vehicular Emissions:</strong> European Environment Agency / WLTP baseline of ~170g CO\\u2082e/km for combustion cars; ~45g CO\\u2082e/km for battery EVs charged on average grid mix (~16 kWh/100km @ 280g/kWh EU grid).</div>\\n              </div>\\n            </div>\\n          `;\\n          openPerspectiveModal(\'Carbon Footprint in Perspective\', \'\\ud83c\\udf31 CARBON PERSPECTIVE\', bodyHtml);\\n        }\\n\\n        // Wire Card 4 (Electricity) hover & click\\n        const cardEnergy = document.getElementById(\'kpiCardElectricity\');\\n        const btnEnergyHelp = document.getElementById(\'kpiEnergyHelpBtn\');\\n        const handleEnergyHover = (ev) => {\\n          const totals = (state.data && state.data.totals) || {};\\n          const html = generateEnergyPerspectiveHTML(totals.electricity_wh || 0);\\n          showTooltip(html, ev.clientX, ev.clientY);\\n        };\\n        if (cardEnergy) {\\n          cardEnergy.addEventListener(\'mouseenter\', handleEnergyHover);\\n          cardEnergy.addEventListener(\'mousemove\', (ev) => positionTooltip(ev.clientX, ev.clientY));\\n          cardEnergy.addEventListener(\'mouseleave\', hideTooltip);\\n          cardEnergy.addEventListener(\'click\', (ev) => {\\n            const totals = (state.data && state.data.totals) || {};\\n            showEnergyPerspectiveModal(totals.electricity_wh || 0);\\n          });\\n        }\\n        if (btnEnergyHelp) {\\n          btnEnergyHelp.addEventListener(\'click\', (ev) => {\\n            ev.stopPropagation();\\n            const totals = (state.data && state.data.totals) || {};\\n            showEnergyPerspectiveModal(totals.electricity_wh || 0);\\n          });\\n        }\\n\\n        // Wire Card 5 (Carbon) hover & click\\n        const cardCarbon = document.getElementById(\'kpiCardCarbon\');\\n        const btnCarbonHelp = document.getElementById(\'kpiCarbonHelpBtn\');\\n        const handleCarbonHover = (ev) => {\\n          const totals = (state.data && state.data.totals) || {};\\n          const html = generateCarbonPerspectiveHTML(totals.co2_grams || 0, totals.electricity_wh || 0);\\n          showTooltip(html, ev.clientX, ev.clientY);\\n        };\\n        if (cardCarbon) {\\n          cardCarbon.addEventListener(\'mouseenter\', handleCarbonHover);\\n          cardCarbon.addEventListener(\'mousemove\', (ev) => positionTooltip(ev.clientX, ev.clientY));\\n          cardCarbon.addEventListener(\'mouseleave\', hideTooltip);\\n          cardCarbon.addEventListener(\'click\', (ev) => {\\n            const totals = (state.data && state.data.totals) || {};\\n            showCarbonPerspectiveModal(totals.co2_grams || 0, totals.electricity_wh || 0);\\n          });\\n        }\\n        if (btnCarbonHelp) {\\n          btnCarbonHelp.addEventListener(\'click\', (ev) => {\\n            ev.stopPropagation();\\n            const totals = (state.data && state.data.totals) || {};\\n            showCarbonPerspectiveModal(totals.co2_grams || 0, totals.electricity_wh || 0);\\n          });\\n        }\\n\\n        // Subtitle hover info\\n        const pathSub = document.getElementById(\'agyPathSubtitle\');\\n        pathSub.addEventListener(\'mouseenter\', (ev) => {\\n          if (!pathSub.dataset.fullInfo) return;\\n          const lines = pathSub.dataset.fullInfo.split(/\\\\r?\\\\n/).map((l) => `<div>${escapeHtml(l)}</div>`).join(\'\');\\n          showTooltip(`<div class=\\"tt-title\\">Telemetry Source</div><div class=\\"mono\\" style=\\"font-size:11px;line-height:1.5;\\">${lines}</div>`, ev.clientX, ev.clientY);\\n        });\\n        pathSub.addEventListener(\'mouseleave\', hideTooltip);\\n\\n        // Demo badge hover tooltip\\n        const demoBadge = document.getElementById(\'demoBadge\');\\n        demoBadge.addEventListener(\'mouseenter\', (ev) => {\\n          showTooltip(`<div class=\\"tt-title\\" style=\\"color:#fbbf24;\\">Demo Preview Active</div><div>${escapeHtml(demoBadge.dataset.reason || \'\')}</div>`, ev.clientX, ev.clientY);\\n        });\\n        demoBadge.addEventListener(\'mouseleave\', hideTooltip);\\n\\n        document.getElementById(\'dismissDemoBanner\').addEventListener(\'click\', () => {\\n          state.demoBannerDismissed = true;\\n          document.getElementById(\'demoBanner\').classList.remove(\'is-visible\');\\n        });\\n\\n        document.getElementById(\'retryApiBtn\').addEventListener(\'click\', () => fetchStats({ force: true }));\\n        document.getElementById(\'loadFallbackDemoBtn\').addEventListener(\'click\', () => {\\n          document.getElementById(\'errorBanner\').classList.remove(\'is-visible\');\\n          state.data = generateOfflineFallbackData();\\n          renderAll();\\n        });\\n\\n        // Pricing dropdown\\n        document.getElementById(\'pricingSelect\').addEventListener(\'change\', (e) => {\\n          state.pricing = e.target.value;\\n          fetchStats({ force: true });\\n        });\\n\\n        // Auto-refresh 10s toggle\\n        const autoBtn = document.getElementById(\'autoRefreshBtn\');\\n        autoBtn.addEventListener(\'click\', () => {\\n          state.autoRefresh = !state.autoRefresh;\\n          autoBtn.classList.toggle(\'is-live\', state.autoRefresh);\\n          if (state.autoRefreshTimer) {\\n            clearInterval(state.autoRefreshTimer);\\n            state.autoRefreshTimer = null;\\n          }\\n          if (state.autoRefresh) {\\n            state.autoRefreshTimer = setInterval(() => {\\n              if (!document.hidden && !state.isRescanning) {\\n                fetchStats();\\n              }\\n            }, 10000);\\n            showToast(\'Live auto-refresh enabled (10s interval)\');\\n          } else {\\n            showToast(\'Live auto-refresh paused\');\\n          }\\n        });\\n\\n        // Rescan button\\n        document.getElementById(\'rescanBtn\').addEventListener(\'click\', triggerRescan);\\n\\n        // Daily chart tabs\\n        document.getElementById(\'chartTabGroup\').addEventListener(\'click\', (e) => {\\n          const btn = e.target.closest(\'.seg-btn\');\\n          if (!btn || btn.dataset.chartTab === state.chartTab) return;\\n          state.chartTab = btn.dataset.chartTab;\\n          document.querySelectorAll(\'#chartTabGroup .seg-btn\').forEach((b) => {\\n            const active = b === btn;\\n            b.classList.toggle(\'is-active\', active);\\n            b.setAttribute(\'aria-selected\', String(active));\\n          });\\n          if (state.data) renderDailyChart(state.data.daily || [], state.data.meta || {});\\n        });\\n\\n        // Heatmap delegated hover tooltip\\n        const hmWeeks = document.getElementById(\'heatmapWeeks\');\\n        hmWeeks.addEventListener(\'mouseover\', (ev) => {\\n          const cell = ev.target.closest(\'.hm-cell:not(.hm-pad)\');\\n          if (!cell) return;\\n          const html = `\\n            <div class=\\"tt-title\\">${escapeHtml(fmtDatePretty(cell.dataset.date))}</div>\\n            <div class=\\"tt-row\\"><span class=\\"tt-label\\">Tokens</span><span class=\\"tt-val\\">${fmtInt(cell.dataset.tokens)}</span></div>\\n            <div class=\\"tt-row\\"><span class=\\"tt-label\\">API Requests</span><span class=\\"tt-val\\">${fmtInt(cell.dataset.reqs)}</span></div>\\n            <div class=\\"tt-row\\"><span class=\\"tt-label\\">Est. Value</span><span class=\\"tt-val\\">${fmtUsd(cell.dataset.cost)}</span></div>\\n          `;\\n          showTooltip(html, ev.clientX, ev.clientY);\\n        });\\n        hmWeeks.addEventListener(\'mousemove\', (ev) => {\\n          if (tooltipEl.classList.contains(\'is-visible\')) positionTooltip(ev.clientX, ev.clientY);\\n        });\\n        hmWeeks.addEventListener(\'mouseleave\', hideTooltip);\\n\\n        // Hourly mode toggle + hover tooltip\\n        document.getElementById(\'hourlyMetricGroup\').addEventListener(\'click\', (e) => {\\n          const btn = e.target.closest(\'.seg-btn\');\\n          if (!btn || btn.dataset.hourlyMode === state.hourlyMode) return;\\n          state.hourlyMode = btn.dataset.hourlyMode;\\n          document.querySelectorAll(\'#hourlyMetricGroup .seg-btn\').forEach((b) => b.classList.toggle(\'is-active\', b === btn));\\n          if (state.data) renderHourly(state.data.hourly || []);\\n        });\\n\\n        const hourlyBars = document.getElementById(\'hourlyBars\');\\n        hourlyBars.addEventListener(\'mouseover\', (ev) => {\\n          const col = ev.target.closest(\'.hour-col\');\\n          if (!col) return;\\n          const h = Number(col.dataset.hour);\\n          const pad2 = (n) => String(n).padStart(2, \'0\');\\n          const html = `\\n            <div class=\\"tt-title\\">${pad2(h)}:00 \\u2013 ${pad2(h)}:59</div>\\n            <div class=\\"tt-row\\"><span class=\\"tt-label\\">Tokens</span><span class=\\"tt-val\\">${fmtInt(col.dataset.tokens)}</span></div>\\n            <div class=\\"tt-row\\"><span class=\\"tt-label\\">API Requests</span><span class=\\"tt-val\\">${fmtInt(col.dataset.reqs)}</span></div>\\n          `;\\n          showTooltip(html, ev.clientX, ev.clientY);\\n        });\\n        hourlyBars.addEventListener(\'mousemove\', (ev) => {\\n          if (tooltipEl.classList.contains(\'is-visible\')) positionTooltip(ev.clientX, ev.clientY);\\n        });\\n        hourlyBars.addEventListener(\'mouseleave\', hideTooltip);\\n\\n        // Conversations search & sort\\n        const searchInput = document.getElementById(\'convSearchInput\');\\n        const clearBtn = document.getElementById(\'searchClearBtn\');\\n        const searchKbd = document.getElementById(\'searchKbd\');\\n\\n        const syncSearchUi = () => {\\n          const hasText = searchInput.value.length > 0;\\n          clearBtn.classList.toggle(\'is-visible\', hasText);\\n          searchKbd.style.display = hasText ? \'none\' : \'inline-block\';\\n        };\\n\\n        searchInput.addEventListener(\'input\', () => {\\n          state.searchQuery = searchInput.value;\\n          state.visibleLimit = 50;\\n          syncSearchUi();\\n          renderConversationsTable();\\n        });\\n\\n        clearBtn.addEventListener(\'click\', () => {\\n          searchInput.value = \'\';\\n          state.searchQuery = \'\';\\n          syncSearchUi();\\n          renderConversationsTable();\\n          searchInput.focus();\\n        });\\n\\n        document.querySelectorAll(\'.conv-table thead th.sortable\').forEach((th) => {\\n          th.addEventListener(\'click\', () => {\\n            const key = th.dataset.sort;\\n            if (state.sortKey === key) {\\n              state.sortDir = state.sortDir === \'desc\' ? \'asc\' : \'desc\';\\n            } else {\\n              state.sortKey = key;\\n              state.sortDir = key === \'title\' ? \'asc\' : \'desc\';\\n            }\\n            renderConversationsTable();\\n          });\\n        });\\n\\n        document.getElementById(\'convShowMoreBtn\').addEventListener(\'click\', () => {\\n          state.visibleLimit += 50;\\n          renderConversationsTable();\\n        });\\n\\n        // Table row click -> open drawer\\n        const tbody = document.getElementById(\'convTableBody\');\\n        tbody.addEventListener(\'click\', (ev) => {\\n          const row = ev.target.closest(\'tr[data-conv-id]\');\\n          if (row) openDrawer(row.dataset.convId);\\n        });\\n        tbody.addEventListener(\'keydown\', (ev) => {\\n          if (ev.key === \'Enter\' || ev.key === \' \') {\\n            const row = ev.target.closest(\'tr[data-conv-id]\');\\n            if (row) {\\n              ev.preventDefault();\\n              openDrawer(row.dataset.convId);\\n            }\\n          }\\n        });\\n\\n        // Drawer controls\\n        document.getElementById(\'drawerCloseBtn\').addEventListener(\'click\', closeDrawer);\\n        document.getElementById(\'drawerBackdrop\').addEventListener(\'click\', closeDrawer);\\n        document.getElementById(\'drawerPrevBtn\').addEventListener(\'click\', () => stepDrawer(-1));\\n        document.getElementById(\'drawerNextBtn\').addEventListener(\'click\', () => stepDrawer(1));\\n\\n        // Global keyboard shortcuts\\n        document.addEventListener(\'keydown\', (ev) => {\\n          const tag = document.activeElement?.tagName;\\n          const inInput = tag === \'INPUT\' || tag === \'TEXTAREA\' || tag === \'SELECT\';\\n          if (ev.key === \'Escape\' && state.selectedConvId) {\\n            closeDrawer();\\n            return;\\n          }\\n          if (state.selectedConvId && !inInput) {\\n            if (ev.key === \'ArrowUp\') { ev.preventDefault(); stepDrawer(-1); return; }\\n            if (ev.key === \'ArrowDown\') { ev.preventDefault(); stepDrawer(1); return; }\\n          }\\n          if ((ev.key === \'/\' && !inInput) || ((ev.metaKey || ev.ctrlKey) && ev.key.toLowerCase() === \'k\')) {\\n            ev.preventDefault();\\n            searchInput.focus();\\n            searchInput.select();\\n          }\\n        });\\n\\n\\n        // Glossar Drawer Controller\\n        const glossaryModal = document.getElementById(\'glossaryModal\');\\n        const glossaryBackdrop = document.getElementById(\'glossaryModalBackdrop\');\\n        const glossaryCloseBtn = document.getElementById(\'glossaryCloseBtn\');\\n        const glossaryBtn = document.getElementById(\'glossaryBtn\');\\n        const glossarySearchInput = document.getElementById(\'glossarySearchInput\');\\n        const glossaryCategoryPills = document.getElementById(\'glossaryCategoryPills\');\\n        const glossaryList = document.getElementById(\'glossaryList\');\\n\\n        const GLOSSARY_DATA = [{\\"term\\": \\"Total Tokens\\", \\"category\\": \\"Tokens & Context\\", \\"badge_color\\": \\"var(--gb-blue)\\", \\"desc\\": \\"Cumulative tokens processed across all input prompt tokens, model output tokens, and chain-of-thought thinking tokens in the active filter window.\\", \\"why\\": \\"Primary workload volume indicator reflecting total compute and context processed.\\"}, {\\"term\\": \\"Context-Billed Input (OpenRouter-style)\\", \\"category\\": \\"Tokens & Context\\", \\"badge_color\\": \\"var(--gb-blue)\\", \\"desc\\": \\"In conversational and autonomous agent workflows, each API turn re-sends the cumulative conversation history, system prompt, and tool returns. Context billing calculates the full input payload sent on every request.\\", \\"why\\": \\"Accurately matches commercial cloud LLM billing invoices (OpenRouter, Google Vertex, Anthropic, OpenAI).\\"}, {\\"term\\": \\"Unique Turn Input (Incremental / Net Input)\\", \\"category\\": \\"Tokens & Context\\", \\"badge_color\\": \\"var(--gb-blue)\\", \\"desc\\": \\"Measures only the fresh, net-new user prompt or tool response text introduced in that specific turn, ignoring re-sent previous conversation context.\\", \\"why\\": \\"Reflects the actual net novel content authored or inspected without context compounding.\\"}, {\\"term\\": \\"Output Tokens\\", \\"category\\": \\"Tokens & Context\\", \\"badge_color\\": \\"var(--gb-blue)\\", \\"desc\\": \\"Tokens generated by the model in response messages, structured answers, and tool-call parameter payloads.\\", \\"why\\": \\"Output tokens are computationally intensive (autoregressive token generation) and typically billed at 3x-4x the rate of input tokens.\\"}, {\\"term\\": \\"Thinking Tokens (Chain-of-Thought / CoT)\\", \\"category\\": \\"Tokens & Context\\", \\"badge_color\\": \\"var(--gb-purple)\\", \\"desc\\": \\"Internal reasoning tokens generated by thinking-enabled models (e.g. Gemini 2.0/3.8 Flash Thinking, Claude 3.7 Sonnet Extended Thinking, OpenAI o1/o3) before emitting visible responses or tool calls.\\", \\"why\\": \\"Provides insight into the cognitive effort and reasoning depth allocated by the model to solve complex programming problems.\\"}, {\\"term\\": \\"Token Velocity / Peak Velocity\\", \\"category\\": \\"Tokens & Context\\", \\"badge_color\\": \\"var(--gb-yellow)\\", \\"desc\\": \\"The single highest 24-hour calendar day token volume observed within the selected time window.\\", \\"why\\": \\"Pinpoints workload burst capacity and heavy multi-file refactoring days.\\"}, {\\"term\\": \\"API Requests (LLM Turns)\\", \\"category\\": \\"Agent Execution\\", \\"badge_color\\": \\"var(--gb-orange)\\", \\"desc\\": \\"Each individual network round-trip call to the LLM backend where the model processes a prompt and responds with thinking, text, or tool calls.\\", \\"why\\": \\"Reflects the frequency of back-and-forth interactions between the agent and the model provider.\\"}, {\\"term\\": \\"Sessions / Conversations\\", \\"category\\": \\"Agent Execution\\", \\"badge_color\\": \\"var(--gb-orange)\\", \\"desc\\": \\"Distinct conversation threads or autonomous task runs stored in independent directories under ~/.gemini/antigravity-cli/brain/<uuid>. Each session contains multiple sequential API turns.\\", \\"why\\": \\"Tracks distinct coding objectives, tasks, or debug investigations.\\"}, {\\"term\\": \\"tok/req (Tokens per Request)\\", \\"category\\": \\"Agent Execution\\", \\"badge_color\\": \\"var(--gb-orange)\\", \\"desc\\": \\"Average token payload (input context + output + thinking) per API request round-trip.\\", \\"why\\": \\"High averages indicate long-running agent threads with large files or extensive tool output in the context window.\\"}, {\\"term\\": \\"Tool Calls (Function Calling)\\", \\"category\\": \\"Agent Execution\\", \\"badge_color\\": \\"var(--gb-aqua)\\", \\"desc\\": \\"Structured programmatic function calls invoked by the model to interact with the system (e.g. read_file, write_file, patch, run_command, search_files).\\", \\"why\\": \\"Distinguishes autonomous agentic execution from passive chat interaction.\\"}, {\\"term\\": \\"Subagent Spawns\\", \\"category\\": \\"Agent Execution\\", \\"badge_color\\": \\"var(--gb-purple)\\", \\"desc\\": \\"Isolated child agents launched by the primary agent to execute independent research, debugging, or parallelized coding subtasks without polluting the main context window.\\", \\"why\\": \\"Key indicator of hierarchical agent orchestration and modular task delegation.\\"}, {\\"term\\": \\"Pricing Profile / Tier\\", \\"category\\": \\"Pricing & Cost\\", \\"badge_color\\": \\"var(--gb-yellow)\\", \\"desc\\": \\"The simulated rate structure ($/1M input tokens, $/1M output tokens) applied to calculate estimated USD cost across historical token usage.\\", \\"why\\": \\"Enables instant \'what-if\' cost projections across different models (Gemini Flash, Blended, Pro, Claude 3.5 Sonnet).\\"}, {\\"term\\": \\"(recommended) Tier\\", \\"category\\": \\"Pricing & Cost\\", \\"badge_color\\": \\"var(--gb-green)\\", \\"desc\\": \\"Specifically flags the Gemini Flash Tier ($0.15 input / $0.60 output per 1M tokens), representing the primary default model (Gemini 3.8 Flash) powering Google Antigravity.\\", \\"why\\": \\"Provides the most accurate cost simulation for standard Antigravity CLI workloads.\\"}, {\\"term\\": \\"Theoretical Cost\\", \\"category\\": \\"Pricing & Cost\\", \\"badge_color\\": \\"var(--gb-yellow)\\", \\"desc\\": \\"Estimated dollar expenditure if all logged tokens were billed at standard commercial pay-as-you-go API rates under the chosen pricing tier and billing mode.\\", \\"why\\": \\"Quantifies the commercial market value of development work delivered by the AI assistant.\\"}, {\\"term\\": \\"Electricity Used (Wh / kWh)\\", \\"category\\": \\"Datacenter Footprint\\", \\"badge_color\\": \\"var(--gb-aqua)\\", \\"desc\\": \\"Datacenter electrical power consumed by GPU accelerators, host servers, and facility cooling (modeled at ~0.08 Wh/1k input, ~0.45 Wh/1k output+thinking with PUE ~1.15). Includes comparisons to sustain an entire 2-person apartment (~285W continuous average load).\\", \\"why\\": \\"Translates abstract token counts into physical energy metrics. Source: Luccioni et al. (2023), Patterson et al. (Google Research), Eurostat household energy statistics (~2,500 kWh/yr for 2-person flat).\\"}, {\\"term\\": \\"Carbon Footprint (gCO2e)\\", \\"category\\": \\"Datacenter Footprint\\", \\"badge_color\\": \\"var(--gb-green)\\", \\"desc\\": \\"Grams of carbon dioxide equivalent emitted based on datacenter power consumption and the global electric grid carbon intensity (~385 gCO\\\\u2082e / kWh, IEA blended baseline).\\", \\"why\\": \\"Quantifies environmental greenhouse gas impact of AI computing. Source: International Energy Agency (IEA) World Energy Outlook, EEA / WLTP transport emission factors, Eurostat residential carbon data.\\"}, {\\"term\\": \\"Water Footprint (ml / L)\\", \\"category\\": \\"Datacenter Footprint\\", \\"badge_color\\": \\"var(--gb-aqua)\\", \\"desc\\": \\"Datacenter water consumption combining Scope 1 direct evaporative cooling tower water plus Scope 2 indirect power plant cooling water, modeled at ~1.80 ml per Wh (~1.80 L/kWh). Includes comparisons to 500ml bottles, water-saving showerhead runtime (~9L/min), and 2-person apartment domestic water usage (~250L/day).\\", \\"why\\": \\"Peer-reviewed academic research: Li, Peng et al. (UC Riverside / ACM 2023, \\\\\\"Making AI Less Thirsty: Uncovering and Addressing the Secret Water Footprint of AI Models\\\\\\"); EPA WaterSense & EEA domestic water data.\\"}, {\\"term\\": \\"PUE (Power Usage Effectiveness)\\", \\"category\\": \\"Datacenter Footprint\\", \\"badge_color\\": \\"var(--gb-aqua)\\", \\"desc\\": \\"Ratio of total facility energy consumed relative to IT equipment energy. A PUE of 1.15 means 15% overhead for cooling, power delivery, and infrastructure.\\", \\"why\\": \\"Reflects modern hyperscale AI datacenter cooling and power delivery efficiency.\\"}, {\\"term\\": \\"Everyday Equivalents\\", \\"category\\": \\"Datacenter Footprint\\", \\"badge_color\\": \\"var(--gb-green)\\", \\"desc\\": \\"Intuitive human-scale physical benchmarks: 2-person apartment power duration (~285W load), smartphone battery cycles (~12Wh), 10W LED illumination hours, electric kettle boils (~15Wh), 500 mL water bottles, shower runtime, and car travel distances.\\", \\"why\\": \\"Grounds abstract kilowatt-hours, liters of water, and grams of carbon into everyday relatable perspective with citations from Eurostat, Destatis, EPA, and UC Riverside.\\"}, {\\"term\\": \\"365-Day Activity Matrix\\", \\"category\\": \\"Visualizations\\", \\"badge_color\\": \\"var(--gb-green)\\", \\"desc\\": \\"A GitHub-style calendar contribution matrix rendering 365 days of token density across 5 Gruvbox intensity shades.\\", \\"why\\": \\"Visualizes consistency, streaks, and long-term project cadences.\\"}, {\\"term\\": \\"Hour-of-Day Activity\\", \\"category\\": \\"Visualizations\\", \\"badge_color\\": \\"var(--gb-blue)\\", \\"desc\\": \\"24-hour histogram (00:00 to 23:00 local time) aggregating API request frequency by hour.\\", \\"why\\": \\"Reveals peak productivity hours, nocturnal coding sessions, and focus windows.\\"}, {\\"term\\": \\"Brain Directory (--brain-dir)\\", \\"category\\": \\"System & Engine\\", \\"badge_color\\": \\"var(--gb-yellow)\\", \\"desc\\": \\"The root filesystem path (default ~/.gemini/antigravity-cli/brain) where Antigravity CLI stores transcripts, tool execution records, and session history.\\", \\"why\\": \\"The authoritative data source parsed by the telemetry scanner.\\"}, {\\"term\\": \\"SQLite Index (~/.cache/agy_usage.db)\\", \\"category\\": \\"System & Engine\\", \\"badge_color\\": \\"var(--gb-yellow)\\", \\"desc\\": \\"Local SQLite database caching parsed session turns and SHA-256 file hashes for fast sub-second incremental scanning.\\", \\"why\\": \\"Prevents redundant JSONL parsing on subsequent dashboard launches or rescans.\\"}, {\\"term\\": \\"Live Polling (10s)\\", \\"category\\": \\"System & Engine\\", \\"badge_color\\": \\"var(--gb-aqua)\\", \\"desc\\": \\"Automatic background polling mode fetching updated telemetry from the local server every 10 seconds without full page reload.\\", \\"why\\": \\"Keeps metrics live and in-sync as the agent runs background turns in terminal.\\"}, {\\"term\\": \\"Context Compression\\", \\"category\\": \\"Tokens & Context\\", \\"badge_color\\": \\"var(--gb-purple)\\", \\"desc\\": \\"When conversation history nears model context boundaries, earlier turns are summarized into compact structured handoffs while retaining key anchors and instructions.\\", \\"why\\": \\"Enables marathon coding sessions without exceeding memory budgets or token limits.\\"}];\\n        let glossaryActiveCategory = \'ALL\';\\n\\n        function openGlossaryModal() {\\n          if (!glossaryModal || !glossaryBackdrop) return;\\n          hideTooltip();\\n          glossaryBackdrop.classList.add(\'is-open\');\\n          glossaryModal.classList.add(\'is-open\');\\n          glossaryModal.setAttribute(\'aria-hidden\', \'false\');\\n          if (glossarySearchInput) {\\n            glossarySearchInput.value = \'\';\\n            setTimeout(() => glossarySearchInput.focus(), 120);\\n          }\\n          renderGlossary();\\n        }\\n\\n        function closeGlossaryModal() {\\n          if (!glossaryModal || !glossaryBackdrop) return;\\n          glossaryModal.classList.remove(\'is-open\');\\n          glossaryBackdrop.classList.remove(\'is-open\');\\n          glossaryModal.setAttribute(\'aria-hidden\', \'true\');\\n        }\\n\\n        if (glossaryBtn) glossaryBtn.addEventListener(\'click\', openGlossaryModal);\\n        if (glossaryCloseBtn) glossaryCloseBtn.addEventListener(\'click\', closeGlossaryModal);\\n        if (glossaryBackdrop) glossaryBackdrop.addEventListener(\'click\', closeGlossaryModal);\\n        window.addEventListener(\'keydown\', (ev) => {\\n          if (ev.key === \'Escape\' && glossaryModal && glossaryModal.classList.contains(\'is-open\')) {\\n            closeGlossaryModal();\\n          }\\n        });\\n\\n        function renderGlossary() {\\n          if (!glossaryList) return;\\n          const q = (glossarySearchInput ? glossarySearchInput.value : \'\').toLowerCase().trim();\\n          const categories = [\'ALL\', ...new Set(GLOSSARY_DATA.map(d => d.category))];\\n\\n          if (glossaryCategoryPills) {\\n            glossaryCategoryPills.innerHTML = categories.map(cat => {\\n              const active = cat === glossaryActiveCategory;\\n              const count = cat === \'ALL\' ? GLOSSARY_DATA.length : GLOSSARY_DATA.filter(d => d.category === cat).length;\\n              const bg = active ? \'var(--gb-purple)\' : \'var(--bg-subtle)\';\\n              const color = active ? \'#fbf1c7\' : \'var(--text-secondary)\';\\n              const border = active ? \'var(--gb-purple)\' : \'var(--border-card)\';\\n              return `<button type=\\"button\\" class=\\"cat-pill\\" data-cat=\\"${escapeHtml(cat)}\\" style=\\"border:1px solid ${border};background:${bg};color:${color};padding:3px 8px;border-radius:12px;cursor:pointer;font-size:10.5px;font-family:inherit;font-weight:600;display:inline-flex;align-items:center;gap:4px;\\">` +\\n                `<span>${escapeHtml(cat)}</span><span style=\\"opacity:0.75;font-size:9.5px;\\">(${count})</span>` +\\n              `</button>`;\\n            }).join(\'\');\\n\\n            glossaryCategoryPills.querySelectorAll(\'.cat-pill\').forEach(btn => {\\n              btn.addEventListener(\'click\', () => {\\n                glossaryActiveCategory = btn.getAttribute(\'data-cat\');\\n                renderGlossary();\\n              });\\n            });\\n          }\\n\\n          const filtered = GLOSSARY_DATA.filter(item => {\\n            if (glossaryActiveCategory !== \'ALL\' && item.category !== glossaryActiveCategory) return false;\\n            if (!q) return true;\\n            return item.term.toLowerCase().includes(q) ||\\n                   item.category.toLowerCase().includes(q) ||\\n                   item.desc.toLowerCase().includes(q) ||\\n                   item.why.toLowerCase().includes(q);\\n          });\\n\\n          if (filtered.length === 0) {\\n            glossaryList.innerHTML = `<div style=\\"padding:32px 16px;text-align:center;color:var(--text-muted);font-size:12px;\\"><span class=\\"nf-icon\\" style=\\"font-size:24px;display:block;margin-bottom:8px;color:var(--gb-yellow);\\">\\uf071</span>No matching glossary terms found for \\"<strong>${escapeHtml(q)}</strong>\\"</div>`;\\n            return;\\n          }\\n\\n          glossaryList.innerHTML = filtered.map(item => {\\n            return `\\n              <article class=\\"glossary-card\\" style=\\"padding:12px 14px;background:var(--bg-subtle);border:1px solid var(--border-card);border-radius:var(--radius-card);display:flex;flex-direction:column;gap:6px;transition:border-color 0.15s ease;\\">\\n                <div style=\\"display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;\\">\\n                  <h4 class=\\"mono\\" style=\\"margin:0;font-size:13px;font-weight:700;color:var(--text-bright);display:flex;align-items:center;gap:6px;\\">\\n                    <span class=\\"nf-icon\\" style=\\"color:${item.badge_color};font-size:11px;\\">\\uf120</span>\\n                    <span>${escapeHtml(item.term)}</span>\\n                  </h4>\\n                  <span class=\\"mono\\" style=\\"font-size:10px;padding:2px 7px;border-radius:10px;border:1px solid ${item.badge_color};color:${item.badge_color};background:rgba(211,134,155,0.06);font-weight:600;\\">${escapeHtml(item.category)}</span>\\n                </div>\\n                <p style=\\"margin:0;font-size:11.5px;color:var(--text-secondary);line-height:1.5;\\">${escapeHtml(item.desc)}</p>\\n                <div style=\\"margin-top:2px;padding:6px 9px;background:rgba(0,0,0,0.15);border-radius:var(--radius-control);border-left:3px solid ${item.badge_color};font-size:11px;line-height:1.4;color:var(--text-muted);\\">\\n                  <strong style=\\"color:var(--text-secondary);font-weight:600;\\">Why it matters:</strong> ${escapeHtml(item.why)}\\n                </div>\\n              </article>\\n            `;\\n          }).join(\'\');\\n        }\\n\\n        if (glossarySearchInput) {\\n          glossarySearchInput.addEventListener(\'input\', () => {\\n            renderGlossary();\\n          });\\n        }\\n\\n\\n        // Gruvbox Dark / Light theme toggle\\n        const themeBtn = document.getElementById(\'themeToggleBtn\');\\n        if (themeBtn) {\\n          themeBtn.addEventListener(\'click\', () => {\\n            const cur = document.documentElement.getAttribute(\'data-theme\') === \'light\' ? \'light\' : \'dark\';\\n            applySavedTheme(cur === \'dark\' ? \'light\' : \'dark\');\\n            if (state.data) renderDailyChart(state.data.daily || [], state.data.meta || {});\\n          });\\n        }\\n\\n        // Resize observer for SVG fallback responsiveness\\n        let resizeTimer = null;\\n        window.addEventListener(\'resize\', () => {\\n          if (typeof window.Chart === \'undefined\' || window.__chartJsLoadFailed) {\\n            if (resizeTimer) clearTimeout(resizeTimer);\\n            resizeTimer = setTimeout(() => {\\n              if (state.data) renderDailyChart(state.data.daily || [], state.data.meta || {});\\n            }, 120);\\n          }\\n        });\\n      }\\n\\n      // --- Boot ---\\n      bindEvents();\\n      fetchStats({ force: true });\\n    })();\\n  </script>\\n</body>\\n</html>\\n"')

# ---------------------------------------------------------------------------
# Constants, Pricing & Environmental Formulas
# ---------------------------------------------------------------------------

MAX_CONTEXT_WINDOW = 1_000_000
DEFAULT_PORT = 8765
DEFAULT_HOST = "127.0.0.1"

DEMO_AUTO_REASON = "No local AGY transcripts found yet — showing sample preview"
DEMO_FLAG_REASON = "Demo mode enabled via --demo flag"

PRICING_PROFILES: Dict[str, Dict[str, Any]] = {
    "blended": {
        "label": "AGY Blended",
        "input": 0.50,
        "output": 2.50,
        "thinking": 2.50,
        "input_per_1m": 0.50,
        "output_per_1m": 2.50,
        "thinking_per_1m": 2.50,
    },
    "pro": {
        "label": "Gemini Pro Tier",
        "input": 1.25,
        "output": 10.00,
        "thinking": 10.00,
        "input_per_1m": 1.25,
        "output_per_1m": 10.00,
        "thinking_per_1m": 10.00,
    },
    "flash": {
        "label": "Gemini Flash Tier (recommended)",
        "input": 0.15,
        "output": 0.60,
        "thinking": 0.60,
        "input_per_1m": 0.15,
        "output_per_1m": 0.60,
        "thinking_per_1m": 0.60,
    },
    "opus": {
        "label": "Frontier Max Tier",
        "input": 5.00,
        "output": 25.00,
        "thinking": 25.00,
        "input_per_1m": 5.00,
        "output_per_1m": 25.00,
        "thinking_per_1m": 25.00,
    },
}

VALID_DAYS_FILTERS = ("1", "7", "30", "90", "365", "all")
VALID_BILLING_MODES = ("context", "turn")
# Available models populated dynamically from DB at runtime
DEFAULT_MODEL_FILTER = "all"

# Energy & Carbon coefficients:
# 0.08 Wh per 1k input tokens = 0.00008 Wh/token
# 0.45 Wh per 1k output/thinking tokens = 0.00045 Wh/token
WH_PER_INPUT_TOKEN = 0.00008
WH_PER_OUTPUT_TOKEN = 0.00045
CO2_GRAMS_PER_KWH = 385.0
WH_PER_PHONE_CHARGE = 12.0
WH_PER_LED_BULB_HOUR = 10.0
# Scope 1 (cooling towers) + Scope 2 (power plant generation) datacenter water intensity
# Literature consensus (Li et al., UC Riverside 2023 'Making AI Less Thirsty'): ~1.80 L/kWh = 1.80 ml/Wh
WATER_ML_PER_WH = 1.80


def compute_cost_usd(
    effective_input_tokens: int,
    output_tokens: int,
    thinking_tokens: int,
    pricing_key: str = "blended",
) -> float:
    """Compute estimated USD cost for token counts under a pricing profile."""
    profile = PRICING_PROFILES.get(pricing_key, PRICING_PROFILES["blended"])
    return (
        effective_input_tokens * profile["input"]
        + output_tokens * profile["output"]
        + thinking_tokens * profile["thinking"]
    ) / 1_000_000.0


def compute_energy_metrics(
    effective_input_tokens: int,
    output_tokens: int,
    thinking_tokens: int,
) -> Dict[str, float]:
    """Compute electricity (Wh/kWh), CO2 (grams), water consumption (ml/L), and everyday equivalents."""
    wh = (effective_input_tokens * WH_PER_INPUT_TOKEN) + (
        (output_tokens + thinking_tokens) * WH_PER_OUTPUT_TOKEN
    )
    kwh = wh / 1000.0
    co2_g = kwh * CO2_GRAMS_PER_KWH
    water_ml = wh * WATER_ML_PER_WH
    water_liters = water_ml / 1000.0
    return {
        "electricity_wh": wh,
        "electricity_kwh": kwh,
        "co2_grams": co2_g,
        "water_liters": water_liters,
        "water_ml": water_ml,
        "phone_charges_equiv": wh / WH_PER_PHONE_CHARGE,
        "led_bulb_hours_equiv": wh / WH_PER_LED_BULB_HOUR,
    }


# ---------------------------------------------------------------------------
# Optional tiktoken + Calibrated Stdlib Token Estimator
# ---------------------------------------------------------------------------

_TIKTOKEN_ENC = None
try:
    import tiktoken as _tiktoken  # type: ignore

    # Only use if encoding is already locally cached (avoid blocking/failing on network)
    _TIKTOKEN_ENC = _tiktoken.get_encoding("cl100k_base")
except Exception:
    _TIKTOKEN_ENC = None

_CODE_PUNCT_CHARS = "{}[]()<>:;,.=+-*/\\|&^%$#@!~?\"'`"


def _flatten_to_text(value: Any) -> str:
    """Convert arbitrary step field content (str, list, dict, None) into string."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        if not value:
            return ""
        # Fast path for content blocks: [{'type': 'text', 'text': '...'}, ...]
        parts: List[str] = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and "text" in item and len(item) <= 3:
                parts.append(str(item.get("text") or ""))
            else:
                try:
                    parts.append(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
                except Exception:
                    parts.append(str(item))
        return "\n".join(p for p in parts if p)
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return str(value)


def estimate_tokens(value: Any) -> int:
    """Estimate LLM token count for text or structured JSON objects.

    Uses `tiktoken` (`cl100k_base`) if importable and available offline;
    otherwise uses a calibrated stdlib heuristic (~3.85 chars/token for prose,
    ~3.0-3.3 chars/token for punctuation-heavy code/JSON, plus UTF-8 multi-byte
    adjustment for CJK/non-ASCII symbols).
    """
    text = value if isinstance(value, str) else _flatten_to_text(value)
    if not text:
        return 0

    if _TIKTOKEN_ENC is not None:
        try:
            return len(_TIKTOKEN_ENC.encode(text, disallowed_special=()))
        except Exception:
            pass

    char_len = len(text)
    if char_len <= 3:
        return 1

    # Sample evenly for large payloads (>16 KB) so token estimation stays O(1)
    if char_len <= 16384:
        sample = text
        scale = 1.0
    else:
        chunk = 2048
        step = (char_len - chunk) // 3
        sample = (
            text[0:chunk]
            + text[step : step + chunk]
            + text[step * 2 : step * 2 + chunk]
            + text[-chunk:]
        )
        scale = char_len / float(len(sample))

    punct_count = sum(sample.count(ch) for ch in _CODE_PUNCT_CHARS)
    newline_count = sample.count("\n")
    indent_runs = sample.count("    ")
    utf8_extra = len(sample.encode("utf-8", errors="replace")) - len(sample)

    # Base prose density ~3.88 chars/token (~0.2577 tokens/char)
    # Structural punctuation adds ~0.28 tokens/symbol; 4-space indents compress slightly
    sample_tokens = (
        (len(sample) / 3.88)
        + (punct_count * 0.28)
        + (newline_count * 0.15)
        + (utf8_extra * 0.45)
        - (indent_runs * 0.45)
    )
    estimated = int(round(max(1.0, sample_tokens * scale)))
    return max(1, estimated)


# ---------------------------------------------------------------------------
# Cross-Platform AGY Brain Directory & DB Path Discovery
# ---------------------------------------------------------------------------

_SKIP_SCAN_DIRS = {
    ".git",
    "node_modules",
    "__pycache__",
    ".venv",
    "venv",
    "site-packages",
    ".npm",
    ".cargo",
    ".rustup",
}


def _expand_path(raw: str) -> Path:
    expanded = os.path.expandvars(os.path.expanduser(raw))
    # Also handle Windows %VAR% syntax even when running on POSIX
    def _win_env_repl(match: re.Match[str]) -> str:
        var_name = match.group(1)
        return os.environ.get(var_name, match.group(0))

    expanded = re.sub(r"%([A-Za-z_][A-Za-z0-9_]*)%", _win_env_repl, expanded)
    return Path(expanded)


def iter_transcript_files(brain_dir: Path) -> List[Tuple[str, Path]]:
    """Return sorted `(conversation_id, transcript_path)` pairs inside `brain_dir`."""
    results: List[Tuple[str, Path]] = []
    if not brain_dir.is_dir():
        return results

    try:
        entries = sorted(brain_dir.iterdir(), key=lambda p: p.name)
    except OSError:
        return results

    for entry in entries:
        if not entry.is_dir() or entry.name.startswith("."):
            continue
        transcript = entry / ".system_generated" / "logs" / "transcript.jsonl"
        if transcript.is_file():
            results.append((entry.name, transcript))
    return results


def _normalize_explicit_agy_dir(candidate: Path) -> Path:
    """Normalize an explicit AGY path (accepting either `<home>` or `<home>/brain`)."""
    if candidate.is_dir():
        if iter_transcript_files(candidate):
            return candidate
        nested_brain = candidate / "brain"
        if nested_brain.is_dir():
            return nested_brain
        nested_cli_brain = candidate / ".gemini" / "antigravity-cli" / "brain"
        if nested_cli_brain.is_dir():
            return nested_cli_brain
    return candidate


def _shallow_find_brain_dirs(root: Path, max_depth: int = 6) -> List[Path]:
    """Shallowly scan `root` up to `max_depth` for AGY `brain` directories."""
    found: List[Path] = []
    if not root.is_dir():
        return found

    queue: List[Tuple[Path, int]] = [(root, 0)]
    while queue:
        current, depth = queue.pop(0)
        if depth > max_depth:
            continue
        try:
            children = list(current.iterdir())
        except OSError:
            continue

        for child in children:
            if not child.is_dir():
                continue
            name = child.name
            if name in _SKIP_SCAN_DIRS:
                continue
            if name == "brain" and (
                current.name == "antigravity-cli" or bool(iter_transcript_files(child))
            ):
                found.append(child)
                continue
            if depth < max_depth:
                queue.append((child, depth + 1))
    return found


def discover_agy_dirs(cli_agy_dir: Optional[str] = None) -> List[Path]:
    """Discover all AGY `brain` directories across standalone CLI & Hermes plugin scratch.

    Aggregates transcripts from standard `~/.gemini/antigravity-cli/brain` (Linux/macOS/Windows)
    AND Hermes AGY plugin scratch/profile homes (`~/.hermes/cache/scratch/hermes_agy_*/...`).
    When `cli_agy_dir` / `AGY_BRAIN_DIR` / `AGY_HOME` is explicitly set, scopes to that
    directory (auto-expanding nested `brain` subdirectories if a parent root is given).
    """
    explicit_raw = (
        cli_agy_dir
        or os.environ.get("AGY_BRAIN_DIR", "").strip()
        or os.environ.get("AGY_HOME", "").strip()
    )
    if explicit_raw:
        norm = _normalize_explicit_agy_dir(_expand_path(explicit_raw))
        if iter_transcript_files(norm):
            return [norm]
        nested = _shallow_find_brain_dirs(norm, max_depth=6)
        if nested:
            return nested
        return [norm]

    home = Path.home()
    candidates: List[Path] = [
        home / ".gemini" / "antigravity-cli" / "brain",
        home / ".gemini" / "antigravity" / "brain",
    ]

    userprofile = os.environ.get("USERPROFILE", "").strip()
    if userprofile:
        candidates.append(Path(userprofile) / ".gemini" / "antigravity-cli" / "brain")
        candidates.append(Path(userprofile) / ".gemini" / "antigravity" / "brain")

    localappdata = os.environ.get("LOCALAPPDATA", "").strip()
    if localappdata:
        candidates.append(Path(localappdata) / "gemini" / "antigravity-cli" / "brain")
        candidates.append(Path(localappdata) / ".gemini" / "antigravity-cli" / "brain")

    xdg_config = os.environ.get("XDG_CONFIG_HOME", "").strip()
    if xdg_config:
        candidates.append(Path(xdg_config) / "antigravity-cli" / "brain")

    # Also scan Hermes AGY plugin scratch & profile directories so Hermes + standalone merge
    scan_roots = [
        home / ".hermes" / "cache" / "scratch",
        home / ".hermes" / "cache",
        home / ".hermes" / "profiles",
        home / ".config",
    ]
    discovered_extra: List[Path] = []
    for root in scan_roots:
        discovered_extra.extend(_shallow_find_brain_dirs(root, max_depth=6))

    active_dirs: List[Path] = []
    seen_resolved: set[str] = set()

    def _add_unique(p: Path) -> None:
        try:
            key = str(p.resolve())
        except OSError:
            key = str(p)
        if key not in seen_resolved:
            seen_resolved.add(key)
            active_dirs.append(p)

    for cand in candidates:
        if cand.is_dir() and iter_transcript_files(cand):
            _add_unique(cand)

    # Sort discovered extra dirs so most-populated / newest comes first
    def _rank_dir(p: Path) -> Tuple[int, float]:
        tx = iter_transcript_files(p)
        try:
            mtime = max((t.stat().st_mtime for _, t in tx), default=p.stat().st_mtime)
        except OSError:
            mtime = 0.0
        return (len(tx), mtime)

    discovered_extra.sort(key=_rank_dir, reverse=True)
    for d in discovered_extra:
        if d.is_dir() and iter_transcript_files(d):
            _add_unique(d)

    if active_dirs:
        return active_dirs

    # Fallback if no transcripts exist yet: first existing dir or canonical default
    for cand in candidates + discovered_extra:
        if cand.is_dir():
            return [cand]
    return [candidates[0]]


def discover_agy_dir(cli_agy_dir: Optional[str] = None) -> Path:
    """Return the primary AGY `brain` directory."""
    return discover_agy_dirs(cli_agy_dir)[0]


def _is_writable_sqlite_target(db_file: Path) -> bool:
    """Check whether `db_file` (or its parent directory) is writable for SQLite."""
    parent = db_file.parent
    try:
        if not parent.exists():
            return False
        if not os.access(parent, os.W_OK):
            return False
        if db_file.exists() and not os.access(db_file, os.W_OK):
            return False
        # Probe creating/opening a lock check cleanly if file doesn't exist
        probe = sqlite3.connect(str(db_file))
        probe.execute("SELECT 1")
        probe.close()
        return True
    except Exception:
        return False


def get_machine_device_name() -> str:
    """Return sanitized machine hostname for multi-device database isolation."""
    raw = socket.gethostname().strip().lower()
    cleaned = re.sub(r"[^a-zA-Z0-9_.-]", "_", raw)
    return cleaned or "local"


DEFAULT_DASHBOARD_DIR = Path.home() / ".agy_dashboard"


def resolve_db_path(
    agy_dir: Path,
    cli_db: Optional[str] = None,
    demo_mode: bool = False,
    device_name: Optional[str] = None,
) -> str:
    """Determine SQLite DB path.

    Priority:
    1. Explicit `--db` path (or `:memory:`)
    2. Demo mode -> `:memory:`
    3. Device-isolated database in `~/.agy_dashboard/<device_name>.db`
    4. Fallbacks to brain sibling, ~/.gemini, ~/.cache, or :memory:
    """
    if cli_db:
        if cli_db == ":memory:":
            return ":memory:"
        target = _expand_path(cli_db)
        target.parent.mkdir(parents=True, exist_ok=True)
        return str(target.resolve())

    if demo_mode:
        return ":memory:"

    dev = device_name or get_machine_device_name()
    try:
        DEFAULT_DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
        target_db = DEFAULT_DASHBOARD_DIR / f"{dev}.db"
        if _is_writable_sqlite_target(target_db):
            return str(target_db)
    except OSError:
        pass

    agy_str = str(agy_dir.resolve()).lower()
    is_ephemeral = any(
        seg in agy_str
        for seg in ("/scratch/", "\\scratch\\", "/tmp/", "\\temp\\", "hermes_agy_")
    )
    if not is_ephemeral:
        sibling_db = agy_dir.resolve().parent / "agy_usage.db"
        if _is_writable_sqlite_target(sibling_db):
            return str(sibling_db)

    home_agy_dir = Path.home() / ".gemini" / "antigravity-cli"
    if home_agy_dir.is_dir():
        home_db = home_agy_dir / "agy_usage.db"
        if _is_writable_sqlite_target(home_db):
            return str(home_db)

    cache_dir = Path.home() / ".cache" / "agy-dashboard"
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_db = cache_dir / "agy_usage.db"
        if _is_writable_sqlite_target(cache_db):
            return str(cache_db)
    except OSError:
        pass

    return ":memory:"


# ---------------------------------------------------------------------------
# Timestamp, Title & Transcript Parsing Helpers
# ---------------------------------------------------------------------------

_ISO_FRAC_RE = re.compile(r"(\.\d{6})\d+(?=[Zz]|[+-]\d{2}:?\d{2}|$)")
_METADATA_BLOCK_RE = re.compile(
    r"<(?:ADDITIONAL_METADATA|USER_SETTINGS_CHANGE|EPHEMERAL_MESSAGE)\b[^>]*>.*?</(?:ADDITIONAL_METADATA|USER_SETTINGS_CHANGE|EPHEMERAL_MESSAGE)>",
    re.DOTALL | re.IGNORECASE,
)
_USER_REQ_RE = re.compile(
    r"<USER_REQUEST\b[^>]*>(.*?)</USER_REQUEST>",
    re.DOTALL | re.IGNORECASE,
)
_XML_TAG_RE = re.compile(r"</?[A-Za-z_][A-Za-z0-9_.:-]*(?:\s+[^>]{0,200})?>")
_STEP_INDEX_BYTES_RE = re.compile(rb'"step_index"\s*:\s*(\d+)')
_HERMES_TRANSCRIPT_USER_RE = re.compile(
    r"Conversation transcript:\s*User:\s*(.*?)(?:\nAssistant:|\nTool Result|\Z)",
    re.DOTALL,
)
_HERMES_LATEST_USER_RE = re.compile(
    r"(?:The user request being served is:|### USER REQUEST THIS TOOL LOOP IS SERVING:\s*User:)\s*\"?(.*?)\"?(?:\n\nCRITICAL:|\nINSTRUCTION:|\Z)",
    re.DOTALL,
)
_OOB_USER_MSG_RE = re.compile(
    r"\[OUT-OF-BAND USER MESSAGE[^\]]*\]\s*(.*?)\s*\[/OUT-OF-BAND USER MESSAGE\]",
    re.DOTALL,
)
_INLINE_TOOL_CALL_RE = re.compile(r"<tool_call>\s*(\{.*?\})\s*</tool_call>", re.DOTALL)


def parse_iso_timestamp(raw: Optional[str], fallback_ts: Optional[float] = None) -> dt.datetime:
    """Parse ISO 8601 timestamp (including Go nanosecond precision & trailing Z)
    and convert to local timezone-aware datetime.
    """
    local_tz = dt.datetime.now().astimezone().tzinfo or dt.timezone.utc
    if raw and isinstance(raw, str):
        cleaned = raw.strip()
        if cleaned:
            if cleaned.endswith(("Z", "z")):
                cleaned = cleaned[:-1] + "+00:00"
            cleaned = _ISO_FRAC_RE.sub(r"\1", cleaned)
            try:
                parsed = dt.datetime.fromisoformat(cleaned)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=local_tz)
                else:
                    parsed = parsed.astimezone(local_tz)
                return parsed
            except ValueError:
                pass

    if fallback_ts is not None:
        return dt.datetime.fromtimestamp(fallback_ts, tz=local_tz)
    return dt.datetime.now(tz=local_tz)


def extract_conversation_title(raw_content: Any, conv_id: str) -> str:
    """Clean first `USER_INPUT` content into a concise <=90 char conversation title."""
    text = _flatten_to_text(raw_content)
    if not text:
        return f"Conversation {conv_id[:8]}"

    text = _METADATA_BLOCK_RE.sub(" ", text)
    req_match = _USER_REQ_RE.search(text)
    if req_match:
        text = req_match.group(1)

    # Handle Hermes Agent Headless Inference Mode prompts cleanly
    if "HEADLESS INFERENCE MODE" in text:
        tx_match = _HERMES_TRANSCRIPT_USER_RE.search(text)
        if tx_match:
            text = tx_match.group(1)
        else:
            latest_match = _HERMES_LATEST_USER_RE.search(text)
            if latest_match:
                text = latest_match.group(1)

    oob_match = _OOB_USER_MSG_RE.search(text)
    if oob_match:
        text = oob_match.group(1)

    text = _XML_TAG_RE.sub(" ", text)

    # Pick first non-empty meaningful line if available
    chosen_line = ""
    for line in text.splitlines():
        stripped = line.strip().lstrip("#>-*` ").strip()
        if stripped:
            chosen_line = stripped
            break

    if not chosen_line:
        chosen_line = " ".join(text.split())
    else:
        chosen_line = " ".join(chosen_line.split())

    if not chosen_line:
        return f"Conversation {conv_id[:8]}"

    if len(chosen_line) > 90:
        return chosen_line[:89].rstrip() + "…"
    return chosen_line


def extract_tool_calls(raw_tool_calls: Any, raw_content: Any = None) -> List[str]:
    """Extract normalized tool names from `tool_calls` array or inline `<tool_call>` XML."""
    names: List[str] = []
    if isinstance(raw_tool_calls, list):
        for tc in raw_tool_calls:
            if not isinstance(tc, dict):
                continue
            name = tc.get("name") or tc.get("tool_name") or tc.get("tool")
            if not name and isinstance(tc.get("function"), dict):
                name = tc["function"].get("name")
            if name and isinstance(name, str):
                cleaned = name.strip()
                if cleaned:
                    names.append(cleaned)
    if names or not raw_content:
        return names

    content_str = raw_content if isinstance(raw_content, str) else _flatten_to_text(raw_content)
    if "<tool_call>" in content_str:
        for m in _INLINE_TOOL_CALL_RE.finditer(content_str):
            try:
                tc_obj = json.loads(m.group(1))
                if isinstance(tc_obj, dict):
                    fn = tc_obj.get("function")
                    t_name = (
                        tc_obj.get("name")
                        or (fn.get("name") if isinstance(fn, dict) else None)
                    )
                    if isinstance(t_name, str) and t_name.strip():
                        names.append(t_name.strip())
            except Exception:
                continue
    return names


class FullTranscriptResolver:
    """Lazily indexes `transcript_full.jsonl` byte offsets to resolve `truncated_fields`
    without loading the entire untruncated transcript into memory.
    """

    def __init__(self, full_path: Path) -> None:
        self.full_path = full_path
        self._indexed = False
        self._by_step: Dict[int, int] = {}
        self._by_line: Dict[int, int] = {}

    def _ensure_index(self) -> None:
        if self._indexed:
            return
        self._indexed = True
        if not self.full_path.is_file():
            return
        try:
            with self.full_path.open("rb") as fh:
                line_num = 0
                while True:
                    offset = fh.tell()
                    raw_line = fh.readline()
                    if not raw_line:
                        break
                    stripped = raw_line.strip()
                    if not stripped:
                        continue
                    self._by_line[line_num] = offset
                    m = _STEP_INDEX_BYTES_RE.search(stripped)
                    if m:
                        try:
                            self._by_step[int(m.group(1))] = offset
                        except ValueError:
                            pass
                    else:
                        try:
                            obj = json.loads(stripped.decode("utf-8", errors="replace"))
                            if isinstance(obj, dict) and isinstance(obj.get("step_index"), int):
                                self._by_step[obj["step_index"]] = offset
                        except Exception:
                            pass
                    line_num += 1
        except OSError:
            return

    def resolve(self, step: Dict[str, Any], line_idx: int) -> Dict[str, Any]:
        """Return `step` with any `truncated_fields` populated from `transcript_full.jsonl`."""
        trunc = step.get("truncated_fields")
        if not trunc or not isinstance(trunc, list):
            return step
        self._ensure_index()
        if not self._by_step and not self._by_line:
            return step

        target_offset: Optional[int] = None
        step_idx = step.get("step_index")
        if isinstance(step_idx, int) and step_idx in self._by_step:
            target_offset = self._by_step[step_idx]
        elif line_idx in self._by_line:
            target_offset = self._by_line[line_idx]

        if target_offset is None:
            return step

        try:
            with self.full_path.open("rb") as fh:
                fh.seek(target_offset)
                raw_line = fh.readline()
            full_obj = json.loads(raw_line.decode("utf-8", errors="replace"))
            if not isinstance(full_obj, dict):
                return step
            merged = dict(step)
            for field in trunc:
                if isinstance(field, str) and field in full_obj:
                    merged[field] = full_obj[field]
            return merged
        except Exception:
            return step


# ---------------------------------------------------------------------------
# Incremental SQLite Cache & Transcript Ingestion
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS conversations (
    id TEXT PRIMARY KEY,
    device TEXT NOT NULL DEFAULT 'local',
    file_path TEXT NOT NULL,
    title TEXT NOT NULL,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    mtime_ns INTEGER NOT NULL DEFAULT 0,
    file_size INTEGER NOT NULL DEFAULT 0,
    byte_offset INTEGER NOT NULL DEFAULT 0,
    lines_parsed INTEGER NOT NULL DEFAULT 0,
    running_context INTEGER NOT NULL DEFAULT 0,
    has_user_title INTEGER NOT NULL DEFAULT 0,
    api_requests INTEGER NOT NULL DEFAULT 0,
    tool_calls INTEGER NOT NULL DEFAULT 0,
    subagent_spawns INTEGER NOT NULL DEFAULT 0,
    unique_input_tokens INTEGER NOT NULL DEFAULT 0,
    context_input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    thinking_tokens INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT NOT NULL,
    device TEXT NOT NULL DEFAULT 'local',
    step_index INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    local_date TEXT NOT NULL,
    local_hour INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT 'unknown',
    is_api_request INTEGER NOT NULL DEFAULT 0,
    unique_input_tokens INTEGER NOT NULL DEFAULT 0,
    context_input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    thinking_tokens INTEGER NOT NULL DEFAULT 0,
    tool_calls_count INTEGER NOT NULL DEFAULT 0,
    subagent_spawns INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tool_usages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT NOT NULL,
    local_date TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    call_count INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_events_date ON events(local_date);
CREATE INDEX IF NOT EXISTS idx_events_conv ON events(conversation_id);
CREATE INDEX IF NOT EXISTS idx_tools_date ON tool_usages(local_date);
CREATE INDEX IF NOT EXISTS idx_tools_conv ON tool_usages(conversation_id);
CREATE INDEX IF NOT EXISTS idx_conv_updated ON conversations(updated_at DESC);
"""

SUBAGENT_TOOL_NAMES = {"invoke_subagent", "delegate_task", "spawn_subagent"}
MODEL_STEP_TYPES = {"PLANNER_RESPONSE", "MODEL_RESPONSE", "ASSISTANT_RESPONSE"}
COMPACTION_STEP_TYPES = {"COMPACTION", "CONTEXT_COMPACTION", "CONVERSATION_SUMMARY", "SUMMARY"}


def init_sqlite_connection(db_path: str) -> sqlite3.Connection:
    """Open SQLite connection configured for concurrent multi-threaded reads/writes."""
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row
    if db_path != ":memory:":
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        except sqlite3.Error:
            pass
    conn.execute("PRAGMA foreign_keys=ON;")
    # Run schema migration & creation
    for tbl, col, col_def in [
        ("conversations", "device", "TEXT NOT NULL DEFAULT 'local'"),
        ("events", "device", "TEXT NOT NULL DEFAULT 'local'"),
        ("events", "model", "TEXT NOT NULL DEFAULT 'unknown'"),
    ]:
        try:
            conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {col_def}")
        except sqlite3.OperationalError:
            pass
    conn.executescript(SCHEMA_SQL)
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_device ON events(device)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_conv_device ON conversations(device)")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE events ADD COLUMN model TEXT NOT NULL DEFAULT 'unknown'")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_model ON events(model)")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    return conn


_MODEL_NAME_RE = re.compile(
    r'(gemini-[\w\.-]+|claude-[\w\.-]+|gpt-[\w\.-]+|chatgpt-[\w\.-]+|deepseek-[\w\.-]+|qwen[\w\.-]*|llama-[\w\.-]+|o\d(?:-[\w\.-]+)?)',
    re.IGNORECASE,
)

def extract_step_model(step: Dict[str, Any], fallback: str = "unknown") -> str:
    """Extract model name from step dictionary, metadata, or inference instructions."""
    for key in ("model", "model_name", "llm", "engine"):
        val = step.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    for meta_key in ("metadata", "meta", "extra", "info"):
        sub = step.get(meta_key)
        if isinstance(sub, dict):
            for key in ("model", "model_name", "llm", "engine"):
                val = sub.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip()
    content = step.get("content")
    if isinstance(content, str):
        m = re.search(r'Model:\s*([a-zA-Z0-9_\.-]+)', content)
        if m:
            return m.group(1).strip()
        m2 = _MODEL_NAME_RE.search(content[:500])
        if m2:
            return m2.group(1).strip()
    return fallback

def parse_conversation_incremental(
    conn: sqlite3.Connection,
    conv_id: str,
    transcript_path: Path,
    force: bool = False,
    device: str = "local",
) -> bool:
    """Parse new (or rewritten) lines from `transcript_path` into SQLite.

    Returns True if the database was updated for this conversation, False if unchanged.
    """
    try:
        stat = transcript_path.stat()
    except OSError:
        return False

    mtime_ns = stat.st_mtime_ns
    file_size = stat.st_size
    fallback_mtime = stat.st_mtime

    existing = conn.execute(
        "SELECT * FROM conversations WHERE id = ?", (conv_id,)
    ).fetchone()

    need_full_reset = force
    if existing is not None and not force:
        last_size = int(existing["file_size"])
        last_mtime_ns = int(existing["mtime_ns"])
        last_offset = int(existing["byte_offset"])
        if file_size == last_size and mtime_ns == last_mtime_ns and last_offset == file_size:
            return False
        if file_size < last_size or last_offset > file_size:
            # File shrank or was rewritten in place -> full re-parse
            need_full_reset = True
        elif file_size == last_size and mtime_ns != last_mtime_ns and last_offset == file_size:
            # Same byte length but mtime changed -> re-parse to stay consistent
            need_full_reset = True

    if need_full_reset and existing is not None:
        conn.execute("DELETE FROM tool_usages WHERE conversation_id = ?", (conv_id,))
        conn.execute("DELETE FROM events WHERE conversation_id = ?", (conv_id,))
        conn.execute("DELETE FROM conversations WHERE id = ?", (conv_id,))
        existing = None

    if existing is None:
        start_offset = 0
        lines_parsed = 0
        running_context = 0
        has_user_title = 0
        title = f"Conversation {conv_id[:8]}"
        started_at: Optional[str] = None
        updated_at: Optional[str] = None
        tot_api_req = 0
        tot_tools = 0
        tot_subagents = 0
        tot_uniq_in = 0
        tot_ctx_in = 0
        tot_out = 0
        tot_think = 0
    else:
        start_offset = int(existing["byte_offset"])
        lines_parsed = int(existing["lines_parsed"])
        running_context = int(existing["running_context"])
        has_user_title = int(existing["has_user_title"])
        title = str(existing["title"])
        started_at = str(existing["started_at"])
        updated_at = str(existing["updated_at"])
        tot_api_req = int(existing["api_requests"])
        tot_tools = int(existing["tool_calls"])
        tot_subagents = int(existing["subagent_spawns"])
        tot_uniq_in = int(existing["unique_input_tokens"])
        tot_ctx_in = int(existing["context_input_tokens"])
        tot_out = int(existing["output_tokens"])
        tot_think = int(existing["thinking_tokens"])

    full_path = transcript_path.parent / "transcript_full.jsonl"
    resolver = FullTranscriptResolver(full_path)

    new_events: List[Tuple[Any, ...]] = []
    new_tools: List[Tuple[str, str, str, int]] = []
    committed_offset = start_offset
    current_conv_model = "unknown"

    try:
        with transcript_path.open("rb") as fh:
            fh.seek(start_offset)
            while True:
                line_start = fh.tell()
                raw_line = fh.readline()
                if not raw_line:
                    break
                line_end = fh.tell()
                has_newline = raw_line.endswith(b"\n")
                stripped = raw_line.strip()

                if not stripped:
                    if has_newline or line_end == file_size:
                        committed_offset = line_end
                    continue

                try:
                    step = json.loads(stripped.decode("utf-8", errors="replace"))
                except json.JSONDecodeError:
                    if not has_newline and line_end >= file_size:
                        # Trailing partially-written JSON line: wait for writer to finish
                        break
                    # Malformed line mid-file: skip safely without blocking progress
                    committed_offset = line_end
                    continue

                if not isinstance(step, dict):
                    committed_offset = line_end
                    continue

                if step.get("truncated_fields"):
                    step = resolver.resolve(step, lines_parsed)

                step_model = extract_step_model(step, fallback=current_conv_model)
                if step_model != "unknown":
                    current_conv_model = step_model

                step_idx_raw = step.get("step_index")
                step_idx = step_idx_raw if isinstance(step_idx_raw, int) else lines_parsed
                step_type = str(step.get("type") or "").strip().upper()
                source = str(step.get("source") or "").strip().upper()

                ts_dt = parse_iso_timestamp(step.get("created_at"), fallback_ts=fallback_mtime)
                ts_iso = ts_dt.isoformat(timespec="seconds")
                local_date = ts_dt.strftime("%Y-%m-%d")
                local_hour = ts_dt.hour

                if started_at is None or ts_iso < started_at:
                    started_at = ts_iso
                if updated_at is None or ts_iso > updated_at:
                    updated_at = ts_iso

                # Extract conversation title from first USER_INPUT
                if not has_user_title and (
                    step_type == "USER_INPUT" or source == "USER_EXPLICIT"
                ):
                    candidate_title = extract_conversation_title(step.get("content"), conv_id)
                    if candidate_title and not candidate_title.startswith("Conversation ") and candidate_title:
                        title = candidate_title
                        has_user_title = 1
                    elif candidate_title:
                        title = candidate_title
                        has_user_title = 1

                is_model_turn = (
                    step_type in MODEL_STEP_TYPES
                    or (not step_type and source == "MODEL")
                    or (
                        source == "MODEL"
                        and step_type not in COMPACTION_STEP_TYPES
                        and bool(step.get("thinking") or step.get("tool_calls"))
                    )
                )

                uniq_in = 0
                ctx_in = 0
                out_tok = 0
                think_tok = 0
                is_api_req = 0
                step_tool_count = 0
                step_subagents = 0

                if step_type in COMPACTION_STEP_TYPES:
                    # Context compaction resets cumulative prompt history to summary size
                    summary_tokens = estimate_tokens(step.get("content"))
                    running_context = min(MAX_CONTEXT_WINDOW, summary_tokens)
                    uniq_in = summary_tokens
                elif is_model_turn:
                    is_api_req = 1
                    content_tok = estimate_tokens(step.get("content"))
                    raw_tcs = step.get("tool_calls")
                    tc_tok = estimate_tokens(raw_tcs) if raw_tcs else 0
                    out_tok = content_tok + tc_tok
                    think_tok = estimate_tokens(step.get("thinking"))

                    # Billed API context includes cumulative conversation tokens prior to this turn
                    ctx_in = min(MAX_CONTEXT_WINDOW, max(running_context, 0))
                    running_context = min(MAX_CONTEXT_WINDOW, running_context + out_tok)

                    tool_names = extract_tool_calls(raw_tcs, step.get("content"))
                    step_tool_count = len(tool_names)
                    if tool_names:
                        per_tool_counts: Dict[str, int] = {}
                        for t_name in tool_names:
                            per_tool_counts[t_name] = per_tool_counts.get(t_name, 0) + 1
                            if t_name in SUBAGENT_TOOL_NAMES:
                                step_subagents += 1
                        for t_name, c_val in per_tool_counts.items():
                            new_tools.append((conv_id, local_date, t_name, c_val))
                else:
                    # USER_INPUT, SYSTEM, TOOL_RESPONSE, etc.
                    payload = step.get("content")
                    if payload is None or payload == "":
                        payload = (
                            step.get("tool_responses")
                            or step.get("output")
                            or step.get("result")
                        )
                    uniq_in = estimate_tokens(payload)
                    # Check if a compaction placeholder reset context drastically
                    flat_str = payload if isinstance(payload, str) else ""
                    if "<conversation_summary>" in flat_str and source == "SYSTEM":
                        running_context = min(MAX_CONTEXT_WINDOW, uniq_in)
                    else:
                        running_context = min(MAX_CONTEXT_WINDOW, running_context + uniq_in)

                tot_api_req += is_api_req
                tot_tools += step_tool_count
                tot_subagents += step_subagents
                tot_uniq_in += uniq_in
                tot_ctx_in += ctx_in
                tot_out += out_tok
                tot_think += think_tok

                new_events.append(
                    (
                        conv_id,
                        device,
                        step_idx,
                        ts_iso,
                        local_date,
                        local_hour,
                        step_type or source or "UNKNOWN",
                        current_conv_model,
                        is_api_req,
                        uniq_in,
                        ctx_in,
                        out_tok,
                        think_tok,
                        step_tool_count,
                        step_subagents,
                    )
                )

                lines_parsed += 1
                committed_offset = line_end
    except OSError:
        return False

    now_iso = parse_iso_timestamp(None, fallback_ts=fallback_mtime).isoformat(timespec="seconds")
    if started_at is None:
        started_at = now_iso
    if updated_at is None:
        updated_at = now_iso

    conn.execute(
        """
        INSERT INTO conversations (
            id, device, file_path, title, started_at, updated_at,
            mtime_ns, file_size, byte_offset, lines_parsed,
            running_context, has_user_title,
            api_requests, tool_calls, subagent_spawns,
            unique_input_tokens, context_input_tokens, output_tokens, thinking_tokens
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            device = excluded.device,
            file_path = excluded.file_path,
            title = excluded.title,
            started_at = excluded.started_at,
            updated_at = excluded.updated_at,
            mtime_ns = excluded.mtime_ns,
            file_size = excluded.file_size,
            byte_offset = excluded.byte_offset,
            lines_parsed = excluded.lines_parsed,
            running_context = excluded.running_context,
            has_user_title = excluded.has_user_title,
            api_requests = excluded.api_requests,
            tool_calls = excluded.tool_calls,
            subagent_spawns = excluded.subagent_spawns,
            unique_input_tokens = excluded.unique_input_tokens,
            context_input_tokens = excluded.context_input_tokens,
            output_tokens = excluded.output_tokens,
            thinking_tokens = excluded.thinking_tokens
        """,
        (
            conv_id,
            device,
            str(transcript_path),
            title,
            started_at,
            updated_at,
            mtime_ns,
            file_size,
            committed_offset,
            lines_parsed,
            running_context,
            has_user_title,
            tot_api_req,
            tot_tools,
            tot_subagents,
            tot_uniq_in,
            tot_ctx_in,
            tot_out,
            tot_think,
        ),
    )

    if new_events:
        conn.executemany(
            """
            INSERT INTO events (
                conversation_id, device, step_index, timestamp, local_date, local_hour,
                event_type, model, is_api_request, unique_input_tokens, context_input_tokens,
                output_tokens, thinking_tokens, tool_calls_count, subagent_spawns
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            new_events,
        )

    if new_tools:
        conn.executemany(
            """
            INSERT INTO tool_usages (conversation_id, local_date, tool_name, call_count)
            VALUES (?, ?, ?, ?)
            """,
            new_tools,
        )

    return True


def scan_agy_directory(
    conn: sqlite3.Connection,
    agy_dir: Union[Path, Sequence[Path]],
    force: bool = False,
    device: str = "local",
) -> Dict[str, Any]:
    """Incrementally (or forcibly) scan all transcripts across one or more `agy_dir` roots."""
    t0 = time.perf_counter()
    if force:
        conn.execute("DELETE FROM tool_usages")
        conn.execute("DELETE FROM events")
        conn.execute("DELETE FROM conversations")

    dirs: List[Path] = [agy_dir] if isinstance(agy_dir, Path) else list(agy_dir)
    merged_by_id: Dict[str, Tuple[float, Path]] = {}
    any_dir_exists = False
    for d in dirs:
        if d.is_dir():
            any_dir_exists = True
        for cid, tx_path in iter_transcript_files(d):
            try:
                mt = tx_path.stat().st_mtime
            except OSError:
                mt = 0.0
            prev = merged_by_id.get(cid)
            if prev is None or mt >= prev[0]:
                merged_by_id[cid] = (mt, tx_path)

    transcripts = sorted([(cid, pair[1]) for cid, pair in merged_by_id.items()], key=lambda x: x[0])
    present_ids = {cid for cid, _ in transcripts}

    # Prune deleted conversations only if explicit single brain_dir is used (keep archived scratch sessions)
    if len(dirs) == 1 and any_dir_exists and not force:
        is_scratch = "hermes_agy_" in str(dirs[0]).lower() or "/scratch/" in str(dirs[0]).lower()
        if not is_scratch:
            existing_ids = {
                str(r["id"]) for r in conn.execute("SELECT id FROM conversations").fetchall()
            }
            stale_ids = existing_ids - present_ids
            for stale in stale_ids:
                conn.execute("DELETE FROM tool_usages WHERE conversation_id = ?", (stale,))
                conn.execute("DELETE FROM events WHERE conversation_id = ?", (stale,))
                conn.execute("DELETE FROM conversations WHERE id = ?", (stale,))

    updated_count = 0
    for conv_id, tx_path in transcripts:
        if parse_conversation_incremental(conn, conv_id, tx_path, force=force, device=device):
            updated_count += 1

    conn.commit()
    elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    total_convs = int(
        conn.execute("SELECT COUNT(*) AS c FROM conversations").fetchone()["c"]
    )
    return {
        "scanned_files": len(transcripts),
        "updated_conversations": updated_count,
        "total_conversations": total_convs,
        "elapsed_ms": elapsed_ms,
    }


# ---------------------------------------------------------------------------
# Rich Realistic 45-Day Demo Data Generator
# ---------------------------------------------------------------------------

_DEMO_TITLES = [
    "Refactor WebSocket reconnection state machine with jittered exponential backoff",
    "Debug SQLite WAL lock contention under concurrent ThreadingHTTPServer workers",
    "Implement OAuth2 PKCE login flow and encrypted OS keyring token storage",
    "Optimize virtualized log viewer rendering for 100k streaming JSONL rows",
    "Build zero-dependency CLI usage analytics dashboard with dark glassmorphic UI",
    "Fix race condition in async subagent message queue cancellation handler",
    "Migrate legacy REST pagination endpoints to keyset cursor pagination",
    "Audit multi-stage Docker build and trim production image below 95MB",
    "Add OpenTelemetry trace context propagation across gRPC microservice hops",
    "Write property-based fuzz tests for incremental JSONL parser edge cases",
    "Investigate memory leak in SSE event stream reconnection buffer",
    "Design schema migration runner with transactional rollback verification",
    "Implement fuzzy file finder index with gitignore rules and ripgrep fallback",
    "Resolve TypeScript strict nullability errors across shared workspace packages",
    "Profile Python AST symbol indexer and cut cold-start latency by 65%",
    "Configure GitHub Actions matrix CI for Linux, macOS ARM64, and Windows",
    "Add keyboard navigation, ARIA live regions, and high-contrast theme tokens",
    "Debug TLS certificate chain validation failure behind corporate HTTP proxy",
    "Implement token bucket rate limiter middleware with Redis Lua script",
    "Extract reusable SVG sparkline and heatmap components from analytics prototype",
    "Fix timezone boundary off-by-one bug in daily rollup aggregation query",
    "Automate release changelog generation from conventional commit trailers",
    "Benchmark B-tree composite indexes for time-range + conversation joins",
    "Synthesize architecture review and decompose monolith billing module",
    "Patch CVE in webhook HMAC signature verification to use constant-time compare",
    "Build interactive terminal summary table formatter with Unicode box drawing",
]

_DEMO_TOOL_WEIGHTS: List[Tuple[str, float]] = [
    ("read_file", 0.30),
    ("edit_file", 0.21),
    ("search_files", 0.17),
    ("run_command", 0.16),
    ("write_file", 0.07),
    ("invoke_subagent", 0.04),
    ("manage_task", 0.03),
    ("ask_question", 0.02),
]


def _pick_weighted_tool(rng: random.Random) -> str:
    r = rng.random()
    acc = 0.0
    for name, weight in _DEMO_TOOL_WEIGHTS:
        acc += weight
        if r <= acc:
            return name
    return _DEMO_TOOL_WEIGHTS[0][0]


def seed_demo_data(conn: sqlite3.Connection, today: Optional[dt.date] = None) -> None:
    """Populate `conn` with 45 days of realistic developer AGY usage (~26 conversations).

    Includes weekday/weekend cadence, working-hour peaks + occasional late-night
    sessions, realistic coding prompts, multi-turn context growth, deep-thinking
    architecture spikes, and realistic tool distributions.
    """
    conn.execute("DELETE FROM tool_usages")
    conn.execute("DELETE FROM events")
    conn.execute("DELETE FROM conversations")

    local_tz = dt.datetime.now().astimezone().tzinfo or dt.timezone.utc
    anchor_date = today or dt.datetime.now(tz=local_tz).date()
    rng = random.Random(20261030)

    # Select active days across a 45-day window ending today (higher probability on weekdays)
    day_offsets: List[int] = []
    for delta in range(44, -1, -1):
        d = anchor_date - dt.timedelta(days=delta)
        is_weekend = d.weekday() >= 5
        # Always make today and yesterday active so 7d/30d views look lively
        if delta in (0, 1, 3):
            active = True
        elif is_weekend:
            active = rng.random() < 0.28
        else:
            active = rng.random() < 0.68
        if active:
            day_offsets.append(delta)

    # Target ~25 conversations distributed across the full 45-day span including today
    target_convs = 25
    must_include = [d for d in (0, 1, 3, 5) if d in day_offsets]
    older_pool = [d for d in day_offsets if d not in must_include]
    if len(must_include) + len(older_pool) >= target_convs:
        need_older = target_convs - len(must_include)
        # Evenly stride across older active days so full 45d history + recent 7d are populated
        sampled_older = [
            older_pool[int(round(i * (len(older_pool) - 1) / max(1, need_older - 1)))]
            for i in range(need_older)
        ] if need_older > 0 and older_pool else []
        chosen_slots = sampled_older + must_include
    else:
        chosen_slots = list(day_offsets)
        while len(chosen_slots) < target_convs:
            chosen_slots.append(rng.choice(day_offsets))
    chosen_slots.sort(reverse=True)

    # Hour weights: peak 10-12 & 14-18, light evening 20-23
    hour_choices = [9, 10, 10, 11, 11, 12, 13, 14, 14, 15, 15, 16, 16, 17, 18, 21, 22]

    conv_rows: List[Tuple[Any, ...]] = []
    event_rows: List[Tuple[Any, ...]] = []
    tool_rows: List[Tuple[str, str, str, int]] = []

    for idx, delta in enumerate(chosen_slots):
        session_date = anchor_date - dt.timedelta(days=delta)
        start_hour = rng.choice(hour_choices)
        start_min = rng.randint(2, 52)
        cur_dt = dt.datetime(
            session_date.year,
            session_date.month,
            session_date.day,
            start_hour,
            start_min,
            rng.randint(0, 59),
            tzinfo=local_tz,
        )
        # Clamp to current time if generated on today's date
        now_dt = dt.datetime.now(tz=local_tz)
        if cur_dt > now_dt:
            cur_dt = now_dt - dt.timedelta(minutes=rng.randint(25, 140))

        conv_id = f"demo-{idx + 1:02d}-{rng.randrange(16**8):08x}"
        title = _DEMO_TITLES[idx % len(_DEMO_TITLES)]
        demo_model = rng.choices(["gemini-3.8-flash", "claude-3-5-sonnet", "gemini-2.5-pro", "chatgpt-4o"], weights=[45, 30, 15, 10])[0]
        # Realistic multi-device distribution across desktop workstation, laptop, and server
        demo_device = rng.choices(["desktop-cachyos", "laptop-thinkpad", "dev-server"], weights=[55, 35, 10])[0]

        # Heavy architecture/debugging spike every ~5th session
        is_deep_session = (idx % 5 == 1) or (delta == 0)
        num_turns = rng.randint(9, 18) if is_deep_session else rng.randint(4, 11)

        running_ctx = rng.randint(2800, 4800)  # Base system + skill prompt context
        started_iso = cur_dt.isoformat(timespec="seconds")

        tot_api_req = 0
        tot_tools = 0
        tot_subagents = 0
        tot_uniq_in = 0
        tot_ctx_in = 0
        tot_out = 0
        tot_think = 0
        step_idx = 0

        # Initial user prompt + base context
        init_prompt_tok = running_ctx + rng.randint(180, 950)
        running_ctx = init_prompt_tok
        tot_uniq_in += init_prompt_tok
        event_rows.append(
            (
                conv_id,
                demo_device,
                step_idx,
                started_iso,
                cur_dt.strftime("%Y-%m-%d"),
                cur_dt.hour,
                "USER_INPUT",
                demo_model,
                0,
                init_prompt_tok,
                0,
                0,
                0,
                0,
                0,
            )
        )
        step_idx += 1

        for turn in range(num_turns):
            cur_dt = cur_dt + dt.timedelta(seconds=rng.randint(18, 95))
            if cur_dt > now_dt:
                cur_dt = now_dt - dt.timedelta(seconds=max(1, (num_turns - turn) * 5))

            ev_iso = cur_dt.isoformat(timespec="seconds")
            ev_date = cur_dt.strftime("%Y-%m-%d")
            ev_hour = cur_dt.hour

            # Planner response (API request)
            ctx_in = min(MAX_CONTEXT_WINDOW, running_ctx)
            out_tok = rng.randint(320, 2100) if is_deep_session else rng.randint(180, 1150)
            if is_deep_session and turn in (0, 2, 5):
                think_tok = rng.randint(1800, 5200)
            else:
                think_tok = rng.randint(160, 1400)

            tc_count = rng.choices([0, 1, 2, 3, 4], weights=[12, 36, 28, 16, 8])[0]
            turn_subagents = 0
            if tc_count > 0:
                per_tool: Dict[str, int] = {}
                for _ in range(tc_count):
                    tname = _pick_weighted_tool(rng)
                    per_tool[tname] = per_tool.get(tname, 0) + 1
                    if tname in SUBAGENT_TOOL_NAMES:
                        turn_subagents += 1
                for tname, cval in per_tool.items():
                    tool_rows.append((conv_id, ev_date, tname, cval))

            running_ctx = min(MAX_CONTEXT_WINDOW, running_ctx + out_tok)

            tot_api_req += 1
            tot_tools += tc_count
            tot_subagents += turn_subagents
            tot_ctx_in += ctx_in
            tot_out += out_tok
            tot_think += think_tok

            event_rows.append(
                (
                    conv_id,
                    demo_device,
                    step_idx,
                    ev_iso,
                    ev_date,
                    ev_hour,
                    "PLANNER_RESPONSE",
                    demo_model,
                    1,
                    0,
                    ctx_in,
                    out_tok,
                    think_tok,
                    tc_count,
                    turn_subagents,
                )
            )
            step_idx += 1

            # Tool results or follow-up user input feeding into next turn
            if turn < num_turns - 1:
                cur_dt = cur_dt + dt.timedelta(seconds=rng.randint(4, 35))
                if cur_dt > now_dt:
                    cur_dt = now_dt
                follow_tok = (
                    rng.randint(450, 3400) if tc_count > 0 else rng.randint(90, 480)
                )
                running_ctx = min(MAX_CONTEXT_WINDOW, running_ctx + follow_tok)
                tot_uniq_in += follow_tok
                event_rows.append(
                    (
                        conv_id,
                        demo_device,
                        step_idx,
                        cur_dt.isoformat(timespec="seconds"),
                        cur_dt.strftime("%Y-%m-%d"),
                        cur_dt.hour,
                        "TOOL_RESPONSE" if tc_count > 0 else "USER_INPUT",
                        demo_model,
                        0,
                        follow_tok,
                        0,
                        0,
                        0,
                        0,
                        0,
                    )
                )
                step_idx += 1

        updated_iso = cur_dt.isoformat(timespec="seconds")
        conv_rows.append(
            (
                conv_id,
                demo_device,
                f"demo://{conv_id}/transcript.jsonl",
                title,
                started_iso,
                updated_iso,
                0,
                0,
                0,
                step_idx,
                running_ctx,
                1,
                tot_api_req,
                tot_tools,
                tot_subagents,
                tot_uniq_in,
                tot_ctx_in,
                tot_out,
                tot_think,
            )
        )

    conn.executemany(
        """
        INSERT INTO conversations (
            id, device, file_path, title, started_at, updated_at,
            mtime_ns, file_size, byte_offset, lines_parsed,
            running_context, has_user_title,
            api_requests, tool_calls, subagent_spawns,
            unique_input_tokens, context_input_tokens, output_tokens, thinking_tokens
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        conv_rows,
    )
    conn.executemany(
        """
        INSERT INTO events (
            conversation_id, device, step_index, timestamp, local_date, local_hour,
            event_type, model, is_api_request, unique_input_tokens, context_input_tokens,
            output_tokens, thinking_tokens, tool_calls_count, subagent_spawns
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        event_rows,
    )
    conn.executemany(
        """
        INSERT INTO tool_usages (conversation_id, local_date, tool_name, call_count)
        VALUES (?, ?, ?, ?)
        """,
        tool_rows,
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Analytics Engine & `/api/stats` Builder
# ---------------------------------------------------------------------------


def _quantile_levels(values: List[int]) -> List[int]:
    """Assign GitHub-style 0..4 quantile intensity levels across daily token values."""
    nonzero = sorted(v for v in values if v > 0)
    if not nonzero:
        return [0 for _ in values]

    n = len(nonzero)
    if len(set(nonzero)) == 1:
        return [3 if v > 0 else 0 for v in values]

    def _pct(p: float) -> float:
        pos = p * (n - 1)
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            return float(nonzero[lo])
        return nonzero[lo] + (nonzero[hi] - nonzero[lo]) * (pos - lo)

    q1, q2, q3 = _pct(0.25), _pct(0.50), _pct(0.75)
    levels: List[int] = []
    for v in values:
        if v <= 0:
            levels.append(0)
        elif v <= q1:
            levels.append(1)
        elif v <= q2:
            levels.append(2)
        elif v <= q3:
            levels.append(3)
        else:
            levels.append(4)
    return levels


class UsageTracker:
    """Thread-safe coordinator managing SQLite state, auto-demo fallback, and queries."""

    def __init__(
        self,
        agy_dir: Optional[Path] = None,
        cli_agy_dir: Optional[str] = None,
        db_path: Optional[str] = None,
        force_demo: bool = False,
        force_rescan: bool = False,
        device_name: Optional[str] = None,
    ) -> None:
        self.lock = threading.RLock()
        self._cli_agy_dir = cli_agy_dir
        self.device_name = device_name or get_machine_device_name()
        if agy_dir is not None and cli_agy_dir is None:
            self.agy_dirs: List[Path] = [agy_dir]
            self._pinned_single = True
        else:
            self.agy_dirs = discover_agy_dirs(cli_agy_dir)
            self._pinned_single = False
        self.agy_dir = self.agy_dirs[0]
        self.force_demo = force_demo
        self.db_path = resolve_db_path(self.agy_dir, cli_db=db_path, demo_mode=force_demo, device_name=self.device_name)

        self._real_conn = init_sqlite_connection(self.db_path)
        self._demo_conn: Optional[sqlite3.Connection] = None
        self._unified_conn: Optional[sqlite3.Connection] = None
        self.is_demo = False
        self.demo_reason: Optional[str] = None
        self.last_scanned_at = dt.datetime.now().astimezone().isoformat(timespec="seconds")

        if self.force_demo:
            seed_demo_data(self._real_conn)
            self.is_demo = True
            self.demo_reason = DEMO_FLAG_REASON
        else:
            self.rescan(force=force_rescan)

    def _rebuild_unified_conn(self) -> None:
        """Discover all device SQLite databases in ~/.agy_dashboard and unify them into an in-memory view."""
        if self.is_demo or self.force_demo:
            return

        peer_dbs: List[Path] = []
        if DEFAULT_DASHBOARD_DIR.is_dir():
            peer_dbs = sorted(DEFAULT_DASHBOARD_DIR.glob("*.db"))

        # If no peer database exists or only local db file which matches self.db_path
        real_path = Path(self.db_path).resolve() if self.db_path != ":memory:" else None
        other_peers = [p for p in peer_dbs if real_path is None or p.resolve() != real_path]

        if not other_peers:
            if self._unified_conn is not None:
                self._unified_conn.close()
                self._unified_conn = None
            return

        # Build unified in-memory DB merging local and all peers
        all_dbs: List[Path] = []
        if real_path and real_path.exists():
            all_dbs.append(real_path)
        all_dbs.extend(other_peers)

        new_unified = init_sqlite_connection(":memory:")
        for idx, db_file in enumerate(all_dbs):
            schema = f"peer_{idx}"
            dev_fallback = db_file.stem
            try:
                new_unified.execute(f"ATTACH DATABASE '{db_file}' AS {schema}")
                new_unified.execute(f"""
                    INSERT OR REPLACE INTO conversations (id, device, file_path, title, started_at, updated_at, total_tokens, cost_usd, electricity_wh, co2_grams, water_liters, water_ml, api_requests, tool_calls)
                    SELECT id, CASE WHEN device IS NULL OR device = '' OR device = 'local' THEN ? ELSE device END, file_path, title, started_at, updated_at, total_tokens, cost_usd, electricity_wh, co2_grams, water_liters, water_ml, api_requests, tool_calls
                    FROM {schema}.conversations
                """, (dev_fallback,))
                new_unified.execute(f"""
                    INSERT OR IGNORE INTO events (conversation_id, device, step_index, timestamp, local_date, local_hour, event_type, model, is_api_request, unique_input_tokens, context_input_tokens, output_tokens, thinking_tokens, tool_calls_count, subagent_spawns)
                    SELECT conversation_id, CASE WHEN device IS NULL OR device = '' OR device = 'local' THEN ? ELSE device END, step_index, timestamp, local_date, local_hour, event_type, model, is_api_request, unique_input_tokens, context_input_tokens, output_tokens, thinking_tokens, tool_calls_count, subagent_spawns
                    FROM {schema}.events
                """, (dev_fallback,))
                new_unified.execute(f"""
                    INSERT OR IGNORE INTO tool_usages (conversation_id, device, step_index, tool_name, call_count, timestamp)
                    SELECT conversation_id, CASE WHEN device IS NULL OR device = '' OR device = 'local' THEN ? ELSE device END, step_index, tool_name, call_count, timestamp
                    FROM {schema}.tool_usages
                """, (dev_fallback,))
                new_unified.commit()
                new_unified.execute(f"DETACH DATABASE {schema}")
            except Exception:
                try:
                    new_unified.execute(f"DETACH DATABASE {schema}")
                except Exception:
                    pass

        if self._unified_conn is not None:
            self._unified_conn.close()
        self._unified_conn = new_unified

    @property
    def _active_conn(self) -> sqlite3.Connection:
        if self.is_demo and not self.force_demo and self._demo_conn is not None:
            return self._demo_conn
        if self._unified_conn is not None:
            return self._unified_conn
        return self._real_conn

    def close(self) -> None:
        with self.lock:
            if self._demo_conn is not None:
                self._demo_conn.close()
                self._demo_conn = None
            if self._unified_conn is not None:
                self._unified_conn.close()
                self._unified_conn = None
            self._real_conn.close()

    def rescan(self, force: bool = False) -> Dict[str, Any]:
        """Trigger incremental or full rescan; auto-toggle demo fallback if empty."""
        with self.lock:
            now_iso = dt.datetime.now().astimezone().isoformat(timespec="seconds")
            self.last_scanned_at = now_iso

            if self.force_demo:
                if force:
                    seed_demo_data(self._real_conn)
                total = int(
                    self._real_conn.execute(
                        "SELECT COUNT(*) AS c FROM conversations"
                    ).fetchone()["c"]
                )
                return {
                    "ok": True,
                    "scanned_files": 0,
                    "updated_conversations": total if force else 0,
                    "total_conversations": total,
                    "elapsed_ms": 0.0,
                    "is_demo": True,
                    "demo_reason": self.demo_reason,
                    "last_scanned_at": self.last_scanned_at,
                }

            if not self._pinned_single:
                self.agy_dirs = discover_agy_dirs(self._cli_agy_dir)
                if self.agy_dirs:
                    self.agy_dir = self.agy_dirs[0]
            scan_info = scan_agy_directory(self._real_conn, self.agy_dirs, force=force, device=self.device_name)
            if scan_info["total_conversations"] > 0:
                self.is_demo = False
                self.demo_reason = None
                if self._demo_conn is not None:
                    self._demo_conn.close()
                    self._demo_conn = None
            else:
                # Zero local transcripts: spin up clean in-memory preview without polluting real DB
                if self._demo_conn is None or force:
                    if self._demo_conn is not None:
                        self._demo_conn.close()
                    self._demo_conn = init_sqlite_connection(":memory:")
                    seed_demo_data(self._demo_conn)
                self.is_demo = True
                self.demo_reason = DEMO_AUTO_REASON

            self._rebuild_unified_conn()
            if self._unified_conn is not None:
                total_unified = int(self._unified_conn.execute("SELECT COUNT(*) AS c FROM conversations").fetchone()["c"])
                if total_unified > 0:
                    self.is_demo = False
                    self.demo_reason = None
                    if self._demo_conn is not None:
                        self._demo_conn.close()
                        self._demo_conn = None
                    scan_info["total_conversations"] = total_unified

            return {
                "ok": True,
                **scan_info,
                "is_demo": self.is_demo,
                "demo_reason": self.demo_reason,
                "last_scanned_at": self.last_scanned_at,
            }

    def get_stats(
        self,
        days: str = "30",
        pricing: str = "blended",
        billing_mode: str = "context",
        model: str = "all",
    ) -> Dict[str, Any]:
        """Compute the full `/api/stats` payload."""
        days_norm = str(days or "30").strip().lower()
        if days_norm not in VALID_DAYS_FILTERS:
            days_norm = "30"

        pricing_norm = str(pricing or "blended").strip().lower()
        if pricing_norm not in PRICING_PROFILES:
            pricing_norm = "blended"

        billing_norm = str(billing_mode or "context").strip().lower()
        if billing_norm == "unique":
            billing_norm = "turn"
        if billing_norm not in VALID_BILLING_MODES:
            billing_norm = "context"

        model_norm = str(model or "all").strip().lower()
        available_models = self._get_available_models()
        if model_norm != "all" and model_norm not in available_models:
            model_norm = "all"

        use_context = billing_norm == "context"

        with self.lock:
            conn = self._active_conn
            local_tz = dt.datetime.now().astimezone().tzinfo or dt.timezone.utc
            today_date = dt.datetime.now(tz=local_tz).date()
            today_str = today_date.strftime("%Y-%m-%d")

            if days_norm == "all":
                cutoff_date: Optional[dt.date] = None
                cutoff_str: Optional[str] = None
            else:
                n_days = int(days_norm)
                cutoff_date = today_date - dt.timedelta(days=n_days - 1)
                cutoff_str = cutoff_date.strftime("%Y-%m-%d")

            # Build model filter clause
            model_filter = "" if model_norm == "all" else "AND model = ?"
            model_params = [] if model_norm == "all" else [model_norm]

            # 1) Daily breakdown within selected window
            if cutoff_str is not None:
                daily_rows = conn.execute(
                    f"""
                    SELECT
                        local_date,
                        SUM(unique_input_tokens) AS uniq_in,
                        SUM(context_input_tokens) AS ctx_in,
                        SUM(output_tokens) AS out_tok,
                        SUM(thinking_tokens) AS think_tok,
                        SUM(is_api_request) AS api_reqs,
                        SUM(tool_calls_count) AS tool_calls,
                        SUM(subagent_spawns) AS subagents,
                        COUNT(DISTINCT conversation_id) AS conv_count
                    FROM events
                    WHERE local_date >= ? AND local_date <= ?
                    GROUP BY local_date
                    ORDER BY local_date ASC
                    """,
                    (cutoff_str, today_str),
                ).fetchall()
            else:
                daily_rows = conn.execute(
                    f"""
                    SELECT
                        local_date,
                        SUM(unique_input_tokens) AS uniq_in,
                        SUM(context_input_tokens) AS ctx_in,
                        SUM(output_tokens) AS out_tok,
                        SUM(thinking_tokens) AS think_tok,
                        SUM(is_api_request) AS api_reqs,
                        SUM(tool_calls_count) AS tool_calls,
                        SUM(subagent_spawns) AS subagents,
                        COUNT(DISTINCT conversation_id) AS conv_count
                    FROM events
                    WHERE 1=1 {model_filter}
                    GROUP BY local_date
                    ORDER BY local_date ASC
                    """,
                    tuple(model_params),
                ).fetchall()

            by_date: Dict[str, sqlite3.Row] = {str(r["local_date"]): r for r in daily_rows}

            if cutoff_date is not None:
                start_day = cutoff_date
            elif daily_rows:
                try:
                    start_day = dt.date.fromisoformat(str(daily_rows[0]["local_date"]))
                except ValueError:
                    start_day = today_date
            else:
                start_day = today_date

            end_day = today_date
            if daily_rows:
                try:
                    last_ev_day = dt.date.fromisoformat(str(daily_rows[-1]["local_date"]))
                    if last_ev_day > end_day:
                        end_day = last_ev_day
                except ValueError:
                    pass

            daily_list: List[Dict[str, Any]] = []
            tot_uniq_in = 0
            tot_ctx_in = 0
            tot_out = 0
            tot_think = 0
            tot_api_reqs = 0
            tot_tool_calls = 0
            tot_subagents = 0
            active_days = 0
            peak_date: Optional[str] = None
            peak_tokens = 0

            span_days = max(0, (end_day - start_day).days)
            for offset in range(span_days + 1):
                d_str = (start_day + dt.timedelta(days=offset)).strftime("%Y-%m-%d")
                row = by_date.get(d_str)
                if row is not None:
                    u_in = int(row["uniq_in"] or 0)
                    c_in = int(row["ctx_in"] or 0)
                    o_tok = int(row["out_tok"] or 0)
                    t_tok = int(row["think_tok"] or 0)
                    reqs = int(row["api_reqs"] or 0)
                    tcalls = int(row["tool_calls"] or 0)
                    subs = int(row["subagents"] or 0)
                    convs = int(row["conv_count"] or 0)
                else:
                    u_in = c_in = o_tok = t_tok = reqs = tcalls = subs = convs = 0

                eff_in = c_in if use_context else u_in
                day_total = eff_in + o_tok + t_tok
                day_cost = compute_cost_usd(eff_in, o_tok, t_tok, pricing_norm)
                day_energy = compute_energy_metrics(eff_in, o_tok, t_tok)

                tot_uniq_in += u_in
                tot_ctx_in += c_in
                tot_out += o_tok
                tot_think += t_tok
                tot_api_reqs += reqs
                tot_tool_calls += tcalls
                tot_subagents += subs

                if day_total > 0 or reqs > 0:
                    active_days += 1
                if day_total > peak_tokens:
                    peak_tokens = day_total
                    peak_date = d_str

                daily_list.append(
                    {
                        "date": d_str,
                        "input_tokens": eff_in,
                        "unique_input_tokens": u_in,
                        "context_input_tokens": c_in,
                        "output_tokens": o_tok,
                        "thinking_tokens": t_tok,
                        "total_tokens": day_total,
                        "api_requests": reqs,
                        "tool_calls": tcalls,
                        "conversations": convs,
                        "cost_usd": round(day_cost, 6),
                        "electricity_wh": round(day_energy["electricity_wh"], 4),
                        "co2_grams": round(day_energy["co2_grams"], 4),
                        "water_liters": round(day_energy["water_liters"], 4),
                        "water_ml": round(day_energy["water_ml"], 2),
                    }
                )

            # 2) Distinct conversation count in range
            if cutoff_str is not None:
                conv_count_total = int(
                    conn.execute(
                        "SELECT COUNT(DISTINCT conversation_id) AS c FROM events WHERE local_date >= ? AND local_date <= ?",
                        (cutoff_str, today_str),
                    ).fetchone()["c"]
                )
            else:
                conv_count_total = int(
                    conn.execute(
                        "SELECT COUNT(*) AS c FROM conversations"
                    ).fetchone()["c"]
                )

            eff_in_total = tot_ctx_in if use_context else tot_uniq_in
            grand_total_tokens = eff_in_total + tot_out + tot_think
            total_cost = compute_cost_usd(eff_in_total, tot_out, tot_think, pricing_norm)
            total_energy = compute_energy_metrics(eff_in_total, tot_out, tot_think)
            avg_tok_per_req = (
                round(grand_total_tokens / float(tot_api_reqs), 1)
                if tot_api_reqs > 0
                else 0.0
            )
            thinking_share = (
                round((tot_think / float(grand_total_tokens)) * 100.0, 2)
                if grand_total_tokens > 0
                else 0.0
            )

            totals_obj: Dict[str, Any] = {
                "total_tokens": grand_total_tokens,
                "effective_input_tokens": eff_in_total,
                "unique_input_tokens": tot_uniq_in,
                "context_input_tokens": tot_ctx_in,
                "output_tokens": tot_out,
                "thinking_tokens": tot_think,
                "api_requests": tot_api_reqs,
                "conversations": conv_count_total,
                "subagent_spawns": tot_subagents,
                "tool_calls": tot_tool_calls,
                "cost_usd": round(total_cost, 4),
                "electricity_wh": round(total_energy["electricity_wh"], 4),
                "electricity_kwh": round(total_energy["electricity_kwh"], 6),
                "co2_grams": round(total_energy["co2_grams"], 4),
                "water_liters": round(total_energy["water_liters"], 4),
                "water_ml": round(total_energy["water_ml"], 2),
                "phone_charges_equiv": round(total_energy["phone_charges_equiv"], 4),
                "led_bulb_hours_equiv": round(total_energy["led_bulb_hours_equiv"], 4),
                "avg_tokens_per_request": avg_tok_per_req,
                "thinking_share_pct": thinking_share,
                "active_days": active_days,
                "peak_day": {
                    "date": peak_date,
                    "tokens": peak_tokens,
                },
            }

            # 3) 365-day activity heatmap ending today
            hm_start = today_date - dt.timedelta(days=364)
            hm_start_str = hm_start.strftime("%Y-%m-%d")
            hm_rows = conn.execute(
                f"""
                SELECT
                    local_date,
                    SUM(unique_input_tokens) AS uniq_in,
                    SUM(context_input_tokens) AS ctx_in,
                    SUM(output_tokens) AS out_tok,
                    SUM(thinking_tokens) AS think_tok,
                    SUM(is_api_request) AS api_reqs
                FROM events
                WHERE local_date >= ? AND local_date <= ? {model_filter}
                GROUP BY local_date
                """,
                (hm_start_str, today_str) + tuple(model_params),
            ).fetchall()
            hm_map = {str(r["local_date"]): r for r in hm_rows}
            hm_raw_entries: List[Dict[str, Any]] = []
            hm_token_vals: List[int] = []
            for offset in range(365):
                d_str = (hm_start + dt.timedelta(days=offset)).strftime("%Y-%m-%d")
                r = hm_map.get(d_str)
                if r is not None:
                    eff_i = int(r["ctx_in"] or 0) if use_context else int(r["uniq_in"] or 0)
                    o_t = int(r["out_tok"] or 0)
                    t_t = int(r["think_tok"] or 0)
                    reqs = int(r["api_reqs"] or 0)
                    tot_t = eff_i + o_t + t_t
                    c_usd = round(compute_cost_usd(eff_i, o_t, t_t, pricing_norm), 6)
                else:
                    tot_t = 0
                    reqs = 0
                    c_usd = 0.0
                hm_token_vals.append(tot_t)
                hm_raw_entries.append(
                    {
                        "date": d_str,
                        "total_tokens": tot_t,
                        "api_requests": reqs,
                        "cost_usd": c_usd,
                    }
                )

            hm_levels = _quantile_levels(hm_token_vals)
            heatmap_list = [
                {**entry, "level": lvl} for entry, lvl in zip(hm_raw_entries, hm_levels)
            ]

            # 4) Hourly distribution (0..23) in selected window
            if cutoff_str is not None:
                hr_rows = conn.execute(
                    f"""
                    SELECT
                        local_hour,
                        SUM(unique_input_tokens) AS uniq_in,
                        SUM(context_input_tokens) AS ctx_in,
                        SUM(output_tokens) AS out_tok,
                        SUM(thinking_tokens) AS think_tok,
                        SUM(is_api_request) AS api_reqs
                    FROM events
                    WHERE local_date >= ? AND local_date <= ? {model_filter}
                    GROUP BY local_hour
                    """,
                    (cutoff_str, today_str) + tuple(model_params),
                ).fetchall()
            else:
                hr_rows = conn.execute(
                    f"""
                    SELECT
                        local_hour,
                        SUM(unique_input_tokens) AS uniq_in,
                        SUM(context_input_tokens) AS ctx_in,
                        SUM(output_tokens) AS out_tok,
                        SUM(thinking_tokens) AS think_tok,
                        SUM(is_api_request) AS api_reqs
                    FROM events
                    WHERE 1=1 {model_filter}
                    GROUP BY local_hour
                    """,
                    tuple(model_params),
                ).fetchall()

            hr_map = {int(r["local_hour"]): r for r in hr_rows}
            hourly_list: List[Dict[str, Any]] = []
            for hr in range(24):
                r = hr_map.get(hr)
                if r is not None:
                    eff_i = int(r["ctx_in"] or 0) if use_context else int(r["uniq_in"] or 0)
                    tot_t = eff_i + int(r["out_tok"] or 0) + int(r["think_tok"] or 0)
                    reqs = int(r["api_reqs"] or 0)
                else:
                    tot_t = 0
                    reqs = 0
                hourly_list.append(
                    {
                        "hour": hr,
                        "total_tokens": tot_t,
                        "api_requests": reqs,
                    }
                )

            # 5) Top 12 tools in selected window
            if cutoff_str is not None:
                if model_norm == "all":
                    tool_total_row = conn.execute(
                        "SELECT COALESCE(SUM(call_count), 0) AS total FROM tool_usages WHERE local_date >= ? AND local_date <= ?",
                        (cutoff_str, today_str),
                    ).fetchone()
                    tool_rows = conn.execute(
                        """
                        SELECT tool_name, SUM(call_count) AS cnt
                        FROM tool_usages
                        WHERE local_date >= ? AND local_date <= ?
                        GROUP BY tool_name
                        ORDER BY cnt DESC, tool_name ASC
                        LIMIT 12
                        """,
                        (cutoff_str, today_str),
                    ).fetchall()
                else:
                    # Filter tools by model via join with events
                    tool_total_row = conn.execute(
                        """SELECT COALESCE(SUM(tu.call_count), 0) AS total
                           FROM tool_usages tu
                           JOIN events e ON tu.conversation_id = e.conversation_id
                           WHERE tu.local_date >= ? AND tu.local_date <= ? AND e.model = ?""",
                        (cutoff_str, today_str, model_norm),
                    ).fetchone()
                    tool_rows = conn.execute(
                        """
                        SELECT tu.tool_name, SUM(tu.call_count) AS cnt
                        FROM tool_usages tu
                        JOIN events e ON tu.conversation_id = e.conversation_id
                        WHERE tu.local_date >= ? AND tu.local_date <= ? AND e.model = ?
                        GROUP BY tu.tool_name
                        ORDER BY cnt DESC, tu.tool_name ASC
                        LIMIT 12
                        """,
                        (cutoff_str, today_str, model_norm),
                    ).fetchall()
            else:
                if model_norm == "all":
                    tool_total_row = conn.execute(
                        "SELECT COALESCE(SUM(call_count), 0) AS total FROM tool_usages"
                    ).fetchone()
                    tool_rows = conn.execute(
                        """
                        SELECT tool_name, SUM(call_count) AS cnt
                        FROM tool_usages
                        GROUP BY tool_name
                        ORDER BY cnt DESC, tool_name ASC
                        LIMIT 12
                        """
                    ).fetchall()
                else:
                    tool_total_row = conn.execute(
                        """SELECT COALESCE(SUM(tu.call_count), 0) AS total
                           FROM tool_usages tu
                           JOIN events e ON tu.conversation_id = e.conversation_id
                           WHERE e.model = ?""",
                        (model_norm,),
                    ).fetchone()
                    tool_rows = conn.execute(
                        """
                        SELECT tu.tool_name, SUM(tu.call_count) AS cnt
                        FROM tool_usages tu
                        JOIN events e ON tu.conversation_id = e.conversation_id
                        WHERE e.model = ?
                        GROUP BY tu.tool_name
                        ORDER BY cnt DESC, tu.tool_name ASC
                        LIMIT 12
                        """,
                        (model_norm,),
                    ).fetchall()

            all_tools_sum = int(tool_total_row["total"] or 0)
            tools_list: List[Dict[str, Any]] = []
            for r in tool_rows:
                cnt = int(r["cnt"] or 0)
                share = round((cnt / float(all_tools_sum)) * 100.0, 1) if all_tools_sum > 0 else 0.0
                tools_list.append(
                    {
                        "name": str(r["tool_name"]),
                        "count": cnt,
                        "share_pct": share,
                    }
                )

            # 6) Up to 100 most recent conversations active in selected window
            if cutoff_str is not None:
                if model_norm == "all":
                    conv_rows = conn.execute(
                        """
                        SELECT c.*
                        FROM conversations c
                        WHERE EXISTS (
                            SELECT 1 FROM events e
                            WHERE e.conversation_id = c.id
                              AND e.local_date >= ? AND e.local_date <= ?
                        )
                        ORDER BY c.updated_at DESC
                        LIMIT 100
                        """,
                        (cutoff_str, today_str),
                    ).fetchall()
                else:
                    conv_rows = conn.execute(
                        """
                        SELECT c.*
                        FROM conversations c
                        WHERE EXISTS (
                            SELECT 1 FROM events e
                            WHERE e.conversation_id = c.id
                              AND e.local_date >= ? AND e.local_date <= ?
                              AND e.model = ?
                        )
                        ORDER BY c.updated_at DESC
                        LIMIT 100
                        """,
                        (cutoff_str, today_str, model_norm),
                    ).fetchall()
            else:
                if model_norm == "all":
                    conv_rows = conn.execute(
                        """
                        SELECT *
                        FROM conversations
                        ORDER BY updated_at DESC
                        LIMIT 100
                        """
                    ).fetchall()
                else:
                    conv_rows = conn.execute(
                        """
                        SELECT c.*
                        FROM conversations c
                        WHERE EXISTS (
                            SELECT 1 FROM events e
                            WHERE e.conversation_id = c.id
                              AND e.model = ?
                        )
                        ORDER BY c.updated_at DESC
                        LIMIT 100
                        """,
                        (model_norm,),
                    ).fetchall()

            conversations_list: List[Dict[str, Any]] = []
            for c in conv_rows:
                u_in = int(c["unique_input_tokens"] or 0)
                c_in = int(c["context_input_tokens"] or 0)
                o_tok = int(c["output_tokens"] or 0)
                t_tok = int(c["thinking_tokens"] or 0)
                eff_in = c_in if use_context else u_in
                tot_t = eff_in + o_tok + t_tok
                c_cost = compute_cost_usd(eff_in, o_tok, t_tok, pricing_norm)
                c_energy = compute_energy_metrics(eff_in, o_tok, t_tok)

                conversations_list.append(
                    {
                        "id": str(c["id"]),
                        "title": str(c["title"]),
                        "started_at": str(c["started_at"]),
                        "updated_at": str(c["updated_at"]),
                        "api_requests": int(c["api_requests"] or 0),
                        "tool_calls": int(c["tool_calls"] or 0),
                        "input_tokens": eff_in,
                        "unique_input_tokens": u_in,
                        "context_input_tokens": c_in,
                        "output_tokens": o_tok,
                        "thinking_tokens": t_tok,
                        "total_tokens": tot_t,
                        "cost_usd": round(c_cost, 6),
                        "electricity_wh": round(c_energy["electricity_wh"], 4),
                    }
                )

            agy_label = str(self.agy_dir)
            if len(self.agy_dirs) > 1:
                agy_label = f"{self.agy_dir} (+{len(self.agy_dirs) - 1} more)"
            meta_obj: Dict[str, Any] = {
                "agy_dir": agy_label,
                "db_path": self.db_path,
                "is_demo": self.is_demo,
                "demo_reason": self.demo_reason,
                "last_scanned_at": self.last_scanned_at,
                "days_filter": days_norm,
                "pricing_profile": pricing_norm,
                "billing_mode": billing_norm,
                "model_filter": model_norm,
                "available_models": available_models,
                "available_devices": self._get_available_devices(),
                "current_device": self.device_name,
                "pricing_profiles": PRICING_PROFILES,
            }

            # Get per-model breakdown for all models (ignoring current model filter)
            models_breakdown = self._get_models_breakdown(days_norm, billing_norm, pricing_norm)

            return {
                "meta": meta_obj,
                "totals": totals_obj,
                "daily": daily_list,
                "heatmap": heatmap_list,
                "hourly": hourly_list,
                "tools": tools_list,
                "conversations": conversations_list,
                "models_breakdown": models_breakdown,
            }

    def _get_available_devices(self) -> List[str]:
        """Return sorted list of distinct devices recorded across events and conversations."""
        with self.lock:
            rows = self._active_conn.execute(
                "SELECT DISTINCT device FROM events WHERE device IS NOT NULL AND device != '' UNION SELECT DISTINCT device FROM conversations WHERE device IS NOT NULL AND device != '' ORDER BY device"
            ).fetchall()
            devs = [r[0] for r in rows if r[0]]
            if not devs and self.device_name:
                devs = [self.device_name]
            return sorted(set(devs))

    def _get_available_models(self) -> List[str]:
        """Return sorted list of distinct models from events table (excludes 'unknown')."""
        with self.lock:
            rows = self._active_conn.execute(
                "SELECT DISTINCT model FROM events WHERE model != 'unknown' ORDER BY model"
            ).fetchall()
            return [r["model"] for r in rows]

    def _get_models_breakdown(self, days_filter: str, billing_mode: str, pricing: str) -> List[Dict[str, Any]]:
        """Return per-model aggregate metrics for the dashboard."""
        with self.lock:
            conn = self._active_conn
            local_tz = dt.datetime.now().astimezone().tzinfo or dt.timezone.utc
            today_date = dt.datetime.now(tz=local_tz).date()
            today_str = today_date.strftime("%Y-%m-%d")

            if days_filter == "all":
                cutoff_str = None
            else:
                n_days = int(days_filter)
                cutoff_date = today_date - dt.timedelta(days=n_days - 1)
                cutoff_str = cutoff_date.strftime("%Y-%m-%d")

            use_context = billing_mode == "context"

            if cutoff_str is not None:
                rows = conn.execute(
                    """
                    SELECT
                        model,
                        SUM(unique_input_tokens) AS uniq_in,
                        SUM(context_input_tokens) AS ctx_in,
                        SUM(output_tokens) AS out_tok,
                        SUM(thinking_tokens) AS think_tok,
                        SUM(is_api_request) AS api_reqs,
                        SUM(tool_calls_count) AS tool_calls,
                        COUNT(DISTINCT conversation_id) AS conv_count
                    FROM events
                    WHERE local_date >= ? AND local_date <= ?
                    GROUP BY model
                    ORDER BY (SUM(context_input_tokens) + SUM(output_tokens) + SUM(thinking_tokens)) DESC
                    """,
                    (cutoff_str, today_str),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        model,
                        SUM(unique_input_tokens) AS uniq_in,
                        SUM(context_input_tokens) AS ctx_in,
                        SUM(output_tokens) AS out_tok,
                        SUM(thinking_tokens) AS think_tok,
                        SUM(is_api_request) AS api_reqs,
                        SUM(tool_calls_count) AS tool_calls,
                        COUNT(DISTINCT conversation_id) AS conv_count
                    FROM events
                    GROUP BY model
                    ORDER BY (SUM(context_input_tokens) + SUM(output_tokens) + SUM(thinking_tokens)) DESC
                    """
                ).fetchall()

            profile = PRICING_PROFILES.get(pricing, PRICING_PROFILES["blended"])
            results = []
            for r in rows:
                model_name = r["model"] or "unknown"
                if model_name == "unknown":
                    continue
                eff_i = int(r["ctx_in"] or 0) if use_context else int(r["uniq_in"] or 0)
                o_t = int(r["out_tok"] or 0)
                t_t = int(r["think_tok"] or 0)
                reqs = int(r["api_reqs"] or 0)
                tools = int(r["tool_calls"] or 0)
                convs = int(r["conv_count"] or 0)
                tot_t = eff_i + o_t + t_t
                cost = compute_cost_usd(eff_i, o_t, t_t, pricing)
                energy = compute_energy_metrics(eff_i, o_t, t_t)
                results.append({
                    "model": model_name,
                    "total_tokens": tot_t,
                    "effective_input_tokens": eff_i,
                    "unique_input_tokens": int(r["uniq_in"] or 0),
                    "context_input_tokens": int(r["ctx_in"] or 0),
                    "output_tokens": o_t,
                    "thinking_tokens": t_t,
                    "api_requests": reqs,
                    "conversations": convs,
                    "tool_calls": tools,
                    "cost_usd": round(cost, 6),
                    "electricity_wh": round(energy["electricity_wh"], 4),
                    "co2_grams": round(energy["co2_grams"], 4),
                    "water_liters": round(energy["water_liters"], 4),
                    "water_ml": round(energy["water_ml"], 2),
                    "avg_tokens_per_req": round(tot_t / reqs, 1) if reqs > 0 else 0.0,
                })
            return results

    def export_daily_csv(
        self,
        days: str = "all",
        pricing: str = "blended",
        billing_mode: str = "context",
    ) -> str:
        """Serialize daily metrics into a clean downloadable CSV string."""
        stats = self.get_stats(days=days, pricing=pricing, billing_mode=billing_mode)
        buf = io.StringIO()
        writer = csv.writer(buf)
        headers = [
            "date",
            "input_tokens",
            "unique_input_tokens",
            "context_input_tokens",
            "output_tokens",
            "thinking_tokens",
            "total_tokens",
            "api_requests",
            "tool_calls",
            "conversations",
            "cost_usd",
            "electricity_wh",
            "co2_grams",
        ]
        writer.writerow(headers)
        for row in stats["daily"]:
            writer.writerow([row[h] for h in headers])
        return buf.getvalue()


# ---------------------------------------------------------------------------
# HTTP Server & Request Handler
# ---------------------------------------------------------------------------


class DashboardHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer holding reference to `UsageTracker` and start timestamp."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address: Tuple[str, int],
        RequestHandlerClass: type[BaseHTTPRequestHandler],
        tracker: UsageTracker,
        quiet: bool = True,
    ) -> None:
        super().__init__(server_address, RequestHandlerClass)
        self.tracker = tracker
        self.started_monotonic = time.monotonic()
        self.quiet = quiet


class DashboardRequestHandler(BaseHTTPRequestHandler):
    """HTTP handler serving dashboard UI and JSON/CSV API endpoints."""

    server: DashboardHTTPServer  # type: ignore[assignment]

    def log_message(self, fmt: str, *args: Any) -> None:
        if not getattr(self.server, "quiet", True):
            super().log_message(fmt, *args)

    def _send_bytes(
        self,
        payload: bytes,
        status: int = HTTPStatus.OK,
        content_type: str = "application/json; charset=utf-8",
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(payload)

    def _send_json(self, data: Any, status: int = HTTPStatus.OK) -> None:
        raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self._send_bytes(raw, status=status, content_type="application/json; charset=utf-8")

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send_bytes(b"", status=HTTPStatus.NO_CONTENT)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlsplit(self.path)
        route = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)

        if route in ("/", "/index.html"):
            body = DASHBOARD_HTML.encode("utf-8")
            self._send_bytes(body, content_type="text/html; charset=utf-8")
            return

        if route == "/favicon.ico":
            self._send_bytes(b"", status=HTTPStatus.NO_CONTENT, content_type="image/x-icon")
            return

        if route == "/api/health":
            uptime = round(time.monotonic() - self.server.started_monotonic, 2)
            self._send_json(
                {
                    "status": "ok",
                    "version": __version__,
                    "uptime_seconds": uptime,
                    "is_demo": self.server.tracker.is_demo,
                }
            )
            return

        if route == "/api/stats":
            days = qs.get("days", ["30"])[0]
            pricing = qs.get("pricing", ["blended"])[0]
            billing_mode = qs.get("billing_mode", ["context"])[0]
            model = qs.get("model", ["all"])[0]
            stats = self.server.tracker.get_stats(
                days=days, pricing=pricing, billing_mode=billing_mode, model=model
            )
            self._send_json(stats)
            return

        if route == "/api/export.csv":
            days = qs.get("days", ["all"])[0]
            pricing = qs.get("pricing", ["blended"])[0]
            billing_mode = qs.get("billing_mode", ["context"])[0]
            model = qs.get("model", ["all"])[0]
            csv_text = self.server.tracker.export_daily_csv(
                days=days, pricing=pricing, billing_mode=billing_mode, model=model
            )
            filename = f"agy_usage_{days}_{billing_mode}.csv"
            self._send_bytes(
                csv_text.encode("utf-8"),
                content_type="text/csv; charset=utf-8",
                extra_headers={
                    "Content-Disposition": f'attachment; filename="{filename}"'
                },
            )
            return

        self._send_json({"error": "Not found", "path": route}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlsplit(self.path)
        route = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)

        if route == "/api/rescan":
            force = qs.get("force", ["0"])[0].lower() in ("1", "true", "yes")
            content_len = int(self.headers.get("Content-Length", "0") or "0")
            if content_len > 0:
                try:
                    body_raw = self.rfile.read(min(content_len, 65536))
                    body_obj = json.loads(body_raw.decode("utf-8", errors="replace"))
                    if isinstance(body_obj, dict) and body_obj.get("force"):
                        force = True
                except Exception:
                    pass
            result = self.server.tracker.rescan(force=force)
            self._send_json(result)
            return

        self._send_json({"error": "Not found", "path": route}, status=HTTPStatus.NOT_FOUND)


def bind_http_server(
    tracker: UsageTracker,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    strict_port: bool = False,
    quiet: bool = True,
    max_port_tries: int = 50,
) -> DashboardHTTPServer:
    """Bind `DashboardHTTPServer`, auto-incrementing port if busy unless `strict_port`."""
    tries = 1 if strict_port else max_port_tries
    last_err: Optional[OSError] = None
    for idx in range(tries):
        candidate_port = port + idx
        if candidate_port > 65535:
            break
        try:
            return DashboardHTTPServer(
                (host, candidate_port),
                DashboardRequestHandler,
                tracker=tracker,
                quiet=quiet,
            )
        except OSError as exc:
            last_err = exc
            if strict_port:
                raise
            continue
    raise OSError(f"Unable to bind server starting at {host}:{port}: {last_err}")


# ---------------------------------------------------------------------------
# Terminal Summary Formatter & CLI
# ---------------------------------------------------------------------------


def _fmt_int(n: int) -> str:
    return f"{n:,}"


def _fmt_compact(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def format_terminal_summary(stats: Dict[str, Any]) -> str:
    """Render a clean, aligned terminal summary table from `/api/stats` data."""
    meta = stats["meta"]
    totals = stats["totals"]
    tools = stats["tools"]
    convs = stats["conversations"]

    profile_info = PRICING_PROFILES.get(
        meta["pricing_profile"], PRICING_PROFILES["blended"]
    )
    mode_badge = "DEMO PREVIEW" if meta["is_demo"] else "LIVE DATA"
    window_label = (
        "All time" if meta["days_filter"] == "all" else f"Last {meta['days_filter']} days"
    )
    billing_label = (
        "API Context Tokens (OpenRouter-style)"
        if meta["billing_mode"] == "context"
        else "Unique Message Tokens"
    )

    lines: List[str] = []
    bar = "═" * 78
    sep = "─" * 78
    lines.append(bar)
    lines.append(f"  AGY CLI USAGE TRACKER  •  {mode_badge}  •  {window_label}")
    lines.append(bar)
    if meta["is_demo"] and meta.get("demo_reason"):
        lines.append(f"  Notice       : {meta['demo_reason']}")
    lines.append(f"  Brain Dir    : {meta['agy_dir']}")
    lines.append(f"  Billing Mode : {billing_label}")
    lines.append(f"  Pricing Tier : {profile_info['label']} (${profile_info['input']:.2f} / ${profile_info['output']:.2f} per 1M tok)")
    lines.append(sep)

    peak = totals["peak_day"]
    peak_str = (
        f"{peak['date']} ({_fmt_compact(peak['tokens'])} tok)"
        if peak.get("date")
        else "—"
    )

    kpis = [
        ("Total Tokens", f"{_fmt_int(totals['total_tokens'])} ({_fmt_compact(totals['total_tokens'])})"),
        ("Effective Input", _fmt_int(totals["effective_input_tokens"])),
        ("  ├─ Context Input", _fmt_int(totals["context_input_tokens"])),
        ("  └─ Unique Input", _fmt_int(totals["unique_input_tokens"])),
        ("Output Tokens", _fmt_int(totals["output_tokens"])),
        ("Thinking Tokens", f"{_fmt_int(totals['thinking_tokens'])} ({totals['thinking_share_pct']:.1f}%)"),
        ("API Requests", f"{_fmt_int(totals['api_requests'])}  (avg {_fmt_int(int(totals['avg_tokens_per_request']))} tok/req)"),
        ("Conversations", f"{_fmt_int(totals['conversations'])}  ({totals['active_days']} active days)"),
        ("Tool Calls", f"{_fmt_int(totals['tool_calls'])}  ({totals['subagent_spawns']} subagent spawns)"),
        ("Peak Day", peak_str),
        ("Estimated Cost", f"${totals['cost_usd']:,.4f} USD"),
        (
            "Energy & Carbon",
            f"{totals['electricity_wh']:.2f} Wh ({totals['electricity_kwh']:.4f} kWh)  •  {totals['co2_grams']:.2f} gCO2e",
        ),
        (
            "Water Consumed",
            f"{totals.get('water_liters', 0.0):.3f} L ({totals.get('water_ml', 0.0):.1f} ml)  •  ~{totals.get('water_ml', 0.0)/500.0:.2f} standard 500ml bottles",
        ),
        (
            "Equivalents",
            f"{totals['phone_charges_equiv']:.2f} phone charges  •  {totals['led_bulb_hours_equiv']:.2f} hrs 10W LED",
        ),
    ]
    for label, val in kpis:
        lines.append(f"  {label:<20} : {val}")

    if tools:
        lines.append(sep)
        lines.append("  TOP TOOLS")
        lines.append(f"  {'Tool Name':<28} {'Calls':>10} {'Share':>10}")
        for t in tools[:8]:
            lines.append(f"  {t['name']:<28} {_fmt_int(t['count']):>10} {t['share_pct']:>9.1f}%")

    if convs:
        lines.append(sep)
        lines.append("  RECENT CONVERSATIONS")
        lines.append(f"  {'Updated':<12} {'Title':<42} {'Reqs':>5} {'Tokens':>9} {'Cost':>8}")
        for c in convs[:6]:
            upd = c["updated_at"][:10]
            ttl = c["title"][:40] + ("…" if len(c["title"]) > 40 else "")
            lines.append(
                f"  {upd:<12} {ttl:<42} {c['api_requests']:>5} {_fmt_compact(c['total_tokens']):>9} ${c['cost_usd']:>7.3f}"
            )

    lines.append(bar)
    return "\n".join(lines)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agy_dashboard.py",
        description="AGY CLI Usage Tracker & Local Web Dashboard (zero dependencies)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"HTTP server port (default: {DEFAULT_PORT}, auto-increments if busy)",
    )
    parser.add_argument(
        "--strict-port",
        action="store_true",
        help="Fail instead of falling back to the next free port if --port is in use",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=DEFAULT_HOST,
        help=f"HTTP bind address (default: {DEFAULT_HOST})",
    )
    parser.add_argument(
        "--agy-dir",
        type=str,
        default=None,
        help="Path to AGY brain directory (overrides AGY_BRAIN_DIR / auto-discovery)",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Path to SQLite cache database (default: <agy-dir>/../agy_usage.db)",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not open the dashboard URL in the default web browser",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Launch with 45 days of realistic synthetic usage data (in-memory)",
    )
    parser.add_argument(
        "--force-rescan",
        action="store_true",
        help="Force full rebuild of SQLite cache from JSONL transcripts on startup",
    )
    parser.add_argument(
        "--export-json",
        type=str,
        default=None,
        metavar="PATH",
        help="Export `/api/stats` JSON payload to PATH and exit",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print formatted terminal usage summary table and exit",
    )
    parser.add_argument(
        "--days",
        type=str,
        default="30",
        choices=list(VALID_DAYS_FILTERS),
        help="Time window for --summary / --export-json (default: 30)",
    )
    parser.add_argument(
        "--pricing",
        type=str,
        default="blended",
        choices=list(PRICING_PROFILES.keys()),
        help="Pricing profile for --summary / --export-json (default: blended)",
    )
    parser.add_argument(
        "--billing-mode",
        type=str,
        default="context",
        choices=list(VALID_BILLING_MODES),
        help="Token billing mode for --summary / --export-json (default: context)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable HTTP request logging to stderr",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    tracker = UsageTracker(
        cli_agy_dir=args.agy_dir,
        db_path=args.db,
        force_demo=args.demo,
        force_rescan=args.force_rescan,
    )

    try:
        if args.export_json or args.summary:
            stats = tracker.get_stats(
                days=args.days,
                pricing=args.pricing,
                billing_mode=args.billing_mode,
            )
            if args.export_json:
                out_path = _expand_path(args.export_json)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(
                    json.dumps(stats, indent=2, ensure_ascii=False) + "\n",
                    encoding="utf-8",
                )
                if not args.summary:
                    print(f"Exported usage stats JSON to {out_path}")
            if args.summary:
                print(format_terminal_summary(stats))
            return 0

        httpd = bind_http_server(
            tracker=tracker,
            host=args.host,
            port=args.port,
            strict_port=args.strict_port,
            quiet=not args.verbose,
        )
        bound_host, bound_port = httpd.server_address[:2]
        display_host = "127.0.0.1" if bound_host in ("0.0.0.0", "::") else str(bound_host)
        url = f"http://{display_host}:{bound_port}"

        mode_note = " (Demo Preview)" if tracker.is_demo else ""
        print(f"AGY Usage Dashboard{mode_note} running at {url}")
        print(f"  Brain directory : {tracker.agy_dir}")
        print(f"  SQLite cache    : {tracker.db_path}")
        if bound_port != args.port:
            print(f"  Port notice     : {args.port} was busy, using {bound_port}")
        print("  Press Ctrl+C to stop.")
        sys.stdout.flush()

        if not args.no_browser:
            threading.Thread(
                target=lambda: (time.sleep(0.25), webbrowser.open(url)),
                daemon=True,
            ).start()

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down AGY Usage Dashboard...")
        finally:
            httpd.server_close()
        return 0
    finally:
        tracker.close()


if __name__ == "__main__":
    raise SystemExit(main())
