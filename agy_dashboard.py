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

DASHBOARD_HTML = json.loads('"<!DOCTYPE html>\\n<html lang=\\"en\\" class=\\"dark\\">\\n<head>\\n  <meta charset=\\"UTF-8\\" />\\n  <meta name=\\"viewport\\" content=\\"width=device-width, initial-scale=1.0\\" />\\n  <meta name=\\"color-scheme\\" content=\\"dark\\" />\\n  <title>AGY Telemetry \\u2014 Usage &amp; Footprint Monitor</title>\\n  <link rel=\\"preconnect\\" href=\\"https://fonts.googleapis.com\\" />\\n  <link rel=\\"preconnect\\" href=\\"https://fonts.gstatic.com\\" crossorigin />\\n  <link href=\\"https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap\\" rel=\\"stylesheet\\" />\\n  <script src=\\"https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js\\" onerror=\\"window.__chartJsLoadFailed=true\\"></script>\\n  <style>\\n    :root {\\n      --bg-canvas: #08090a;\\n      --bg-header: rgba(15, 16, 17, 0.85);\\n      --bg-panel: #0f1011;\\n      --bg-elevated: #161719;\\n      --bg-popover: #141517;\\n      --bg-card: rgba(255, 255, 255, 0.025);\\n      --bg-card-hover: rgba(255, 255, 255, 0.04);\\n      --bg-subtle: rgba(255, 255, 255, 0.02);\\n      --bg-control: rgba(255, 255, 255, 0.03);\\n      --bg-control-hover: rgba(255, 255, 255, 0.06);\\n      --bg-active: rgba(94, 106, 210, 0.18);\\n\\n      --border-header: rgba(255, 255, 255, 0.06);\\n      --border-subtle: rgba(255, 255, 255, 0.05);\\n      --border-card: rgba(255, 255, 255, 0.08);\\n      --border-strong: rgba(255, 255, 255, 0.14);\\n      --border-active: rgba(113, 112, 255, 0.45);\\n\\n      --text-primary: #f7f8f8;\\n      --text-secondary: #d0d6e0;\\n      --text-muted: #8a8f98;\\n      --text-subtle: #62666d;\\n\\n      --indigo-brand: #5e6ad2;\\n      --indigo-accent: #7170ff;\\n      --indigo-hover: #828fff;\\n      --cyan-output: #2dd4bf;\\n      --violet-thinking: #a855f7;\\n      --emerald-live: #10b981;\\n      --amber-warn: #f59e0b;\\n      --danger: #ef4444;\\n\\n      --radius-card: 10px;\\n      --radius-control: 6px;\\n      --radius-sm: 4px;\\n\\n      --font-sans: \'Inter\', -apple-system, BlinkMacSystemFont, \'Segoe UI\', Roboto, Oxygen, Ubuntu, sans-serif;\\n      --font-mono: \'JetBrains Mono\', ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, \'Liberation Mono\', monospace;\\n    }\\n\\n    *, *::before, *::after {\\n      box-sizing: border-box;\\n      margin: 0;\\n      padding: 0;\\n    }\\n\\n    html, body {\\n      background-color: var(--bg-canvas);\\n      color: var(--text-primary);\\n      font-family: var(--font-sans);\\n      font-feature-settings: \'cv01\', \'ss03\';\\n      -webkit-font-smoothing: antialiased;\\n      -moz-osx-font-smoothing: grayscale;\\n      font-size: 13px;\\n      line-height: 1.5;\\n      min-height: 100vh;\\n      overflow-x: hidden;\\n    }\\n\\n    .mono, [data-mono] {\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      font-feature-settings: \'zero\' 0;\\n    }\\n\\n    button, input, select {\\n      font-family: inherit;\\n      font-feature-settings: inherit;\\n      color: inherit;\\n    }\\n\\n    a {\\n      color: inherit;\\n      text-decoration: none;\\n    }\\n\\n    /* Scrollbars */\\n    ::-webkit-scrollbar {\\n      width: 7px;\\n      height: 7px;\\n    }\\n    ::-webkit-scrollbar-track {\\n      background: transparent;\\n    }\\n    ::-webkit-scrollbar-thumb {\\n      background: rgba(255, 255, 255, 0.1);\\n      border-radius: 999px;\\n    }\\n    ::-webkit-scrollbar-thumb:hover {\\n      background: rgba(255, 255, 255, 0.18);\\n    }\\n\\n    /* Sticky Top Header */\\n    .topbar {\\n      position: sticky;\\n      top: 0;\\n      z-index: 40;\\n      background: var(--bg-header);\\n      backdrop-filter: blur(12px);\\n      -webkit-backdrop-filter: blur(12px);\\n      border-bottom: 1px solid var(--border-header);\\n    }\\n\\n    .topbar-inner {\\n      max-width: 1440px;\\n      margin: 0 auto;\\n      padding: 10px 24px;\\n      display: flex;\\n      align-items: center;\\n      justify-content: space-between;\\n      gap: 16px;\\n      flex-wrap: wrap;\\n    }\\n\\n    .brand-cluster {\\n      display: flex;\\n      align-items: center;\\n      gap: 12px;\\n      min-width: 0;\\n    }\\n\\n    .app-mark {\\n      width: 28px;\\n      height: 28px;\\n      border-radius: 7px;\\n      background: rgba(94, 106, 210, 0.14);\\n      border: 1px solid rgba(113, 112, 255, 0.35);\\n      display: inline-flex;\\n      align-items: center;\\n      justify-content: center;\\n      flex-shrink: 0;\\n      color: var(--indigo-accent);\\n    }\\n\\n    .brand-titles {\\n      display: flex;\\n      flex-direction: column;\\n      min-width: 0;\\n      gap: 1px;\\n    }\\n\\n    .brand-row {\\n      display: flex;\\n      align-items: center;\\n      gap: 8px;\\n    }\\n\\n    .brand-name {\\n      font-size: 14px;\\n      font-weight: 600;\\n      letter-spacing: -0.02em;\\n      color: var(--text-primary);\\n      white-space: nowrap;\\n    }\\n\\n    .brand-subtitle {\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      font-size: 11px;\\n      color: var(--text-muted);\\n      white-space: nowrap;\\n      overflow: hidden;\\n      text-overflow: ellipsis;\\n      max-width: 340px;\\n      cursor: default;\\n    }\\n\\n    .demo-badge {\\n      display: none;\\n      align-items: center;\\n      gap: 5px;\\n      padding: 2px 8px;\\n      border-radius: 999px;\\n      background: rgba(245, 158, 11, 0.12);\\n      border: 1px solid rgba(245, 158, 11, 0.32);\\n      color: #fbbf24;\\n      font-family: var(--font-mono);\\n      font-size: 10px;\\n      font-weight: 600;\\n      letter-spacing: 0.04em;\\n      text-transform: uppercase;\\n      cursor: help;\\n      white-space: nowrap;\\n    }\\n\\n    .demo-badge.is-visible {\\n      display: inline-flex;\\n    }\\n\\n    .demo-badge-dot {\\n      width: 5px;\\n      height: 5px;\\n      border-radius: 50%;\\n      background: #f59e0b;\\n    }\\n\\n    .controls-cluster {\\n      display: flex;\\n      align-items: center;\\n      gap: 8px;\\n      flex-wrap: wrap;\\n    }\\n\\n    /* Segmented Controls */\\n    .segmented {\\n      display: inline-flex;\\n      align-items: center;\\n      background: rgba(255, 255, 255, 0.025);\\n      border: 1px solid var(--border-card);\\n      border-radius: var(--radius-control);\\n      padding: 2px;\\n      gap: 1px;\\n    }\\n\\n    .seg-btn {\\n      appearance: none;\\n      background: transparent;\\n      border: 1px solid transparent;\\n      color: var(--text-muted);\\n      font-size: 11.5px;\\n      font-weight: 500;\\n      padding: 4px 8px;\\n      border-radius: var(--radius-sm);\\n      cursor: pointer;\\n      transition: color 0.14s ease, background 0.14s ease, border-color 0.14s ease;\\n      white-space: nowrap;\\n      line-height: 1.2;\\n    }\\n\\n    .seg-btn.mono-label {\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      font-size: 11px;\\n    }\\n\\n    .seg-btn:hover {\\n      color: var(--text-secondary);\\n    }\\n\\n    .seg-btn.is-active {\\n      background: var(--bg-active);\\n      border-color: var(--border-active);\\n      color: var(--text-primary);\\n    }\\n\\n    .control-with-help {\\n      display: inline-flex;\\n      align-items: center;\\n      gap: 4px;\\n    }\\n\\n    .help-trigger {\\n      width: 22px;\\n      height: 22px;\\n      border-radius: var(--radius-sm);\\n      border: 1px solid var(--border-subtle);\\n      background: rgba(255, 255, 255, 0.02);\\n      color: var(--text-muted);\\n      font-family: var(--font-mono);\\n      font-size: 11px;\\n      display: inline-flex;\\n      align-items: center;\\n      justify-content: center;\\n      cursor: help;\\n      transition: border-color 0.14s ease, color 0.14s ease;\\n    }\\n\\n    .help-trigger:hover {\\n      border-color: var(--border-strong);\\n      color: var(--text-primary);\\n    }\\n\\n    /* Select Dropdown */\\n    .select-wrap {\\n      position: relative;\\n      display: inline-flex;\\n      align-items: center;\\n    }\\n\\n    .linear-select {\\n      appearance: none;\\n      -webkit-appearance: none;\\n      background: var(--bg-control);\\n      border: 1px solid var(--border-card);\\n      border-radius: var(--radius-control);\\n      color: var(--text-secondary);\\n      font-size: 12px;\\n      font-weight: 500;\\n      padding: 5px 28px 5px 10px;\\n      height: 28px;\\n      cursor: pointer;\\n      transition: background 0.14s ease, border-color 0.14s ease;\\n      max-width: 220px;\\n      text-overflow: ellipsis;\\n    }\\n\\n    .linear-select:hover {\\n      background: var(--bg-control-hover);\\n      border-color: var(--border-strong);\\n      color: var(--text-primary);\\n    }\\n\\n    .linear-select:focus {\\n      outline: none;\\n      border-color: var(--indigo-accent);\\n    }\\n\\n    .linear-select option {\\n      background: #141517;\\n      color: var(--text-primary);\\n    }\\n\\n    .select-chevron {\\n      position: absolute;\\n      right: 8px;\\n      pointer-events: none;\\n      color: var(--text-muted);\\n      display: flex;\\n    }\\n\\n    /* Action Buttons */\\n    .btn-ghost {\\n      appearance: none;\\n      display: inline-flex;\\n      align-items: center;\\n      gap: 6px;\\n      height: 28px;\\n      padding: 0 10px;\\n      border-radius: var(--radius-control);\\n      background: var(--bg-control);\\n      border: 1px solid var(--border-card);\\n      color: var(--text-secondary);\\n      font-size: 12px;\\n      font-weight: 500;\\n      cursor: pointer;\\n      transition: background 0.14s ease, border-color 0.14s ease, color 0.14s ease;\\n      white-space: nowrap;\\n    }\\n\\n    .btn-ghost:hover:not(:disabled) {\\n      background: var(--bg-control-hover);\\n      border-color: var(--border-strong);\\n      color: var(--text-primary);\\n    }\\n\\n    .btn-ghost:disabled {\\n      opacity: 0.55;\\n      cursor: not-allowed;\\n    }\\n\\n    .btn-ghost.is-live {\\n      border-color: rgba(16, 185, 129, 0.35);\\n      background: rgba(16, 185, 129, 0.08);\\n      color: var(--text-primary);\\n    }\\n\\n    .pulse-dot {\\n      width: 6px;\\n      height: 6px;\\n      border-radius: 50%;\\n      background: var(--text-subtle);\\n      transition: background 0.2s ease;\\n      flex-shrink: 0;\\n    }\\n\\n    .btn-ghost.is-live .pulse-dot {\\n      background: var(--emerald-live);\\n      box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.6);\\n      animation: pulseEmerald 2s infinite;\\n    }\\n\\n    @keyframes pulseEmerald {\\n      0% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.55); }\\n      70% { box-shadow: 0 0 0 5px rgba(16, 185, 129, 0); }\\n      100% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }\\n    }\\n\\n    @keyframes spin {\\n      to { transform: rotate(360deg); }\\n    }\\n\\n    .is-spinning svg {\\n      animation: spin 0.75s linear infinite;\\n    }\\n\\n    /* Notice / Demo / Error Banners */\\n    .banner-strip {\\n      display: none;\\n      border-bottom: 1px solid var(--border-subtle);\\n      font-size: 12px;\\n    }\\n\\n    .banner-strip.is-visible {\\n      display: block;\\n    }\\n\\n    .banner-strip.banner-demo {\\n      background: rgba(245, 158, 11, 0.06);\\n      border-bottom-color: rgba(245, 158, 11, 0.18);\\n      color: #fcd34d;\\n    }\\n\\n    .banner-strip.banner-error {\\n      background: rgba(239, 68, 68, 0.08);\\n      border-bottom-color: rgba(239, 68, 68, 0.24);\\n      color: #fca5a5;\\n    }\\n\\n    .banner-inner {\\n      max-width: 1440px;\\n      margin: 0 auto;\\n      padding: 7px 24px;\\n      display: flex;\\n      align-items: center;\\n      justify-content: space-between;\\n      gap: 12px;\\n    }\\n\\n    .banner-msg {\\n      display: flex;\\n      align-items: center;\\n      gap: 8px;\\n      min-width: 0;\\n    }\\n\\n    .banner-actions {\\n      display: flex;\\n      align-items: center;\\n      gap: 8px;\\n      flex-shrink: 0;\\n    }\\n\\n    .banner-btn {\\n      appearance: none;\\n      background: rgba(255, 255, 255, 0.06);\\n      border: 1px solid rgba(255, 255, 255, 0.14);\\n      color: var(--text-primary);\\n      border-radius: var(--radius-sm);\\n      padding: 2px 8px;\\n      font-size: 11px;\\n      font-weight: 500;\\n      cursor: pointer;\\n    }\\n\\n    .banner-btn:hover {\\n      background: rgba(255, 255, 255, 0.12);\\n    }\\n\\n    /* Main Workspace Container */\\n    .main-container {\\n      max-width: 1440px;\\n      margin: 0 auto;\\n      padding: 20px 24px 48px;\\n      display: flex;\\n      flex-direction: column;\\n      gap: 16px;\\n    }\\n\\n    /* Subheader telemetry status bar */\\n    .status-strip {\\n      display: flex;\\n      align-items: center;\\n      justify-content: space-between;\\n      gap: 12px;\\n      flex-wrap: wrap;\\n      color: var(--text-subtle);\\n      font-size: 11.5px;\\n      padding: 0 2px;\\n    }\\n\\n    .status-meta-group {\\n      display: flex;\\n      align-items: center;\\n      gap: 14px;\\n      flex-wrap: wrap;\\n    }\\n\\n    .status-item {\\n      display: inline-flex;\\n      align-items: center;\\n      gap: 5px;\\n    }\\n\\n    .status-sep {\\n      color: rgba(255, 255, 255, 0.12);\\n    }\\n\\n    /* Surface Card Base */\\n    .surface-card {\\n      background: var(--bg-card);\\n      border: 1px solid var(--border-card);\\n      border-radius: var(--radius-card);\\n      transition: background 0.16s ease, border-color 0.16s ease;\\n    }\\n\\n    .surface-card.interactive-card:hover {\\n      background: var(--bg-card-hover);\\n      border-color: rgba(255, 255, 255, 0.11);\\n    }\\n\\n    /* KPI Grid (6 cards) */\\n    .kpi-grid {\\n      display: grid;\\n      grid-template-columns: repeat(3, minmax(0, 1fr));\\n      gap: 12px;\\n    }\\n\\n    @media (max-width: 1080px) {\\n      .kpi-grid {\\n        grid-template-columns: repeat(2, minmax(0, 1fr));\\n      }\\n    }\\n\\n    @media (max-width: 640px) {\\n      .kpi-grid {\\n        grid-template-columns: 1fr;\\n      }\\n      .topbar-inner, .main-container {\\n        padding-left: 14px;\\n        padding-right: 14px;\\n      }\\n    }\\n\\n    .kpi-card {\\n      padding: 16px 18px;\\n      display: flex;\\n      flex-direction: column;\\n      justify-content: space-between;\\n      gap: 12px;\\n      min-height: 138px;\\n    }\\n\\n    .kpi-top {\\n      display: flex;\\n      align-items: center;\\n      justify-content: space-between;\\n      gap: 8px;\\n    }\\n\\n    .kpi-label {\\n      font-size: 12px;\\n      font-weight: 500;\\n      color: var(--text-muted);\\n      letter-spacing: -0.01em;\\n      display: inline-flex;\\n      align-items: center;\\n      gap: 6px;\\n    }\\n\\n    .kpi-tag {\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      font-size: 10.5px;\\n      color: var(--text-subtle);\\n      padding: 1px 6px;\\n      border-radius: 4px;\\n      background: rgba(255, 255, 255, 0.03);\\n      border: 1px solid var(--border-subtle);\\n      white-space: nowrap;\\n    }\\n\\n    .kpi-tag.tag-emerald {\\n      color: var(--emerald-live);\\n      border-color: rgba(16, 185, 129, 0.25);\\n      background: rgba(16, 185, 129, 0.07);\\n    }\\n\\n    .kpi-tag.tag-violet {\\n      color: #c084fc;\\n      border-color: rgba(168, 85, 247, 0.28);\\n      background: rgba(168, 85, 247, 0.08);\\n    }\\n\\n    .kpi-value-row {\\n      display: flex;\\n      align-items: baseline;\\n      gap: 8px;\\n      flex-wrap: wrap;\\n    }\\n\\n    .kpi-value {\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      font-size: 24px;\\n      font-weight: 600;\\n      letter-spacing: -0.03em;\\n      line-height: 1.15;\\n      color: var(--text-primary);\\n    }\\n\\n    .kpi-value-sub {\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      font-size: 11.5px;\\n      color: var(--text-subtle);\\n    }\\n\\n    /* Mini Segmented Composition Bar */\\n    .seg-bar {\\n      width: 100%;\\n      height: 5px;\\n      border-radius: 999px;\\n      background: rgba(255, 255, 255, 0.05);\\n      display: flex;\\n      overflow: hidden;\\n      gap: 1px;\\n    }\\n\\n    .seg-bar-piece {\\n      height: 100%;\\n      transition: width 0.28s ease;\\n      min-width: 0;\\n    }\\n\\n    .kpi-breakdown {\\n      display: flex;\\n      flex-wrap: wrap;\\n      align-items: center;\\n      justify-content: space-between;\\n      gap: 6px 12px;\\n      font-size: 11.5px;\\n      color: var(--text-muted);\\n    }\\n\\n    .legend-dot {\\n      width: 6px;\\n      height: 6px;\\n      border-radius: 2px;\\n      display: inline-block;\\n      flex-shrink: 0;\\n    }\\n\\n    .bd-item {\\n      display: inline-flex;\\n      align-items: center;\\n      gap: 5px;\\n      white-space: nowrap;\\n    }\\n\\n    .bd-val {\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      color: var(--text-secondary);\\n      font-size: 11.5px;\\n    }\\n\\n    .eco-pills {\\n      display: flex;\\n      flex-wrap: wrap;\\n      gap: 6px;\\n    }\\n\\n    .eco-pill {\\n      display: inline-flex;\\n      align-items: center;\\n      gap: 5px;\\n      padding: 2px 7px;\\n      border-radius: 4px;\\n      background: rgba(16, 185, 129, 0.06);\\n      border: 1px solid rgba(16, 185, 129, 0.2);\\n      color: var(--text-secondary);\\n      font-size: 11px;\\n    }\\n\\n    .eco-pill .eco-dot {\\n      width: 5px;\\n      height: 5px;\\n      border-radius: 50%;\\n      background: var(--emerald-live);\\n    }\\n\\n    /* Card Header Shared */\\n    .card-header {\\n      padding: 14px 18px;\\n      border-bottom: 1px solid var(--border-subtle);\\n      display: flex;\\n      align-items: center;\\n      justify-content: space-between;\\n      gap: 12px;\\n      flex-wrap: wrap;\\n    }\\n\\n    .card-title-group {\\n      display: flex;\\n      flex-direction: column;\\n      gap: 2px;\\n    }\\n\\n    .card-title-row {\\n      display: flex;\\n      align-items: center;\\n      gap: 8px;\\n    }\\n\\n    .card-title {\\n      font-size: 13.5px;\\n      font-weight: 600;\\n      letter-spacing: -0.015em;\\n      color: var(--text-primary);\\n    }\\n\\n    .card-subtitle {\\n      font-size: 11.5px;\\n      color: var(--text-muted);\\n    }\\n\\n    .card-controls {\\n      display: flex;\\n      align-items: center;\\n      gap: 12px;\\n      flex-wrap: wrap;\\n    }\\n\\n    .chart-legend {\\n      display: flex;\\n      align-items: center;\\n      gap: 12px;\\n      font-size: 11.5px;\\n      color: var(--text-muted);\\n    }\\n\\n    .chart-legend-item {\\n      display: inline-flex;\\n      align-items: center;\\n      gap: 5px;\\n    }\\n\\n    /* Main Daily Chart Body */\\n    .chart-body {\\n      padding: 16px 18px 14px;\\n      position: relative;\\n      height: 310px;\\n    }\\n\\n    .chart-canvas-wrap {\\n      width: 100%;\\n      height: 100%;\\n      position: relative;\\n    }\\n\\n    .chart-empty {\\n      position: absolute;\\n      inset: 0;\\n      display: none;\\n      align-items: center;\\n      justify-content: center;\\n      color: var(--text-subtle);\\n      font-family: var(--font-mono);\\n      font-size: 12px;\\n    }\\n\\n    .chart-empty.is-visible {\\n      display: flex;\\n    }\\n\\n    .svg-fallback-container {\\n      width: 100%;\\n      height: 100%;\\n      display: none;\\n    }\\n\\n    .svg-fallback-container.is-visible {\\n      display: block;\\n    }\\n\\n    /* 365-Day Heatmap Card */\\n    .heatmap-body {\\n      padding: 16px 18px 12px;\\n      overflow-x: auto;\\n    }\\n\\n    .heatmap-scroll-inner {\\n      min-width: 740px;\\n    }\\n\\n    .heatmap-months {\\n      position: relative;\\n      height: 16px;\\n      margin-left: 32px;\\n      margin-bottom: 4px;\\n      font-size: 11px;\\n      color: var(--text-muted);\\n    }\\n\\n    .heatmap-month-label {\\n      position: absolute;\\n      top: 0;\\n      font-size: 10.5px;\\n      color: var(--text-muted);\\n      white-space: nowrap;\\n    }\\n\\n    .heatmap-grid-row {\\n      display: flex;\\n      align-items: flex-start;\\n      gap: 6px;\\n    }\\n\\n    .heatmap-weekdays {\\n      width: 26px;\\n      display: grid;\\n      grid-template-rows: repeat(7, 12px);\\n      gap: 3px;\\n      font-size: 10px;\\n      color: var(--text-subtle);\\n      line-height: 12px;\\n      flex-shrink: 0;\\n    }\\n\\n    .heatmap-weeks {\\n      flex: 1;\\n      display: grid;\\n      grid-template-columns: repeat(53, minmax(11px, 1fr));\\n      gap: 3px;\\n    }\\n\\n    .heatmap-week-col {\\n      display: grid;\\n      grid-template-rows: repeat(7, 12px);\\n      gap: 3px;\\n    }\\n\\n    .hm-cell {\\n      width: 100%;\\n      height: 12px;\\n      border-radius: 2.5px;\\n      border: 1px solid rgba(255, 255, 255, 0.035);\\n      transition: transform 0.1s ease, border-color 0.1s ease;\\n      cursor: crosshair;\\n    }\\n\\n    .hm-cell.hm-pad {\\n      background: transparent;\\n      border-color: transparent;\\n      pointer-events: none;\\n    }\\n\\n    .hm-cell.hm-lvl-0 { background: rgba(255, 255, 255, 0.03); }\\n    .hm-cell.hm-lvl-1 { background: rgba(94, 106, 210, 0.25); border-color: rgba(113, 112, 255, 0.2); }\\n    .hm-cell.hm-lvl-2 { background: rgba(94, 106, 210, 0.48); border-color: rgba(113, 112, 255, 0.32); }\\n    .hm-cell.hm-lvl-3 { background: rgba(113, 112, 255, 0.74); border-color: rgba(130, 143, 255, 0.45); }\\n    .hm-cell.hm-lvl-4 { background: #7170ff; border-color: #939eff; }\\n\\n    .hm-cell:not(.hm-pad):hover {\\n      border-color: var(--text-primary);\\n      transform: scale(1.14);\\n      z-index: 2;\\n    }\\n\\n    .heatmap-footer {\\n      padding: 10px 18px 14px;\\n      border-top: 1px solid var(--border-subtle);\\n      display: flex;\\n      align-items: center;\\n      justify-content: space-between;\\n      gap: 12px;\\n      flex-wrap: wrap;\\n      font-size: 11.5px;\\n      color: var(--text-muted);\\n    }\\n\\n    .heatmap-stats {\\n      display: flex;\\n      align-items: center;\\n      gap: 16px;\\n      flex-wrap: wrap;\\n    }\\n\\n    .heatmap-legend {\\n      display: inline-flex;\\n      align-items: center;\\n      gap: 4px;\\n      font-size: 11px;\\n      color: var(--text-subtle);\\n    }\\n\\n    .heatmap-legend .hm-swatch {\\n      width: 11px;\\n      height: 11px;\\n      border-radius: 2px;\\n      border: 1px solid rgba(255, 255, 255, 0.05);\\n    }\\n\\n    /* Secondary Insights Row (2 columns) */\\n    .insights-grid {\\n      display: grid;\\n      grid-template-columns: repeat(2, minmax(0, 1fr));\\n      gap: 12px;\\n    }\\n\\n    @media (max-width: 960px) {\\n      .insights-grid {\\n        grid-template-columns: 1fr;\\n      }\\n    }\\n\\n    /* Hourly Histogram */\\n    .hourly-body {\\n      padding: 18px 18px 14px;\\n      display: flex;\\n      flex-direction: column;\\n      justify-content: space-between;\\n      gap: 12px;\\n      min-height: 245px;\\n    }\\n\\n    .hourly-bars {\\n      display: grid;\\n      grid-template-columns: repeat(24, minmax(0, 1fr));\\n      gap: 4px;\\n      height: 162px;\\n      align-items: end;\\n      padding-top: 8px;\\n      border-bottom: 1px solid var(--border-subtle);\\n    }\\n\\n    .hour-col {\\n      height: 100%;\\n      display: flex;\\n      flex-direction: column;\\n      justify-content: flex-end;\\n      align-items: center;\\n      cursor: crosshair;\\n      position: relative;\\n      padding-bottom: 1px;\\n    }\\n\\n    .hour-col:hover .hour-bar {\\n      filter: brightness(1.25);\\n    }\\n\\n    .hour-bar {\\n      width: 100%;\\n      min-height: 2px;\\n      border-radius: 3px 3px 0 0;\\n      background: rgba(94, 106, 210, 0.45);\\n      border-top: 1px solid rgba(113, 112, 255, 0.65);\\n      transition: height 0.25s ease, background 0.15s ease;\\n    }\\n\\n    .hour-bar.is-zero {\\n      background: rgba(255, 255, 255, 0.05);\\n      border-top-color: transparent;\\n    }\\n\\n    .hour-bar.is-peak {\\n      background: linear-gradient(180deg, #7170ff 0%, rgba(94, 106, 210, 0.72) 100%);\\n      border-top-color: #a5b4fc;\\n    }\\n\\n    .hourly-axis {\\n      display: grid;\\n      grid-template-columns: repeat(24, minmax(0, 1fr));\\n      gap: 4px;\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      font-size: 10px;\\n      color: var(--text-subtle);\\n      text-align: center;\\n    }\\n\\n    .hourly-footer {\\n      display: flex;\\n      align-items: center;\\n      justify-content: space-between;\\n      gap: 8px;\\n      font-size: 11.5px;\\n      color: var(--text-muted);\\n    }\\n\\n    /* Tool Invocations Breakdown */\\n    .tools-body {\\n      padding: 12px 18px 16px;\\n      max-height: 265px;\\n      overflow-y: auto;\\n      display: flex;\\n      flex-direction: column;\\n      gap: 10px;\\n    }\\n\\n    .tool-row {\\n      display: flex;\\n      flex-direction: column;\\n      gap: 4px;\\n    }\\n\\n    .tool-row-header {\\n      display: flex;\\n      align-items: center;\\n      justify-content: space-between;\\n      gap: 10px;\\n    }\\n\\n    .tool-name-wrap {\\n      display: inline-flex;\\n      align-items: center;\\n      gap: 8px;\\n      min-width: 0;\\n    }\\n\\n    .tool-rank {\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      font-size: 10.5px;\\n      color: var(--text-subtle);\\n      width: 18px;\\n      flex-shrink: 0;\\n    }\\n\\n    .tool-name {\\n      font-family: var(--font-mono);\\n      font-size: 12px;\\n      color: var(--text-primary);\\n      overflow: hidden;\\n      text-overflow: ellipsis;\\n      white-space: nowrap;\\n    }\\n\\n    .tool-metrics {\\n      display: inline-flex;\\n      align-items: center;\\n      gap: 10px;\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      font-size: 11.5px;\\n      flex-shrink: 0;\\n    }\\n\\n    .tool-count {\\n      color: var(--text-secondary);\\n    }\\n\\n    .tool-share {\\n      color: var(--text-muted);\\n      min-width: 44px;\\n      text-align: right;\\n    }\\n\\n    .tool-track {\\n      width: 100%;\\n      height: 5px;\\n      background: rgba(255, 255, 255, 0.04);\\n      border-radius: 999px;\\n      overflow: hidden;\\n    }\\n\\n    .tool-fill {\\n      height: 100%;\\n      border-radius: 999px;\\n      background: var(--indigo-brand);\\n      transition: width 0.3s ease;\\n    }\\n\\n    .tool-row:first-child .tool-fill {\\n      background: var(--indigo-accent);\\n    }\\n\\n    /* Recent Conversations Table */\\n    .search-box {\\n      position: relative;\\n      display: inline-flex;\\n      align-items: center;\\n    }\\n\\n    .search-icon {\\n      position: absolute;\\n      left: 9px;\\n      color: var(--text-subtle);\\n      pointer-events: none;\\n      display: flex;\\n    }\\n\\n    .search-input {\\n      background: rgba(255, 255, 255, 0.03);\\n      border: 1px solid var(--border-card);\\n      border-radius: var(--radius-control);\\n      color: var(--text-primary);\\n      font-size: 12px;\\n      height: 30px;\\n      width: 260px;\\n      padding: 0 52px 0 28px;\\n      transition: border-color 0.14s ease, background 0.14s ease;\\n    }\\n\\n    @media (max-width: 560px) {\\n      .search-input {\\n        width: 100%;\\n      }\\n    }\\n\\n    .search-input::placeholder {\\n      color: var(--text-subtle);\\n    }\\n\\n    .search-input:focus {\\n      outline: none;\\n      border-color: var(--indigo-accent);\\n      background: rgba(255, 255, 255, 0.045);\\n    }\\n\\n    .search-kbd {\\n      position: absolute;\\n      right: 7px;\\n      font-family: var(--font-mono);\\n      font-size: 10px;\\n      color: var(--text-subtle);\\n      border: 1px solid var(--border-subtle);\\n      background: rgba(255, 255, 255, 0.02);\\n      padding: 1px 5px;\\n      border-radius: 3px;\\n      pointer-events: none;\\n    }\\n\\n    .search-clear {\\n      position: absolute;\\n      right: 7px;\\n      display: none;\\n      appearance: none;\\n      border: none;\\n      background: rgba(255, 255, 255, 0.08);\\n      color: var(--text-secondary);\\n      width: 16px;\\n      height: 16px;\\n      border-radius: 50%;\\n      font-size: 11px;\\n      line-height: 1;\\n      cursor: pointer;\\n      align-items: center;\\n      justify-content: center;\\n    }\\n\\n    .search-clear.is-visible {\\n      display: inline-flex;\\n    }\\n\\n    .table-wrap {\\n      width: 100%;\\n      overflow-x: auto;\\n    }\\n\\n    .conv-table {\\n      width: 100%;\\n      border-collapse: collapse;\\n      font-size: 12.5px;\\n      min-width: 940px;\\n    }\\n\\n    .conv-table thead th {\\n      text-align: left;\\n      padding: 9px 14px;\\n      font-size: 11px;\\n      font-weight: 500;\\n      color: var(--text-muted);\\n      border-bottom: 1px solid var(--border-card);\\n      background: rgba(255, 255, 255, 0.015);\\n      user-select: none;\\n      white-space: nowrap;\\n    }\\n\\n    .conv-table thead th.num-col {\\n      text-align: right;\\n    }\\n\\n    .conv-table thead th.sortable {\\n      cursor: pointer;\\n      transition: color 0.12s ease, background 0.12s ease;\\n    }\\n\\n    .conv-table thead th.sortable:hover {\\n      color: var(--text-primary);\\n      background: rgba(255, 255, 255, 0.03);\\n    }\\n\\n    .conv-table thead th.is-sorted {\\n      color: var(--text-primary);\\n    }\\n\\n    .th-inner {\\n      display: inline-flex;\\n      align-items: center;\\n      gap: 4px;\\n    }\\n\\n    .sort-arrow {\\n      font-family: var(--font-mono);\\n      font-size: 10px;\\n      color: var(--indigo-accent);\\n      opacity: 0;\\n      width: 10px;\\n      display: inline-block;\\n      text-align: center;\\n    }\\n\\n    th.sortable:hover .sort-arrow {\\n      opacity: 0.45;\\n      color: var(--text-muted);\\n    }\\n\\n    th.is-sorted .sort-arrow {\\n      opacity: 1 !important;\\n      color: var(--indigo-accent) !important;\\n    }\\n\\n    .conv-table tbody tr {\\n      border-bottom: 1px solid var(--border-subtle);\\n      transition: background 0.12s ease;\\n      cursor: pointer;\\n    }\\n\\n    .conv-table tbody tr:last-child {\\n      border-bottom: none;\\n    }\\n\\n    .conv-table tbody tr:hover {\\n      background: var(--bg-card-hover);\\n    }\\n\\n    .conv-table tbody tr.is-selected {\\n      background: rgba(94, 106, 210, 0.12);\\n    }\\n\\n    .conv-table td {\\n      padding: 10px 14px;\\n      vertical-align: middle;\\n      color: var(--text-secondary);\\n      white-space: nowrap;\\n    }\\n\\n    .conv-table td.num-col {\\n      text-align: right;\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      font-size: 12px;\\n    }\\n\\n    .conv-title-cell {\\n      max-width: 310px;\\n      display: flex;\\n      flex-direction: column;\\n      gap: 2px;\\n    }\\n\\n    .conv-title-text {\\n      color: var(--text-primary);\\n      font-weight: 500;\\n      overflow: hidden;\\n      text-overflow: ellipsis;\\n      white-space: nowrap;\\n    }\\n\\n    .conv-id-short {\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      font-size: 10.5px;\\n      color: var(--text-subtle);\\n    }\\n\\n    .tok-input { color: #9aa5ff; }\\n    .tok-output { color: #5eead4; }\\n    .tok-thinking { color: #c084fc; }\\n    .tok-total { color: var(--text-primary); font-weight: 500; }\\n    .cost-cell { color: var(--text-primary); }\\n\\n    .table-footer {\\n      padding: 10px 18px;\\n      border-top: 1px solid var(--border-subtle);\\n      display: flex;\\n      align-items: center;\\n      justify-content: space-between;\\n      gap: 12px;\\n      font-size: 11.5px;\\n      color: var(--text-muted);\\n    }\\n\\n    .empty-state {\\n      padding: 36px 18px;\\n      text-align: center;\\n      color: var(--text-subtle);\\n      font-size: 12.5px;\\n    }\\n\\n    /* Floating Tooltip */\\n    .floating-tooltip {\\n      position: fixed;\\n      z-index: 90;\\n      pointer-events: none;\\n      background: var(--bg-popover);\\n      border: 1px solid rgba(255, 255, 255, 0.12);\\n      border-radius: 7px;\\n      padding: 8px 11px;\\n      box-shadow: 0 10px 28px rgba(0, 0, 0, 0.65);\\n      font-size: 11.5px;\\n      color: var(--text-secondary);\\n      max-width: 300px;\\n      opacity: 0;\\n      transform: translateY(3px);\\n      transition: opacity 0.1s ease, transform 0.1s ease;\\n    }\\n\\n    .floating-tooltip.is-visible {\\n      opacity: 1;\\n      transform: translateY(0);\\n    }\\n\\n    .tt-title {\\n      font-weight: 600;\\n      color: var(--text-primary);\\n      margin-bottom: 4px;\\n      font-size: 12px;\\n    }\\n\\n    .tt-row {\\n      display: flex;\\n      align-items: center;\\n      justify-content: space-between;\\n      gap: 14px;\\n      line-height: 1.55;\\n    }\\n\\n    .tt-label {\\n      color: var(--text-muted);\\n      display: inline-flex;\\n      align-items: center;\\n      gap: 5px;\\n    }\\n\\n    .tt-val {\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      color: var(--text-primary);\\n      font-weight: 500;\\n    }\\n\\n    /* Slide-over Conversation Drawer */\\n    .drawer-backdrop {\\n      position: fixed;\\n      inset: 0;\\n      z-index: 60;\\n      background: rgba(0, 0, 0, 0.72);\\n      backdrop-filter: blur(3px);\\n      -webkit-backdrop-filter: blur(3px);\\n      opacity: 0;\\n      pointer-events: none;\\n      transition: opacity 0.18s ease;\\n    }\\n\\n    .drawer-backdrop.is-open {\\n      opacity: 1;\\n      pointer-events: auto;\\n    }\\n\\n    .drawer-panel {\\n      position: fixed;\\n      top: 0;\\n      right: 0;\\n      bottom: 0;\\n      z-index: 70;\\n      width: min(520px, 100vw);\\n      background: var(--bg-panel);\\n      border-left: 1px solid var(--border-card);\\n      box-shadow: -16px 0 48px rgba(0, 0, 0, 0.75);\\n      display: flex;\\n      flex-direction: column;\\n      transform: translateX(100%);\\n      transition: transform 0.22s cubic-bezier(0.16, 1, 0.3, 1);\\n    }\\n\\n    .drawer-panel.is-open {\\n      transform: translateX(0);\\n    }\\n\\n    .drawer-header {\\n      padding: 16px 20px;\\n      border-bottom: 1px solid var(--border-card);\\n      display: flex;\\n      align-items: flex-start;\\n      justify-content: space-between;\\n      gap: 12px;\\n    }\\n\\n    .drawer-title {\\n      font-size: 15px;\\n      font-weight: 600;\\n      letter-spacing: -0.015em;\\n      color: var(--text-primary);\\n      line-height: 1.35;\\n      word-break: break-word;\\n    }\\n\\n    .drawer-header-actions {\\n      display: inline-flex;\\n      align-items: center;\\n      gap: 6px;\\n      flex-shrink: 0;\\n    }\\n\\n    .icon-btn {\\n      appearance: none;\\n      width: 28px;\\n      height: 28px;\\n      border-radius: var(--radius-control);\\n      background: rgba(255, 255, 255, 0.03);\\n      border: 1px solid var(--border-card);\\n      color: var(--text-muted);\\n      display: inline-flex;\\n      align-items: center;\\n      justify-content: center;\\n      cursor: pointer;\\n      transition: background 0.14s ease, color 0.14s ease, border-color 0.14s ease;\\n    }\\n\\n    .icon-btn:hover:not(:disabled) {\\n      background: rgba(255, 255, 255, 0.07);\\n      color: var(--text-primary);\\n      border-color: var(--border-strong);\\n    }\\n\\n    .icon-btn:disabled {\\n      opacity: 0.35;\\n      cursor: not-allowed;\\n    }\\n\\n    .drawer-content {\\n      padding: 18px 20px 28px;\\n      overflow-y: auto;\\n      display: flex;\\n      flex-direction: column;\\n      gap: 18px;\\n      flex: 1;\\n    }\\n\\n    .id-box {\\n      display: flex;\\n      align-items: center;\\n      justify-content: space-between;\\n      gap: 10px;\\n      padding: 9px 12px;\\n      border-radius: var(--radius-control);\\n      background: rgba(255, 255, 255, 0.025);\\n      border: 1px solid var(--border-card);\\n    }\\n\\n    .id-box-code {\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      font-size: 11.5px;\\n      color: var(--text-secondary);\\n      word-break: break-all;\\n    }\\n\\n    .copy-btn {\\n      appearance: none;\\n      display: inline-flex;\\n      align-items: center;\\n      gap: 5px;\\n      padding: 4px 9px;\\n      border-radius: var(--radius-sm);\\n      background: rgba(94, 106, 210, 0.16);\\n      border: 1px solid rgba(113, 112, 255, 0.38);\\n      color: var(--text-primary);\\n      font-size: 11px;\\n      font-weight: 500;\\n      cursor: pointer;\\n      flex-shrink: 0;\\n      transition: background 0.14s ease, border-color 0.14s ease;\\n    }\\n\\n    .copy-btn:hover {\\n      background: rgba(94, 106, 210, 0.28);\\n    }\\n\\n    .copy-btn.is-copied {\\n      background: rgba(16, 185, 129, 0.16);\\n      border-color: rgba(16, 185, 129, 0.45);\\n      color: #6ee7b7;\\n    }\\n\\n    .drawer-section {\\n      display: flex;\\n      flex-direction: column;\\n      gap: 8px;\\n    }\\n\\n    .drawer-section-title {\\n      font-size: 11px;\\n      font-weight: 600;\\n      text-transform: uppercase;\\n      letter-spacing: 0.05em;\\n      color: var(--text-subtle);\\n    }\\n\\n    .detail-grid {\\n      display: grid;\\n      grid-template-columns: repeat(2, minmax(0, 1fr));\\n      gap: 8px;\\n    }\\n\\n    .detail-tile {\\n      padding: 10px 12px;\\n      border-radius: var(--radius-control);\\n      background: rgba(255, 255, 255, 0.02);\\n      border: 1px solid var(--border-subtle);\\n      display: flex;\\n      flex-direction: column;\\n      gap: 3px;\\n    }\\n\\n    .detail-tile-label {\\n      font-size: 11px;\\n      color: var(--text-muted);\\n    }\\n\\n    .detail-tile-val {\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      font-size: 14px;\\n      font-weight: 600;\\n      color: var(--text-primary);\\n    }\\n\\n    .detail-tile-sub {\\n      font-family: var(--font-mono);\\n      font-variant-numeric: tabular-nums;\\n      font-size: 10.5px;\\n      color: var(--text-subtle);\\n    }\\n\\n    .comp-list {\\n      border: 1px solid var(--border-card);\\n      border-radius: var(--radius-control);\\n      overflow: hidden;\\n    }\\n\\n    .comp-row {\\n      display: flex;\\n      align-items: center;\\n      justify-content: space-between;\\n      padding: 8px 12px;\\n      border-bottom: 1px solid var(--border-subtle);\\n      font-size: 12px;\\n    }\\n\\n    .comp-row:last-child {\\n      border-bottom: none;\\n    }\\n\\n    .comp-row.comp-total {\\n      background: rgba(255, 255, 255, 0.025);\\n      font-weight: 600;\\n    }\\n\\n    /* Toast Notification */\\n    .toast {\\n      position: fixed;\\n      bottom: 20px;\\n      right: 20px;\\n      z-index: 95;\\n      background: #16181b;\\n      border: 1px solid var(--border-strong);\\n      border-radius: 7px;\\n      padding: 9px 14px;\\n      font-size: 12px;\\n      color: var(--text-primary);\\n      box-shadow: 0 12px 32px rgba(0, 0, 0, 0.65);\\n      display: flex;\\n      align-items: center;\\n      gap: 8px;\\n      opacity: 0;\\n      transform: translateY(8px);\\n      pointer-events: none;\\n      transition: opacity 0.18s ease, transform 0.18s ease;\\n    }\\n\\n    .toast.is-visible {\\n      opacity: 1;\\n      transform: translateY(0);\\n    }\\n  </style>\\n</head>\\n<body>\\n  <!-- Sticky Top Bar -->\\n  <header class=\\"topbar\\">\\n    <div class=\\"topbar-inner\\">\\n      <div class=\\"brand-cluster\\">\\n        <div class=\\"app-mark\\" aria-hidden=\\"true\\">\\n          <svg width=\\"16\\" height=\\"16\\" viewBox=\\"0 0 16 16\\" fill=\\"none\\" stroke=\\"currentColor\\" stroke-width=\\"1.5\\" stroke-linecap=\\"round\\" stroke-linejoin=\\"round\\">\\n            <path d=\\"M2 11.5L5.5 7.5L8.5 10L14 3.5\\" />\\n            <path d=\\"M10.5 3.5H14V7\\" />\\n            <path d=\\"M2 13.5H14\\" stroke-opacity=\\"0.45\\" />\\n          </svg>\\n        </div>\\n        <div class=\\"brand-titles\\">\\n          <div class=\\"brand-row\\">\\n            <span class=\\"brand-name\\">AGY Telemetry</span>\\n            <span id=\\"demoBadge\\" class=\\"demo-badge\\" role=\\"status\\">\\n              <span class=\\"demo-badge-dot\\"></span>\\n              <span>DEMO PREVIEW</span>\\n            </span>\\n          </div>\\n          <div id=\\"agyPathSubtitle\\" class=\\"brand-subtitle\\">Initializing telemetry workspace\\u2026</div>\\n        </div>\\n      </div>\\n\\n      <div class=\\"controls-cluster\\">\\n        <!-- Segmented Date Filter -->\\n        <div class=\\"segmented\\" id=\\"dateFilterGroup\\" role=\\"group\\" aria-label=\\"Date range filter\\">\\n          <button type=\\"button\\" class=\\"seg-btn mono-label\\" data-days=\\"7\\">7D</button>\\n          <button type=\\"button\\" class=\\"seg-btn mono-label is-active\\" data-days=\\"30\\">30D</button>\\n          <button type=\\"button\\" class=\\"seg-btn mono-label\\" data-days=\\"90\\">90D</button>\\n          <button type=\\"button\\" class=\\"seg-btn mono-label\\" data-days=\\"365\\">365D</button>\\n          <button type=\\"button\\" class=\\"seg-btn mono-label\\" data-days=\\"all\\">ALL</button>\\n        </div>\\n\\n        <!-- Billing Mode Toggle -->\\n        <div class=\\"control-with-help\\">\\n          <div class=\\"segmented\\" id=\\"billingModeGroup\\" role=\\"group\\" aria-label=\\"Token billing mode\\">\\n            <button type=\\"button\\" class=\\"seg-btn is-active\\" data-billing=\\"context\\">Context Tokens</button>\\n            <button type=\\"button\\" class=\\"seg-btn\\" data-billing=\\"turn\\">Turn Tokens</button>\\n          </div>\\n          <button type=\\"button\\" id=\\"billingHelpBtn\\" class=\\"help-trigger\\" aria-label=\\"Explain billing modes\\">?</button>\\n        </div>\\n\\n        <!-- Model Pricing Selector -->\\n        <div class=\\"select-wrap\\">\\n          <select id=\\"pricingSelect\\" class=\\"linear-select\\" aria-label=\\"Model pricing tier\\">\\n            <option value=\\"\\">Loading pricing\\u2026</option>\\n          </select>\\n          <span class=\\"select-chevron\\">\\n            <svg width=\\"12\\" height=\\"12\\" viewBox=\\"0 0 12 12\\" fill=\\"none\\" stroke=\\"currentColor\\" stroke-width=\\"1.5\\" stroke-linecap=\\"round\\" stroke-linejoin=\\"round\\">\\n              <path d=\\"M3 4.5L6 7.5L9 4.5\\" />\\n            </svg>\\n          </span>\\n        </div>\\n\\n        <!-- Auto-refresh 10s toggle -->\\n        <button type=\\"button\\" id=\\"autoRefreshBtn\\" class=\\"btn-ghost\\" title=\\"Poll /api/stats every 10 seconds\\">\\n          <span class=\\"pulse-dot\\"></span>\\n          <span>Live</span>\\n          <span class=\\"mono\\" style=\\"font-size:10.5px;color:var(--text-muted);\\">10s</span>\\n        </button>\\n\\n        <!-- Rescan Button -->\\n        <button type=\\"button\\" id=\\"rescanBtn\\" class=\\"btn-ghost\\" title=\\"Trigger workspace telemetry rescan\\">\\n          <svg width=\\"13\\" height=\\"13\\" viewBox=\\"0 0 16 16\\" fill=\\"none\\" stroke=\\"currentColor\\" stroke-width=\\"1.5\\" stroke-linecap=\\"round\\" stroke-linejoin=\\"round\\">\\n            <path d=\\"M13.5 8A5.5 5.5 0 1 1 11.9 4.1L13.5 5.5\\" />\\n            <path d=\\"M13.5 2.5V5.5H10.5\\" />\\n          </svg>\\n          <span>Rescan</span>\\n        </button>\\n\\n        <!-- Export CSV Link -->\\n        <a id=\\"exportCsvLink\\" href=\\"/api/export.csv?days=30&billing_mode=context\\" class=\\"btn-ghost\\" download title=\\"Export filtered telemetry as CSV\\">\\n          <svg width=\\"13\\" height=\\"13\\" viewBox=\\"0 0 16 16\\" fill=\\"none\\" stroke=\\"currentColor\\" stroke-width=\\"1.5\\" stroke-linecap=\\"round\\" stroke-linejoin=\\"round\\">\\n            <path d=\\"M8 2.5V10.5\\" />\\n            <path d=\\"M5 7.5L8 10.5L11 7.5\\" />\\n            <path d=\\"M2.5 13H13.5\\" />\\n          </svg>\\n          <span>Export CSV</span>\\n        </a>\\n      </div>\\n    </div>\\n  </header>\\n\\n  <!-- Demo Preview Info Banner -->\\n  <div id=\\"demoBanner\\" class=\\"banner-strip banner-demo\\">\\n    <div class=\\"banner-inner\\">\\n      <div class=\\"banner-msg\\">\\n        <span class=\\"mono\\" style=\\"font-weight:600;\\">[DEMO]</span>\\n        <span id=\\"demoBannerText\\">Synthetic telemetry preview is active.</span>\\n      </div>\\n      <div class=\\"banner-actions\\">\\n        <button type=\\"button\\" id=\\"dismissDemoBanner\\" class=\\"banner-btn\\">Dismiss</button>\\n      </div>\\n    </div>\\n  </div>\\n\\n  <!-- API Error Banner -->\\n  <div id=\\"errorBanner\\" class=\\"banner-strip banner-error\\">\\n    <div class=\\"banner-inner\\">\\n      <div class=\\"banner-msg\\">\\n        <span class=\\"mono\\" style=\\"font-weight:600;\\">[OFFLINE]</span>\\n        <span id=\\"errorBannerText\\">Unable to reach /api/stats.</span>\\n      </div>\\n      <div class=\\"banner-actions\\">\\n        <button type=\\"button\\" id=\\"retryApiBtn\\" class=\\"banner-btn\\">Retry</button>\\n        <button type=\\"button\\" id=\\"loadFallbackDemoBtn\\" class=\\"banner-btn\\">Preview Demo Data</button>\\n      </div>\\n    </div>\\n  </div>\\n\\n  <!-- Main Telemetry Surface -->\\n  <main class=\\"main-container\\">\\n    <!-- Metadata Status Strip -->\\n    <div class=\\"status-strip\\">\\n      <div class=\\"status-meta-group\\">\\n        <span class=\\"status-item\\">DB: <span id=\\"metaDbPath\\" class=\\"mono\\" style=\\"color:var(--text-muted);\\">\\u2014</span></span>\\n        <span class=\\"status-sep\\">\\u00b7</span>\\n        <span class=\\"status-item\\">Last scanned: <span id=\\"metaLastScanned\\" class=\\"mono\\" style=\\"color:var(--text-muted);\\">\\u2014</span></span>\\n        <span class=\\"status-sep\\">\\u00b7</span>\\n        <span class=\\"status-item\\">Chart engine: <span id=\\"metaChartEngine\\" class=\\"mono\\" style=\\"color:var(--text-muted);\\">\\u2014</span></span>\\n      </div>\\n      <div class=\\"status-meta-group\\">\\n        <span class=\\"status-item\\">Billing: <span id=\\"metaBillingBadge\\" class=\\"mono\\" style=\\"color:var(--text-secondary);\\">context</span></span>\\n      </div>\\n    </div>\\n\\n    <!-- Section 2: 6 KPI Summary Cards -->\\n    <section class=\\"kpi-grid\\" aria-label=\\"Key performance indicators\\">\\n      <!-- Card 1: Total Tokens -->\\n      <article class=\\"surface-card interactive-card kpi-card\\">\\n        <div class=\\"kpi-top\\">\\n          <span class=\\"kpi-label\\">Total Tokens</span>\\n          <span id=\\"kpiTokenModeTag\\" class=\\"kpi-tag\\">Context</span>\\n        </div>\\n        <div class=\\"kpi-value-row\\">\\n          <span id=\\"kpiTotalTokens\\" class=\\"kpi-value\\">\\u2014</span>\\n          <span id=\\"kpiTotalTokensExact\\" class=\\"kpi-value-sub\\"></span>\\n        </div>\\n        <div style=\\"display:flex;flex-direction:column;gap:7px;\\">\\n          <div class=\\"seg-bar\\" aria-label=\\"Token composition bar\\">\\n            <div id=\\"barInputShare\\" class=\\"seg-bar-piece\\" style=\\"width:0%;background:var(--indigo-brand);\\"></div>\\n            <div id=\\"barOutputShare\\" class=\\"seg-bar-piece\\" style=\\"width:0%;background:var(--cyan-output);\\"></div>\\n            <div id=\\"barThinkingShare\\" class=\\"seg-bar-piece\\" style=\\"width:0%;background:var(--violet-thinking);\\"></div>\\n          </div>\\n          <div class=\\"kpi-breakdown\\">\\n            <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--indigo-brand);\\"></span>In <span id=\\"kpiInputTokens\\" class=\\"bd-val\\">\\u2014</span></span>\\n            <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--cyan-output);\\"></span>Out <span id=\\"kpiOutputTokens\\" class=\\"bd-val\\">\\u2014</span></span>\\n            <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--violet-thinking);\\"></span>Think <span id=\\"kpiThinkingTokens\\" class=\\"bd-val\\">\\u2014</span></span>\\n          </div>\\n        </div>\\n      </article>\\n\\n      <!-- Card 2: API Requests -->\\n      <article class=\\"surface-card interactive-card kpi-card\\">\\n        <div class=\\"kpi-top\\">\\n          <span class=\\"kpi-label\\">API Requests</span>\\n          <span id=\\"kpiToolCallsTag\\" class=\\"kpi-tag\\">\\u2014 tools</span>\\n        </div>\\n        <div class=\\"kpi-value-row\\">\\n          <span id=\\"kpiApiRequests\\" class=\\"kpi-value\\">\\u2014</span>\\n          <span id=\\"kpiAvgTokPerReq\\" class=\\"kpi-value-sub\\">\\u2014 tok/req</span>\\n        </div>\\n        <div class=\\"kpi-breakdown\\">\\n          <span class=\\"bd-item\\">Conversations <span id=\\"kpiConversations\\" class=\\"bd-val\\">\\u2014</span></span>\\n          <span class=\\"bd-item\\">Subagents <span id=\\"kpiSubagents\\" class=\\"bd-val\\">\\u2014</span></span>\\n        </div>\\n      </article>\\n\\n      <!-- Card 3: Est. API Value / Cost -->\\n      <article class=\\"surface-card interactive-card kpi-card\\">\\n        <div class=\\"kpi-top\\">\\n          <span class=\\"kpi-label\\">Est. API Value / Cost</span>\\n          <span id=\\"kpiPricingTag\\" class=\\"kpi-tag\\">\\u2014</span>\\n        </div>\\n        <div class=\\"kpi-value-row\\">\\n          <span id=\\"kpiCostUsd\\" class=\\"kpi-value\\">\\u2014</span>\\n          <span id=\\"kpiCostPerReq\\" class=\\"kpi-value-sub\\"></span>\\n        </div>\\n        <div style=\\"display:flex;flex-direction:column;gap:7px;\\">\\n          <div class=\\"seg-bar\\">\\n            <div id=\\"barCostInput\\" class=\\"seg-bar-piece\\" style=\\"width:50%;background:var(--indigo-brand);\\"></div>\\n            <div id=\\"barCostOutput\\" class=\\"seg-bar-piece\\" style=\\"width:50%;background:var(--cyan-output);\\"></div>\\n          </div>\\n          <div class=\\"kpi-breakdown\\">\\n            <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--indigo-brand);\\"></span>Input <span id=\\"kpiInputCost\\" class=\\"bd-val\\">\\u2014</span></span>\\n            <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--cyan-output);\\"></span>Out+Think <span id=\\"kpiOutThinkCost\\" class=\\"bd-val\\">\\u2014</span></span>\\n          </div>\\n        </div>\\n      </article>\\n\\n      <!-- Card 4: Electricity Used -->\\n      <article class=\\"surface-card interactive-card kpi-card\\">\\n        <div class=\\"kpi-top\\">\\n          <span class=\\"kpi-label\\">Electricity Used</span>\\n          <span id=\\"kpiEnergyAlt\\" class=\\"kpi-tag tag-emerald\\">\\u2014</span>\\n        </div>\\n        <div class=\\"kpi-value-row\\">\\n          <span id=\\"kpiElectricity\\" class=\\"kpi-value\\">\\u2014</span>\\n        </div>\\n        <div class=\\"eco-pills\\">\\n          <span class=\\"eco-pill\\"><span class=\\"eco-dot\\"></span><span id=\\"kpiPhoneCharges\\" class=\\"mono\\">~0.0 phone charges</span></span>\\n          <span class=\\"eco-pill\\"><span class=\\"eco-dot\\"></span><span id=\\"kpiLedHours\\" class=\\"mono\\">~0.0 hrs 10W LED</span></span>\\n        </div>\\n      </article>\\n\\n      <!-- Card 5: Carbon Footprint -->\\n      <article class=\\"surface-card interactive-card kpi-card\\">\\n        <div class=\\"kpi-top\\">\\n          <span class=\\"kpi-label\\">Carbon Footprint</span>\\n          <span class=\\"kpi-tag tag-emerald\\">@ 385g/kWh grid avg</span>\\n        </div>\\n        <div class=\\"kpi-value-row\\">\\n          <span id=\\"kpiCarbon\\" class=\\"kpi-value\\">\\u2014</span>\\n          <span id=\\"kpiCarbonSub\\" class=\\"kpi-value-sub\\"></span>\\n        </div>\\n        <div class=\\"kpi-breakdown\\">\\n          <span class=\\"bd-item\\">Per request <span id=\\"kpiCo2PerReq\\" class=\\"bd-val\\">\\u2014</span></span>\\n          <span class=\\"bd-item\\">Per 1M tok <span id=\\"kpiCo2PerM\\" class=\\"bd-val\\">\\u2014</span></span>\\n        </div>\\n      </article>\\n\\n      <!-- Card 6: Reasoning & Peak Velocity -->\\n      <article class=\\"surface-card interactive-card kpi-card\\">\\n        <div class=\\"kpi-top\\">\\n          <span class=\\"kpi-label\\">Reasoning &amp; Peak Velocity</span>\\n          <span id=\\"kpiActiveDaysTag\\" class=\\"kpi-tag tag-violet\\">\\u2014 active days</span>\\n        </div>\\n        <div class=\\"kpi-value-row\\">\\n          <span id=\\"kpiThinkingShare\\" class=\\"kpi-value\\">\\u2014</span>\\n          <span class=\\"kpi-value-sub\\">thinking share</span>\\n        </div>\\n        <div style=\\"display:flex;flex-direction:column;gap:7px;\\">\\n          <div class=\\"seg-bar\\">\\n            <div id=\\"barReasoningGauge\\" class=\\"seg-bar-piece\\" style=\\"width:0%;background:var(--violet-thinking);\\"></div>\\n          </div>\\n          <div class=\\"kpi-breakdown\\">\\n            <span class=\\"bd-item\\">Peak day <span id=\\"kpiPeakDate\\" class=\\"bd-val\\">\\u2014</span></span>\\n            <span class=\\"bd-item\\"><span id=\\"kpiPeakTokens\\" class=\\"bd-val\\">\\u2014</span></span>\\n          </div>\\n        </div>\\n      </article>\\n    </section>\\n\\n    <!-- Section 3: Main Daily Chart Card -->\\n    <section class=\\"surface-card\\" aria-label=\\"Daily telemetry chart\\">\\n      <div class=\\"card-header\\">\\n        <div class=\\"card-title-group\\">\\n          <div class=\\"card-title-row\\">\\n            <h2 class=\\"card-title\\">Daily Telemetry</h2>\\n            <span id=\\"dailyRangeBadge\\" class=\\"kpi-tag\\">30D</span>\\n          </div>\\n          <span id=\\"dailySubtitle\\" class=\\"card-subtitle\\">Stacked token composition by UTC day</span>\\n        </div>\\n        <div class=\\"card-controls\\">\\n          <div id=\\"dailyChartLegend\\" class=\\"chart-legend\\"></div>\\n          <div class=\\"segmented\\" id=\\"chartTabGroup\\" role=\\"tablist\\" aria-label=\\"Daily chart metric\\">\\n            <button type=\\"button\\" class=\\"seg-btn is-active\\" role=\\"tab\\" aria-selected=\\"true\\" data-chart-tab=\\"tokens\\">Token Usage</button>\\n            <button type=\\"button\\" class=\\"seg-btn\\" role=\\"tab\\" aria-selected=\\"false\\" data-chart-tab=\\"cost\\">Est. Value ($)</button>\\n            <button type=\\"button\\" class=\\"seg-btn\\" role=\\"tab\\" aria-selected=\\"false\\" data-chart-tab=\\"requests\\">API Requests &amp; Tools</button>\\n          </div>\\n        </div>\\n      </div>\\n      <div class=\\"chart-body\\">\\n        <div class=\\"chart-canvas-wrap\\">\\n          <canvas id=\\"dailyChartCanvas\\"></canvas>\\n          <div id=\\"dailySvgFallback\\" class=\\"svg-fallback-container\\"></div>\\n          <div id=\\"dailyChartEmpty\\" class=\\"chart-empty\\">No daily telemetry recorded in this window</div>\\n        </div>\\n      </div>\\n    </section>\\n\\n    <!-- Section 4: 365-Day Activity Heatmap Card -->\\n    <section class=\\"surface-card\\" aria-label=\\"365-day activity heatmap\\">\\n      <div class=\\"card-header\\">\\n        <div class=\\"card-title-group\\">\\n          <div class=\\"card-title-row\\">\\n            <h2 class=\\"card-title\\">365-Day Activity Matrix</h2>\\n            <span id=\\"heatmapYearSpan\\" class=\\"kpi-tag\\">53 weeks</span>\\n          </div>\\n          <span class=\\"card-subtitle\\">Daily token throughput intensity over the trailing year</span>\\n        </div>\\n      </div>\\n      <div class=\\"heatmap-body\\">\\n        <div class=\\"heatmap-scroll-inner\\">\\n          <div id=\\"heatmapMonths\\" class=\\"heatmap-months\\"></div>\\n          <div class=\\"heatmap-grid-row\\">\\n            <div class=\\"heatmap-weekdays\\" aria-hidden=\\"true\\">\\n              <span></span>\\n              <span>Mon</span>\\n              <span></span>\\n              <span>Wed</span>\\n              <span></span>\\n              <span>Fri</span>\\n              <span></span>\\n            </div>\\n            <div id=\\"heatmapWeeks\\" class=\\"heatmap-weeks\\" role=\\"grid\\" aria-label=\\"365 day token activity grid\\"></div>\\n          </div>\\n        </div>\\n      </div>\\n      <div class=\\"heatmap-footer\\">\\n        <div class=\\"heatmap-stats\\">\\n          <span><strong id=\\"hmActiveDays\\" class=\\"mono\\" style=\\"color:var(--text-primary);font-weight:600;\\">0</strong> active days</span>\\n          <span>365d Volume: <span id=\\"hmYearTokens\\" class=\\"mono\\" style=\\"color:var(--text-secondary);\\">0</span></span>\\n          <span>Longest Streak: <span id=\\"hmLongestStreak\\" class=\\"mono\\" style=\\"color:var(--text-secondary);\\">0d</span></span>\\n        </div>\\n        <div class=\\"heatmap-legend\\" aria-label=\\"Intensity legend\\">\\n          <span>Less</span>\\n          <span class=\\"hm-swatch hm-lvl-0\\"></span>\\n          <span class=\\"hm-swatch hm-lvl-1\\"></span>\\n          <span class=\\"hm-swatch hm-lvl-2\\"></span>\\n          <span class=\\"hm-swatch hm-lvl-3\\"></span>\\n          <span class=\\"hm-swatch hm-lvl-4\\"></span>\\n          <span>More</span>\\n        </div>\\n      </div>\\n    </section>\\n\\n    <!-- Section 5: Secondary Insights Row (Hourly + Tools) -->\\n    <section class=\\"insights-grid\\" aria-label=\\"Hourly distribution and tool breakdown\\">\\n      <!-- Left: Hour-of-Day Activity Distribution -->\\n      <article class=\\"surface-card\\">\\n        <div class=\\"card-header\\">\\n          <div class=\\"card-title-group\\">\\n            <div class=\\"card-title-row\\">\\n              <h2 class=\\"card-title\\">Hour-of-Day Activity</h2>\\n              <span id=\\"hourlyPeakTag\\" class=\\"kpi-tag\\">Peak \\u2014</span>\\n            </div>\\n            <span id=\\"hourlyTzSubtitle\\" class=\\"card-subtitle\\">24-hour request &amp; token cadence</span>\\n          </div>\\n          <div class=\\"segmented\\" id=\\"hourlyMetricGroup\\" role=\\"group\\" aria-label=\\"Hourly metric\\">\\n            <button type=\\"button\\" class=\\"seg-btn is-active\\" data-hourly-mode=\\"tokens\\">Tokens</button>\\n            <button type=\\"button\\" class=\\"seg-btn\\" data-hourly-mode=\\"requests\\">Requests</button>\\n          </div>\\n        </div>\\n        <div class=\\"hourly-body\\">\\n          <div>\\n            <div id=\\"hourlyBars\\" class=\\"hourly-bars\\"></div>\\n            <div id=\\"hourlyAxis\\" class=\\"hourly-axis\\" style=\\"margin-top:6px;\\"></div>\\n          </div>\\n          <div class=\\"hourly-footer\\">\\n            <span id=\\"hourlyTzNote\\">Hours in recorded session clock (00\\u201323)</span>\\n            <span id=\\"hourlyPeakShare\\" class=\\"mono\\" style=\\"color:var(--text-secondary);\\">\\u2014</span>\\n          </div>\\n        </div>\\n      </article>\\n\\n      <!-- Right: Tool Invocations Breakdown -->\\n      <article class=\\"surface-card\\">\\n        <div class=\\"card-header\\">\\n          <div class=\\"card-title-group\\">\\n            <div class=\\"card-title-row\\">\\n              <h2 class=\\"card-title\\">Tool Invocations Breakdown</h2>\\n              <span id=\\"toolsTotalBadge\\" class=\\"kpi-tag\\">0 calls</span>\\n            </div>\\n            <span class=\\"card-subtitle\\">Share of executed agent tool calls in window</span>\\n          </div>\\n        </div>\\n        <div id=\\"toolsListBody\\" class=\\"tools-body\\">\\n          <div class=\\"empty-state\\">No tool invocations recorded</div>\\n        </div>\\n      </article>\\n    </section>\\n\\n    <!-- Section 6: Recent Conversations Table -->\\n    <section class=\\"surface-card\\" aria-label=\\"Recent conversations\\">\\n      <div class=\\"card-header\\">\\n        <div class=\\"card-title-group\\">\\n          <div class=\\"card-title-row\\">\\n            <h2 class=\\"card-title\\">Recent Conversations</h2>\\n            <span id=\\"convCountBadge\\" class=\\"kpi-tag\\">0</span>\\n          </div>\\n          <span class=\\"card-subtitle\\">Click any session row to inspect token composition, footprint &amp; metadata</span>\\n        </div>\\n        <div class=\\"card-controls\\">\\n          <div class=\\"search-box\\">\\n            <span class=\\"search-icon\\">\\n              <svg width=\\"13\\" height=\\"13\\" viewBox=\\"0 0 16 16\\" fill=\\"none\\" stroke=\\"currentColor\\" stroke-width=\\"1.5\\" stroke-linecap=\\"round\\" stroke-linejoin=\\"round\\">\\n                <circle cx=\\"7\\" cy=\\"7\\" r=\\"4.5\\" />\\n                <path d=\\"M10.5 10.5L14 14\\" />\\n              </svg>\\n            </span>\\n            <input type=\\"text\\" id=\\"convSearchInput\\" class=\\"search-input\\" placeholder=\\"Filter by title or session ID\\u2026\\" autocomplete=\\"off\\" spellcheck=\\"false\\" />\\n            <span id=\\"searchKbd\\" class=\\"search-kbd\\">/</span>\\n            <button type=\\"button\\" id=\\"searchClearBtn\\" class=\\"search-clear\\" aria-label=\\"Clear search\\">&times;</button>\\n          </div>\\n        </div>\\n      </div>\\n\\n      <div class=\\"table-wrap\\">\\n        <table class=\\"conv-table\\">\\n          <thead>\\n            <tr>\\n              <th class=\\"sortable\\" data-sort=\\"title\\"><span class=\\"th-inner\\">Conversation <span class=\\"sort-arrow\\">\\u2193</span></span></th>\\n              <th class=\\"sortable is-sorted\\" data-sort=\\"updated_at\\"><span class=\\"th-inner\\">Updated <span class=\\"sort-arrow\\">\\u2193</span></span></th>\\n              <th class=\\"sortable num-col\\" data-sort=\\"api_requests\\"><span class=\\"th-inner\\"><span class=\\"sort-arrow\\">\\u2193</span> Requests</span></th>\\n              <th class=\\"sortable num-col\\" data-sort=\\"input_tokens\\"><span class=\\"th-inner\\"><span class=\\"sort-arrow\\">\\u2193</span> Input</span></th>\\n              <th class=\\"sortable num-col\\" data-sort=\\"output_tokens\\"><span class=\\"th-inner\\"><span class=\\"sort-arrow\\">\\u2193</span> Output</span></th>\\n              <th class=\\"sortable num-col\\" data-sort=\\"thinking_tokens\\"><span class=\\"th-inner\\"><span class=\\"sort-arrow\\">\\u2193</span> Thinking</span></th>\\n              <th class=\\"sortable num-col\\" data-sort=\\"total_tokens\\"><span class=\\"th-inner\\"><span class=\\"sort-arrow\\">\\u2193</span> Total Tokens</span></th>\\n              <th class=\\"sortable num-col\\" data-sort=\\"cost_usd\\"><span class=\\"th-inner\\"><span class=\\"sort-arrow\\">\\u2193</span> Est. Cost</span></th>\\n              <th class=\\"sortable num-col\\" data-sort=\\"electricity_wh\\"><span class=\\"th-inner\\"><span class=\\"sort-arrow\\">\\u2193</span> Energy</span></th>\\n            </tr>\\n          </thead>\\n          <tbody id=\\"convTableBody\\">\\n            <tr><td colspan=\\"9\\" class=\\"empty-state\\">Loading conversations\\u2026</td></tr>\\n          </tbody>\\n        </table>\\n      </div>\\n\\n      <div class=\\"table-footer\\">\\n        <span id=\\"convFooterSummary\\">Showing 0 conversations</span>\\n        <div style=\\"display:flex;align-items:center;gap:8px;\\">\\n          <button type=\\"button\\" id=\\"convShowMoreBtn\\" class=\\"btn-ghost\\" style=\\"display:none;height:24px;font-size:11px;\\">Show more</button>\\n        </div>\\n      </div>\\n    </section>\\n  </main>\\n\\n  <!-- Slide-Over Conversation Detail Drawer -->\\n  <div id=\\"drawerBackdrop\\" class=\\"drawer-backdrop\\" aria-hidden=\\"true\\"></div>\\n  <aside id=\\"drawerPanel\\" class=\\"drawer-panel\\" role=\\"dialog\\" aria-modal=\\"true\\" aria-labelledby=\\"drawerConvTitle\\" aria-hidden=\\"true\\">\\n    <div class=\\"drawer-header\\">\\n      <div style=\\"min-width:0;\\">\\n        <div style=\\"font-size:11px;color:var(--text-subtle);margin-bottom:3px;\\" class=\\"mono\\">SESSION INSPECTOR</div>\\n        <h3 id=\\"drawerConvTitle\\" class=\\"drawer-title\\">Conversation Details</h3>\\n      </div>\\n      <div class=\\"drawer-header-actions\\">\\n        <button type=\\"button\\" id=\\"drawerPrevBtn\\" class=\\"icon-btn\\" title=\\"Previous conversation (\\u2191)\\" aria-label=\\"Previous conversation\\">\\n          <svg width=\\"13\\" height=\\"13\\" viewBox=\\"0 0 16 16\\" fill=\\"none\\" stroke=\\"currentColor\\" stroke-width=\\"1.5\\" stroke-linecap=\\"round\\" stroke-linejoin=\\"round\\"><path d=\\"M4 10L8 6L12 10\\"/></svg>\\n        </button>\\n        <button type=\\"button\\" id=\\"drawerNextBtn\\" class=\\"icon-btn\\" title=\\"Next conversation (\\u2193)\\" aria-label=\\"Next conversation\\">\\n          <svg width=\\"13\\" height=\\"13\\" viewBox=\\"0 0 16 16\\" fill=\\"none\\" stroke=\\"currentColor\\" stroke-width=\\"1.5\\" stroke-linecap=\\"round\\" stroke-linejoin=\\"round\\"><path d=\\"M4 6L8 10L12 6\\"/></svg>\\n        </button>\\n        <button type=\\"button\\" id=\\"drawerCloseBtn\\" class=\\"icon-btn\\" title=\\"Close inspector (Esc)\\" aria-label=\\"Close inspector\\">\\n          <svg width=\\"14\\" height=\\"14\\" viewBox=\\"0 0 16 16\\" fill=\\"none\\" stroke=\\"currentColor\\" stroke-width=\\"1.5\\" stroke-linecap=\\"round\\" stroke-linejoin=\\"round\\"><path d=\\"M4 4L12 12M12 4L4 12\\"/></svg>\\n        </button>\\n      </div>\\n    </div>\\n    <div id=\\"drawerBody\\" class=\\"drawer-content\\"></div>\\n  </aside>\\n\\n  <!-- Shared Floating Tooltip -->\\n  <div id=\\"floatingTooltip\\" class=\\"floating-tooltip\\" role=\\"tooltip\\"></div>\\n\\n  <!-- Toast -->\\n  <div id=\\"toast\\" class=\\"toast\\" role=\\"status\\" aria-live=\\"polite\\">\\n    <span class=\\"pulse-dot\\" style=\\"background:var(--emerald-live);\\"></span>\\n    <span id=\\"toastText\\">Updated</span>\\n  </div>\\n\\n  <script>\\n    (function () {\\n      \'use strict\';\\n\\n      // --- State ---\\n      const urlParams = new URLSearchParams(window.location.search);\\n      const state = {\\n        days: urlParams.get(\'days\') || \'30\',\\n        pricing: urlParams.get(\'pricing\') || \'\',\\n        billingMode: urlParams.get(\'billing_mode\') || \'context\',\\n        chartTab: \'tokens\',\\n        hourlyMode: \'tokens\',\\n        autoRefresh: false,\\n        autoRefreshTimer: null,\\n        isRescanning: false,\\n        isFetching: false,\\n        demoBannerDismissed: false,\\n        searchQuery: \'\',\\n        sortKey: \'updated_at\',\\n        sortDir: \'desc\',\\n        visibleLimit: 50,\\n        selectedConvId: null,\\n        data: null,\\n        normalizedProfiles: [],\\n        chartInstance: null\\n      };\\n\\n      // --- Formatting & Escape Utilities ---\\n      const intFmt = new Intl.NumberFormat(\'en-US\', { maximumFractionDigits: 0 });\\n      const dec1Fmt = new Intl.NumberFormat(\'en-US\', { minimumFractionDigits: 1, maximumFractionDigits: 1 });\\n      const dec2Fmt = new Intl.NumberFormat(\'en-US\', { minimumFractionDigits: 2, maximumFractionDigits: 2 });\\n\\n      function escapeHtml(val) {\\n        if (val === null || val === undefined) return \'\';\\n        return String(val)\\n          .replace(/&/g, \'&amp;\')\\n          .replace(/</g, \'&lt;\')\\n          .replace(/>/g, \'&gt;\')\\n          .replace(/\\"/g, \'&quot;\')\\n          .replace(/\'/g, \'&#39;\');\\n      }\\n\\n      function num(v, fallback = 0) {\\n        const n = Number(v);\\n        return Number.isFinite(n) ? n : fallback;\\n      }\\n\\n      function fmtInt(v) {\\n        return intFmt.format(Math.round(num(v)));\\n      }\\n\\n      function fmtCompact(v) {\\n        const n = num(v);\\n        const abs = Math.abs(n);\\n        if (abs >= 1e9) return (n / 1e9).toFixed(2) + \'B\';\\n        if (abs >= 1e6) return (n / 1e6).toFixed(2) + \'M\';\\n        if (abs >= 1e4) return (n / 1e3).toFixed(1) + \'K\';\\n        return intFmt.format(Math.round(n));\\n      }\\n\\n      function fmtUsd(v) {\\n        const n = num(v);\\n        if (n === 0) return \'$0.00\';\\n        if (n > 0 && n < 0.01) return \'$\' + n.toFixed(4);\\n        if (n >= 1000) return \'$\' + intFmt.format(Math.round(n));\\n        return \'$\' + dec2Fmt.format(n);\\n      }\\n\\n      function fmtEnergy(whVal, kwhVal) {\\n        const wh = num(whVal);\\n        const kwh = kwhVal !== undefined && kwhVal !== null ? num(kwhVal) : wh / 1000;\\n        if (wh >= 1000) {\\n          return { primary: dec2Fmt.format(kwh) + \' kWh\', secondary: fmtInt(wh) + \' Wh\' };\\n        }\\n        if (wh > 0 && wh < 0.1) {\\n          return { primary: wh.toFixed(3) + \' Wh\', secondary: (kwh * 1000).toFixed(3) + \' Wh\' };\\n        }\\n        return { primary: dec1Fmt.format(wh) + \' Wh\', secondary: kwh.toFixed(3) + \' kWh\' };\\n      }\\n\\n      function fmtEnergyShort(whVal) {\\n        const wh = num(whVal);\\n        if (wh >= 1000) return (wh / 1000).toFixed(2) + \' kWh\';\\n        if (wh === 0) return \'0.0 Wh\';\\n        if (wh < 0.1) return wh.toFixed(2) + \' Wh\';\\n        return dec1Fmt.format(wh) + \' Wh\';\\n      }\\n\\n      function fmtCarbon(gramsVal) {\\n        const g = num(gramsVal);\\n        if (g >= 1000) {\\n          return { primary: dec2Fmt.format(g / 1000) + \' kg CO\\u2082e\', secondary: fmtInt(g) + \' g CO\\u2082e\' };\\n        }\\n        if (g > 0 && g < 0.1) {\\n          return { primary: g.toFixed(3) + \' g CO\\u2082e\', secondary: \'\' };\\n        }\\n        return { primary: dec1Fmt.format(g) + \' g CO\\u2082e\', secondary: (g / 1000).toFixed(3) + \' kg\' };\\n      }\\n\\n      function parseTimestamp(raw) {\\n        if (!raw && raw !== 0) return null;\\n        if (typeof raw === \'number\') {\\n          const ms = raw < 1e12 ? raw * 1000 : raw;\\n          const d = new Date(ms);\\n          return Number.isNaN(d.getTime()) ? null : d;\\n        }\\n        const s = String(raw).trim();\\n        if (!s) return null;\\n        if (/^\\\\d+(\\\\.\\\\d+)?$/.test(s)) {\\n          const v = parseFloat(s);\\n          const d = new Date(v < 1e12 ? v * 1000 : v);\\n          return Number.isNaN(d.getTime()) ? null : d;\\n        }\\n        const isoCandidate = s.includes(\'T\') ? s : s.replace(\' \', \'T\');\\n        let d = new Date(isoCandidate);\\n        if (Number.isNaN(d.getTime())) d = new Date(s);\\n        return Number.isNaN(d.getTime()) ? null : d;\\n      }\\n\\n      function fmtTimestamp(raw) {\\n        const d = parseTimestamp(raw);\\n        if (!d) return raw ? String(raw) : \'\\u2014\';\\n        const pad = (n) => String(n).padStart(2, \'0\');\\n        return d.getFullYear() + \'-\' + pad(d.getMonth() + 1) + \'-\' + pad(d.getDate()) + \' \' + pad(d.getHours()) + \':\' + pad(d.getMinutes());\\n      }\\n\\n      function fmtDatePretty(dateStr) {\\n        if (!dateStr) return \'\\u2014\';\\n        const parts = String(dateStr).split(\'-\');\\n        if (parts.length === 3) {\\n          const d = new Date(Number(parts[0]), Number(parts[1]) - 1, Number(parts[2]));\\n          if (!Number.isNaN(d.getTime())) {\\n            return d.toLocaleDateString(\'en-US\', { weekday: \'short\', month: \'short\', day: \'numeric\', year: \'numeric\' });\\n          }\\n        }\\n        return String(dateStr);\\n      }\\n\\n      function fmtShortDate(dateStr) {\\n        if (!dateStr) return \'\';\\n        const parts = String(dateStr).split(\'-\');\\n        if (parts.length === 3) {\\n          const months = [\'Jan\', \'Feb\', \'Mar\', \'Apr\', \'May\', \'Jun\', \'Jul\', \'Aug\', \'Sep\', \'Oct\', \'Nov\', \'Dec\'];\\n          const m = months[Number(parts[1]) - 1] || parts[1];\\n          return m + \' \' + Number(parts[2]);\\n        }\\n        return String(dateStr);\\n      }\\n\\n      // --- Pricing Profile Normalization ---\\n      function normalizePricingProfiles(rawProfiles) {\\n        if (!rawProfiles) return [];\\n        const result = [];\\n        const extractRates = (obj) => {\\n          if (!obj || typeof obj !== \'object\') return { inRate: null, outRate: null, thinkRate: null };\\n          const inRate = obj.input_per_1m ?? obj.input_cost_per_1m ?? obj.input_per_m ?? obj.input_cost_per_million ?? obj.input_usd_per_1m ?? obj.input_rate ?? obj.input ?? null;\\n          const outRate = obj.output_per_1m ?? obj.output_cost_per_1m ?? obj.output_per_m ?? obj.output_cost_per_million ?? obj.output_usd_per_1m ?? obj.output_rate ?? obj.output ?? null;\\n          const thinkRate = obj.thinking_per_1m ?? obj.thinking_cost_per_1m ?? obj.thinking_per_m ?? obj.thinking_rate ?? obj.thinking ?? outRate;\\n          return {\\n            inRate: inRate !== null ? num(inRate, null) : null,\\n            outRate: outRate !== null ? num(outRate, null) : null,\\n            thinkRate: thinkRate !== null ? num(thinkRate, null) : null\\n          };\\n        };\\n\\n        if (Array.isArray(rawProfiles)) {\\n          rawProfiles.forEach((item) => {\\n            if (typeof item === \'string\') {\\n              result.push({ id: item, label: item, inRate: null, outRate: null, thinkRate: null });\\n            } else if (item && typeof item === \'object\') {\\n              const id = String(item.id ?? item.key ?? item.slug ?? item.profile ?? item.name ?? \'\');\\n              if (!id) return;\\n              const baseName = String(item.label ?? item.name ?? item.title ?? id);\\n              const rates = extractRates(item);\\n              const rateSuffix = (rates.inRate !== null && rates.outRate !== null && !baseName.includes(\'$\'))\\n                ? ` ($${rates.inRate}/$${rates.outRate})`\\n                : \'\';\\n              result.push({ id, label: baseName + rateSuffix, shortName: baseName, ...rates });\\n            }\\n          });\\n        } else if (typeof rawProfiles === \'object\') {\\n          Object.entries(rawProfiles).forEach(([key, val]) => {\\n            if (typeof val === \'string\') {\\n              result.push({ id: key, label: val, shortName: val, inRate: null, outRate: null, thinkRate: null });\\n            } else if (val && typeof val === \'object\') {\\n              const baseName = String(val.label ?? val.name ?? val.title ?? key);\\n              const rates = extractRates(val);\\n              const rateSuffix = (rates.inRate !== null && rates.outRate !== null && !baseName.includes(\'$\'))\\n                ? ` ($${rates.inRate}/$${rates.outRate})`\\n                : \'\';\\n              result.push({ id: key, label: baseName + rateSuffix, shortName: baseName, ...rates });\\n            }\\n          });\\n        }\\n        return result;\\n      }\\n\\n      function isTurnBilling(mode) {\\n        const m = String(mode || \'\').toLowerCase();\\n        return m === \'turn\' || m === \'unique\' || m === \'turn_tokens\' || m === \'unique_input\';\\n      }\\n\\n      function resolveConvInputTokens(conv) {\\n        if (conv.input_tokens !== undefined && conv.input_tokens !== null) return num(conv.input_tokens);\\n        return isTurnBilling(state.billingMode)\\n          ? num(conv.unique_input_tokens)\\n          : num(conv.context_input_tokens);\\n      }\\n\\n      // --- Tooltip Controller ---\\n      const tooltipEl = document.getElementById(\'floatingTooltip\');\\n      function showTooltip(html, clientX, clientY) {\\n        tooltipEl.innerHTML = html;\\n        tooltipEl.classList.add(\'is-visible\');\\n        positionTooltip(clientX, clientY);\\n      }\\n\\n      function positionTooltip(clientX, clientY) {\\n        const pad = 12;\\n        const rect = tooltipEl.getBoundingClientRect();\\n        let x = clientX + 14;\\n        let y = clientY - rect.height - 10;\\n        if (x + rect.width + pad > window.innerWidth) {\\n          x = Math.max(pad, clientX - rect.width - 14);\\n        }\\n        if (y < pad) {\\n          y = clientY + 18;\\n        }\\n        tooltipEl.style.left = x + \'px\';\\n        tooltipEl.style.top = y + \'px\';\\n      }\\n\\n      function hideTooltip() {\\n        tooltipEl.classList.remove(\'is-visible\');\\n      }\\n\\n      // --- Toast Notification ---\\n      let toastTimer = null;\\n      function showToast(msg) {\\n        const toast = document.getElementById(\'toast\');\\n        document.getElementById(\'toastText\').textContent = msg;\\n        toast.classList.add(\'is-visible\');\\n        if (toastTimer) clearTimeout(toastTimer);\\n        toastTimer = setTimeout(() => toast.classList.remove(\'is-visible\'), 2600);\\n      }\\n\\n      // --- Export Link Updater ---\\n      function buildQueryParams() {\\n        const p = new URLSearchParams();\\n        p.set(\'days\', state.days);\\n        if (state.pricing) p.set(\'pricing\', state.pricing);\\n        if (state.billingMode) p.set(\'billing_mode\', state.billingMode);\\n        return p.toString();\\n      }\\n\\n      function syncExportLink() {\\n        const link = document.getElementById(\'exportCsvLink\');\\n        link.href = \'/api/export.csv?\' + buildQueryParams();\\n      }\\n\\n      // --- Synthetic Fallback Dataset (for file:// offline preview or explicit demo button) ---\\n      function generateOfflineFallbackData() {\\n        const today = new Date();\\n        const profiles = {\\n          \'claude-sonnet-4-6\': { name: \'Claude Sonnet 4.6\', input_per_1m: 3.0, output_per_1m: 15.0, thinking_per_1m: 15.0 },\\n          \'claude-opus-4-6\': { name: \'Claude Opus 4.6\', input_per_1m: 15.0, output_per_1m: 75.0, thinking_per_1m: 75.0 },\\n          \'gemini-2-5-pro\': { name: \'Gemini 2.5 Pro\', input_per_1m: 1.25, output_per_1m: 10.0, thinking_per_1m: 10.0 },\\n          \'gpt-5\': { name: \'GPT-5\', input_per_1m: 2.5, output_per_1m: 10.0, thinking_per_1m: 10.0 }\\n        };\\n        const activeProfKey = state.pricing && profiles[state.pricing] ? state.pricing : \'claude-sonnet-4-6\';\\n        const prof = profiles[activeProfKey];\\n        const turnMode = isTurnBilling(state.billingMode);\\n\\n        const heatmap = [];\\n        const allDaily = [];\\n        for (let i = 364; i >= 0; i--) {\\n          const d = new Date(today.getFullYear(), today.getMonth(), today.getDate() - i);\\n          const iso = d.toISOString().slice(0, 10);\\n          const seed = (i * 73 + 19) % 97;\\n          const active = seed > 28;\\n          const reqs = active ? Math.max(4, Math.round((seed - 25) * 2.2)) : 0;\\n          const ctxIn = reqs * (16500 + (seed % 9) * 1800);\\n          const uniqIn = Math.round(ctxIn * 0.19);\\n          const effIn = turnMode ? uniqIn : ctxIn;\\n          const outTok = reqs * (1350 + (seed % 5) * 240);\\n          const thinkTok = reqs * (920 + (seed % 7) * 310);\\n          const tot = effIn + outTok + thinkTok;\\n          const cost = (effIn * prof.input_per_1m + (outTok + thinkTok) * prof.output_per_1m) / 1e6;\\n          const wh = (tot / 1e6) * 42.5;\\n          const co2 = (wh / 1000) * 385;\\n          let lvl = 0;\\n          if (tot > 0) lvl = tot > 2600000 ? 4 : tot > 1600000 ? 3 : tot > 800000 ? 2 : 1;\\n          heatmap.push({ date: iso, total_tokens: tot, api_requests: reqs, cost_usd: cost, level: lvl });\\n          allDaily.push({\\n            date: iso,\\n            input_tokens: effIn,\\n            unique_input_tokens: uniqIn,\\n            context_input_tokens: ctxIn,\\n            output_tokens: outTok,\\n            thinking_tokens: thinkTok,\\n            total_tokens: tot,\\n            api_requests: reqs,\\n            tool_calls: Math.round(reqs * 1.45),\\n            conversations: active ? Math.max(1, Math.round(reqs / 14)) : 0,\\n            cost_usd: cost,\\n            electricity_wh: wh,\\n            co2_grams: co2\\n          });\\n        }\\n\\n        const dayCount = state.days === \'all\' ? 365 : Math.min(365, Math.max(1, parseInt(state.days, 10) || 30));\\n        const daily = allDaily.slice(-dayCount);\\n\\n        let sumEffIn = 0, sumUniqIn = 0, sumCtxIn = 0, sumOut = 0, sumThink = 0, sumReqs = 0, sumTools = 0, sumConvs = 0, sumCost = 0, sumWh = 0;\\n        let peakDay = { date: daily[0]?.date || \'\', tokens: 0 };\\n        daily.forEach((r) => {\\n          sumEffIn += r.input_tokens;\\n          sumUniqIn += r.unique_input_tokens;\\n          sumCtxIn += r.context_input_tokens;\\n          sumOut += r.output_tokens;\\n          sumThink += r.thinking_tokens;\\n          sumReqs += r.api_requests;\\n          sumTools += r.tool_calls;\\n          sumConvs += r.conversations;\\n          sumCost += r.cost_usd;\\n          sumWh += r.electricity_wh;\\n          if (r.total_tokens > peakDay.tokens) peakDay = { date: r.date, tokens: r.total_tokens };\\n        });\\n        const sumTotal = sumEffIn + sumOut + sumThink;\\n        const kwh = sumWh / 1000;\\n        const co2 = kwh * 385;\\n        const activeDays365 = heatmap.filter((h) => h.total_tokens > 0).length;\\n\\n        const hourlyWeights = [2, 1, 0, 0, 1, 2, 4, 9, 18, 28, 36, 33, 24, 38, 44, 41, 35, 29, 21, 17, 15, 12, 7, 4];\\n        const weightSum = hourlyWeights.reduce((a, b) => a + b, 0);\\n        const hourly = hourlyWeights.map((w, hour) => ({\\n          hour,\\n          total_tokens: Math.round((sumTotal * w) / weightSum),\\n          api_requests: Math.round((sumReqs * w) / weightSum)\\n        }));\\n\\n        const toolSpec = [\\n          [\'read_file\', 0.312],\\n          [\'edit_file\', 0.214],\\n          [\'run_command\', 0.186],\\n          [\'search_files\', 0.142],\\n          [\'write_file\', 0.078],\\n          [\'invoke_subagent\', 0.041],\\n          [\'search_web\', 0.027]\\n        ];\\n        const tools = toolSpec.map(([name, ratio]) => ({\\n          name,\\n          count: Math.round(sumTools * ratio),\\n          share_pct: +(ratio * 100).toFixed(1)\\n        }));\\n\\n        const sampleTitles = [\\n          \'Implement SQLite WAL indexer and delta scanner for AGY logs\',\\n          \'Refactor Linear dark-mode token palette and tabular metrics\',\\n          \'Debug streaming SSE parser chunk boundary edge case\',\\n          \'Add OpenRouter cumulative context vs turn token billing toggle\',\\n          \'Optimize 365-day activity matrix query latency under 8ms\',\\n          \'Build zero-dependency CSV streaming exporter endpoint\',\\n          \'Audit subagent spawn tree attribution and thinking tokens\',\\n          \'Fix timezone bucketing in hourly spark histogram aggregation\',\\n          \'Benchmark token electricity and 385g/kWh CO2e footprint model\',\\n          \'Verify offline SVG fallback chart renderer on headless Linux\'\\n        ];\\n        const conversations = sampleTitles.map((title, idx) => {\\n          const reqs = 14 + ((idx * 17) % 48);\\n          const cIn = reqs * (21000 + idx * 2400);\\n          const uIn = Math.round(cIn * 0.18);\\n          const eIn = turnMode ? uIn : cIn;\\n          const out = reqs * (1520 + idx * 140);\\n          const thk = reqs * (1100 + (idx % 4) * 580);\\n          const tot = eIn + out + thk;\\n          const cCost = (eIn * prof.input_per_1m + (out + thk) * prof.output_per_1m) / 1e6;\\n          const cWh = (tot / 1e6) * 42.5;\\n          const upd = new Date(today.getTime() - idx * 7.5 * 3600 * 1000);\\n          const st = new Date(upd.getTime() - (22 + idx * 9) * 60 * 1000);\\n          const hex = ((idx + 1) * 0x19a7f3b).toString(16).padStart(8, \'0\');\\n          return {\\n            id: `${hex}-4c2b-49e1-9a1${idx}-7f8e2b4d90${idx}${idx}`,\\n            title,\\n            started_at: st.toISOString(),\\n            updated_at: upd.toISOString(),\\n            api_requests: reqs,\\n            tool_calls: Math.round(reqs * 1.5),\\n            input_tokens: eIn,\\n            unique_input_tokens: uIn,\\n            context_input_tokens: cIn,\\n            output_tokens: out,\\n            thinking_tokens: thk,\\n            total_tokens: tot,\\n            cost_usd: cCost,\\n            electricity_wh: cWh\\n          };\\n        });\\n\\n        return {\\n          meta: {\\n            agy_dir: \'~/.gemini/antigravity\',\\n            db_path: \'~/.agy-usage-tracker/telemetry.sqlite3\',\\n            is_demo: true,\\n            demo_reason: \'Local preview mode \\u2014 backend /api/stats unreachable, displaying synthetic telemetry dataset.\',\\n            last_scanned_at: today.toISOString(),\\n            days_filter: state.days,\\n            pricing_profile: activeProfKey,\\n            billing_mode: state.billingMode,\\n            pricing_profiles: profiles\\n          },\\n          totals: {\\n            total_tokens: sumTotal,\\n            effective_input_tokens: sumEffIn,\\n            unique_input_tokens: sumUniqIn,\\n            context_input_tokens: sumCtxIn,\\n            output_tokens: sumOut,\\n            thinking_tokens: sumThink,\\n            api_requests: sumReqs,\\n            conversations: sumConvs,\\n            subagent_spawns: Math.max(2, Math.round(sumConvs * 0.34)),\\n            tool_calls: sumTools,\\n            cost_usd: sumCost,\\n            electricity_wh: sumWh,\\n            electricity_kwh: kwh,\\n            co2_grams: co2,\\n            phone_charges_equiv: +(sumWh / 12.0).toFixed(1),\\n            led_bulb_hours_equiv: +(sumWh / 10.0).toFixed(1),\\n            avg_tokens_per_request: sumReqs > 0 ? Math.round(sumTotal / sumReqs) : 0,\\n            thinking_share_pct: sumTotal > 0 ? +((sumThink / sumTotal) * 100).toFixed(1) : 0,\\n            active_days: activeDays365,\\n            peak_day: peakDay\\n          },\\n          daily,\\n          heatmap,\\n          hourly,\\n          tools,\\n          conversations\\n        };\\n      }\\n\\n      // --- API Fetching ---\\n      async function fetchStats(options = {}) {\\n        if (state.isFetching && !options.force) return;\\n        state.isFetching = true;\\n        syncExportLink();\\n\\n        if (window.location.protocol === \'file:\') {\\n          state.data = generateOfflineFallbackData();\\n          state.isFetching = false;\\n          renderAll();\\n          return;\\n        }\\n\\n        try {\\n          const res = await fetch(\'/api/stats?\' + buildQueryParams(), {\\n            headers: { \'Accept\': \'application/json\' },\\n            cache: \'no-store\'\\n          });\\n          if (!res.ok) {\\n            throw new Error(`HTTP ${res.status} ${res.statusText}`);\\n          }\\n          const payload = await res.json();\\n          document.getElementById(\'errorBanner\').classList.remove(\'is-visible\');\\n          state.data = payload;\\n          syncControlsFromMeta(payload.meta || {});\\n          renderAll();\\n        } catch (err) {\\n          const errBanner = document.getElementById(\'errorBanner\');\\n          document.getElementById(\'errorBannerText\').textContent =\\n            `Could not fetch /api/stats (${err.message || \'network error\'}).`;\\n          errBanner.classList.add(\'is-visible\');\\n          if (!state.data) {\\n            state.data = generateOfflineFallbackData();\\n            renderAll();\\n          }\\n        } finally {\\n          state.isFetching = false;\\n        }\\n      }\\n\\n      async function triggerRescan() {\\n        if (state.isRescanning) return;\\n        state.isRescanning = true;\\n        const btn = document.getElementById(\'rescanBtn\');\\n        btn.disabled = true;\\n        btn.classList.add(\'is-spinning\');\\n\\n        try {\\n          if (window.location.protocol === \'file:\') {\\n            await new Promise((r) => setTimeout(r, 420));\\n            state.data = generateOfflineFallbackData();\\n            renderAll();\\n            showToast(\'Demo telemetry rescanned\');\\n            return;\\n          }\\n          const res = await fetch(\'/api/rescan\', { method: \'POST\', headers: { \'Accept\': \'application/json\' } });\\n          if (!res.ok) throw new Error(`HTTP ${res.status}`);\\n          let body = null;\\n          try { body = await res.json(); } catch (_) { body = null; }\\n          if (body && body.totals && body.meta) {\\n            state.data = body;\\n            syncControlsFromMeta(body.meta);\\n            renderAll();\\n          } else {\\n            await fetchStats({ force: true });\\n          }\\n          showToast(\'Rescan complete \\u2014 telemetry updated\');\\n        } catch (err) {\\n          showToast(\'Rescan failed: \' + (err.message || \'error\'));\\n        } finally {\\n          state.isRescanning = false;\\n          btn.disabled = false;\\n          btn.classList.remove(\'is-spinning\');\\n        }\\n      }\\n\\n      function syncControlsFromMeta(meta) {\\n        if (meta.days_filter !== undefined && meta.days_filter !== null) {\\n          const rawDays = String(meta.days_filter).toLowerCase();\\n          state.days = (rawDays === \'0\' || rawDays === \'all\' || rawDays === \'max\') ? \'all\' : rawDays;\\n        }\\n        if (meta.billing_mode) {\\n          state.billingMode = String(meta.billing_mode);\\n        }\\n        if (meta.pricing_profile) {\\n          state.pricing = String(meta.pricing_profile);\\n        }\\n        syncExportLink();\\n      }\\n\\n      // --- Master Render ---\\n      function renderAll() {\\n        if (!state.data) return;\\n        const data = state.data;\\n        const meta = data.meta || {};\\n        const totals = data.totals || {};\\n\\n        renderHeaderAndMeta(meta);\\n        renderKpis(meta, totals);\\n        renderDailyChart(data.daily || [], meta);\\n        renderHeatmap(data.heatmap || [], totals);\\n        renderHourly(data.hourly || []);\\n        renderTools(data.tools || [], totals);\\n        renderConversationsTable(data.conversations || []);\\n\\n        if (state.selectedConvId) {\\n          const found = (data.conversations || []).find((c) => String(c.id) === String(state.selectedConvId));\\n          if (found) renderDrawerContent(found);\\n        }\\n      }\\n\\n      // --- Section 1: Header & Meta ---\\n      function renderHeaderAndMeta(meta) {\\n        const pathEl = document.getElementById(\'agyPathSubtitle\');\\n        const agyDir = meta.agy_dir || \'~/.gemini/antigravity\';\\n        pathEl.textContent = agyDir;\\n        pathEl.dataset.fullInfo = `AGY Dir: ${agyDir}\\nSQLite DB: ${meta.db_path || \'\\u2014\'}\\nLast Scan: ${fmtTimestamp(meta.last_scanned_at)}`;\\n\\n        document.getElementById(\'metaDbPath\').textContent = meta.db_path || \'\\u2014\';\\n        document.getElementById(\'metaLastScanned\').textContent = fmtTimestamp(meta.last_scanned_at);\\n        document.getElementById(\'metaBillingBadge\').textContent = isTurnBilling(state.billingMode) ? \'turn (new tokens)\' : \'context (cumulative)\';\\n\\n        const hasChartJs = typeof window.Chart !== \'undefined\' && !window.__chartJsLoadFailed;\\n        document.getElementById(\'metaChartEngine\').textContent = hasChartJs ? \'Chart.js 4\' : \'SVG Fallback\';\\n\\n        // Demo badge + banner\\n        const demoBadge = document.getElementById(\'demoBadge\');\\n        const demoBanner = document.getElementById(\'demoBanner\');\\n        const demoReason = meta.demo_reason || \'Synthetic demonstration telemetry is active.\';\\n        if (meta.is_demo) {\\n          demoBadge.classList.add(\'is-visible\');\\n          demoBadge.dataset.reason = demoReason;\\n          document.getElementById(\'demoBannerText\').textContent = demoReason;\\n          if (!state.demoBannerDismissed) {\\n            demoBanner.classList.add(\'is-visible\');\\n          }\\n        } else {\\n          demoBadge.classList.remove(\'is-visible\');\\n          demoBanner.classList.remove(\'is-visible\');\\n        }\\n\\n        // Active date button\\n        document.querySelectorAll(\'#dateFilterGroup .seg-btn\').forEach((btn) => {\\n          btn.classList.toggle(\'is-active\', btn.dataset.days === String(state.days));\\n        });\\n\\n        // Active billing button\\n        const turnActive = isTurnBilling(state.billingMode);\\n        document.querySelectorAll(\'#billingModeGroup .seg-btn\').forEach((btn) => {\\n          const btnIsTurn = btn.dataset.billing === \'turn\';\\n          btn.classList.toggle(\'is-active\', btnIsTurn === turnActive);\\n        });\\n\\n        // Pricing dropdown\\n        state.normalizedProfiles = normalizePricingProfiles(meta.pricing_profiles);\\n        const select = document.getElementById(\'pricingSelect\');\\n        if (state.normalizedProfiles.length > 0) {\\n          select.innerHTML = state.normalizedProfiles\\n            .map((p) => `<option value=\\"${escapeHtml(p.id)}\\">${escapeHtml(p.label)}</option>`)\\n            .join(\'\');\\n          if (state.pricing && state.normalizedProfiles.some((p) => p.id === state.pricing)) {\\n            select.value = state.pricing;\\n          } else {\\n            state.pricing = state.normalizedProfiles[0].id;\\n            select.value = state.pricing;\\n          }\\n        } else {\\n          const fallbackId = meta.pricing_profile || state.pricing || \'default\';\\n          select.innerHTML = `<option value=\\"${escapeHtml(fallbackId)}\\">${escapeHtml(fallbackId)}</option>`;\\n        }\\n      }\\n\\n      // --- Section 2: KPI Cards ---\\n      function renderKpis(meta, totals) {\\n        const effInput = num(totals.effective_input_tokens ?? totals.input_tokens ?? (isTurnBilling(state.billingMode) ? totals.unique_input_tokens : totals.context_input_tokens));\\n        const outTok = num(totals.output_tokens);\\n        const thinkTok = num(totals.thinking_tokens);\\n        const totalTok = num(totals.total_tokens, effInput + outTok + thinkTok);\\n\\n        // Card 1: Total Tokens\\n        document.getElementById(\'kpiTokenModeTag\').textContent = isTurnBilling(state.billingMode) ? \'Turn Mode\' : \'Context Mode\';\\n        document.getElementById(\'kpiTotalTokens\').textContent = fmtCompact(totalTok);\\n        document.getElementById(\'kpiTotalTokensExact\').textContent = totalTok >= 10000 ? fmtInt(totalTok) : \'\';\\n        document.getElementById(\'kpiInputTokens\').textContent = fmtCompact(effInput);\\n        document.getElementById(\'kpiOutputTokens\').textContent = fmtCompact(outTok);\\n        document.getElementById(\'kpiThinkingTokens\').textContent = fmtCompact(thinkTok);\\n\\n        const denom = Math.max(1, effInput + outTok + thinkTok);\\n        document.getElementById(\'barInputShare\').style.width = ((effInput / denom) * 100).toFixed(2) + \'%\';\\n        document.getElementById(\'barOutputShare\').style.width = ((outTok / denom) * 100).toFixed(2) + \'%\';\\n        document.getElementById(\'barThinkingShare\').style.width = ((thinkTok / denom) * 100).toFixed(2) + \'%\';\\n\\n        // Card 2: API Requests\\n        const reqs = num(totals.api_requests);\\n        const avgTok = num(totals.avg_tokens_per_request, reqs > 0 ? totalTok / reqs : 0);\\n        document.getElementById(\'kpiApiRequests\').textContent = fmtInt(reqs);\\n        document.getElementById(\'kpiAvgTokPerReq\').textContent = fmtCompact(avgTok) + \' tok/req\';\\n        document.getElementById(\'kpiConversations\').textContent = fmtInt(totals.conversations);\\n        document.getElementById(\'kpiSubagents\').textContent = fmtInt(totals.subagent_spawns);\\n        document.getElementById(\'kpiToolCallsTag\').textContent = fmtInt(totals.tool_calls) + \' tools\';\\n\\n        // Card 3: Est. API Value / Cost\\n        const totalCost = num(totals.cost_usd);\\n        const activeProfile = state.normalizedProfiles.find((p) => p.id === state.pricing);\\n        document.getElementById(\'kpiPricingTag\').textContent = activeProfile ? activeProfile.shortName : (meta.pricing_profile || \'Standard\');\\n        document.getElementById(\'kpiCostUsd\').textContent = fmtUsd(totalCost);\\n        document.getElementById(\'kpiCostPerReq\').textContent = reqs > 0 ? fmtUsd(totalCost / reqs) + \'/req\' : \'\';\\n\\n        let inCost = totals.input_cost_usd !== undefined ? num(totals.input_cost_usd) : null;\\n        let outThinkCost = (totals.output_cost_usd !== undefined || totals.thinking_cost_usd !== undefined)\\n          ? num(totals.output_cost_usd) + num(totals.thinking_cost_usd)\\n          : null;\\n\\n        if (inCost === null || outThinkCost === null) {\\n          if (activeProfile && activeProfile.inRate !== null && activeProfile.outRate !== null) {\\n            const tRate = activeProfile.thinkRate !== null ? activeProfile.thinkRate : activeProfile.outRate;\\n            const rawIn = (effInput * activeProfile.inRate) / 1e6;\\n            const rawOut = (outTok * activeProfile.outRate + thinkTok * tRate) / 1e6;\\n            const rawSum = rawIn + rawOut;\\n            if (rawSum > 0) {\\n              inCost = totalCost * (rawIn / rawSum);\\n              outThinkCost = totalCost - inCost;\\n            }\\n          }\\n        }\\n        if (inCost === null || outThinkCost === null) {\\n          const weightedIn = effInput;\\n          const weightedOut = (outTok + thinkTok) * 5;\\n          const wSum = Math.max(1, weightedIn + weightedOut);\\n          inCost = totalCost * (weightedIn / wSum);\\n          outThinkCost = totalCost - inCost;\\n        }\\n\\n        const costSum = Math.max(1e-9, inCost + outThinkCost);\\n        document.getElementById(\'barCostInput\').style.width = (totalCost > 0 ? (inCost / costSum) * 100 : 50).toFixed(1) + \'%\';\\n        document.getElementById(\'barCostOutput\').style.width = (totalCost > 0 ? (outThinkCost / costSum) * 100 : 50).toFixed(1) + \'%\';\\n        document.getElementById(\'kpiInputCost\').textContent = fmtUsd(inCost);\\n        document.getElementById(\'kpiOutThinkCost\').textContent = fmtUsd(outThinkCost);\\n\\n        // Card 4: Electricity Used\\n        const energy = fmtEnergy(totals.electricity_wh, totals.electricity_kwh);\\n        document.getElementById(\'kpiElectricity\').textContent = energy.primary;\\n        document.getElementById(\'kpiEnergyAlt\').textContent = energy.secondary;\\n        document.getElementById(\'kpiPhoneCharges\').textContent = `~${dec1Fmt.format(num(totals.phone_charges_equiv))} phone charges`;\\n        document.getElementById(\'kpiLedHours\').textContent = `~${dec1Fmt.format(num(totals.led_bulb_hours_equiv))} hrs 10W LED`;\\n\\n        // Card 5: Carbon Footprint\\n        const co2g = num(totals.co2_grams);\\n        const carbon = fmtCarbon(co2g);\\n        document.getElementById(\'kpiCarbon\').textContent = carbon.primary;\\n        document.getElementById(\'kpiCarbonSub\').textContent = carbon.secondary;\\n        document.getElementById(\'kpiCo2PerReq\').textContent = reqs > 0 ? (co2g / reqs).toFixed(2) + \' g\' : \'0.00 g\';\\n        document.getElementById(\'kpiCo2PerM\').textContent = totalTok > 0 ? ((co2g / totalTok) * 1e6).toFixed(1) + \' g\' : \'0.0 g\';\\n\\n        // Card 6: Reasoning & Peak Velocity\\n        const thinkPct = num(totals.thinking_share_pct, totalTok > 0 ? (thinkTok / totalTok) * 100 : 0);\\n        document.getElementById(\'kpiThinkingShare\').textContent = dec1Fmt.format(thinkPct) + \'%\';\\n        document.getElementById(\'barReasoningGauge\').style.width = Math.min(100, Math.max(0, thinkPct)).toFixed(1) + \'%\';\\n        document.getElementById(\'kpiActiveDaysTag\').textContent = `${fmtInt(totals.active_days)} active days`;\\n\\n        if (totals.peak_day && totals.peak_day.date && num(totals.peak_day.tokens) > 0) {\\n          document.getElementById(\'kpiPeakDate\').textContent = totals.peak_day.date;\\n          document.getElementById(\'kpiPeakTokens\').textContent = fmtCompact(totals.peak_day.tokens) + \' tok\';\\n        } else {\\n          document.getElementById(\'kpiPeakDate\').textContent = \'None\';\\n          document.getElementById(\'kpiPeakTokens\').textContent = \'0 tok\';\\n        }\\n      }\\n\\n      // --- Section 3: Main Daily Chart (Chart.js 4 + Offline SVG Fallback) ---\\n      function renderDailyChart(dailyRaw) {\\n        const daily = Array.isArray(dailyRaw) ? [...dailyRaw].sort((a, b) => String(a.date).localeCompare(String(b.date))) : [];\\n        const canvas = document.getElementById(\'dailyChartCanvas\');\\n        const svgWrap = document.getElementById(\'dailySvgFallback\');\\n        const emptyEl = document.getElementById(\'dailyChartEmpty\');\\n\\n        const rangeLabel = state.days === \'all\' ? \'ALL TIME\' : `${state.days}D`;\\n        document.getElementById(\'dailyRangeBadge\').textContent = rangeLabel;\\n\\n        const subtitles = {\\n          tokens: \'Stacked daily token volume \\u2014 Input, Output, and Reasoning tokens\',\\n          cost: \'Estimated daily API equivalent spend in USD\',\\n          requests: \'Daily API request volume vs executed tool calls\'\\n        };\\n        document.getElementById(\'dailySubtitle\').textContent = subtitles[state.chartTab] || subtitles.tokens;\\n\\n        // Legend\\n        const legendEl = document.getElementById(\'dailyChartLegend\');\\n        if (state.chartTab === \'tokens\') {\\n          legendEl.innerHTML = `\\n            <span class=\\"chart-legend-item\\"><span class=\\"legend-dot\\" style=\\"background:#5e6ad2;\\"></span>Input</span>\\n            <span class=\\"chart-legend-item\\"><span class=\\"legend-dot\\" style=\\"background:#2dd4bf;\\"></span>Output</span>\\n            <span class=\\"chart-legend-item\\"><span class=\\"legend-dot\\" style=\\"background:#a855f7;\\"></span>Thinking</span>\\n          `;\\n        } else if (state.chartTab === \'cost\') {\\n          legendEl.innerHTML = `<span class=\\"chart-legend-item\\"><span class=\\"legend-dot\\" style=\\"background:#7170ff;\\"></span>Est. Value ($)</span>`;\\n        } else {\\n          legendEl.innerHTML = `\\n            <span class=\\"chart-legend-item\\"><span class=\\"legend-dot\\" style=\\"background:#5e6ad2;\\"></span>API Requests</span>\\n            <span class=\\"chart-legend-item\\"><span class=\\"legend-dot\\" style=\\"background:#2dd4bf;\\"></span>Tool Calls</span>\\n          `;\\n        }\\n\\n        if (daily.length === 0) {\\n          if (state.chartInstance) {\\n            state.chartInstance.destroy();\\n            state.chartInstance = null;\\n          }\\n          canvas.style.display = \'none\';\\n          svgWrap.classList.remove(\'is-visible\');\\n          emptyEl.classList.add(\'is-visible\');\\n          return;\\n        }\\n        emptyEl.classList.remove(\'is-visible\');\\n\\n        const hasChartJs = typeof window.Chart !== \'undefined\' && !window.__chartJsLoadFailed;\\n        if (hasChartJs) {\\n          svgWrap.classList.remove(\'is-visible\');\\n          canvas.style.display = \'block\';\\n          renderDailyWithChartJs(canvas, daily);\\n        } else {\\n          canvas.style.display = \'none\';\\n          svgWrap.classList.add(\'is-visible\');\\n          renderDailyWithSvgFallback(svgWrap, daily);\\n        }\\n      }\\n\\n      function getDailyInput(row) {\\n        if (row.input_tokens !== undefined && row.input_tokens !== null) return num(row.input_tokens);\\n        return isTurnBilling(state.billingMode) ? num(row.unique_input_tokens) : num(row.context_input_tokens);\\n      }\\n\\n      function renderDailyWithChartJs(canvas, daily) {\\n        if (state.chartInstance) {\\n          state.chartInstance.destroy();\\n          state.chartInstance = null;\\n        }\\n        const ctx = canvas.getContext(\'2d\');\\n        const labels = daily.map((d) => fmtShortDate(d.date));\\n        const monoFont = { family: \\"\'JetBrains Mono\', ui-monospace, monospace\\", size: 11 };\\n\\n        let datasets = [];\\n        let scales = {};\\n\\n        if (state.chartTab === \'tokens\') {\\n          datasets = [\\n            {\\n              type: \'bar\',\\n              label: \'Input Tokens\',\\n              data: daily.map((d) => getDailyInput(d)),\\n              backgroundColor: \'#5e6ad2\',\\n              hoverBackgroundColor: \'#7170ff\',\\n              borderRadius: 2,\\n              stack: \'tok\'\\n            },\\n            {\\n              type: \'bar\',\\n              label: \'Output Tokens\',\\n              data: daily.map((d) => num(d.output_tokens)),\\n              backgroundColor: \'#2dd4bf\',\\n              hoverBackgroundColor: \'#5eead4\',\\n              borderRadius: 2,\\n              stack: \'tok\'\\n            },\\n            {\\n              type: \'bar\',\\n              label: \'Thinking Tokens\',\\n              data: daily.map((d) => num(d.thinking_tokens)),\\n              backgroundColor: \'#a855f7\',\\n              hoverBackgroundColor: \'#c084fc\',\\n              borderRadius: { topLeft: 3, topRight: 3, bottomLeft: 0, bottomRight: 0 },\\n              stack: \'tok\'\\n            }\\n          ];\\n          scales = {\\n            x: {\\n              stacked: true,\\n              grid: { display: false },\\n              border: { color: \'rgba(255,255,255,0.08)\' },\\n              ticks: { color: \'#62666d\', font: monoFont, maxTicksLimit: 12, maxRotation: 0 }\\n            },\\n            y: {\\n              stacked: true,\\n              beginAtZero: true,\\n              border: { display: false },\\n              grid: { color: \'rgba(255,255,255,0.05)\' },\\n              ticks: { color: \'#62666d\', font: monoFont, callback: (v) => fmtCompact(v) }\\n            }\\n          };\\n        } else if (state.chartTab === \'cost\') {\\n          const grad = ctx.createLinearGradient(0, 0, 0, 270);\\n          grad.addColorStop(0, \'rgba(113, 112, 255, 0.24)\');\\n          grad.addColorStop(1, \'rgba(113, 112, 255, 0.0)\');\\n          datasets = [\\n            {\\n              type: \'line\',\\n              label: \'Est. Value ($)\',\\n              data: daily.map((d) => num(d.cost_usd)),\\n              borderColor: \'#7170ff\',\\n              backgroundColor: grad,\\n              borderWidth: 2,\\n              fill: true,\\n              tension: 0.22,\\n              pointRadius: daily.length <= 35 ? 2.5 : 0,\\n              pointHoverRadius: 4.5,\\n              pointBackgroundColor: \'#7170ff\',\\n              pointBorderColor: \'#08090a\',\\n              pointBorderWidth: 1.5\\n            }\\n          ];\\n          scales = {\\n            x: {\\n              grid: { display: false },\\n              border: { color: \'rgba(255,255,255,0.08)\' },\\n              ticks: { color: \'#62666d\', font: monoFont, maxTicksLimit: 12, maxRotation: 0 }\\n            },\\n            y: {\\n              beginAtZero: true,\\n              border: { display: false },\\n              grid: { color: \'rgba(255,255,255,0.05)\' },\\n              ticks: { color: \'#62666d\', font: monoFont, callback: (v) => fmtUsd(v) }\\n            }\\n          };\\n        } else {\\n          datasets = [\\n            {\\n              type: \'bar\',\\n              label: \'API Requests\',\\n              data: daily.map((d) => num(d.api_requests)),\\n              backgroundColor: \'rgba(94, 106, 210, 0.72)\',\\n              hoverBackgroundColor: \'#7170ff\',\\n              borderRadius: 3,\\n              order: 2\\n            },\\n            {\\n              type: \'line\',\\n              label: \'Tool Calls\',\\n              data: daily.map((d) => num(d.tool_calls)),\\n              borderColor: \'#2dd4bf\',\\n              backgroundColor: \'#2dd4bf\',\\n              borderWidth: 2,\\n              tension: 0.2,\\n              pointRadius: daily.length <= 45 ? 2.5 : 0,\\n              pointHoverRadius: 4,\\n              order: 1\\n            }\\n          ];\\n          scales = {\\n            x: {\\n              grid: { display: false },\\n              border: { color: \'rgba(255,255,255,0.08)\' },\\n              ticks: { color: \'#62666d\', font: monoFont, maxTicksLimit: 12, maxRotation: 0 }\\n            },\\n            y: {\\n              beginAtZero: true,\\n              border: { display: false },\\n              grid: { color: \'rgba(255,255,255,0.05)\' },\\n              ticks: { color: \'#62666d\', font: monoFont, callback: (v) => fmtCompact(v) }\\n            }\\n          };\\n        }\\n\\n        state.chartInstance = new window.Chart(ctx, {\\n          data: { labels, datasets },\\n          options: {\\n            responsive: true,\\n            maintainAspectRatio: false,\\n            animation: { duration: 220 },\\n            interaction: { mode: \'index\', intersect: false },\\n            plugins: {\\n              legend: { display: false },\\n              tooltip: {\\n                backgroundColor: \'#141517\',\\n                titleColor: \'#f7f8f8\',\\n                bodyColor: \'#d0d6e0\',\\n                borderColor: \'rgba(255,255,255,0.12)\',\\n                borderWidth: 1,\\n                padding: 10,\\n                cornerRadius: 7,\\n                titleFont: { family: \\"\'Inter\', sans-serif\\", size: 12, weight: \'600\' },\\n                bodyFont: { family: \\"\'JetBrains Mono\', monospace\\", size: 11.5 },\\n                callbacks: {\\n                  title: (items) => {\\n                    const idx = items[0]?.dataIndex ?? 0;\\n                    return fmtDatePretty(daily[idx]?.date);\\n                  },\\n                  label: (item) => {\\n                    const val = item.raw;\\n                    if (state.chartTab === \'cost\') return ` ${item.dataset.label}: ${fmtUsd(val)}`;\\n                    return ` ${item.dataset.label}: ${fmtInt(val)}`;\\n                  },\\n                  afterBody: (items) => {\\n                    const idx = items[0]?.dataIndex ?? 0;\\n                    const row = daily[idx];\\n                    if (!row) return [];\\n                    if (state.chartTab === \'tokens\') {\\n                      return [` Total: ${fmtInt(row.total_tokens)}  (${fmtUsd(row.cost_usd)})`];\\n                    }\\n                    return [];\\n                  }\\n                }\\n              }\\n            },\\n            scales\\n          }\\n        });\\n      }\\n\\n      // --- Custom Responsive SVG Chart Renderer (Offline Fallback) ---\\n      function renderDailyWithSvgFallback(container, daily) {\\n        const rect = container.getBoundingClientRect();\\n        const width = Math.max(360, Math.floor(rect.width || 920));\\n        const height = Math.max(220, Math.floor(rect.height || 278));\\n        const padL = 56, padR = 16, padT = 14, padB = 28;\\n        const plotW = width - padL - padR;\\n        const plotH = height - padT - padB;\\n        const n = daily.length;\\n\\n        let maxVal = 0;\\n        daily.forEach((d) => {\\n          if (state.chartTab === \'tokens\') {\\n            maxVal = Math.max(maxVal, getDailyInput(d) + num(d.output_tokens) + num(d.thinking_tokens));\\n          } else if (state.chartTab === \'cost\') {\\n            maxVal = Math.max(maxVal, num(d.cost_usd));\\n          } else {\\n            maxVal = Math.max(maxVal, num(d.api_requests), num(d.tool_calls));\\n          }\\n        });\\n        if (maxVal <= 0) maxVal = 1;\\n        const yMax = maxVal * 1.08;\\n\\n        const yTicks = 4;\\n        let gridSvg = \'\';\\n        for (let i = 0; i <= yTicks; i++) {\\n          const ratio = i / yTicks;\\n          const y = (padT + plotH - ratio * plotH).toFixed(1);\\n          const tickVal = yMax * ratio;\\n          const label = state.chartTab === \'cost\' ? fmtUsd(tickVal) : fmtCompact(tickVal);\\n          gridSvg += `<line x1=\\"${padL}\\" y1=\\"${y}\\" x2=\\"${width - padR}\\" y2=\\"${y}\\" stroke=\\"rgba(255,255,255,0.05)\\" stroke-width=\\"1\\" />`;\\n          gridSvg += `<text x=\\"${padL - 8}\\" y=\\"${Number(y) + 3.5}\\" text-anchor=\\"end\\" fill=\\"#62666d\\" font-family=\\"JetBrains Mono, monospace\\" font-size=\\"10.5\\">${escapeHtml(label)}</text>`;\\n        }\\n\\n        const stepX = plotW / Math.max(1, n);\\n        const labelStride = Math.max(1, Math.ceil(n / 10));\\n        let xLabelsSvg = \'\';\\n        daily.forEach((d, idx) => {\\n          if (idx % labelStride === 0 || idx === n - 1) {\\n            const cx = (padL + idx * stepX + stepX / 2).toFixed(1);\\n            xLabelsSvg += `<text x=\\"${cx}\\" y=\\"${height - 7}\\" text-anchor=\\"middle\\" fill=\\"#62666d\\" font-family=\\"JetBrains Mono, monospace\\" font-size=\\"10\\">${escapeHtml(fmtShortDate(d.date))}</text>`;\\n          }\\n        });\\n\\n        let seriesSvg = \'\';\\n        if (state.chartTab === \'tokens\') {\\n          const barW = Math.max(2, Math.min(28, stepX * 0.72));\\n          daily.forEach((d, idx) => {\\n            const x = padL + idx * stepX + (stepX - barW) / 2;\\n            const vIn = getDailyInput(d);\\n            const vOut = num(d.output_tokens);\\n            const vThk = num(d.thinking_tokens);\\n            const hIn = (vIn / yMax) * plotH;\\n            const hOut = (vOut / yMax) * plotH;\\n            const hThk = (vThk / yMax) * plotH;\\n            const yIn = padT + plotH - hIn;\\n            const yOut = yIn - hOut;\\n            const yThk = yOut - hThk;\\n            if (hIn > 0) seriesSvg += `<rect x=\\"${x.toFixed(1)}\\" y=\\"${yIn.toFixed(1)}\\" width=\\"${barW.toFixed(1)}\\" height=\\"${hIn.toFixed(1)}\\" fill=\\"#5e6ad2\\" />`;\\n            if (hOut > 0) seriesSvg += `<rect x=\\"${x.toFixed(1)}\\" y=\\"${yOut.toFixed(1)}\\" width=\\"${barW.toFixed(1)}\\" height=\\"${hOut.toFixed(1)}\\" fill=\\"#2dd4bf\\" />`;\\n            if (hThk > 0) seriesSvg += `<rect x=\\"${x.toFixed(1)}\\" y=\\"${yThk.toFixed(1)}\\" width=\\"${barW.toFixed(1)}\\" height=\\"${hThk.toFixed(1)}\\" fill=\\"#a855f7\\" rx=\\"1.5\\" />`;\\n          });\\n        } else if (state.chartTab === \'cost\') {\\n          const pts = daily.map((d, idx) => {\\n            const cx = padL + (n === 1 ? plotW / 2 : (idx / (n - 1)) * plotW);\\n            const cy = padT + plotH - (num(d.cost_usd) / yMax) * plotH;\\n            return [cx, cy];\\n          });\\n          const linePath = pts.map((p, i) => (i === 0 ? \'M\' : \'L\') + p[0].toFixed(1) + \',\' + p[1].toFixed(1)).join(\' \');\\n          const areaPath = `${linePath} L${pts[pts.length - 1][0].toFixed(1)},${(padT + plotH).toFixed(1)} L${pts[0][0].toFixed(1)},${(padT + plotH).toFixed(1)} Z`;\\n          seriesSvg += `\\n            <defs>\\n              <linearGradient id=\\"svgCostGrad\\" x1=\\"0\\" y1=\\"0\\" x2=\\"0\\" y2=\\"1\\">\\n                <stop offset=\\"0%\\" stop-color=\\"#7170ff\\" stop-opacity=\\"0.26\\" />\\n                <stop offset=\\"100%\\" stop-color=\\"#7170ff\\" stop-opacity=\\"0.0\\" />\\n              </linearGradient>\\n            </defs>\\n            <path d=\\"${areaPath}\\" fill=\\"url(#svgCostGrad)\\" />\\n            <path d=\\"${linePath}\\" fill=\\"none\\" stroke=\\"#7170ff\\" stroke-width=\\"2\\" />\\n          `;\\n          if (n <= 40) {\\n            pts.forEach((p) => {\\n              seriesSvg += `<circle cx=\\"${p[0].toFixed(1)}\\" cy=\\"${p[1].toFixed(1)}\\" r=\\"2.6\\" fill=\\"#7170ff\\" stroke=\\"#08090a\\" stroke-width=\\"1.2\\" />`;\\n            });\\n          }\\n        } else {\\n          const barW = Math.max(2, Math.min(26, stepX * 0.68));\\n          const toolPts = [];\\n          daily.forEach((d, idx) => {\\n            const cx = padL + idx * stepX + stepX / 2;\\n            const x = cx - barW / 2;\\n            const hReq = (num(d.api_requests) / yMax) * plotH;\\n            const yReq = padT + plotH - hReq;\\n            if (hReq > 0) {\\n              seriesSvg += `<rect x=\\"${x.toFixed(1)}\\" y=\\"${yReq.toFixed(1)}\\" width=\\"${barW.toFixed(1)}\\" height=\\"${hReq.toFixed(1)}\\" fill=\\"rgba(94,106,210,0.72)\\" rx=\\"2\\" />`;\\n            }\\n            const cyTool = padT + plotH - (num(d.tool_calls) / yMax) * plotH;\\n            toolPts.push([cx, cyTool]);\\n          });\\n          if (toolPts.length > 1) {\\n            const tPath = toolPts.map((p, i) => (i === 0 ? \'M\' : \'L\') + p[0].toFixed(1) + \',\' + p[1].toFixed(1)).join(\' \');\\n            seriesSvg += `<path d=\\"${tPath}\\" fill=\\"none\\" stroke=\\"#2dd4bf\\" stroke-width=\\"2\\" />`;\\n          }\\n          if (n <= 45) {\\n            toolPts.forEach((p) => {\\n              seriesSvg += `<circle cx=\\"${p[0].toFixed(1)}\\" cy=\\"${p[1].toFixed(1)}\\" r=\\"2.5\\" fill=\\"#2dd4bf\\" />`;\\n            });\\n          }\\n        }\\n\\n        let hitSvg = \'\';\\n        daily.forEach((_, idx) => {\\n          const x = padL + idx * stepX;\\n          hitSvg += `<rect class=\\"svg-day-hit\\" data-idx=\\"${idx}\\" x=\\"${x.toFixed(1)}\\" y=\\"${padT}\\" width=\\"${stepX.toFixed(1)}\\" height=\\"${plotH}\\" fill=\\"transparent\\" style=\\"cursor:crosshair;\\" />`;\\n        });\\n\\n        container.innerHTML = `\\n          <svg width=\\"100%\\" height=\\"100%\\" viewBox=\\"0 0 ${width} ${height}\\" preserveAspectRatio=\\"none\\">\\n            ${gridSvg}\\n            <line x1=\\"${padL}\\" y1=\\"${padT + plotH}\\" x2=\\"${width - padR}\\" y2=\\"${padT + plotH}\\" stroke=\\"rgba(255,255,255,0.08)\\" stroke-width=\\"1\\" />\\n            ${seriesSvg}\\n            ${xLabelsSvg}\\n            ${hitSvg}\\n          </svg>\\n        `;\\n\\n        container.querySelectorAll(\'.svg-day-hit\').forEach((el) => {\\n          el.addEventListener(\'mouseenter\', (ev) => {\\n            const d = daily[Number(el.dataset.idx)];\\n            if (!d) return;\\n            let rows = \'\';\\n            if (state.chartTab === \'tokens\') {\\n              rows = `\\n                <div class=\\"tt-row\\"><span class=\\"tt-label\\"><span class=\\"legend-dot\\" style=\\"background:#5e6ad2;\\"></span>Input</span><span class=\\"tt-val\\">${fmtInt(getDailyInput(d))}</span></div>\\n                <div class=\\"tt-row\\"><span class=\\"tt-label\\"><span class=\\"legend-dot\\" style=\\"background:#2dd4bf;\\"></span>Output</span><span class=\\"tt-val\\">${fmtInt(d.output_tokens)}</span></div>\\n                <div class=\\"tt-row\\"><span class=\\"tt-label\\"><span class=\\"legend-dot\\" style=\\"background:#a855f7;\\"></span>Thinking</span><span class=\\"tt-val\\">${fmtInt(d.thinking_tokens)}</span></div>\\n                <div class=\\"tt-row\\" style=\\"border-top:1px solid rgba(255,255,255,0.08);margin-top:3px;padding-top:3px;\\"><span class=\\"tt-label\\">Total</span><span class=\\"tt-val\\">${fmtInt(d.total_tokens)}</span></div>\\n              `;\\n            } else if (state.chartTab === \'cost\') {\\n              rows = `<div class=\\"tt-row\\"><span class=\\"tt-label\\">Est. Value</span><span class=\\"tt-val\\">${fmtUsd(d.cost_usd)}</span></div>`;\\n            } else {\\n              rows = `\\n                <div class=\\"tt-row\\"><span class=\\"tt-label\\">API Requests</span><span class=\\"tt-val\\">${fmtInt(d.api_requests)}</span></div>\\n                <div class=\\"tt-row\\"><span class=\\"tt-label\\">Tool Calls</span><span class=\\"tt-val\\">${fmtInt(d.tool_calls)}</span></div>\\n              `;\\n            }\\n            showTooltip(`<div class=\\"tt-title\\">${escapeHtml(fmtDatePretty(d.date))}</div>${rows}`, ev.clientX, ev.clientY);\\n          });\\n          el.addEventListener(\'mousemove\', (ev) => positionTooltip(ev.clientX, ev.clientY));\\n          el.addEventListener(\'mouseleave\', hideTooltip);\\n        });\\n      }\\n\\n      // --- Section 4: 365-Day Activity Heatmap ---\\n      function renderHeatmap(heatmapRaw, totals) {\\n        let items = Array.isArray(heatmapRaw) ? [...heatmapRaw] : [];\\n        if (items.length === 0) {\\n          const now = new Date();\\n          for (let i = 364; i >= 0; i--) {\\n            const d = new Date(now.getFullYear(), now.getMonth(), now.getDate() - i);\\n            items.push({ date: d.toISOString().slice(0, 10), total_tokens: 0, api_requests: 0, cost_usd: 0, level: 0 });\\n          }\\n        }\\n        items.sort((a, b) => String(a.date).localeCompare(String(b.date)));\\n        if (items.length > 371) items = items.slice(-371);\\n\\n        const firstParts = String(items[0].date).split(\'-\');\\n        const firstDate = firstParts.length === 3\\n          ? new Date(Number(firstParts[0]), Number(firstParts[1]) - 1, Number(firstParts[2]))\\n          : new Date();\\n        const startDow = Number.isNaN(firstDate.getTime()) ? 0 : firstDate.getDay(); // 0=Sun..6=Sat\\n\\n        const padded = [];\\n        for (let p = 0; p < startDow; p++) padded.push(null);\\n        items.forEach((it) => padded.push(it));\\n        while (padded.length < 53 * 7) padded.push(null);\\n        const totalWeeks = Math.ceil(padded.length / 7);\\n\\n        const weeksEl = document.getElementById(\'heatmapWeeks\');\\n        weeksEl.style.gridTemplateColumns = `repeat(${totalWeeks}, minmax(11px, 1fr))`;\\n\\n        const monthNames = [\'Jan\', \'Feb\', \'Mar\', \'Apr\', \'May\', \'Jun\', \'Jul\', \'Aug\', \'Sep\', \'Oct\', \'Nov\', \'Dec\'];\\n        const monthPositions = [];\\n        let lastMonth = -1;\\n\\n        let weeksHtml = \'\';\\n        for (let w = 0; w < totalWeeks; w++) {\\n          let colHtml = \'<div class=\\"heatmap-week-col\\">\';\\n          for (let dow = 0; dow < 7; dow++) {\\n            const cell = padded[w * 7 + dow];\\n            if (!cell) {\\n              colHtml += \'<div class=\\"hm-cell hm-pad\\"></div>\';\\n            } else {\\n              const parts = String(cell.date).split(\'-\');\\n              if (parts.length === 3) {\\n                const mIdx = Number(parts[1]) - 1;\\n                const dayNum = Number(parts[2]);\\n                if (mIdx !== lastMonth && (dayNum <= 7 || w === 0)) {\\n                  if (monthPositions.length === 0 || w - monthPositions[monthPositions.length - 1].week >= 3) {\\n                    monthPositions.push({ week: w, label: monthNames[mIdx] || \'\' });\\n                  }\\n                  lastMonth = mIdx;\\n                }\\n              }\\n              const lvl = Math.min(4, Math.max(0, Math.round(num(cell.level))));\\n              colHtml += `<div class=\\"hm-cell hm-lvl-${lvl}\\" data-date=\\"${escapeHtml(cell.date)}\\" data-tokens=\\"${num(cell.total_tokens)}\\" data-reqs=\\"${num(cell.api_requests)}\\" data-cost=\\"${num(cell.cost_usd)}\\"></div>`;\\n            }\\n          }\\n          colHtml += \'</div>\';\\n          weeksHtml += colHtml;\\n        }\\n        weeksEl.innerHTML = weeksHtml;\\n\\n        const monthsEl = document.getElementById(\'heatmapMonths\');\\n        monthsEl.innerHTML = monthPositions\\n          .map((mp) => `<span class=\\"heatmap-month-label\\" style=\\"left:${((mp.week / totalWeeks) * 100).toFixed(2)}%;\\">${escapeHtml(mp.label)}</span>`)\\n          .join(\'\');\\n\\n        // Stats & Streak\\n        let yearTokens = 0;\\n        let activeCount = 0;\\n        let curStreak = 0;\\n        let maxStreak = 0;\\n        items.forEach((it) => {\\n          const t = num(it.total_tokens);\\n          yearTokens += t;\\n          if (t > 0 || num(it.api_requests) > 0) {\\n            activeCount++;\\n            curStreak++;\\n            if (curStreak > maxStreak) maxStreak = curStreak;\\n          } else {\\n            curStreak = 0;\\n          }\\n        });\\n\\n        const shownActive = totals.active_days !== undefined ? num(totals.active_days) : activeCount;\\n        document.getElementById(\'hmActiveDays\').textContent = fmtInt(shownActive);\\n        document.getElementById(\'hmYearTokens\').textContent = fmtCompact(yearTokens);\\n        document.getElementById(\'hmLongestStreak\').textContent = `${maxStreak}d`;\\n        if (items.length > 0) {\\n          document.getElementById(\'heatmapYearSpan\').textContent = `${items[0].date} \\u2192 ${items[items.length - 1].date}`;\\n        }\\n      }\\n\\n      // --- Section 5A: Hour-of-Day Activity ---\\n      function renderHourly(hourlyRaw) {\\n        const byHour = new Array(24).fill(null).map((_, h) => ({ hour: h, total_tokens: 0, api_requests: 0 }));\\n        if (Array.isArray(hourlyRaw)) {\\n          hourlyRaw.forEach((item) => {\\n            const h = Number(item.hour);\\n            if (h >= 0 && h < 24) {\\n              byHour[h] = {\\n                hour: h,\\n                total_tokens: num(item.total_tokens),\\n                api_requests: num(item.api_requests)\\n              };\\n            }\\n          });\\n        }\\n\\n        const metricKey = state.hourlyMode === \'requests\' ? \'api_requests\' : \'total_tokens\';\\n        let maxVal = 0;\\n        let sumVal = 0;\\n        let peakHour = 0;\\n        byHour.forEach((h) => {\\n          const v = h[metricKey];\\n          sumVal += v;\\n          if (v > maxVal) {\\n            maxVal = v;\\n            peakHour = h.hour;\\n          }\\n        });\\n\\n        const pad2 = (n) => String(n).padStart(2, \'0\');\\n        document.getElementById(\'hourlyPeakTag\').textContent =\\n          maxVal > 0 ? `Peak ${pad2(peakHour)}:00` : \'No activity\';\\n        document.getElementById(\'hourlyPeakShare\').textContent =\\n          sumVal > 0 ? `Peak share: ${((maxVal / sumVal) * 100).toFixed(1)}%` : \'0%\';\\n\\n        const barsEl = document.getElementById(\'hourlyBars\');\\n        barsEl.innerHTML = byHour\\n          .map((h) => {\\n            const v = h[metricKey];\\n            const pct = maxVal > 0 ? Math.max(v > 0 ? 4 : 1.5, (v / maxVal) * 100) : 1.5;\\n            const isPeak = maxVal > 0 && v === maxVal;\\n            const cls = v === 0 ? \'hour-bar is-zero\' : isPeak ? \'hour-bar is-peak\' : \'hour-bar\';\\n            return `\\n              <div class=\\"hour-col\\" data-hour=\\"${h.hour}\\" data-tokens=\\"${h.total_tokens}\\" data-reqs=\\"${h.api_requests}\\">\\n                <div class=\\"${cls}\\" style=\\"height:${pct.toFixed(1)}%;\\"></div>\\n              </div>\\n            `;\\n          })\\n          .join(\'\');\\n\\n        const axisEl = document.getElementById(\'hourlyAxis\');\\n        axisEl.innerHTML = byHour\\n          .map((h) => `<span>${h.hour % 3 === 0 ? pad2(h.hour) : \'\'}</span>`)\\n          .join(\'\');\\n      }\\n\\n      // --- Section 5B: Tool Invocations Breakdown ---\\n      function renderTools(toolsRaw, totals) {\\n        const tools = Array.isArray(toolsRaw) ? [...toolsRaw].sort((a, b) => num(b.count) - num(a.count)) : [];\\n        const totalCalls = totals.tool_calls !== undefined\\n          ? num(totals.tool_calls)\\n          : tools.reduce((acc, t) => acc + num(t.count), 0);\\n        document.getElementById(\'toolsTotalBadge\').textContent = `${fmtInt(totalCalls)} calls`;\\n\\n        const body = document.getElementById(\'toolsListBody\');\\n        if (tools.length === 0) {\\n          body.innerHTML = \'<div class=\\"empty-state\\">No tool invocations recorded in selected window</div>\';\\n          return;\\n        }\\n\\n        const maxShare = Math.max(1, ...tools.map((t) => num(t.share_pct)));\\n        body.innerHTML = tools\\n          .map((t, idx) => {\\n            const share = num(t.share_pct, totalCalls > 0 ? (num(t.count) / totalCalls) * 100 : 0);\\n            const barWidth = Math.min(100, Math.max(2, (share / maxShare) * 100));\\n            return `\\n              <div class=\\"tool-row\\">\\n                <div class=\\"tool-row-header\\">\\n                  <div class=\\"tool-name-wrap\\">\\n                    <span class=\\"tool-rank\\">${String(idx + 1).padStart(2, \'0\')}</span>\\n                    <span class=\\"tool-name\\" title=\\"${escapeHtml(t.name)}\\">${escapeHtml(t.name)}</span>\\n                  </div>\\n                  <div class=\\"tool-metrics\\">\\n                    <span class=\\"tool-count\\">${fmtInt(t.count)}</span>\\n                    <span class=\\"tool-share\\">${dec1Fmt.format(share)}%</span>\\n                  </div>\\n                </div>\\n                <div class=\\"tool-track\\">\\n                  <div class=\\"tool-fill\\" style=\\"width:${barWidth.toFixed(1)}%;\\"></div>\\n                </div>\\n              </div>\\n            `;\\n          })\\n          .join(\'\');\\n      }\\n\\n      // --- Section 6: Recent Conversations Table ---\\n      function getFilteredSortedConversations() {\\n        const raw = (state.data && Array.isArray(state.data.conversations)) ? state.data.conversations : [];\\n        const q = state.searchQuery.trim().toLowerCase();\\n        const filtered = q\\n          ? raw.filter((c) =>\\n              String(c.title || \'\').toLowerCase().includes(q) ||\\n              String(c.id || \'\').toLowerCase().includes(q)\\n            )\\n          : [...raw];\\n\\n        const dir = state.sortDir === \'asc\' ? 1 : -1;\\n        const key = state.sortKey;\\n\\n        filtered.sort((a, b) => {\\n          if (key === \'title\') {\\n            return String(a.title || a.id || \'\').localeCompare(String(b.title || b.id || \'\')) * dir;\\n          }\\n          if (key === \'updated_at\') {\\n            const ta = parseTimestamp(a.updated_at)?.getTime() ?? 0;\\n            const tb = parseTimestamp(b.updated_at)?.getTime() ?? 0;\\n            return (ta - tb) * dir;\\n          }\\n          if (key === \'input_tokens\') {\\n            return (resolveConvInputTokens(a) - resolveConvInputTokens(b)) * dir;\\n          }\\n          return (num(a[key]) - num(b[key])) * dir;\\n        });\\n\\n        return { filtered, total: raw.length };\\n      }\\n\\n      function renderConversationsTable() {\\n        const { filtered, total } = getFilteredSortedConversations();\\n        document.getElementById(\'convCountBadge\').textContent = fmtInt(total);\\n\\n        // Update header sort indicators\\n        document.querySelectorAll(\'.conv-table thead th.sortable\').forEach((th) => {\\n          const isSorted = th.dataset.sort === state.sortKey;\\n          th.classList.toggle(\'is-sorted\', isSorted);\\n          const arrow = th.querySelector(\'.sort-arrow\');\\n          if (arrow) arrow.textContent = isSorted && state.sortDir === \'asc\' ? \'\\u2191\' : \'\\u2193\';\\n        });\\n\\n        const tbody = document.getElementById(\'convTableBody\');\\n        const showMoreBtn = document.getElementById(\'convShowMoreBtn\');\\n\\n        if (filtered.length === 0) {\\n          tbody.innerHTML = `<tr><td colspan=\\"9\\" class=\\"empty-state\\">${state.searchQuery ? \'No conversations match your filter.\' : \'No conversations recorded in this window.\'}</td></tr>`;\\n          document.getElementById(\'convFooterSummary\').textContent = `Showing 0 of ${fmtInt(total)} conversations`;\\n          showMoreBtn.style.display = \'none\';\\n          return;\\n        }\\n\\n        const slice = filtered.slice(0, state.visibleLimit);\\n        tbody.innerHTML = slice\\n          .map((c) => {\\n            const idStr = String(c.id || \'\');\\n            const shortId = idStr.length > 14 ? idStr.slice(0, 8) + \'\\u2026\' + idStr.slice(-4) : idStr;\\n            const title = c.title && String(c.title).trim() ? c.title : \'Untitled Session\';\\n            const inTok = resolveConvInputTokens(c);\\n            const selectedCls = String(state.selectedConvId) === idStr ? \' class=\\"is-selected\\"\' : \'\';\\n            return `\\n              <tr data-conv-id=\\"${escapeHtml(idStr)}\\" tabindex=\\"0\\"${selectedCls}>\\n                <td>\\n                  <div class=\\"conv-title-cell\\">\\n                    <span class=\\"conv-title-text\\" title=\\"${escapeHtml(title)}\\">${escapeHtml(title)}</span>\\n                    <span class=\\"conv-id-short\\">${escapeHtml(shortId)}${num(c.tool_calls) > 0 ? \' \\u00b7 \' + fmtInt(c.tool_calls) + \' tools\' : \'\'}</span>\\n                  </div>\\n                </td>\\n                <td class=\\"mono\\" style=\\"font-size:11.5px;color:var(--text-muted);\\">${escapeHtml(fmtTimestamp(c.updated_at))}</td>\\n                <td class=\\"num-col\\">${fmtInt(c.api_requests)}</td>\\n                <td class=\\"num-col tok-input\\">${fmtCompact(inTok)}</td>\\n                <td class=\\"num-col tok-output\\">${fmtCompact(c.output_tokens)}</td>\\n                <td class=\\"num-col tok-thinking\\">${fmtCompact(c.thinking_tokens)}</td>\\n                <td class=\\"num-col tok-total\\">${fmtCompact(c.total_tokens)}</td>\\n                <td class=\\"num-col cost-cell\\">${fmtUsd(c.cost_usd)}</td>\\n                <td class=\\"num-col\\">${escapeHtml(fmtEnergyShort(c.electricity_wh))}</td>\\n              </tr>\\n            `;\\n          })\\n          .join(\'\');\\n\\n        document.getElementById(\'convFooterSummary\').textContent =\\n          `Showing ${fmtInt(slice.length)} of ${fmtInt(filtered.length)} matching (${fmtInt(total)} total)`;\\n\\n        if (filtered.length > state.visibleLimit) {\\n          showMoreBtn.style.display = \'inline-flex\';\\n          showMoreBtn.textContent = `Show more (${fmtInt(filtered.length - state.visibleLimit)} remaining)`;\\n        } else {\\n          showMoreBtn.style.display = \'none\';\\n        }\\n      }\\n\\n      // --- Conversation Detail Slide-over Drawer ---\\n      function openDrawer(convId) {\\n        const list = getFilteredSortedConversations().filtered;\\n        const conv = list.find((c) => String(c.id) === String(convId)) ||\\n          (state.data?.conversations || []).find((c) => String(c.id) === String(convId));\\n        if (!conv) return;\\n\\n        state.selectedConvId = String(conv.id);\\n        renderConversationsTable();\\n        renderDrawerContent(conv);\\n\\n        document.getElementById(\'drawerBackdrop\').classList.add(\'is-open\');\\n        const panel = document.getElementById(\'drawerPanel\');\\n        panel.classList.add(\'is-open\');\\n        panel.setAttribute(\'aria-hidden\', \'false\');\\n      }\\n\\n      function closeDrawer() {\\n        state.selectedConvId = null;\\n        document.getElementById(\'drawerBackdrop\').classList.remove(\'is-open\');\\n        const panel = document.getElementById(\'drawerPanel\');\\n        panel.classList.remove(\'is-open\');\\n        panel.setAttribute(\'aria-hidden\', \'true\');\\n        renderConversationsTable();\\n      }\\n\\n      function stepDrawer(delta) {\\n        if (!state.selectedConvId) return;\\n        const list = getFilteredSortedConversations().filtered;\\n        const idx = list.findIndex((c) => String(c.id) === String(state.selectedConvId));\\n        if (idx === -1) return;\\n        const next = list[idx + delta];\\n        if (next) openDrawer(next.id);\\n      }\\n\\n      function renderDrawerContent(conv) {\\n        const list = getFilteredSortedConversations().filtered;\\n        const idx = list.findIndex((c) => String(c.id) === String(conv.id));\\n        document.getElementById(\'drawerPrevBtn\').disabled = idx <= 0;\\n        document.getElementById(\'drawerNextBtn\').disabled = idx === -1 || idx >= list.length - 1;\\n\\n        const title = conv.title && String(conv.title).trim() ? conv.title : \'Untitled Session\';\\n        document.getElementById(\'drawerConvTitle\').textContent = title;\\n\\n        const inTok = resolveConvInputTokens(conv);\\n        const ctxTok = num(conv.context_input_tokens, inTok);\\n        const uniqTok = num(conv.unique_input_tokens, inTok);\\n        const outTok = num(conv.output_tokens);\\n        const thinkTok = num(conv.thinking_tokens);\\n        const totTok = num(conv.total_tokens, inTok + outTok + thinkTok);\\n        const denom = Math.max(1, inTok + outTok + thinkTok);\\n\\n        const reqs = num(conv.api_requests);\\n        const tools = num(conv.tool_calls);\\n        const wh = num(conv.electricity_wh);\\n        const co2g = conv.co2_grams !== undefined ? num(conv.co2_grams) : (wh / 1000) * 385;\\n\\n        const startDt = parseTimestamp(conv.started_at);\\n        const endDt = parseTimestamp(conv.updated_at);\\n        let durationStr = \'\\u2014\';\\n        if (startDt && endDt && endDt >= startDt) {\\n          const mins = Math.round((endDt - startDt) / 60000);\\n          durationStr = mins < 1 ? \'< 1 min\' : mins < 60 ? `${mins}m` : `${Math.floor(mins / 60)}h ${mins % 60}m`;\\n        }\\n\\n        const body = document.getElementById(\'drawerBody\');\\n        body.innerHTML = `\\n          <div class=\\"drawer-section\\">\\n            <span class=\\"drawer-section-title\\">Conversation ID</span>\\n            <div class=\\"id-box\\">\\n              <span class=\\"id-box-code\\">${escapeHtml(conv.id)}</span>\\n              <button type=\\"button\\" id=\\"copyConvIdBtn\\" class=\\"copy-btn\\" data-copy-id=\\"${escapeHtml(conv.id)}\\">\\n                <span>Copy ID</span>\\n              </button>\\n            </div>\\n          </div>\\n\\n          <div class=\\"drawer-section\\">\\n            <span class=\\"drawer-section-title\\">Session Timeline</span>\\n            <div class=\\"detail-grid\\">\\n              <div class=\\"detail-tile\\">\\n                <span class=\\"detail-tile-label\\">Started At</span>\\n                <span class=\\"detail-tile-val\\" style=\\"font-size:12.5px;\\">${escapeHtml(fmtTimestamp(conv.started_at))}</span>\\n                <span class=\\"detail-tile-sub\\">Duration: ${escapeHtml(durationStr)}</span>\\n              </div>\\n              <div class=\\"detail-tile\\">\\n                <span class=\\"detail-tile-label\\">Last Updated</span>\\n                <span class=\\"detail-tile-val\\" style=\\"font-size:12.5px;\\">${escapeHtml(fmtTimestamp(conv.updated_at))}</span>\\n                <span class=\\"detail-tile-sub\\">${fmtInt(reqs)} turns \\u00b7 ${fmtInt(tools)} tool calls</span>\\n              </div>\\n            </div>\\n          </div>\\n\\n          <div class=\\"drawer-section\\">\\n            <span class=\\"drawer-section-title\\">Token Composition</span>\\n            <div class=\\"seg-bar\\" style=\\"height:8px;\\">\\n              <div class=\\"seg-bar-piece\\" style=\\"width:${((inTok / denom) * 100).toFixed(2)}%;background:var(--indigo-brand);\\"></div>\\n              <div class=\\"seg-bar-piece\\" style=\\"width:${((outTok / denom) * 100).toFixed(2)}%;background:var(--cyan-output);\\"></div>\\n              <div class=\\"seg-bar-piece\\" style=\\"width:${((thinkTok / denom) * 100).toFixed(2)}%;background:var(--violet-thinking);\\"></div>\\n            </div>\\n            <div class=\\"comp-list\\">\\n              <div class=\\"comp-row\\">\\n                <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--indigo-brand);\\"></span>Effective Input (${isTurnBilling(state.billingMode) ? \'Turn\' : \'Context\'})</span>\\n                <span class=\\"mono\\">${fmtInt(inTok)} <span style=\\"color:var(--text-subtle);\\">(${((inTok / denom) * 100).toFixed(1)}%)</span></span>\\n              </div>\\n              <div class=\\"comp-row\\" style=\\"color:var(--text-muted);font-size:11.5px;\\">\\n                <span style=\\"padding-left:11px;\\">\\u21b3 Cumulative Context Input</span>\\n                <span class=\\"mono\\">${fmtInt(ctxTok)}</span>\\n              </div>\\n              <div class=\\"comp-row\\" style=\\"color:var(--text-muted);font-size:11.5px;\\">\\n                <span style=\\"padding-left:11px;\\">\\u21b3 Unique Turn Input</span>\\n                <span class=\\"mono\\">${fmtInt(uniqTok)}</span>\\n              </div>\\n              <div class=\\"comp-row\\">\\n                <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--cyan-output);\\"></span>Output Tokens</span>\\n                <span class=\\"mono\\">${fmtInt(outTok)} <span style=\\"color:var(--text-subtle);\\">(${((outTok / denom) * 100).toFixed(1)}%)</span></span>\\n              </div>\\n              <div class=\\"comp-row\\">\\n                <span class=\\"bd-item\\"><span class=\\"legend-dot\\" style=\\"background:var(--violet-thinking);\\"></span>Thinking / Reasoning</span>\\n                <span class=\\"mono\\">${fmtInt(thinkTok)} <span style=\\"color:var(--text-subtle);\\">(${((thinkTok / denom) * 100).toFixed(1)}%)</span></span>\\n              </div>\\n              <div class=\\"comp-row comp-total\\">\\n                <span>Total Session Tokens</span>\\n                <span class=\\"mono\\">${fmtInt(totTok)}</span>\\n              </div>\\n            </div>\\n          </div>\\n\\n          <div class=\\"drawer-section\\">\\n            <span class=\\"drawer-section-title\\">Cost, Throughput &amp; Environmental Footprint</span>\\n            <div class=\\"detail-grid\\">\\n              <div class=\\"detail-tile\\">\\n                <span class=\\"detail-tile-label\\">Est. API Cost</span>\\n                <span class=\\"detail-tile-val\\">${fmtUsd(conv.cost_usd)}</span>\\n                <span class=\\"detail-tile-sub\\">${reqs > 0 ? fmtUsd(num(conv.cost_usd) / reqs) + \' avg / req\' : \'\\u2014\'}</span>\\n              </div>\\n              <div class=\\"detail-tile\\">\\n                <span class=\\"detail-tile-label\\">API Requests &amp; Tools</span>\\n                <span class=\\"detail-tile-val\\">${fmtInt(reqs)} reqs</span>\\n                <span class=\\"detail-tile-sub\\">${fmtInt(tools)} tool calls \\u00b7 ${reqs > 0 ? fmtCompact(totTok / reqs) : \'0\'} tok/req</span>\\n              </div>\\n              <div class=\\"detail-tile\\">\\n                <span class=\\"detail-tile-label\\">Electricity Used</span>\\n                <span class=\\"detail-tile-val\\" style=\\"color:#6ee7b7;\\">${escapeHtml(fmtEnergyShort(wh))}</span>\\n                <span class=\\"detail-tile-sub\\">${(wh / 1000).toFixed(4)} kWh</span>\\n              </div>\\n              <div class=\\"detail-tile\\">\\n                <span class=\\"detail-tile-label\\">Carbon Footprint</span>\\n                <span class=\\"detail-tile-val\\" style=\\"color:#6ee7b7;\\">${escapeHtml(fmtCarbon(co2g).primary)}</span>\\n                <span class=\\"detail-tile-sub\\">@ 385g CO\\u2082e / kWh</span>\\n              </div>\\n            </div>\\n          </div>\\n        `;\\n\\n        const copyBtn = document.getElementById(\'copyConvIdBtn\');\\n        if (copyBtn) {\\n          copyBtn.addEventListener(\'click\', () => copyTextToClipboard(String(conv.id), copyBtn));\\n        }\\n      }\\n\\n      function copyTextToClipboard(text, btnEl) {\\n        const onDone = () => {\\n          if (!btnEl) return;\\n          btnEl.classList.add(\'is-copied\');\\n          btnEl.innerHTML = \'<span>\\u2713 Copied</span>\';\\n          setTimeout(() => {\\n            btnEl.classList.remove(\'is-copied\');\\n            btnEl.innerHTML = \'<span>Copy ID</span>\';\\n          }, 1600);\\n        };\\n\\n        if (navigator.clipboard && navigator.clipboard.writeText) {\\n          navigator.clipboard.writeText(text).then(onDone).catch(() => fallbackCopy(text, onDone));\\n        } else {\\n          fallbackCopy(text, onDone);\\n        }\\n      }\\n\\n      function fallbackCopy(text, cb) {\\n        const ta = document.createElement(\'textarea\');\\n        ta.value = text;\\n        ta.style.position = \'fixed\';\\n        ta.style.opacity = \'0\';\\n        document.body.appendChild(ta);\\n        ta.select();\\n        try { document.execCommand(\'copy\'); cb(); } catch (_) {}\\n        document.body.removeChild(ta);\\n      }\\n\\n      // --- Event Wiring ---\\n      function bindEvents() {\\n        // Date range buttons\\n        document.getElementById(\'dateFilterGroup\').addEventListener(\'click\', (e) => {\\n          const btn = e.target.closest(\'.seg-btn\');\\n          if (!btn || btn.dataset.days === state.days) return;\\n          state.days = btn.dataset.days;\\n          document.querySelectorAll(\'#dateFilterGroup .seg-btn\').forEach((b) => b.classList.toggle(\'is-active\', b === btn));\\n          fetchStats({ force: true });\\n        });\\n\\n        // Billing mode toggle\\n        document.getElementById(\'billingModeGroup\').addEventListener(\'click\', (e) => {\\n          const btn = e.target.closest(\'.seg-btn\');\\n          if (!btn) return;\\n          const targetMode = btn.dataset.billing;\\n          if ((targetMode === \'turn\') === isTurnBilling(state.billingMode)) return;\\n          state.billingMode = targetMode;\\n          document.querySelectorAll(\'#billingModeGroup .seg-btn\').forEach((b) => b.classList.toggle(\'is-active\', b === btn));\\n          fetchStats({ force: true });\\n        });\\n\\n        // Billing mode help tooltip\\n        const billHelp = document.getElementById(\'billingHelpBtn\');\\n        const showBillingHelp = (ev) => {\\n          const rect = billHelp.getBoundingClientRect();\\n          const cx = ev.clientX || (rect.left + rect.width / 2);\\n          const cy = ev.clientY || rect.bottom;\\n          showTooltip(\\n            `<div class=\\"tt-title\\">Token Billing Modes</div>\\n             <div style=\\"color:var(--text-secondary);line-height:1.45;margin-bottom:6px;\\"><strong style=\\"color:#9aa5ff;\\">Context Tokens:</strong> OpenRouter-style cumulative prompt billing \\u2014 counts the full conversation context window sent on every API turn.</div>\\n             <div style=\\"color:var(--text-secondary);line-height:1.45;\\"><strong style=\\"color:#5eead4;\\">Turn Tokens:</strong> Incremental turn billing \\u2014 counts only newly added message characters/tokens per turn, excluding repeated history.</div>`,\\n            cx,\\n            cy\\n          );\\n        };\\n        billHelp.addEventListener(\'mouseenter\', showBillingHelp);\\n        billHelp.addEventListener(\'focus\', showBillingHelp);\\n        billHelp.addEventListener(\'mouseleave\', hideTooltip);\\n        billHelp.addEventListener(\'blur\', hideTooltip);\\n\\n        // Subtitle hover info\\n        const pathSub = document.getElementById(\'agyPathSubtitle\');\\n        pathSub.addEventListener(\'mouseenter\', (ev) => {\\n          if (!pathSub.dataset.fullInfo) return;\\n          const lines = pathSub.dataset.fullInfo.split(/\\\\r?\\\\n/).map((l) => `<div>${escapeHtml(l)}</div>`).join(\'\');\\n          showTooltip(`<div class=\\"tt-title\\">Telemetry Source</div><div class=\\"mono\\" style=\\"font-size:11px;line-height:1.5;\\">${lines}</div>`, ev.clientX, ev.clientY);\\n        });\\n        pathSub.addEventListener(\'mouseleave\', hideTooltip);\\n\\n        // Demo badge hover tooltip\\n        const demoBadge = document.getElementById(\'demoBadge\');\\n        demoBadge.addEventListener(\'mouseenter\', (ev) => {\\n          showTooltip(`<div class=\\"tt-title\\" style=\\"color:#fbbf24;\\">Demo Preview Active</div><div>${escapeHtml(demoBadge.dataset.reason || \'\')}</div>`, ev.clientX, ev.clientY);\\n        });\\n        demoBadge.addEventListener(\'mouseleave\', hideTooltip);\\n\\n        document.getElementById(\'dismissDemoBanner\').addEventListener(\'click\', () => {\\n          state.demoBannerDismissed = true;\\n          document.getElementById(\'demoBanner\').classList.remove(\'is-visible\');\\n        });\\n\\n        document.getElementById(\'retryApiBtn\').addEventListener(\'click\', () => fetchStats({ force: true }));\\n        document.getElementById(\'loadFallbackDemoBtn\').addEventListener(\'click\', () => {\\n          document.getElementById(\'errorBanner\').classList.remove(\'is-visible\');\\n          state.data = generateOfflineFallbackData();\\n          renderAll();\\n        });\\n\\n        // Pricing dropdown\\n        document.getElementById(\'pricingSelect\').addEventListener(\'change\', (e) => {\\n          state.pricing = e.target.value;\\n          fetchStats({ force: true });\\n        });\\n\\n        // Auto-refresh 10s toggle\\n        const autoBtn = document.getElementById(\'autoRefreshBtn\');\\n        autoBtn.addEventListener(\'click\', () => {\\n          state.autoRefresh = !state.autoRefresh;\\n          autoBtn.classList.toggle(\'is-live\', state.autoRefresh);\\n          if (state.autoRefreshTimer) {\\n            clearInterval(state.autoRefreshTimer);\\n            state.autoRefreshTimer = null;\\n          }\\n          if (state.autoRefresh) {\\n            state.autoRefreshTimer = setInterval(() => {\\n              if (!document.hidden && !state.isRescanning) {\\n                fetchStats();\\n              }\\n            }, 10000);\\n            showToast(\'Live auto-refresh enabled (10s interval)\');\\n          } else {\\n            showToast(\'Live auto-refresh paused\');\\n          }\\n        });\\n\\n        // Rescan button\\n        document.getElementById(\'rescanBtn\').addEventListener(\'click\', triggerRescan);\\n\\n        // Daily chart tabs\\n        document.getElementById(\'chartTabGroup\').addEventListener(\'click\', (e) => {\\n          const btn = e.target.closest(\'.seg-btn\');\\n          if (!btn || btn.dataset.chartTab === state.chartTab) return;\\n          state.chartTab = btn.dataset.chartTab;\\n          document.querySelectorAll(\'#chartTabGroup .seg-btn\').forEach((b) => {\\n            const active = b === btn;\\n            b.classList.toggle(\'is-active\', active);\\n            b.setAttribute(\'aria-selected\', String(active));\\n          });\\n          if (state.data) renderDailyChart(state.data.daily || [], state.data.meta || {});\\n        });\\n\\n        // Heatmap delegated hover tooltip\\n        const hmWeeks = document.getElementById(\'heatmapWeeks\');\\n        hmWeeks.addEventListener(\'mouseover\', (ev) => {\\n          const cell = ev.target.closest(\'.hm-cell:not(.hm-pad)\');\\n          if (!cell) return;\\n          const html = `\\n            <div class=\\"tt-title\\">${escapeHtml(fmtDatePretty(cell.dataset.date))}</div>\\n            <div class=\\"tt-row\\"><span class=\\"tt-label\\">Tokens</span><span class=\\"tt-val\\">${fmtInt(cell.dataset.tokens)}</span></div>\\n            <div class=\\"tt-row\\"><span class=\\"tt-label\\">API Requests</span><span class=\\"tt-val\\">${fmtInt(cell.dataset.reqs)}</span></div>\\n            <div class=\\"tt-row\\"><span class=\\"tt-label\\">Est. Value</span><span class=\\"tt-val\\">${fmtUsd(cell.dataset.cost)}</span></div>\\n          `;\\n          showTooltip(html, ev.clientX, ev.clientY);\\n        });\\n        hmWeeks.addEventListener(\'mousemove\', (ev) => {\\n          if (tooltipEl.classList.contains(\'is-visible\')) positionTooltip(ev.clientX, ev.clientY);\\n        });\\n        hmWeeks.addEventListener(\'mouseleave\', hideTooltip);\\n\\n        // Hourly mode toggle + hover tooltip\\n        document.getElementById(\'hourlyMetricGroup\').addEventListener(\'click\', (e) => {\\n          const btn = e.target.closest(\'.seg-btn\');\\n          if (!btn || btn.dataset.hourlyMode === state.hourlyMode) return;\\n          state.hourlyMode = btn.dataset.hourlyMode;\\n          document.querySelectorAll(\'#hourlyMetricGroup .seg-btn\').forEach((b) => b.classList.toggle(\'is-active\', b === btn));\\n          if (state.data) renderHourly(state.data.hourly || []);\\n        });\\n\\n        const hourlyBars = document.getElementById(\'hourlyBars\');\\n        hourlyBars.addEventListener(\'mouseover\', (ev) => {\\n          const col = ev.target.closest(\'.hour-col\');\\n          if (!col) return;\\n          const h = Number(col.dataset.hour);\\n          const pad2 = (n) => String(n).padStart(2, \'0\');\\n          const html = `\\n            <div class=\\"tt-title\\">${pad2(h)}:00 \\u2013 ${pad2(h)}:59</div>\\n            <div class=\\"tt-row\\"><span class=\\"tt-label\\">Tokens</span><span class=\\"tt-val\\">${fmtInt(col.dataset.tokens)}</span></div>\\n            <div class=\\"tt-row\\"><span class=\\"tt-label\\">API Requests</span><span class=\\"tt-val\\">${fmtInt(col.dataset.reqs)}</span></div>\\n          `;\\n          showTooltip(html, ev.clientX, ev.clientY);\\n        });\\n        hourlyBars.addEventListener(\'mousemove\', (ev) => {\\n          if (tooltipEl.classList.contains(\'is-visible\')) positionTooltip(ev.clientX, ev.clientY);\\n        });\\n        hourlyBars.addEventListener(\'mouseleave\', hideTooltip);\\n\\n        // Conversations search & sort\\n        const searchInput = document.getElementById(\'convSearchInput\');\\n        const clearBtn = document.getElementById(\'searchClearBtn\');\\n        const searchKbd = document.getElementById(\'searchKbd\');\\n\\n        const syncSearchUi = () => {\\n          const hasText = searchInput.value.length > 0;\\n          clearBtn.classList.toggle(\'is-visible\', hasText);\\n          searchKbd.style.display = hasText ? \'none\' : \'inline-block\';\\n        };\\n\\n        searchInput.addEventListener(\'input\', () => {\\n          state.searchQuery = searchInput.value;\\n          state.visibleLimit = 50;\\n          syncSearchUi();\\n          renderConversationsTable();\\n        });\\n\\n        clearBtn.addEventListener(\'click\', () => {\\n          searchInput.value = \'\';\\n          state.searchQuery = \'\';\\n          syncSearchUi();\\n          renderConversationsTable();\\n          searchInput.focus();\\n        });\\n\\n        document.querySelectorAll(\'.conv-table thead th.sortable\').forEach((th) => {\\n          th.addEventListener(\'click\', () => {\\n            const key = th.dataset.sort;\\n            if (state.sortKey === key) {\\n              state.sortDir = state.sortDir === \'desc\' ? \'asc\' : \'desc\';\\n            } else {\\n              state.sortKey = key;\\n              state.sortDir = key === \'title\' ? \'asc\' : \'desc\';\\n            }\\n            renderConversationsTable();\\n          });\\n        });\\n\\n        document.getElementById(\'convShowMoreBtn\').addEventListener(\'click\', () => {\\n          state.visibleLimit += 50;\\n          renderConversationsTable();\\n        });\\n\\n        // Table row click -> open drawer\\n        const tbody = document.getElementById(\'convTableBody\');\\n        tbody.addEventListener(\'click\', (ev) => {\\n          const row = ev.target.closest(\'tr[data-conv-id]\');\\n          if (row) openDrawer(row.dataset.convId);\\n        });\\n        tbody.addEventListener(\'keydown\', (ev) => {\\n          if (ev.key === \'Enter\' || ev.key === \' \') {\\n            const row = ev.target.closest(\'tr[data-conv-id]\');\\n            if (row) {\\n              ev.preventDefault();\\n              openDrawer(row.dataset.convId);\\n            }\\n          }\\n        });\\n\\n        // Drawer controls\\n        document.getElementById(\'drawerCloseBtn\').addEventListener(\'click\', closeDrawer);\\n        document.getElementById(\'drawerBackdrop\').addEventListener(\'click\', closeDrawer);\\n        document.getElementById(\'drawerPrevBtn\').addEventListener(\'click\', () => stepDrawer(-1));\\n        document.getElementById(\'drawerNextBtn\').addEventListener(\'click\', () => stepDrawer(1));\\n\\n        // Global keyboard shortcuts\\n        document.addEventListener(\'keydown\', (ev) => {\\n          const tag = document.activeElement?.tagName;\\n          const inInput = tag === \'INPUT\' || tag === \'TEXTAREA\' || tag === \'SELECT\';\\n          if (ev.key === \'Escape\' && state.selectedConvId) {\\n            closeDrawer();\\n            return;\\n          }\\n          if (state.selectedConvId && !inInput) {\\n            if (ev.key === \'ArrowUp\') { ev.preventDefault(); stepDrawer(-1); return; }\\n            if (ev.key === \'ArrowDown\') { ev.preventDefault(); stepDrawer(1); return; }\\n          }\\n          if ((ev.key === \'/\' && !inInput) || ((ev.metaKey || ev.ctrlKey) && ev.key.toLowerCase() === \'k\')) {\\n            ev.preventDefault();\\n            searchInput.focus();\\n            searchInput.select();\\n          }\\n        });\\n\\n        // Resize observer for SVG fallback responsiveness\\n        let resizeTimer = null;\\n        window.addEventListener(\'resize\', () => {\\n          if (typeof window.Chart === \'undefined\' || window.__chartJsLoadFailed) {\\n            if (resizeTimer) clearTimeout(resizeTimer);\\n            resizeTimer = setTimeout(() => {\\n              if (state.data) renderDailyChart(state.data.daily || [], state.data.meta || {});\\n            }, 120);\\n          }\\n        });\\n      }\\n\\n      // --- Boot ---\\n      bindEvents();\\n      fetchStats({ force: true });\\n    })();\\n  </script>\\n</body>\\n</html>\\n"')

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
        "label": "Gemini Flash Tier",
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

VALID_DAYS_FILTERS = ("7", "30", "90", "365", "all")
VALID_BILLING_MODES = ("context", "turn")

# Energy & Carbon coefficients:
# 0.08 Wh per 1k input tokens = 0.00008 Wh/token
# 0.45 Wh per 1k output/thinking tokens = 0.00045 Wh/token
WH_PER_INPUT_TOKEN = 0.00008
WH_PER_OUTPUT_TOKEN = 0.00045
CO2_GRAMS_PER_KWH = 385.0
WH_PER_PHONE_CHARGE = 12.0
WH_PER_LED_BULB_HOUR = 10.0


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
    """Compute electricity (Wh/kWh), CO2 (grams), and everyday equivalents."""
    wh = (effective_input_tokens * WH_PER_INPUT_TOKEN) + (
        (output_tokens + thinking_tokens) * WH_PER_OUTPUT_TOKEN
    )
    kwh = wh / 1000.0
    co2_g = kwh * CO2_GRAMS_PER_KWH
    return {
        "electricity_wh": wh,
        "electricity_kwh": kwh,
        "co2_grams": co2_g,
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

_CODE_PUNCT_CHARS = "{}[]()<>:;,.=+-*/\|&^%$#@!~?\"'`"


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


def resolve_db_path(
    agy_dir: Path,
    cli_db: Optional[str] = None,
    demo_mode: bool = False,
) -> str:
    """Determine SQLite DB path.

    Default: `<brain_dir>/../agy_usage.db` if writable, else
    `~/.cache/agy-dashboard/agy_usage.db` (or `:memory:` for `--demo`).
    """
    if cli_db:
        if cli_db == ":memory:":
            return ":memory:"
        target = _expand_path(cli_db)
        target.parent.mkdir(parents=True, exist_ok=True)
        return str(target.resolve())

    if demo_mode:
        return ":memory:"

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
    step_index INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    local_date TEXT NOT NULL,
    local_hour INTEGER NOT NULL,
    event_type TEXT NOT NULL,
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
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


def parse_conversation_incremental(
    conn: sqlite3.Connection,
    conv_id: str,
    transcript_path: Path,
    force: bool = False,
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
                        step_idx,
                        ts_iso,
                        local_date,
                        local_hour,
                        step_type or source or "UNKNOWN",
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
            id, file_path, title, started_at, updated_at,
            mtime_ns, file_size, byte_offset, lines_parsed,
            running_context, has_user_title,
            api_requests, tool_calls, subagent_spawns,
            unique_input_tokens, context_input_tokens, output_tokens, thinking_tokens
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
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
                conversation_id, step_index, timestamp, local_date, local_hour,
                event_type, is_api_request, unique_input_tokens, context_input_tokens,
                output_tokens, thinking_tokens, tool_calls_count, subagent_spawns
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        if parse_conversation_incremental(conn, conv_id, tx_path, force=force):
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
                step_idx,
                started_iso,
                cur_dt.strftime("%Y-%m-%d"),
                cur_dt.hour,
                "USER_INPUT",
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
                    step_idx,
                    ev_iso,
                    ev_date,
                    ev_hour,
                    "PLANNER_RESPONSE",
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
                        step_idx,
                        cur_dt.isoformat(timespec="seconds"),
                        cur_dt.strftime("%Y-%m-%d"),
                        cur_dt.hour,
                        "TOOL_RESPONSE" if tc_count > 0 else "USER_INPUT",
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
            id, file_path, title, started_at, updated_at,
            mtime_ns, file_size, byte_offset, lines_parsed,
            running_context, has_user_title,
            api_requests, tool_calls, subagent_spawns,
            unique_input_tokens, context_input_tokens, output_tokens, thinking_tokens
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        conv_rows,
    )
    conn.executemany(
        """
        INSERT INTO events (
            conversation_id, step_index, timestamp, local_date, local_hour,
            event_type, is_api_request, unique_input_tokens, context_input_tokens,
            output_tokens, thinking_tokens, tool_calls_count, subagent_spawns
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    ) -> None:
        self.lock = threading.RLock()
        self._cli_agy_dir = cli_agy_dir
        if agy_dir is not None and cli_agy_dir is None:
            self.agy_dirs: List[Path] = [agy_dir]
            self._pinned_single = True
        else:
            self.agy_dirs = discover_agy_dirs(cli_agy_dir)
            self._pinned_single = False
        self.agy_dir = self.agy_dirs[0]
        self.force_demo = force_demo
        self.db_path = resolve_db_path(self.agy_dir, cli_db=db_path, demo_mode=force_demo)

        self._real_conn = init_sqlite_connection(self.db_path)
        self._demo_conn: Optional[sqlite3.Connection] = None
        self.is_demo = False
        self.demo_reason: Optional[str] = None
        self.last_scanned_at = dt.datetime.now().astimezone().isoformat(timespec="seconds")

        if self.force_demo:
            seed_demo_data(self._real_conn)
            self.is_demo = True
            self.demo_reason = DEMO_FLAG_REASON
        else:
            self.rescan(force=force_rescan)

    @property
    def _active_conn(self) -> sqlite3.Connection:
        if self.is_demo and not self.force_demo and self._demo_conn is not None:
            return self._demo_conn
        return self._real_conn

    def close(self) -> None:
        with self.lock:
            if self._demo_conn is not None:
                self._demo_conn.close()
                self._demo_conn = None
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
            scan_info = scan_agy_directory(self._real_conn, self.agy_dirs, force=force)
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

            # 1) Daily breakdown within selected window
            if cutoff_str is not None:
                daily_rows = conn.execute(
                    """
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
                    """
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
                    GROUP BY local_date
                    ORDER BY local_date ASC
                    """
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
                """
                SELECT
                    local_date,
                    SUM(unique_input_tokens) AS uniq_in,
                    SUM(context_input_tokens) AS ctx_in,
                    SUM(output_tokens) AS out_tok,
                    SUM(thinking_tokens) AS think_tok,
                    SUM(is_api_request) AS api_reqs
                FROM events
                WHERE local_date >= ? AND local_date <= ?
                GROUP BY local_date
                """,
                (hm_start_str, today_str),
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
                    """
                    SELECT
                        local_hour,
                        SUM(unique_input_tokens) AS uniq_in,
                        SUM(context_input_tokens) AS ctx_in,
                        SUM(output_tokens) AS out_tok,
                        SUM(thinking_tokens) AS think_tok,
                        SUM(is_api_request) AS api_reqs
                    FROM events
                    WHERE local_date >= ? AND local_date <= ?
                    GROUP BY local_hour
                    """,
                    (cutoff_str, today_str),
                ).fetchall()
            else:
                hr_rows = conn.execute(
                    """
                    SELECT
                        local_hour,
                        SUM(unique_input_tokens) AS uniq_in,
                        SUM(context_input_tokens) AS ctx_in,
                        SUM(output_tokens) AS out_tok,
                        SUM(thinking_tokens) AS think_tok,
                        SUM(is_api_request) AS api_reqs
                    FROM events
                    GROUP BY local_hour
                    """
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
                    SELECT *
                    FROM conversations
                    ORDER BY updated_at DESC
                    LIMIT 100
                    """
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
                "pricing_profiles": PRICING_PROFILES,
            }

            return {
                "meta": meta_obj,
                "totals": totals_obj,
                "daily": daily_list,
                "heatmap": heatmap_list,
                "hourly": hourly_list,
                "tools": tools_list,
                "conversations": conversations_list,
            }

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
            stats = self.server.tracker.get_stats(
                days=days, pricing=pricing, billing_mode=billing_mode
            )
            self._send_json(stats)
            return

        if route == "/api/export.csv":
            days = qs.get("days", ["all"])[0]
            pricing = qs.get("pricing", ["blended"])[0]
            billing_mode = qs.get("billing_mode", ["context"])[0]
            csv_text = self.server.tracker.export_daily_csv(
                days=days, pricing=pricing, billing_mode=billing_mode
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
