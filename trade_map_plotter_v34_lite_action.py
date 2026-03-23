#!/usr/bin/env python3
"""
Trade Map Plotter V34 LITE ACTION - Scan-based signal detection.

Uses scan.py's WMA image detector (render WMA line -> Canny edge detection
-> peak finding -> P/C signal) with PPCC/CCPP pattern matching for trade entry.

Live mode: catches up through existing steps, then polls for new ones.

Usage:
    python trade_map_plotter_v34_lite_action.py
    python trade_map_plotter_v34_lite_action.py step_4680.csv
"""

import argparse
from datetime import datetime, timedelta
import glob
import math
import os
import sys
import pandas as pd
import numpy as np
import tkinter as tk
from tkinter import ttk

import logging
_log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lite_action_debug.log')
_logger = logging.getLogger('lite_action')
_logger.setLevel(logging.DEBUG)
_fh = logging.FileHandler(_log_path, mode='w')
_fh.setFormatter(logging.Formatter('%(asctime)s %(message)s', datefmt='%H:%M:%S'))
_logger.addHandler(_fh)
_logger.info(f"Log started: {_log_path}")

try:
    import ib_broker
    IB_ENABLED = True
except ImportError:
    IB_ENABLED = False

# Import scan module
import scan

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MAX_STEPS = 4680
MIN_FIRST_BAR = 13


# =============================================================================
# Helpers
# =============================================================================

def get_next_expiration(date_str):
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    weekday = dt.weekday()
    if weekday == 0:
        return dt
    elif weekday == 1:
        return dt + timedelta(days=1)
    elif weekday == 2:
        return dt
    elif weekday == 3:
        return dt + timedelta(days=1)
    elif weekday == 4:
        return dt
    elif weekday == 5:
        return dt + timedelta(days=2)
    else:
        return dt + timedelta(days=1)


def build_option_symbol(strike, opt_type, expiration_dt):
    date_part = expiration_dt.strftime('%y%m%d')
    strike_int = int(strike * 1000)
    strike_part = f"{strike_int:08d}"
    return f"SOFI{date_part}{opt_type}{strike_part}"


def get_available_dates():
    run_data = os.path.join(SCRIPT_DIR, 'run-data')
    run_dirs = sorted(glob.glob(os.path.join(run_data, 'GAI_*')) +
                      glob.glob(os.path.join(run_data, 'IB_*')))
    dates = []
    for d in run_dirs:
        if not os.path.isdir(d):
            continue
        basename = os.path.basename(d)
        parts = basename.split('_')
        flip_prefix = ""
        if parts[0] == 'GAI':
            if len(parts) >= 3 and parts[1].endswith("FLIP"):
                flip_prefix = parts[1] + " "
                date_str = parts[2]
            elif len(parts) >= 2:
                date_str = parts[1]
            else:
                continue
        elif parts[0] == 'IB':
            if len(parts) >= 2:
                date_str = parts[1]
            else:
                continue
        else:
            continue
        if len(date_str) == 8 and date_str.isdigit():
            formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            label = formatted
            if flip_prefix:
                label = f"{formatted} ({flip_prefix.strip()})"
            if parts[0] == 'IB':
                label = f"{formatted} (IB)"
            dates.append((label, d))
    return dates


def _find_step_files(run_dir):
    """Find step CSV files -- checks root first, then bar/ subfolder."""
    files = sorted(glob.glob(os.path.join(run_dir, 'step_*.csv')))
    if files:
        return files
    bar_dir = os.path.join(run_dir, 'bar')
    if os.path.isdir(bar_dir):
        files = sorted(glob.glob(os.path.join(bar_dir, 'step_*.csv')))
        if files:
            return files
    return []


def _step_dir(run_dir):
    if glob.glob(os.path.join(run_dir, 'step_*.csv')):
        return run_dir
    bar_dir = os.path.join(run_dir, 'bar')
    if os.path.isdir(bar_dir) and glob.glob(os.path.join(bar_dir, 'step_*.csv')):
        return bar_dir
    return run_dir


def get_resolutions_from_dir(run_dir):
    step_files = _find_step_files(run_dir)
    if step_files:
        try:
            df = pd.read_csv(step_files[0], nrows=1)
            prefixes = []
            for col in df.columns:
                if col.endswith('_close'):
                    prefix = col.replace('_close', '')
                    if prefix.endswith('k'):
                        prefixes.append(prefix)
            if prefixes:
                return prefixes
        except Exception:
            pass
    return ['360k']


def find_latest_step_file():
    run_data = os.path.join(SCRIPT_DIR, 'run-data')
    run_dirs = sorted(glob.glob(os.path.join(run_data, 'GAI_*')) +
                      glob.glob(os.path.join(run_data, 'IB_*')))
    if not run_dirs:
        print("No run directories found")
        return None
    run_dirs = [d for d in run_dirs if os.path.isdir(d)]
    if not run_dirs:
        print("No run directories found")
        return None
    originals = [d for d in run_dirs if 'FLIP' not in os.path.basename(d)]
    latest_run = originals[-1] if originals else run_dirs[-1]
    step_files = _find_step_files(latest_run)
    if step_files:
        return step_files[-1]
    print(f"No step files in {latest_run}")
    return latest_run


def get_run_dir(csv_path):
    parent = os.path.dirname(csv_path)
    if os.path.basename(parent) == 'bar':
        return os.path.dirname(parent)
    return parent


def get_step_number(csv_path):
    basename = os.path.basename(csv_path)
    return int(basename.replace('step_', '').replace('.csv', ''))


def get_step_file(run_dir, step_num):
    return os.path.join(_step_dir(run_dir), f'step_{step_num:05d}.csv')


def _find_aggregate_file(run_dir):
    """Find the aggregate CSV in a run directory."""
    agg_files = glob.glob(os.path.join(run_dir, '*_aggregate.csv'))
    if agg_files:
        return sorted(agg_files)[-1]
    return None


def _find_prev_aggregate(run_dir):
    """Find the previous day's aggregate CSV for WMA warmup."""
    run_data = os.path.join(SCRIPT_DIR, 'run-data')
    all_dirs = sorted(d for d in glob.glob(os.path.join(run_data, 'GAI_*')) +
                      glob.glob(os.path.join(run_data, 'IB_*')) if os.path.isdir(d))
    try:
        idx = all_dirs.index(run_dir)
    except ValueError:
        return None
    if idx > 0:
        prev_agg = _find_aggregate_file(all_dirs[idx - 1])
        return prev_agg
    return None


# =============================================================================
# Scan-based Viewer
# =============================================================================

class TradeMapLiteActionViewer:
    def __init__(self, csv_path):
        if os.path.isdir(csv_path):
            self.run_dir = csv_path
            self.current_step = 0
        else:
            self.run_dir = get_run_dir(csv_path)
            self.current_step = get_step_number(csv_path)

        self.available_dates = get_available_dates()
        self.date_to_dir = {d[0]: d[1] for d in self.available_dates}
        self.available_resolutions = get_resolutions_from_dir(self.run_dir)
        self.current_resolution = '360k' if '360k' in self.available_resolutions else self.available_resolutions[0]

        step_files = _find_step_files(self.run_dir)
        self.max_available_step = get_step_number(step_files[-1]) if step_files else 0

        # Scan detector state
        self.reading_history = []
        self.position = None
        self.position_symbol = None
        self.position_side = None  # "call" or "put"
        self.entry_close = None
        self.trade_num = 0
        self.first_trade = True
        self.total_pnl = 0.0
        self.signals = []  # list of dicts for table display
        self.current_close = 0.0

        # Cached WMA state
        self._cached_agg_path = None
        self._cached_agg_mtime = 0
        self._cached_df = None
        self._cached_wma = None
        self._cached_close_values = None
        self._cached_close_col = None

        # Action state
        self.prev_action = 'NA'
        self.prev_symbol = 'NA'
        self.live_running = False
        self.ib_active = False
        self.trade_mode = 'stock'  # 'stock' or 'option'
        self.stock_ticker = 'SOFI'
        self.stock_qty = 100

        self.root = tk.Tk()
        self.root.title(f"Scan Detector - Step {self.current_step}")
        self.root.geometry("900x700")

        self.setup_ui()
        self.load_and_show()

    def setup_ui(self):
        # Control frame
        control_frame = ttk.Frame(self.root)
        control_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)

        # Date selector
        ttk.Label(control_frame, text="Date:").pack(side=tk.LEFT, padx=(0, 2))
        self.date_var = tk.StringVar()
        date_list = [d[0] for d in self.available_dates]
        self.date_combo = ttk.Combobox(control_frame, textvariable=self.date_var,
                                        values=date_list, width=12, state='readonly')
        self.date_combo.pack(side=tk.LEFT, padx=2)
        current_date = None
        for date_str, dir_path in self.available_dates:
            if dir_path == self.run_dir:
                current_date = date_str
                break
        if current_date:
            self.date_combo.set(current_date)
        self.date_combo.bind('<<ComboboxSelected>>', self.on_date_change)

        # Resolution selector
        ttk.Label(control_frame, text="Res:").pack(side=tk.LEFT, padx=(5, 2))
        self.res_var = tk.StringVar(value=self.current_resolution)
        self.res_combo = ttk.Combobox(control_frame, textvariable=self.res_var,
                                       values=self.available_resolutions, width=6, state='readonly')
        self.res_combo.pack(side=tk.LEFT, padx=2)
        self.res_combo.bind('<<ComboboxSelected>>', self.on_resolution_change)

        ttk.Separator(control_frame, orient='vertical').pack(side=tk.LEFT, fill='y', padx=10)

        # Navigation
        ttk.Button(control_frame, text="<< 100", command=lambda: self.navigate(-100)).pack(side=tk.LEFT, padx=2)
        ttk.Button(control_frame, text="< 10", command=lambda: self.navigate(-10)).pack(side=tk.LEFT, padx=2)
        ttk.Button(control_frame, text="< 1", command=lambda: self.navigate(-1)).pack(side=tk.LEFT, padx=2)

        self.step_label = ttk.Label(control_frame, text=f"Step: {self.current_step}", font=('Arial', 12, 'bold'))
        self.step_label.pack(side=tk.LEFT, padx=20)

        ttk.Button(control_frame, text="1 >", command=lambda: self.navigate(1)).pack(side=tk.LEFT, padx=2)
        ttk.Button(control_frame, text="10 >", command=lambda: self.navigate(10)).pack(side=tk.LEFT, padx=2)
        ttk.Button(control_frame, text="100 >>", command=lambda: self.navigate(100)).pack(side=tk.LEFT, padx=2)

        ttk.Separator(control_frame, orient='vertical').pack(side=tk.LEFT, fill='y', padx=10)

        # Expiration date selector
        ttk.Label(control_frame, text="Exp:").pack(side=tk.LEFT)
        self.exp_var = tk.StringVar()
        self.exp_combo = ttk.Combobox(control_frame, textvariable=self.exp_var, width=10, state='readonly')
        self.exp_combo.pack(side=tk.LEFT, padx=2)
        self.exp_combo.bind('<<ComboboxSelected>>', lambda e: self.load_and_show())
        self.update_exp_dates()

        self.info_label = ttk.Label(control_frame, text="", font=('Arial', 10))
        self.info_label.pack(side=tk.RIGHT, padx=10)

        # Live mode control frame
        live_frame = ttk.Frame(self.root)
        live_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=(0, 5))

        self.go_live_btn = ttk.Button(live_frame, text="Go Live", command=self.go_live, width=10)
        self.go_live_btn.pack(side=tk.LEFT, padx=2)

        self.stop_live_btn = ttk.Button(live_frame, text="Stop", command=self.stop_live, width=6, state='disabled')
        self.stop_live_btn.pack(side=tk.LEFT, padx=2)

        self.live_status = ttk.Label(live_frame, text="", font=('Arial', 10, 'bold'), foreground='#CC0000')
        self.live_status.pack(side=tk.LEFT, padx=10)

        self.action_label = ttk.Label(live_frame, text="", font=('Arial', 9), foreground='#006600')
        self.action_label.pack(side=tk.LEFT, padx=10)

        ttk.Separator(live_frame, orient='vertical').pack(side=tk.LEFT, fill='y', padx=10)

        # IB toggle
        self.ib_btn = tk.Button(live_frame, text="IB OFF", fg='#CC0000', font=('Arial', 9, 'bold'),
                                width=8, command=self.toggle_ib,
                                state='normal' if IB_ENABLED else 'disabled')
        self.ib_btn.pack(side=tk.LEFT, padx=2)

        self.ib_status_label = ttk.Label(live_frame, text="(not installed)" if not IB_ENABLED else "",
                                         font=('Arial', 9), foreground='#888888')
        self.ib_status_label.pack(side=tk.LEFT, padx=5)

        ttk.Separator(live_frame, orient='vertical').pack(side=tk.LEFT, fill='y', padx=10)

        # Mode: Stock / Option
        ttk.Label(live_frame, text="Mode:").pack(side=tk.LEFT)
        self.mode_var = tk.StringVar(value='stock')
        ttk.Radiobutton(live_frame, text="Stock", variable=self.mode_var, value='stock',
                         command=self._on_mode_change).pack(side=tk.LEFT, padx=2)
        ttk.Radiobutton(live_frame, text="Option", variable=self.mode_var, value='option',
                         command=self._on_mode_change).pack(side=tk.LEFT, padx=2)

        ttk.Label(live_frame, text="Ticker:").pack(side=tk.LEFT, padx=(6, 0))
        self.ticker_var = tk.StringVar(value='SOFI')
        self.ticker_entry = ttk.Entry(live_frame, textvariable=self.ticker_var, width=6)
        self.ticker_entry.pack(side=tk.LEFT, padx=2)

        ttk.Label(live_frame, text="Qty:").pack(side=tk.LEFT)
        self.qty_var = tk.IntVar(value=100)
        ttk.Spinbox(live_frame, from_=1, to=10000, increment=1,
                    textvariable=self.qty_var, width=5).pack(side=tk.LEFT, padx=2)

        # Signal + Trade table
        table_frame = ttk.Frame(self.root)
        table_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Signal history table
        sig_header = ttk.Frame(table_frame)
        sig_header.pack(fill=tk.X)
        self.sig_header_label = ttk.Label(sig_header, text="Signals", font=('Arial', 10, 'bold'))
        self.sig_header_label.pack(side=tk.LEFT)

        sig_table_frame = ttk.Frame(table_frame)
        sig_table_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.sig_cols = ('Step', 'Close', 'WMA55', 'Signal', 'Pattern', 'Action', 'Side', 'PNL', 'TotalPNL')
        self.sig_tree = ttk.Treeview(sig_table_frame, columns=self.sig_cols, show='headings', height=20)
        col_widths = {'Step': 60, 'Close': 80, 'WMA55': 80, 'Signal': 60,
                      'Pattern': 60, 'Action': 80, 'Side': 50, 'PNL': 70, 'TotalPNL': 80}
        for col in self.sig_cols:
            self.sig_tree.heading(col, text=col)
            self.sig_tree.column(col, width=col_widths.get(col, 60), anchor='center')
        sig_scroll = ttk.Scrollbar(sig_table_frame, orient=tk.VERTICAL, command=self.sig_tree.yview)
        self.sig_tree.configure(yscrollcommand=sig_scroll.set)
        self.sig_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sig_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Key bindings
        self.root.bind('<Left>', lambda e: self.navigate(-1))
        self.root.bind('<Right>', lambda e: self.navigate(1))
        self.root.bind('<Prior>', lambda e: self.navigate(-100))
        self.root.bind('<Next>', lambda e: self.navigate(100))

    def update_exp_dates(self):
        current_date = self.date_var.get()
        if not current_date:
            return
        clean_date = current_date.split(' ')[0] if ' (' in current_date else current_date
        dt = datetime.strptime(clean_date, '%Y-%m-%d')
        holiday_expirations = {'2026-02-17'}
        exp_dates = []
        for i in range(28):
            check_dt = dt + timedelta(days=i)
            check_str = check_dt.strftime('%Y-%m-%d')
            if check_dt.weekday() in (0, 2, 4) or check_str in holiday_expirations:
                exp_dates.append(check_str)
        self.exp_combo['values'] = exp_dates
        if exp_dates:
            next_exp = get_next_expiration(clean_date)
            next_exp_str = next_exp.strftime('%Y-%m-%d')
            if next_exp_str in exp_dates:
                self.exp_var.set(next_exp_str)
            else:
                self.exp_var.set(exp_dates[0])

    def on_date_change(self, event=None):
        selected_date = self.date_var.get()
        if selected_date in self.date_to_dir:
            new_run_dir = self.date_to_dir[selected_date]
            if new_run_dir != self.run_dir:
                self.run_dir = new_run_dir
                self.available_resolutions = get_resolutions_from_dir(self.run_dir)
                self.res_combo['values'] = self.available_resolutions
                self.current_resolution = '360k' if '360k' in self.available_resolutions else self.available_resolutions[0]
                self.res_var.set(self.current_resolution)
                step_files = _find_step_files(self.run_dir)
                self.max_available_step = get_step_number(step_files[-1]) if step_files else 1
                self.current_step = min(MAX_STEPS, self.max_available_step)
                self.update_exp_dates()
                self._reset_scan_state()
                self._cached_agg_path = None
                self.load_and_show()

    def on_resolution_change(self, event=None):
        selected_res = self.res_var.get()
        if selected_res != self.current_resolution:
            self.current_resolution = selected_res
            self.load_and_show()

    def navigate(self, delta):
        step_files = _find_step_files(self.run_dir)
        if step_files:
            self.max_available_step = get_step_number(step_files[-1])
        new_step = max(1, min(self.current_step + delta, self.max_available_step))
        if new_step != self.current_step:
            self.current_step = new_step
            self.load_and_show()

    def _on_mode_change(self):
        self.trade_mode = self.mode_var.get()
        _logger.info(f"[MODE] Switched to {self.trade_mode}")

    def _reset_scan_state(self):
        """Reset all scan detector state for a new run."""
        self.reading_history = []
        self.position = None
        self.position_symbol = None
        self.position_side = None
        self.entry_close = None
        self.trade_num = 0
        self.first_trade = True
        self.total_pnl = 0.0
        self.signals = []
        self.prev_action = 'NA'
        self.prev_symbol = 'NA'

    def _load_aggregate(self):
        """Load and cache aggregate CSV + WMA computation."""
        agg_path = _find_aggregate_file(self.run_dir)
        if agg_path is None:
            return False

        try:
            mtime = os.path.getmtime(agg_path)
        except OSError:
            return False

        if agg_path == self._cached_agg_path and mtime == self._cached_agg_mtime:
            return True

        prev_agg = _find_prev_aggregate(self.run_dir)
        try:
            df, wma, close_vals, close_col, prepend = scan.load_and_compute_wma(agg_path, prev_agg)
            # Strip prepended warmup rows from df for display
            if prepend > 0:
                df = df.iloc[prepend:].reset_index(drop=True)
                wma = wma[prepend:]
                close_vals = close_vals[prepend:]
            self._cached_df = df
            self._cached_wma = wma
            self._cached_close_values = close_vals
            self._cached_close_col = close_col
            self._cached_agg_path = agg_path
            self._cached_agg_mtime = mtime
            return True
        except Exception as e:
            _logger.error(f"Failed to load aggregate: {e}")
            return False

    def _detect_at_step(self, step_idx):
        """Run scan detector for a single step. Returns signal ("call"/"put"/None)."""
        if self._cached_wma is None:
            return None
        total_len = len(self._cached_wma)
        if step_idx < 0 or step_idx >= total_len:
            return None
        wma_val = self._cached_wma[step_idx]
        if np.isnan(wma_val):
            return None

        # Build WMA series up to this step
        full_series = np.full(total_len, np.nan)
        full_series[:step_idx + 1] = self._cached_wma[:step_idx + 1]

        img = scan.render_to_image(full_series, total_len)
        if img is None:
            return None
        return scan.detect_signal(img)

    def load_and_show(self):
        """Load aggregate, detect signal at current step, update table."""
        if self.current_step == 0:
            self.step_label.config(text="Step: 0 / 0")
            self.info_label.config(text="Waiting for data...")
            return

        self.root.title(f"Scan Detector - Step {self.current_step}")
        self.step_label.config(text=f"Step: {self.current_step} / {self.max_available_step}")

        if not self._load_aggregate():
            self.info_label.config(text="No aggregate CSV found")
            return

        df = self._cached_df
        if 'step' not in df.columns:
            self.info_label.config(text="No step column in aggregate")
            return

        # Find row index for current step
        step_mask = df['step'] == self.current_step
        if not step_mask.any():
            self.info_label.config(text=f"Step {self.current_step} not in aggregate")
            return

        row_idx = step_mask.idxmax()
        close_val = float(self._cached_close_values[row_idx])
        wma_val = self._cached_wma[row_idx]
        wma_display = f"{wma_val:.4f}" if not np.isnan(wma_val) else "---"
        self.current_close = close_val

        # Detect signal at this step
        signal = self._detect_at_step(row_idx)

        self.info_label.config(
            text=f"Bars: {len(df)} | Close: {close_val:.4f} | WMA: {wma_display} | Signal: {signal or '---'}"
        )

    # =========================================================================
    # Action computation - scan.py pattern-based
    # =========================================================================

    def compute_actions(self):
        """Detect signal at current step and compute order list.

        Uses scan.py's pattern matching:
        - First trade: immediate entry on first reading
        - Subsequent: PPCC -> call, CCPP -> put

        Returns list of orders: [{'action': str, 'side': str, 'symbol': str}, ...]
        On a flip: returns [Sell old, Buy new]. Otherwise single order.
        """
        if not self._load_aggregate():
            return [{'action': 'NA', 'side': None, 'symbol': 'NA'}]

        df = self._cached_df
        if 'step' not in df.columns:
            return [{'action': 'NA', 'side': None, 'symbol': 'NA'}]

        step_mask = df['step'] == self.current_step
        if not step_mask.any():
            return [{'action': 'NA', 'side': None, 'symbol': 'NA'}]

        row_idx = step_mask.idxmax()
        close_val = float(self._cached_close_values[row_idx])
        wma_val = self._cached_wma[row_idx]
        self.current_close = close_val

        reading = self._detect_at_step(row_idx)

        # Get expiration for option symbol building
        current_date_str = self.date_var.get()
        clean_date = current_date_str.split(' ')[0] if ' (' in current_date_str else current_date_str
        exp_str = self.exp_var.get()
        if exp_str:
            expiration_dt = datetime.strptime(exp_str, '%Y-%m-%d')
        else:
            expiration_dt = get_next_expiration(clean_date)

        ticker = self.ticker_var.get().strip().upper()

        def _build_symbol(side):
            if self.mode_var.get() == 'stock':
                return ticker
            if side == "call":
                strike = (close_val // 2.5) * 2.5
                return build_option_symbol(strike, 'C', expiration_dt)
            else:
                strike = math.ceil(close_val / 2.5) * 2.5
                return build_option_symbol(strike, 'P', expiration_dt)

        orders = []

        if reading is None:
            action = 'HoldCurrent' if self.position is not None else 'NA'
            self.signals.append({
                'step': self.current_step, 'close': close_val,
                'wma55': wma_val if not np.isnan(wma_val) else None,
                'signal': None, 'pattern': None,
                'action': action, 'side': self.position_side, 'pnl': None,
                'total_pnl': round(self.total_pnl, 4),
            })
            return [{'action': action, 'side': self.position_side, 'symbol': self.position_symbol or 'NA'}]

        # We have a reading
        self.reading_history.append(reading)
        pattern = scan.check_pattern(self.reading_history, self.first_trade)

        if self.position is None:
            # No position -- look for entry pattern
            if pattern is not None:
                if self.first_trade:
                    self.first_trade = False
                self.trade_num += 1
                self.position = pattern
                self.position_side = pattern
                self.entry_close = close_val
                symbol = _build_symbol(pattern)
                self.position_symbol = symbol

                self.signals.append({
                    'step': self.current_step, 'close': close_val,
                    'wma55': wma_val if not np.isnan(wma_val) else None,
                    'signal': reading, 'pattern': pattern,
                    'action': 'Buy', 'side': pattern, 'pnl': None,
                    'total_pnl': round(self.total_pnl, 4),
                })
                orders.append({'action': 'Buy', 'side': pattern, 'symbol': symbol})
            else:
                self.signals.append({
                    'step': self.current_step, 'close': close_val,
                    'wma55': wma_val if not np.isnan(wma_val) else None,
                    'signal': reading, 'pattern': None,
                    'action': 'NA', 'side': None, 'pnl': None,
                    'total_pnl': round(self.total_pnl, 4),
                })
                orders.append({'action': 'NA', 'side': None, 'symbol': 'NA'})

        elif pattern is not None and pattern != self.position:
            # Signal flip -- two orders: Sell old, Buy new
            if self.position_side == "call":
                pnl = round(close_val - self.entry_close, 4)
            else:
                pnl = round(self.entry_close - close_val, 4)
            self.total_pnl += pnl

            old_symbol = self.position_symbol
            old_side = self.position_side

            # Order 1: Sell old
            self.signals.append({
                'step': self.current_step, 'close': close_val,
                'wma55': wma_val if not np.isnan(wma_val) else None,
                'signal': reading, 'pattern': pattern,
                'action': 'Sell', 'side': old_side, 'pnl': pnl,
                'total_pnl': round(self.total_pnl, 4),
            })
            orders.append({'action': 'Sell', 'side': old_side, 'symbol': old_symbol})

            # Order 2: Buy new
            self.trade_num += 1
            self.position = pattern
            self.position_side = pattern
            self.entry_close = close_val
            new_symbol = _build_symbol(pattern)
            self.position_symbol = new_symbol

            self.signals.append({
                'step': self.current_step, 'close': close_val,
                'wma55': wma_val if not np.isnan(wma_val) else None,
                'signal': reading, 'pattern': pattern,
                'action': 'Buy', 'side': pattern, 'pnl': None,
                'total_pnl': round(self.total_pnl, 4),
            })
            orders.append({'action': 'Buy', 'side': pattern, 'symbol': new_symbol})
        else:
            # Hold current position
            self.signals.append({
                'step': self.current_step, 'close': close_val,
                'wma55': wma_val if not np.isnan(wma_val) else None,
                'signal': reading, 'pattern': pattern,
                'action': 'HoldCurrent', 'side': self.position_side, 'pnl': None,
                'total_pnl': round(self.total_pnl, 4),
            })
            orders.append({'action': 'HoldCurrent', 'side': self.position_side, 'symbol': self.position_symbol or 'NA'})

        return orders

    def _update_signal_table(self):
        """Refresh the signal treeview from self.signals."""
        for item in self.sig_tree.get_children():
            self.sig_tree.delete(item)

        for sig in self.signals:
            wma_str = f"{sig['wma55']:.4f}" if sig['wma55'] is not None else "---"
            sig_str = sig['signal'] or "---"
            pat_str = sig['pattern'] or "---"
            act_str = sig['action'] or "NA"
            side_str = (sig['side'] or "---").upper() if sig['side'] else "---"
            pnl_str = f"{sig['pnl']:+.4f}" if sig['pnl'] is not None else ""
            tpnl_str = f"{sig['total_pnl']:+.4f}" if sig['total_pnl'] is not None else ""

            tag = ''
            if act_str == 'Buy':
                tag = 'buy'
            elif act_str == 'Sell':
                tag = 'sell'

            self.sig_tree.insert('', 'end', values=(
                sig['step'], f"{sig['close']:.4f}", wma_str,
                sig_str, pat_str, act_str, side_str, pnl_str, tpnl_str,
            ), tags=(tag,))

        self.sig_tree.tag_configure('buy', foreground='#008800')
        self.sig_tree.tag_configure('sell', foreground='#CC0000')

        # Auto-scroll to bottom
        children = self.sig_tree.get_children()
        if children:
            self.sig_tree.see(children[-1])

        # Update header
        buy_count = sum(1 for s in self.signals if s['action'] == 'Buy')
        sell_count = sum(1 for s in self.signals if s['action'] == 'Sell')
        pos_str = f"{self.position_side.upper()} @ {self.position_symbol}" if self.position else "FLAT"
        self.sig_header_label.config(
            text=f"Signals - {len(self.signals)} steps | {buy_count} buys, {sell_count} sells | "
                 f"PNL: {self.total_pnl:+.4f} | Position: {pos_str}"
        )

    def _fire_ib_order(self, order, step):
        """Fire a single IB order based on current mode (stock/option)."""
        action = order['action']
        side = order['side']
        symbol = order['symbol']
        if action not in ('Buy', 'Sell'):
            return

        mode = self.mode_var.get()
        side_upper = (side or '').upper()
        qty = self.qty_var.get()

        if mode == 'stock':
            ticker = self.ticker_var.get().strip().upper()
            _logger.info(f"[IB-STK] {action} {side_upper} {qty}x {ticker} @ step {step}")
            ib_broker.execute_stock(action, ticker, qty, step, side_upper)
        else:
            _logger.info(f"[IB-OPT] {action} {side_upper} {symbol} @ step {step}")
            ib_broker.execute(action, symbol, step, side_upper)

    def log_action(self, step, orders):
        """Log both call and put side actions for every step, matching original format:
        step-XXXX_close-X.XX_callAction-{action}_callSymbol-{sym}_putAction-{action}_putSymbol-{sym}
        """
        close_str = f"{self.current_close:.2f}" if hasattr(self, 'current_close') else "0.00"

        # Build per-side action from the order list
        call_action = 'NA'
        call_symbol = 'NA'
        put_action = 'NA'
        put_symbol = 'NA'

        for order in orders:
            act = order['action']
            side = order.get('side')
            sym = order.get('symbol', 'NA')
            if side == 'call':
                call_action = act
                call_symbol = sym if sym else 'NA'
            elif side == 'put':
                put_action = act
                put_symbol = sym if sym else 'NA'
            elif act == 'HoldCurrent' or act == 'NA':
                # No flip — attribute to current position side
                if self.position_side == 'call':
                    call_action = act
                    call_symbol = self.position_symbol or 'NA'
                elif self.position_side == 'put':
                    put_action = act
                    put_symbol = self.position_symbol or 'NA'

        line = (f"step-{step:04d}_close-{close_str}"
                f"_callAction-{call_action}_callSymbol-{call_symbol}"
                f"_putAction-{put_action}_putSymbol-{put_symbol}")
        _logger.info(f"[ACTION] {line}")

        has_trade = any(o['action'] in ('Buy', 'Sell') for o in orders)
        if has_trade:
            _logger.info(f"[ACTION] Trade #{self.trade_num} | PNL: {self.total_pnl:+.4f}")

    # =========================================================================
    # Live mode
    # =========================================================================

    def go_live(self):
        _logger.info(f"[LIVE] go_live() called. live_running={self.live_running}, run_dir={self.run_dir}")
        if self.live_running:
            return

        try:
            self.live_running = True
            self.go_live_btn.config(state='disabled')
            self.stop_live_btn.config(state='normal')

            # Reset scan state for clean run
            self._reset_scan_state()
            self._cached_agg_path = None
            self._poll_miss_count = 0

            step_files = _find_step_files(self.run_dir)
            if step_files:
                self.max_available_step = get_step_number(step_files[-1])

            self.live_status.config(text="LIVE MODE")
            _logger.info(f"[LIVE] Starting. run_dir={self.run_dir}, max_step={self.max_available_step}")

            # Phase 1: Catch up through all existing steps
            for step in range(1, self.max_available_step + 1):
                if not self.live_running:
                    break

                csv_path = get_step_file(self.run_dir, step)
                if not os.path.exists(csv_path):
                    continue

                self.current_step = step
                # Force aggregate reload on each step during catchup
                self._cached_agg_mtime = 0

                orders = self.compute_actions()
                self.log_action(step, orders)

                if step % 100 == 0:
                    _logger.info(f"[CATCHUP] Step {step}/{self.max_available_step}")
                    self.live_status.config(text=f"Catching up: {step}/{self.max_available_step}")
                    last = orders[-1]
                    self.action_label.config(text=f"{last['action']} {(last['side'] or '').upper()} {last['symbol']}")
                    self._update_signal_table()
                    self.root.update()

            if not self.live_running:
                _logger.info(f"[LIVE] Stopped during catch-up")
                self._live_cleanup()
                return

            self._update_signal_table()
            _logger.info(f"[LIVE] Catch-up done at step {self.max_available_step}. Starting poll...")
            self.live_status.config(text=f"Caught up to step {self.max_available_step}. Polling...")
            pos_str = f"{(self.position_side or '').upper()} {self.position_symbol}" if self.position else "FLAT"
            self.action_label.config(text=f"Position: {pos_str}")
            self.root.update()

            # Phase 2: Poll for new step files
            self.poll_for_new_steps()
        except Exception as e:
            import traceback
            _logger.error(f"[LIVE] CRASH in go_live: {e}")
            _logger.error(traceback.format_exc())
            self._live_cleanup()

    def poll_for_new_steps(self):
        if not self.live_running:
            self._live_cleanup()
            return

        next_step = self.max_available_step + 1
        csv_path = get_step_file(self.run_dir, next_step)

        try:
            exists = os.path.exists(csv_path)
            size = os.path.getsize(csv_path) if exists else 0
            if exists and size > 100:
                self._poll_miss_count = 0
                _logger.info(f"[POLL] Processing step {next_step} (size={size})")
                self.current_step = next_step
                self.max_available_step = next_step
                # Force aggregate reload
                self._cached_agg_mtime = 0

                orders = self.compute_actions()
                _logger.info(f"[POLL] Step {next_step}: {len(orders)} order(s)")

                # Fire IB orders (all of them -- on flip this is Sell + Buy)
                if self.ib_active:
                    for order in orders:
                        self._fire_ib_order(order, next_step)

                self.log_action(next_step, orders)
                self._update_signal_table()

                last = orders[-1]
                self.live_status.config(text=f"Live: step {next_step}")
                self.action_label.config(text=f"{last['action']} {(last['side'] or '').upper()} {last['symbol']}")
                self.step_label.config(text=f"Step: {next_step} / {self.max_available_step}")
                self.root.update()
            else:
                self._poll_miss_count += 1
                if self._poll_miss_count >= 21:
                    print(f"[POLL] No step file for 21 polls (step {next_step}) -- AUTO-STOPPING")
                    _logger.error(f"[POLL] No step file for 21 polls -- auto-stopping")
                    self.stop_live()
                    self._live_cleanup()
                    return
        except Exception as e:
            _logger.info(f"[POLL] ERROR at step {next_step}: {e}")
            import traceback
            _logger.error(traceback.format_exc())

        self.root.after(1000, self.poll_for_new_steps)

    def toggle_ib(self):
        if not IB_ENABLED:
            return
        if not self.ib_active:
            trade_logs_dir = os.path.join(self.run_dir, 'trade-logs')
            os.makedirs(trade_logs_dir, exist_ok=True)
            ok = ib_broker.connect(log_dir=trade_logs_dir)
            if ok:
                self.ib_active = True
                self.ib_btn.config(text="IB ON", fg='#008800')
                mode = "paper" if ib_broker.CONFIG['port'] == 4002 else "LIVE"
                self.ib_status_label.config(text=f"Connected ({mode})", foreground='#008800')
            else:
                self.ib_status_label.config(text="Connection failed", foreground='#CC0000')
        else:
            ib_broker.disconnect()
            self.ib_active = False
            self.ib_btn.config(text="IB OFF", fg='#CC0000')
            self.ib_status_label.config(text="Disconnected", foreground='#888888')

    def stop_live(self):
        print(f"[STOP] Stop pressed -- live_running=False")
        self.live_running = False
        if self.ib_active:
            port = ib_broker.CONFIG['port']
            mode = "PAPER" if port == 4002 else "LIVE"
            print(f"[STOP] IB active ({mode} port {port}) -- closing all positions...")
            from ib_close_panel import _close_positions
            _close_positions(port, filter_right=None)
            print(f"[STOP] Positions closed -- disconnecting broker")
            ib_broker.disconnect()
            self.ib_active = False
            self.ib_btn.config(text="IB OFF", fg='#CC0000')
            self.ib_status_label.config(text="", foreground='#888888')
            print(f"[STOP] Done -- IB disconnected")
        else:
            print(f"[STOP] IB not active -- no positions to close")

    def _live_cleanup(self):
        self.live_status.config(text="Stopped")
        self.go_live_btn.config(state='normal')
        self.stop_live_btn.config(state='disabled')
        self.root.after(3000, lambda: self.live_status.config(text=""))

    def run(self):
        self.root.mainloop()


def main():
    parser = argparse.ArgumentParser(description='Scan Detector - WMA Image Signal Detection')
    parser.add_argument('csv_file', nargs='?', help='Path to step CSV file or run directory')
    parser.add_argument('--today', action='store_true', help='Use latest step file')
    parser.add_argument('--mode', choices=['stock', 'option'], default='stock',
                       help='Trading mode: stock or option (default: stock)')
    parser.add_argument('--ticker', type=str, default='SOFI', help='Stock ticker (default: SOFI)')
    parser.add_argument('--qty', type=int, default=100, help='Shares per trade (default: 100)')
    args = parser.parse_args()

    if args.today or not args.csv_file:
        csv_path = find_latest_step_file()
        if not csv_path:
            sys.exit(1)
    else:
        csv_path = args.csv_file
        if not os.path.exists(csv_path):
            print(f"File not found: {csv_path}")
            sys.exit(1)

    viewer = TradeMapLiteActionViewer(csv_path)
    viewer.mode_var.set(args.mode)
    viewer.ticker_var.set(args.ticker)
    viewer.qty_var.set(args.qty)
    viewer.trade_mode = args.mode
    viewer.stock_ticker = args.ticker
    viewer.stock_qty = args.qty
    viewer.run()


if __name__ == '__main__':
    main()
