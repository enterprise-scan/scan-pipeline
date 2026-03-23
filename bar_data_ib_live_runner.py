#!/usr/bin/env python3
"""
IB Live Runner - Fetch raw ticks from IB, build 360K volume bars.

Catchup: paginate raw ticks from market open to now, then step through.
Live: refetch latest ticks every 5s.

Usage:
    python bar_data_ib_live_runner.py --paper
    python bar_data_ib_live_runner.py
    python bar_data_ib_live_runner.py --step-size 2
"""

import argparse
import os
import signal
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from ib_insync import IB

sys.stdout.reconfigure(line_buffering=True)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Configuration
STEP_SIZE = 2
MAX_STEPS = 11700
VOLUMES = [720000, 1440000]

# Market open = 9:30 AM US/Eastern
# Auto-detect DST: Summer (Mar-Nov) = 13:30 UTC, Winter (Nov-Mar) = 14:30 UTC
import zoneinfo

def _market_open_utc(date_str):
    """Return (hour, minute) in UTC for 9:30 AM Eastern on the given date."""
    from datetime import datetime as _dt
    eastern = zoneinfo.ZoneInfo("America/New_York")
    d = _dt.strptime(date_str, '%Y-%m-%d')
    local_open = d.replace(hour=9, minute=30, second=0, tzinfo=eastern)
    utc_open = local_open.astimezone(zoneinfo.ZoneInfo("UTC"))
    return utc_open.hour, utc_open.minute

# Defaults (overridden per-date in run())
MARKET_OPEN_HOUR = 13
MARKET_OPEN_MIN = 30
MARKET_OPEN_SEC = 0

# Import modules
import importlib.util

_fetcher_path = os.path.join(SCRIPT_DIR, 'bar_data_module_1-ib_fetcher.py')
_fetcher_spec = importlib.util.spec_from_file_location("ib_fetcher", _fetcher_path)
_fetcher_module = importlib.util.module_from_spec(_fetcher_spec)
_fetcher_spec.loader.exec_module(_fetcher_module)
fetch_ticks = _fetcher_module.fetch_ticks
fetch_5sec_bars = _fetcher_module.fetch_5sec_bars
set_ticker = _fetcher_module.set_ticker

_pipeline_path = os.path.join(SCRIPT_DIR, 'bar_data_module_2-ib_volume_bar_pipeline.py')
_pipeline_spec = importlib.util.spec_from_file_location("ib_volume_bar_pipeline", _pipeline_path)
_pipeline_module = importlib.util.module_from_spec(_pipeline_spec)
_pipeline_spec.loader.exec_module(_pipeline_module)
format_prefix = _pipeline_module.format_prefix
compute_volume_bars = _pipeline_module.compute_volume_bars


PAPER_DELAY_MIN = 15  # IB paper account data delay

def get_market_open_ts(date_str, paper=False):
    h, m = _market_open_utc(date_str)
    date = datetime.strptime(date_str, '%Y-%m-%d')
    market_open = datetime(date.year, date.month, date.day, h, m, 0,
                          tzinfo=timezone.utc)
    ts = int(market_open.timestamp() * 1000)
    if paper:
        ts += PAPER_DELAY_MIN * 60 * 1000
    return ts


def get_step_timestamp(market_open_ts, step):
    return market_open_ts + (step - 1) * STEP_SIZE * 1000


def run(date_str, port, max_step=None, volumes=None, use_bars5s=False, skip_step_files=False, core_only=False, paper=False):
    if volumes is None:
        volumes = VOLUMES

    date_fmt = date_str.replace('-', '')
    if len(volumes) > 3:
        vol_str = f'{format_prefix(min(volumes))}-{format_prefix(max(volumes))}_x{len(volumes)}'
    else:
        vol_str = '_'.join([format_prefix(v) for v in volumes])
    data_tag = '5s' if use_bars5s else 'tick'
    run_id = f'IB_{date_fmt}_{TICKER}_{vol_str}__{STEP_SIZE}s_{data_tag}'
    run_dir = os.path.join(SCRIPT_DIR, 'run-data', run_id)

    bar_dir = os.path.join(run_dir, 'bar')
    os.makedirs(bar_dir, exist_ok=True)

    market_open_ts = get_market_open_ts(date_str, paper=paper)
    market_open_dt = datetime.fromtimestamp(market_open_ts / 1000, tz=timezone.utc)
    step_limit = max_step if max_step else MAX_STEPS

    mode_str = "PAPER" if port == 4002 else "LIVE"
    print(f"Run ID: {run_id}")
    print(f"Output: {run_dir}")
    print(f"IB: {mode_str} (port {port})")
    print(f"Max steps: {step_limit}, step size: {STEP_SIZE}s")
    print(f"Market open: {market_open_dt}")
    print("=" * 60)

    # Persistent IB connection
    ib = IB()
    print("Connecting to IB...")
    try:
        ib.connect('127.0.0.1', port, clientId=_fetcher_module.CLIENT_ID, timeout=15)
    except Exception as e:
        print(f"Connection failed: {e}")
        return
    ib.reqMarketDataType(3)
    print("Connected.")

    running = True

    def shutdown(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)

    # Fetch data from market open
    start_dt = market_open_dt.strftime('%Y%m%d %H:%M:%S UTC')
    if use_bars5s:
        print(f"Fetching 5-sec bars from {start_dt}...")
        raw_data = fetch_5sec_bars(ib, start_dt)
        total_vol = sum(t['v'] for t in raw_data)
        print(f"Fetched {len(raw_data)} bars, total volume: {total_vol:,}")
    else:
        print(f"Fetching ticks from {start_dt}...")
        raw_data = fetch_ticks(ib, start_dt)
        total_vol = sum(t['size'] for t in raw_data)
        print(f"Fetched {len(raw_data)} ticks, total volume: {total_vol:,}")
    last_fetch_ts = int(datetime.now(timezone.utc).timestamp() * 1000)

    if not raw_data:
        print("[RUNNER] No data fetched. Exiting.")
        ib.disconnect()
        return

    agg_df = None
    aggregate_file = os.path.join(run_dir, f'{run_id}_aggregate.csv')

    step = 1

    while running and step <= step_limit:
        step_ts = get_step_timestamp(market_open_ts, step)
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        # If step is in the future, wait
        if step_ts > now_ms:
            wait_sec = (step_ts - now_ms) / 1000 + 0.5
            step_dt = datetime.fromtimestamp(step_ts / 1000, tz=timezone.utc)
            print(f"Step {step}: Waiting {wait_sec:.1f}s for {step_dt.strftime('%H:%M:%S')} UTC")
            time.sleep(min(wait_sec, 5))
            continue

        is_catchup = step_ts < now_ms - 10000

        # Live: refetch latest data every 5s and merge
        if not is_catchup and now_ms - last_fetch_ts > 5000:
            last_data_dt = datetime.fromtimestamp(
                raw_data[-1]['t'] / 1000, tz=timezone.utc
            ).strftime('%Y%m%d %H:%M:%S UTC')
            if use_bars5s:
                new_data = fetch_5sec_bars(ib, last_data_dt)
                if new_data:
                    seen = {t['t'] for t in raw_data[-2000:]}
                    for bar in new_data:
                        if bar['t'] not in seen:
                            raw_data.append(bar)
                            seen.add(bar['t'])
            else:
                new_data = fetch_ticks(ib, last_data_dt)
                if new_data:
                    seen = {(t['t'], t['price'], t['size']) for t in raw_data[-5000:]}
                    for tick in new_data:
                        key = (tick['t'], tick['price'], tick['size'])
                        if key not in seen:
                            raw_data.append(tick)
                            seen.add(key)
            last_fetch_ts = now_ms

        # Cut data at step timestamp
        cut_data = [tick for tick in raw_data if tick['t'] <= step_ts]

        if not cut_data:
            print(f"Step {step}: No data, skipping")
            step += 1
            continue

        # Build volume bars
        bar_row = compute_volume_bars(cut_data, volumes, step_ts)
        if not bar_row:
            print(f"Step {step}: Failed to build volume bar")
            step += 1
            continue

        bar_row['step'] = step

        if core_only:
            core_keys = {'timestamp', 'last_raw_ts', 'step'}
            for vol in volumes:
                prefix = format_prefix(vol)
                core_keys.update({f'{prefix}_open', f'{prefix}_high', f'{prefix}_low', f'{prefix}_close'})
            bar_row = {k: v for k, v in bar_row.items() if k in core_keys}

        # Save single bar row
        bar_file = os.path.join(bar_dir, f'step_{step:05d}.csv')
        pd.DataFrame([bar_row]).to_csv(bar_file, index=False)

        # Update aggregate in memory
        new_row_df = pd.DataFrame([bar_row])
        if agg_df is None:
            agg_df = new_row_df
        else:
            agg_df = pd.concat([agg_df, new_row_df], ignore_index=True)

        # Save per-step aggregate (full aggregate up to this step)
        if not skip_step_files:
            step_agg_file = os.path.join(run_dir, f'step_{step:05d}.csv')
            agg_df.to_csv(step_agg_file, index=False)

        # Save aggregate
        agg_df.to_csv(aggregate_file, index=False)

        # Log
        step_dt = datetime.fromtimestamp(step_ts / 1000, tz=timezone.utc)
        close_val = bar_row.get('360k_close', 'N/A')
        high_val = bar_row.get('360k_high', 'N/A')
        low_val = bar_row.get('360k_low', 'N/A')
        now_str = datetime.now().strftime('%H:%M:%S')
        mode = "CATCHUP" if is_catchup else "LIVE"

        if mode == "CATCHUP" and step % 100 == 0:
            print(f"[{now_str}] [{mode}] Step {step}: ts={step_dt.strftime('%H:%M:%S')} "
                  f"close={close_val}  ticks={len(cut_data)}")
        elif mode == "LIVE":
            print(f"[{now_str}] [{mode}] Step {step}: ts={step_dt.strftime('%H:%M:%S')} "
                  f"close={close_val}  high={high_val}  low={low_val}  "
                  f"ticks={len(cut_data)}")

        step += 1

    # Cleanup
    print("\n" + "=" * 60)
    ib.disconnect()
    print(f"Complete. {step - 1} steps processed.")
    print(f"Output: {run_dir}")


def main():
    parser = argparse.ArgumentParser(description='IB Live Runner - Volume bars from IB data')
    parser.add_argument('--paper', action='store_true', help='Paper account (port 4002)')
    parser.add_argument('--bars5s', action='store_true', help='Use 5-sec bars instead of raw ticks')
    parser.add_argument('--max-step', type=int, help='Stop after this step')
    parser.add_argument('--volumes', type=str, default='720000,1440000',
                       help='Comma-separated volume thresholds (default: 720000,1440000)')
    parser.add_argument('--step-size', type=int, default=2,
                       help='Seconds per step (default: 2)')
    parser.add_argument('--date', type=str, default=None,
                       help='Date to run for, YYYY-MM-DD (default: today)')
    parser.add_argument('--symbol', type=str, default='SOFI',
                       help='Ticker symbol (default: SOFI)')
    parser.add_argument('--no-step-files', action='store_true',
                       help='Skip writing per-step aggregate CSVs (step_NNNNN.csv)')
    parser.add_argument('--core-only', action='store_true',
                       help='Only output core columns: timestamp, last_raw_ts, OHLC, step')

    args = parser.parse_args()

    global STEP_SIZE, MAX_STEPS
    if args.bars5s and args.step_size == 2:
        STEP_SIZE = 5
    else:
        STEP_SIZE = args.step_size
    MAX_STEPS = int(6.5 * 3600 / STEP_SIZE)

    port = 4002 if args.paper else 4001
    date_str = args.date if args.date else datetime.now().strftime('%Y-%m-%d')
    volumes = [int(v.strip()) for v in args.volumes.split(',')]
    global TICKER
    TICKER = args.symbol
    set_ticker(args.symbol)

    run(date_str, port, args.max_step, volumes, use_bars5s=args.bars5s,
        skip_step_files=args.no_step_files, core_only=args.core_only, paper=args.paper)


if __name__ == '__main__':
    main()
