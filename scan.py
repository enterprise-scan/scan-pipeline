import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import cv2
from scipy.signal import find_peaks
from scipy.ndimage import gaussian_filter1d
from multiprocessing import Pool, cpu_count
import sys
import io

WMA_PERIOD = 55


def compute_midline(df):
    if "720k_high" in df.columns and "1440k_high" in df.columns:
        cur = df["720k_high"] + df["720k_low"] + df["1440k_high"] + df["1440k_low"]
        prev = cur.shift(1)
        df["midline"] = (cur + prev) / 8.0
        df.loc[df.index[0], "midline"] = cur.iloc[0] / 4.0
        return "midline"
    else:
        return [c for c in df.columns if c.lower().endswith("close")][0]


def compute_wma(series, period=WMA_PERIOD):
    """Compute WMA over a pandas Series or numpy array. Returns numpy array."""
    weights = np.arange(1, period + 1, dtype=float)
    weight_sum = weights.sum()
    return pd.Series(series).rolling(period).apply(
        lambda vals: np.dot(vals, weights) / weight_sum, raw=True
    ).values


def load_and_compute_wma(csv_path, prev_csv_path=None):
    """Load aggregate CSV, optionally prepend previous day for WMA warmup.
    Returns (df, wma55_full, close_values, close_col, prepend_rows).
    """
    df = pd.read_csv(csv_path)
    prepend_rows = 0

    if prev_csv_path:
        prev_df = pd.read_csv(prev_csv_path)
        tail = prev_df.tail(WMA_PERIOD).copy()
        prepend_rows = len(tail)
        df = pd.concat([tail, df], ignore_index=True)

    close_col = compute_midline(df)
    close_values = df[close_col].values
    wma55_full = compute_wma(df[close_col])

    return df, wma55_full, close_values, close_col, prepend_rows


def render_to_image(full_series_arr, total_len):
    valid = ~np.isnan(full_series_arr)
    if valid.sum() < 3:
        return None

    fig = plt.figure(figsize=(10, 5))
    ax = plt.gca()
    x = np.arange(total_len)
    ax.plot(x, full_series_arr)
    ax.set_xlim(0, total_len - 1)
    ymin = np.nanmin(full_series_arr[valid])
    ymax = np.nanmax(full_series_arr[valid])
    ax.set_ylim(ymin, ymax)
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", pad_inches=0)
    plt.close(fig)
    buf.seek(0)
    arr = np.frombuffer(buf.read(), dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    return img


def detect_signal(img):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    edges = cv2.Canny(gray, 50, 150)

    h, w = edges.shape
    y_vals = []
    for xx in range(w):
        ys = np.where(edges[:, xx] > 0)[0]
        y_vals.append(np.mean(ys) if len(ys) else np.nan)

    y_vals = np.array(y_vals)
    valid_cols = ~np.isnan(y_vals)
    x_vals = np.arange(w)[valid_cols]
    y_vals = y_vals[valid_cols]

    if len(y_vals) < 3:
        return None

    y_smooth = gaussian_filter1d(y_vals, sigma=1)

    bottoms, _ = find_peaks(y_smooth, distance=15, prominence=5)
    tops, _ = find_peaks(-y_smooth, distance=15, prominence=5)

    added_right_edge = None
    if len(y_smooth) >= 3:
        if y_smooth[-1] > y_smooth[-2] and y_smooth[-2] < y_smooth[-3]:
            added_right_edge = len(y_smooth) - 2
        elif y_smooth[-1] < y_smooth[-2]:
            added_right_edge = len(y_smooth) - 1

    if added_right_edge is not None:
        bottoms = np.append(bottoms, added_right_edge)

    bottoms = np.unique(bottoms)

    rightmost_top = int(tops[-1]) if len(tops) else None
    rightmost_bottom = int(bottoms[-1]) if len(bottoms) else None

    if rightmost_top is None and rightmost_bottom is None:
        return None

    if rightmost_top is None:
        return "call"
    elif rightmost_bottom is None:
        return "put"
    elif rightmost_bottom > rightmost_top:
        return "call"
    else:
        return "put"


def process_step(args):
    row_idx, step_value, raw_close, wma_value, wma_slice, total_len = args

    # build full_series: wma values up to row_idx, NaN after
    full_series_arr = np.full(total_len, np.nan)
    full_series_arr[:len(wma_slice)] = wma_slice

    img = render_to_image(full_series_arr, total_len)
    if img is None:
        return {"step": int(step_value), "close": raw_close, "wma55": wma_value, "reading": None}

    signal = detect_signal(img)
    return {"step": int(step_value), "close": raw_close, "wma55": wma_value, "reading": signal}


def check_pattern(hist, is_first_trade):
    """Check reading history for entry pattern.
    First trade: immediate entry on first reading.
    Subsequent: PPCC→call, CCPP→put."""
    if is_first_trade:
        if len(hist) < 1:
            return None
        return hist[-1]
    else:
        if len(hist) < 4:
            return None
        last4 = hist[-4:]
        if last4 == ["put", "put", "call", "call"]:
            return "call"
        if last4 == ["call", "call", "put", "put"]:
            return "put"
        return None


SL_LEVELS = [0.01, 0.02, 0.03, 0.04, 0.05]


def run_trades(out_df, stop_loss=None):
    """Run trade logic. stop_loss=None means no stop loss."""
    position = None
    entry_close = None
    entry_wma = None
    stopped_out = False
    reading_history = []
    total_pnl = 0.0
    first_trade = True
    trades_list = []
    trade_num = 0
    current_trade = None

    def open_trade(trade_num, side, step, close, wma):
        t = {
            "trade": trade_num, "side": side,
            "entry_step": step, "entry_close": close, "entry_wma55": wma,
            "exit_step": None, "exit_close": None, "exit_wma55": None,
            "exit_reason": None, "hold_steps": 0,
            "min_pnl": 0.0, "max_pnl": 0.0,
            "pnl": None, "total_pnl": None,
        }
        for sl in SL_LEVELS:
            t[f"sl{sl}_step"] = None
        return t

    def update_minmax(trade, close, step):
        if trade["side"] == "call":
            unrealized = close - trade["entry_close"]
        else:
            unrealized = trade["entry_close"] - close
        unrealized = round(unrealized, 4)
        if unrealized < trade["min_pnl"]:
            trade["min_pnl"] = unrealized
        if unrealized > trade["max_pnl"]:
            trade["max_pnl"] = unrealized
        for sl in SL_LEVELS:
            if trade[f"sl{sl}_step"] is None and unrealized <= -sl:
                trade[f"sl{sl}_step"] = step

    def close_trade(trade, step, close, wma, reason, total_pnl):
        update_minmax(trade, close, step)
        trade["min_pnl"] = round(trade["min_pnl"], 4)
        trade["max_pnl"] = round(trade["max_pnl"], 4)
        trade["exit_step"] = step
        trade["exit_close"] = close
        trade["exit_wma55"] = wma
        trade["exit_reason"] = reason
        if trade["side"] == "call":
            trade["pnl"] = round(close - trade["entry_close"], 4)
        else:
            trade["pnl"] = round(trade["entry_close"] - close, 4)
        total_pnl += trade["pnl"]
        trade["total_pnl"] = round(total_pnl, 4)
        return total_pnl

    def check_stop(position, entry_close, close, stop_loss):
        if stop_loss is None or position is None:
            return False
        if position == "call":
            return (close - entry_close) <= -stop_loss
        else:
            return (entry_close - close) <= -stop_loss

    for i, row in out_df.iterrows():
        reading = row["reading"]
        close = row["close"]
        wma = row["wma55"]

        if reading is None:
            # check stop loss during None
            if position is not None and check_stop(position, entry_close, close, stop_loss):
                total_pnl = close_trade(current_trade, row["step"], close, wma, "stop_loss", total_pnl)
                trades_list.append(current_trade)
                current_trade = None
                stopped_out = True
                position = None
                entry_close = None
                entry_wma = None
            elif current_trade is not None:
                update_minmax(current_trade, close, row["step"])
                current_trade["hold_steps"] += 1
            continue

        reading_history.append(reading)
        pattern = check_pattern(reading_history, first_trade)

        # check stop loss on reading steps
        just_stopped = False
        if position is not None and check_stop(position, entry_close, close, stop_loss):
            total_pnl = close_trade(current_trade, row["step"], close, wma, "stop_loss", total_pnl)
            trades_list.append(current_trade)
            current_trade = None
            stopped_out = True
            just_stopped = True
            position = None
            entry_close = None
            entry_wma = None

        # stopped out or no position — wait for pattern (not same step as stop)
        if position is None:
            if pattern is not None and not just_stopped:
                if first_trade:
                    first_trade = False
                stopped_out = False
                trade_num += 1
                position = pattern
                entry_close = close
                entry_wma = wma
                current_trade = open_trade(trade_num, pattern, row["step"], close, wma)
            continue

        # in position — check for flip
        if pattern is not None and pattern != position:
            total_pnl = close_trade(current_trade, row["step"], close, wma, "signal_flip", total_pnl)
            trades_list.append(current_trade)
            trade_num += 1
            position = pattern
            entry_close = close
            entry_wma = wma
            current_trade = open_trade(trade_num, pattern, row["step"], close, wma)
            continue

        # hold
        if current_trade is not None:
            update_minmax(current_trade, close, row["step"])
            current_trade["hold_steps"] += 1

    # close open trade at end
    if current_trade is not None:
        last = out_df.iloc[-1]
        update_minmax(current_trade, last["close"], last["step"])
        current_trade["exit_step"] = last["step"]
        current_trade["exit_close"] = last["close"]
        current_trade["exit_wma55"] = last["wma55"]
        current_trade["exit_reason"] = "open"
        if current_trade["side"] == "call":
            current_trade["pnl"] = round(last["close"] - current_trade["entry_close"], 4)
        else:
            current_trade["pnl"] = round(current_trade["entry_close"] - last["close"], 4)
        current_trade["min_pnl"] = round(current_trade["min_pnl"], 4)
        current_trade["max_pnl"] = round(current_trade["max_pnl"], 4)
        current_trade["total_pnl"] = round(total_pnl + current_trade["pnl"], 4)
        trades_list.append(current_trade)

    trades_df = pd.DataFrame(trades_list)

    # delayed execution columns
    step_to_close = dict(zip(out_df["step"], out_df["close"]))
    step_to_reading = dict(zip(out_df["step"], out_df["reading"]))

    d_cols = {"d_entry_step": [], "d_entry_close": [], "d_exit_step": [], "d_exit_close": [], "d_pnl": [], "d_total_pnl": []}
    d_total = 0.0

    sl_cols = {}
    for sl in SL_LEVELS:
        sl_cols[f"sl{sl}_next_step"] = []
        sl_cols[f"sl{sl}_next_close"] = []
        sl_cols[f"sl{sl}_next_reading"] = []
        sl_cols[f"sl{sl}_exit_pnl"] = []

    for _, t in trades_df.iterrows():
        e_step = int(t["entry_step"]) + 1
        x_step = int(t["exit_step"]) + 1 if pd.notna(t["exit_step"]) else None
        e_close = step_to_close.get(e_step)
        x_close = step_to_close.get(x_step) if x_step else None
        d_cols["d_entry_step"].append(e_step)
        d_cols["d_entry_close"].append(e_close)
        d_cols["d_exit_step"].append(x_step)
        d_cols["d_exit_close"].append(x_close)
        if e_close is not None and x_close is not None:
            dpnl = round((x_close - e_close) if t["side"] == "call" else (e_close - x_close), 4)
            d_total += dpnl
            d_cols["d_pnl"].append(dpnl)
            d_cols["d_total_pnl"].append(round(d_total, 4))
        else:
            d_cols["d_pnl"].append(None)
            d_cols["d_total_pnl"].append(round(d_total, 4))

        for sl in SL_LEVELS:
            sl_step = t.get(f"sl{sl}_step")
            if pd.notna(sl_step) and sl_step is not None:
                sl_step = int(sl_step)
                next_step = sl_step + 1
                next_close = step_to_close.get(next_step)
                next_reading = step_to_reading.get(next_step)
                sl_cols[f"sl{sl}_next_step"].append(next_step)
                sl_cols[f"sl{sl}_next_close"].append(next_close)
                sl_cols[f"sl{sl}_next_reading"].append(next_reading)
                if next_close is not None:
                    if t["side"] == "call":
                        exit_pnl = round(next_close - t["entry_close"], 4)
                    else:
                        exit_pnl = round(t["entry_close"] - next_close, 4)
                    sl_cols[f"sl{sl}_exit_pnl"].append(exit_pnl)
                else:
                    sl_cols[f"sl{sl}_exit_pnl"].append(None)
            else:
                sl_cols[f"sl{sl}_next_step"].append(None)
                sl_cols[f"sl{sl}_next_close"].append(None)
                sl_cols[f"sl{sl}_next_reading"].append(None)
                sl_cols[f"sl{sl}_exit_pnl"].append(None)

    for k, v in d_cols.items():
        trades_df[k] = v
    for k, v in sl_cols.items():
        trades_df[k] = v

    return trades_df


if __name__ == "__main__":
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "IB_SOFI_360k-7200k_x39_aggregates/IB_20260102_SOFI_360k-7200k_x39__5s_5s_aggregate.csv"
    prev_csv_path = sys.argv[2] if len(sys.argv) > 2 else None

    df, wma55_full, close_values, close_col, prepend_rows = load_and_compute_wma(csv_path, prev_csv_path)
    total_rows = len(df)
    steps = df["step"].values

    # build work items — each step gets all WMA values up to that row
    work = []
    for i in range(len(steps)):
        if np.isnan(wma55_full[i]):
            continue
        if i < prepend_rows:
            continue
        step_value = steps[i]
        raw_close = float(close_values[i])
        wma_value = float(wma55_full[i])
        wma_slice = wma55_full[:i + 1].copy()
        work.append((i, int(step_value), raw_close, wma_value, wma_slice, total_rows))

    print(f"Processing {len(work)} steps ({prepend_rows} warmup rows from prev day) across {cpu_count()} cores...", file=sys.stderr)

    num_workers = max(1, cpu_count() - 1)
    results = []

    for i in range(prepend_rows, len(steps)):
        if np.isnan(wma55_full[i]):
            results.append({"step": int(steps[i]), "close": float(close_values[i]), "wma55": None, "reading": None})

    with Pool(num_workers) as pool:
        for i, result in enumerate(pool.imap(process_step, work, chunksize=10)):
            results.append(result)
            if (i + 1) % 100 == 0:
                print(f"  {i + 1}/{len(work)} steps done...", file=sys.stderr)

    out_df = pd.DataFrame(results)
    out_df = out_df.sort_values("step").reset_index(drop=True)

    trades_df = run_trades(out_df, stop_loss=None)

    out_path = csv_path.replace(".csv", "_signals2.csv")
    out_df.to_csv(out_path, index=False)

    trades_path = csv_path.replace(".csv", "_trades.csv")
    trades_df.to_csv(trades_path, index=False)

    closed = trades_df[trades_df["exit_reason"] != "open"]
    w = (closed["pnl"] > 0).sum()
    l = (closed["pnl"] < 0).sum()
    e = (closed["pnl"] == 0).sum()
    pnl = closed["pnl"].sum()
    dpnl = closed["d_pnl"].dropna().sum()

    print(f"Done. {len(out_df)} steps -> {out_path}")
    print(f"Trades: {len(trades_df)} -> {trades_path}")
    print(f"\nSignal counts:")
    print(out_df["reading"].value_counts(dropna=False).to_string())
    print(f"\n{len(closed)} closed trades (W={w} L={l} E={e}) PnL={pnl:+.4f} dPnL={dpnl:+.4f}")
    print(f"\n{'='*90}")
    print(trades_df.to_string(index=False))
    print(f"{'='*90}")
