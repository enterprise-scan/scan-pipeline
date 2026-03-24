# Scan Pipeline

WMA image-based signal detection pipeline for live stock/option trading via Interactive Brokers.

## Architecture

```
IB Gateway (TWS)
      |
      v
bar_data_ib_live_runner.py          <-- fetches ticks/bars, builds volume bars per step
      |
      v
run-data/{RUN_ID}/
  |- bar/step_NNNNN.csv             <-- individual step files
  |- {RUN_ID}_aggregate.csv         <-- growing aggregate (all steps)
      |
      v
trade_map_plotter_v34_lite_action.py  <-- GUI: reads aggregate, detects signals, fires orders
      |
      +-- scan.py                    <-- WMA image detector module
      +-- ib_broker.py               <-- IB order execution (stock + option)
      +-- ib_close_panel.py          <-- emergency position closer
```

Batch analysis (post-day):
```
run_all.py          <-- runs scan.py across multiple daily aggregates
sl_analysis.py      <-- aggregates stop-loss scenarios across days
```

## Signal Detection: How It Works

### 1. Volume Bar Construction

The bar generator fetches raw tick data (or 5-second bars) from IB and constructs volume bars at configurable thresholds. Default: **720k** and **1440k** shares per bar.

Each step (every 5 seconds) cuts the accumulated tick data at the step timestamp and builds a volume bar by walking backwards from the latest tick until the volume threshold is reached. This produces OHLC values for each resolution.

### 2. Midline Computation

From the two volume bar resolutions, a midline is computed:

```
midline = (720k_high + 720k_low + 1440k_high + 1440k_low + prev_sum) / 8
```

This smoothed price level serves as the input to the WMA.

### 3. Weighted Moving Average (WMA-55)

A 55-period Weighted Moving Average is computed over the midline:

```
weights = [1, 2, 3, ..., 55]
WMA = dot(last_55_values, weights) / sum(weights)
```

The WMA curve is the core signal — its shape determines whether we're in a call or put regime.

### 4. Image Rendering

For each step, the entire WMA series (up to the current step) is rendered as a matplotlib line chart:
- 10x5 inch figure, no axes, no labels
- Just the WMA curve on a white background
- Rendered to an in-memory PNG, decoded to a cv2 image array

### 5. Edge Detection and Peak Finding

The rendered image is processed through computer vision:

1. **Grayscale conversion** (`cv2.cvtColor`)
2. **Canny edge detection** (`cv2.Canny`, thresholds 50/150) — finds the line edges
3. **Column scan** — for each x-pixel, compute the mean y-coordinate of edge pixels
4. **Gaussian smoothing** (`scipy.ndimage.gaussian_filter1d`, sigma=1)
5. **Peak finding** (`scipy.signal.find_peaks`, distance=15, prominence=5):
   - `find_peaks(y_smooth)` — finds **bottoms** (valleys, local minima in price because y-axis is inverted)
   - `find_peaks(-y_smooth)` — finds **tops** (peaks, local maxima)
6. **Right-edge handling** — detects nascent peaks/valleys at the rightmost edge that haven't fully formed

### 6. Signal Decision

The signal is determined by which feature is rightmost in the image:

| Condition | Signal |
|-----------|--------|
| Rightmost bottom > rightmost top | **call** (uptrend, price bouncing off bottom) |
| Rightmost top > rightmost bottom | **put** (downtrend, price rejecting from top) |
| Only bottoms found | **call** |
| Only tops found | **put** |
| Neither found | **None** (no signal) |

### 7. Pattern Matching (Trade Entry)

Signals feed into a pattern matcher that determines when to enter/exit trades:

- **First trade**: Immediate entry on the first non-None reading
- **Subsequent trades**: Requires a 4-reading confirmation pattern:
  - `put, put, call, call` (PPCC) -> enter **call** (go long)
  - `call, call, put, put` (CCPP) -> enter **put** (go short)
- **Signal flip**: When pattern changes from call to put (or vice versa), the current position is closed and a new one opened

### 8. Order Execution

On Buy/Sell signals, orders are sent to IB via `ib_broker.py`:

**Stock mode** (two separate orders on flip):
```
Call signal:  execute_stock('Buy',  'SOFI', 100, step, 'CALL')  -> BUY 100 shares
Put signal:   execute_stock('Buy',  'SOFI', 100, step, 'PUT')   -> SELL 100 shares (short)
Flip C->P:    execute_stock('Sell', 'SOFI', 100, step, 'CALL')  -> SELL 100 (close long)
              execute_stock('Buy',  'SOFI', 100, step, 'PUT')   -> SELL 100 (open short)
```

**Option mode** (two separate orders on flip):
```
Call signal:  execute('Buy',  'SOFI260323C00227500', step, 'CALL')
Put signal:   execute('Buy',  'SOFI260323P00230000', step, 'PUT')
Flip:         execute('Sell', old_symbol, step, old_side)
              execute('Buy',  new_symbol, step, new_side)
```

## Trade Tracking

Every trade records the full set of fields from scan.py:

**Core fields**: trade #, side, entry/exit step/close/wma55, exit reason (signal_flip/eod/open), hold steps, min/max PNL, realized PNL, cumulative PNL

**Delayed execution**: d_entry_step, d_entry_close, d_exit_step, d_exit_close, d_pnl, d_total_pnl — computed from step+1 prices to simulate execution delay

**Stop-loss tracking**: For each level (0.01, 0.02, 0.03, 0.04, 0.05):
- `sl{X}_step` — step where unrealized PNL first hit -X
- `sl{X}_next_step/close/reading` — the step+1 data at that point
- `sl{X}_exit_pnl` — what PNL would be if exited at step+1 after SL trigger

## End of Day

At step >= **4600** (configurable `EOD_STEP`), any open trade is closed with reason "eod" and no new trades are opened.

## GUI Controls

| Control | Description |
|---------|-------------|
| **Date** | Select run date / directory |
| **Res** | Volume bar resolution (720k, 1440k, etc.) |
| **Navigation** | Step forward/back (arrows, page up/down) |
| **Exp** | Option expiration date (option mode) |
| **Go Live / Stop** | Start/stop live polling + order execution |
| **IB ON/OFF** | Toggle IB broker connection |
| **Mode** | Stock or Option trading mode |
| **Ticker** | Stock symbol (default: SOFI) |
| **Qty** | Shares per trade (default: 100) |
| **Batch Catchup** | ON: parallel signal detection for fast catch-up. OFF: step-by-step |
| **Save Images** | Write WMA chart PNG per step to `run-dir/images/` |
| **Save Actions** | Write .action file per step to `run-dir/actions/` |

## Batch Analysis

### run_all.py

Runs scan.py across all daily aggregate CSVs in sequence, chaining previous day's data for WMA warmup:

```bash
python run_all.py
```

For each day, produces:
- `*_signals2.csv` — step, close, wma55, reading
- `*_trades.csv` — full trade records with SL/delayed columns

### sl_analysis.py

Aggregates trade results across days, comparing base PNL vs delayed PNL vs various stop-loss scenarios:

```bash
python sl_analysis.py <folder>
```

Output table:
```
    Date | Tr |   NoSL |   dPnL | SL0.01 | SL0.02 | SL0.03 | SL0.04 | SL0.05
20260102 |   5 |  +0.12 |  +0.08 |  +0.05 |  +0.07 |  +0.09 |  +0.10 |  +0.11
   TOTAL |     |  +1.23 |  +0.98 |  +0.67 |  +0.82 |  +0.91 |  +0.95 |  +1.01
```

## Paper Trading Notes

- IB paper account data is **15 minutes delayed**
- Bar generator automatically shifts market open +15min when `--paper` is set
- All data within the paper feed is self-consistent (signals, prices, fills all on same delayed timeline)
- Real market open data arrives at 9:45 AM wall clock

## Pre-Market Bars

```bash
python bar_data_ib_live_runner.py --paper --bars5s --previousbars 60
```

Shifts fetch start back by 60 steps (5 minutes at 5s/step) before market open. Pre-trading data flows into volume bars, giving the WMA-55 a head start so signals begin earlier after open. Total step count stays at 4680.

## Dependencies

Pinned in `requirements.txt`. Vendor wheels archived on `deps` branch.

```bash
pip install -r requirements.txt

# Or from vendor archive:
git checkout deps -- vendor/
pip install --no-index --find-links vendor/ -r requirements.txt
```

| Package | Version | Role |
|---------|---------|------|
| opencv-python | 4.13.0.92 | Canny edge detection, image decode |
| scipy | 1.17.1 | Peak finding, Gaussian smoothing |
| matplotlib | 3.10.8 | WMA line chart rendering |
| numpy | 2.4.2 | Array operations |
| pandas | 3.0.0 | CSV/DataFrame handling |
| ib_insync | 0.9.86 | IB Gateway API |

## Logging

All per-step actions logged to `lite_action_debug.log`:

```
[ACTION] step-0123_close-8.45_callAction-HoldCurrent_callSymbol-SOFI_putAction-NA_putSymbol-NA
[ACTION] step-0456_close-8.52_callAction-Sell_callSymbol-SOFI_putAction-Buy_putSymbol-SOFI
[ACTION] Trade #3 | PNL: +0.2100
```

Both call and put sides recorded on every step regardless of which side is active.
