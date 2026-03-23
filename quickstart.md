# Day Quickstart

## 1. Start IB Gateway

Open IB Gateway (paper: port 4002, live: port 4001). Make sure it's connected.

## 2. Start Volume Bar Generator

```bash
# Paper account, SOFI, default 720k+1440k volumes, 5s bars
python bar_data_ib_live_runner.py --paper --bars5s

# Live account
python bar_data_ib_live_runner.py --bars5s

# Custom date / volumes
python bar_data_ib_live_runner.py --paper --bars5s --date 2026-03-23 --symbol SOFI --volumes 720000,1440000
```

This fetches ticks from market open, builds volume bars every 5s, writes:
- `run-data/{RUN_ID}/bar/step_NNNNN.csv` (per step)
- `run-data/{RUN_ID}/{RUN_ID}_aggregate.csv` (growing file)

Wait for it to start producing steps before launching the detector.

## 3. Start Scan Detector GUI

```bash
# Stock mode (default) - 100 shares SOFI
python trade_map_plotter_v34_lite_action.py --mode stock --ticker SOFI --qty 100

# Option mode
python trade_map_plotter_v34_lite_action.py --mode option
```

### In the GUI:

1. Select today's date from the **Date** dropdown
2. Set **Exp** to the next expiration (option mode only)
3. Verify **Mode** / **Ticker** / **Qty** in the live frame
4. Click **IB ON** to connect broker (toggles paper/live based on `ib_broker.py` CONFIG port)
5. Click **Go Live**

### What happens:

- **Catch-up**: Replays all existing steps (no orders fired), builds signal history
- **Live polling**: Every 1s checks for new step file, runs WMA image detector, fires IB orders on Buy/Sell signals

### Signal logic:

- Per step: compute WMA(55) on midline -> render to image -> Canny edge detection -> P or C
- First trade: immediate entry on first reading
- Subsequent: pattern PPCC -> call (buy long), CCPP -> put (sell short)
- On flip: two orders fired (Sell old position, Buy new position)

### Stop:

Click **Stop** - closes all open IB positions and disconnects.

## 4. End of Day / Batch Analysis

```bash
# Run scan across all daily aggregates
python run_all.py

# Stop loss analysis across days
python sl_analysis.py IB_2026_SOFI_720k-1440k_x2__5s_5s_aggregates
```

## Files

| File | Role |
|------|------|
| `bar_data_ib_live_runner.py` | IB data fetcher, builds volume bars |
| `scan.py` | WMA image detector module (also standalone batch) |
| `trade_map_plotter_v34_lite_action.py` | GUI: live detector + IB order execution |
| `run_all.py` | Batch: run scan across multiple days |
| `sl_analysis.py` | Batch: aggregate stop loss analysis |
| `ib_broker.py` | IB order execution (stock + option) |
| `ib_close_panel.py` | Emergency position closer |
