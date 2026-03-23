import pandas as pd
import glob
import sys

folder = sys.argv[1] if len(sys.argv) > 1 else "IB_2026_SOFI_360k-1800k_x4__5s_5s_aggregates"
sl_levels = [0.01, 0.02, 0.03, 0.04, 0.05]

files = sorted(glob.glob(f"{folder}/IB_????????_SOFI_*_aggregate_trades.csv"))

header = f"{'Date':>8} | {'Tr':>3} | {'NoSL':>7} | {'dPnL':>7}"
for sl in sl_levels:
    header += f" | {'SL'+str(sl):>7}"
print(header)
print("-" * len(header))

totals = {"nosl": 0, "dpnl": 0}
for sl in sl_levels:
    totals[sl] = 0

for f in files:
    day = f.split("IB_")[2][:8]
    df = pd.read_csv(f)
    closed = df[df["exit_reason"] != "open"]
    if len(closed) == 0:
        continue

    orig_pnl = closed["pnl"].sum()
    dpnl = closed["d_pnl"].dropna().sum()
    totals["nosl"] += orig_pnl
    totals["dpnl"] += dpnl

    row = f"{day} | {len(closed):3d} | {orig_pnl:+7.2f} | {dpnl:+7.2f}"

    for sl in sl_levels:
        col_step = f"sl{sl}_step"
        col_exit = f"sl{sl}_exit_pnl"

        if col_step not in closed.columns:
            row += " |     N/A"
            continue

        # hit = trades that touched this SL level -> exit at next step price
        hit = closed[closed[col_step].notna()]
        not_hit = closed[closed[col_step].isna()]

        # hit trades: use sl exit pnl (price at step+1 after SL trigger)
        # not hit trades: use delayed pnl (step+1 on normal exit)
        sl_pnl = hit[col_exit].sum() + not_hit["d_pnl"].dropna().sum()

        totals[sl] += sl_pnl
        row += f" | {sl_pnl:+7.2f}"

    print(row)

print("-" * len(header))
row = f"{'TOTAL':>8} | {'':>3} | {totals['nosl']:+7.2f} | {totals['dpnl']:+7.2f}"
for sl in sl_levels:
    row += f" | {totals[sl]:+7.2f}"
print(row)

print(f"\n{len(files)} days")
