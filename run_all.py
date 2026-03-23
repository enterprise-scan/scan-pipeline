import glob
import subprocess
import sys

files = sorted(glob.glob("IB_2026_SOFI_360k-1800k_x4__5s_5s_aggregates/IB_????????_SOFI_360k-1800k_x4__5s_5s_aggregate.csv"))

for i, f in enumerate(files):
    prev = files[i - 1] if i > 0 else None
    day = f.split("IB_")[2][:8]

    cmd = ["python", "scan.py", f]
    if prev:
        cmd.append(prev)

    print(f"=== {day} (prev={'yes' if prev else 'no'}) ===")
    result = subprocess.run(cmd, capture_output=True, text=True)
    # print only summary lines
    for line in result.stdout.splitlines():
        if line.startswith("Done") or line.startswith("Trades") or "closed trades" in line:
            print(line)
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr[-200:]}")
    print()
