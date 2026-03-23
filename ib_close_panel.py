"""
ib_close_panel.py — Small GUI panel to close IB positions.

10 buttons: Close All / Close Puts / Close Calls / Sell Longs / Show Positions  x  Paper / Live

Usage:
    python ib_close_panel.py
"""

import asyncio
import sys
import time
import threading
import tkinter as tk

try:
    from ib_insync import IB, MarketOrder
except ImportError:
    print("ERROR: ib_insync not installed. Run: pip install ib_insync")
    sys.exit(1)

PAPER_PORT = 4002
LIVE_PORT = 4001


def _close_positions(port, filter_right=None):
    """Close positions. filter_right: None=all, 'P'=puts, 'C'=calls, 'LONGS'=all longs."""
    asyncio.set_event_loop(asyncio.new_event_loop())
    ib = IB()
    label = {None: "ALL", "P": "PUTS", "C": "CALLS", "LONGS": "LONGS"}[filter_right]
    mode = "LIVE" if port == LIVE_PORT else "PAPER"
    print(f"\n=== CLOSE {label} — {mode} (port {port}) ===")

    try:
        ib.connect('127.0.0.1', port, clientId=99, timeout=15)
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    print(f"Connected. Account: {ib.managedAccounts()}")

    positions = ib.positions()
    if filter_right == 'LONGS':
        positions = [p for p in positions if p.position > 0]
    elif filter_right:
        positions = [p for p in positions
                     if p.contract.secType == 'OPT'
                     and p.contract.right == filter_right
                     and p.position > 0]
    else:
        positions = [p for p in positions if p.position != 0]

    if not positions:
        print(f"No {label} positions to close.")
        ib.disconnect()
        return

    print(f"{len(positions)} position(s):")
    for p in positions:
        print(f"  {p.contract.localSymbol}  qty={p.position}  avg={p.avgCost}")

    for p in positions:
        qty = abs(int(p.position))
        if qty == 0:
            continue
        action = 'SELL' if p.position > 0 else 'BUY'
        order = MarketOrder(action, qty)
        if not p.contract.exchange:
            p.contract.exchange = 'SMART'

        print(f"  {action} {qty}x {p.contract.localSymbol} ... ", end="", flush=True)
        trade = ib.placeOrder(p.contract, order)

        t_start = time.time()
        while time.time() - t_start < 30:
            ib.sleep(0.5)
            if trade.orderStatus.status in ('Filled', 'Cancelled', 'ApiCancelled'):
                break

        status = trade.orderStatus.status
        fill_price = trade.orderStatus.avgFillPrice or '---'
        print(f"{status} @ {fill_price}")

    ib.sleep(2)
    remaining = ib.positions()
    if filter_right == 'LONGS':
        remaining = [p for p in remaining if p.position > 0]
    elif filter_right:
        remaining = [p for p in remaining
                     if p.contract.secType == 'OPT'
                     and p.contract.right == filter_right
                     and p.position > 0]
    else:
        remaining = [p for p in remaining if p.position != 0]

    if not remaining:
        print(f"All {label} positions closed.")
    else:
        print(f"WARNING: {len(remaining)} position(s) still open.")

    ib.disconnect()
    print("Disconnected.")


def _show_positions(port):
    """Connect, fetch all open positions, print to terminal and show popup."""
    asyncio.set_event_loop(asyncio.new_event_loop())
    ib = IB()
    mode = "LIVE" if port == LIVE_PORT else "PAPER"
    print(f"\n=== POSITIONS — {mode} (port {port}) ===")

    try:
        ib.connect('127.0.0.1', port, clientId=98, timeout=15)
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    positions = [p for p in ib.positions() if p.position != 0]

    if not positions:
        print("No open positions.")
        ib.disconnect()
        return

    for p in positions:
        sym = p.contract.localSymbol
        right = getattr(p.contract, 'right', '')
        side = "CALL" if right == 'C' else "PUT" if right == 'P' else ""
        direction = "LONG" if p.position > 0 else "SHORT"
        print(f"  {sym}  {side} {direction}  qty={int(p.position)}  avg={p.avgCost:.2f}")

    ib.disconnect()
    print(f"{len(positions)} position(s) total. Disconnected.")


def _run_in_thread(port, filter_right, btn):
    """Run close operation in a background thread so the GUI doesn't freeze."""
    btn.config(state=tk.DISABLED)
    try:
        _close_positions(port, filter_right)
    finally:
        btn.config(state=tk.NORMAL)


def _run_show_thread(port, btn):
    btn.config(state=tk.DISABLED)
    try:
        _show_positions(port)
    finally:
        btn.config(state=tk.NORMAL)


def make_show_btn(parent, text, bg, port, row, col):
    btn = tk.Button(parent, text=text, width=18, height=2,
                    bg=bg, fg='white', font=('Arial', 10, 'bold'),
                    activebackground=bg, activeforeground='white')
    btn.config(command=lambda: threading.Thread(
        target=_run_show_thread, args=(port, btn), daemon=True
    ).start())
    btn.grid(row=row, column=col, padx=4, pady=4)
    return btn


def make_btn(parent, text, bg, port, filter_right, row, col):
    btn = tk.Button(parent, text=text, width=18, height=2,
                    bg=bg, fg='white', font=('Arial', 10, 'bold'),
                    activebackground=bg, activeforeground='white')
    btn.config(command=lambda: threading.Thread(
        target=_run_in_thread, args=(port, filter_right, btn), daemon=True
    ).start())
    btn.grid(row=row, column=col, padx=4, pady=4)
    return btn


def main():
    root = tk.Tk()
    root.title("IB Close Panel")
    root.resizable(False, False)

    # Header labels
    tk.Label(root, text="PAPER", font=('Arial', 11, 'bold'),
             fg='#C62828').grid(row=0, column=0, columnspan=4, pady=(8, 0))
    tk.Label(root, text="LIVE", font=('Arial', 11, 'bold'),
             fg='#1565C0').grid(row=2, column=0, columnspan=4, pady=(8, 0))

    paper_bg = '#1976D2'
    live_bg = '#D32F2F'
    show_bg = '#616161'

    # Paper row
    make_btn(root, "Close All",   paper_bg, PAPER_PORT, None, 1, 0)
    make_btn(root, "Close Puts",  paper_bg, PAPER_PORT, 'P',  1, 1)
    make_btn(root, "Close Calls", paper_bg, PAPER_PORT, 'C',  1, 2)
    make_btn(root, "Sell Longs",  paper_bg, PAPER_PORT, 'LONGS', 1, 3)
    make_show_btn(root, "Show Positions", '#D32F2F', PAPER_PORT, 1, 4)

    # Live row
    make_btn(root, "Close All",   live_bg, LIVE_PORT, None, 3, 0)
    make_btn(root, "Close Puts",  live_bg, LIVE_PORT, 'P',  3, 1)
    make_btn(root, "Close Calls", live_bg, LIVE_PORT, 'C',  3, 2)
    make_btn(root, "Sell Longs",  live_bg, LIVE_PORT, 'LONGS', 3, 3)
    make_show_btn(root, "Show Positions", '#1976D2', LIVE_PORT, 3, 4)

    root.mainloop()


if __name__ == '__main__':
    main()
