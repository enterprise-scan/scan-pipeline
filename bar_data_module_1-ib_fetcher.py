"""
IB Data Fetcher - Fetches historical data from IB.

Two modes:
  fetch_ticks()     — raw tick data {t, price, size}, ~10 min for full day
  fetch_5sec_bars() — 5-sec OHLCV bars {t, o, h, l, c, v}, ~10 sec for full day
"""

import time
from datetime import datetime, timedelta, timezone

from ib_insync import IB, Stock

HOST = '127.0.0.1'
CLIENT_ID = 62
TICKER = 'AAPL'

_qualified_stock = None


def set_ticker(symbol):
    global TICKER, _qualified_stock
    TICKER = symbol
    _qualified_stock = None


def _get_stock(ib):
    global _qualified_stock
    if _qualified_stock is None:
        stock = Stock(TICKER, 'SMART', 'USD')
        ib.qualifyContracts(stock)
        _qualified_stock = stock
    return _qualified_stock


def fetch_ticks(ib, start_dt, end_dt=''):
    """
    Fetch raw historical ticks from IB, paginated forward.

    Args:
        ib: Active IB connection
        start_dt: Start datetime string ('YYYYMMDD HH:MM:SS UTC')
        end_dt: End datetime string ('' = up to now)

    Returns:
        List of dicts with keys: t (ms), price, size
    """
    stock = _get_stock(ib)
    all_ticks = []
    current_start = start_dt
    page = 0

    while True:
        ticks = ib.reqHistoricalTicks(
            stock, current_start, end_dt, 1000, 'TRADES', useRth=True
        )

        if not ticks:
            break

        page += 1
        for tick in ticks:
            ts_ms = int(tick.time.timestamp() * 1000)
            all_ticks.append({
                't': ts_ms,
                'price': tick.price,
                'size': int(tick.size),
            })

        if page % 100 == 0:
            print(f"  [FETCH] page {page}: {len(all_ticks)} ticks, last={ticks[-1].time.strftime('%H:%M:%S')}")

        # 3 requests per second pacing
        time.sleep(0.34)

        # Advance 1 second past last tick for next page
        next_start = ticks[-1].time + timedelta(seconds=1)
        current_start = next_start.strftime('%Y%m%d %H:%M:%S UTC')

        # Stop if ticks wrapped back to earlier time (IB looped)
        if len(all_ticks) > 1000:
            last_ts = all_ticks[-1]['t']
            prev_ts = all_ticks[-1001]['t']
            if last_ts < prev_ts:
                # Trim the wrapped ticks
                all_ticks = [t for t in all_ticks if t['t'] <= prev_ts]
                print(f"  [FETCH] Detected wrap-around at page {page}, stopping")
                break

        # If end_dt specified, stop when we've passed it
        if end_dt and next_start.strftime('%Y%m%d %H:%M:%S UTC') >= end_dt:
            break

    all_ticks.sort(key=lambda x: x['t'])
    return all_ticks


def fetch_5sec_bars(ib, start_dt, end_dt=''):
    """
    Fetch 5-sec historical bars from IB in 1-hour chunks.

    Args:
        ib: Active IB connection
        start_dt: Start datetime string ('YYYYMMDD HH:MM:SS UTC')
        end_dt: End datetime string ('' = up to now)

    Returns:
        List of dicts with keys: t (ms), o, h, l, c, v
    """
    stock = _get_stock(ib)
    all_bars = []
    seen_ts = set()

    # Parse start time
    start = datetime.strptime(start_dt, '%Y%m%d %H:%M:%S UTC').replace(tzinfo=timezone.utc)

    # Fetch in 1-hour chunks (3600 S)
    chunk_end = start + timedelta(hours=1)
    chunk_num = 0

    while True:
        end_str = chunk_end.strftime('%Y%m%d %H:%M:%S UTC')

        bars = ib.reqHistoricalData(
            stock,
            endDateTime=end_str,
            durationStr='3600 S',
            barSizeSetting='5 secs',
            whatToShow='TRADES',
            useRTH=True,
            formatDate=2,
        )

        chunk_num += 1
        added = 0
        for bar in bars:
            ts_ms = int(bar.date.timestamp() * 1000)
            if ts_ms not in seen_ts:
                seen_ts.add(ts_ms)
                all_bars.append({
                    't': ts_ms,
                    'o': bar.open,
                    'h': bar.high,
                    'l': bar.low,
                    'c': bar.close,
                    'v': int(bar.volume),
                })
                added += 1

        print(f"  [FETCH] chunk {chunk_num}: got={len(bars)}  new={added}  total={len(all_bars)}")

        # No bars returned means we've passed available data
        if len(bars) == 0:
            break

        # Stop if 3 consecutive chunks with no new data
        if added == 0:
            if not hasattr(fetch_5sec_bars, '_empty_count'):
                fetch_5sec_bars._empty_count = 0
            fetch_5sec_bars._empty_count += 1
            if fetch_5sec_bars._empty_count >= 3:
                fetch_5sec_bars._empty_count = 0
                break
        else:
            fetch_5sec_bars._empty_count = 0

        chunk_end += timedelta(hours=1)

        # Stop if past end_dt or well past now
        now = datetime.now(timezone.utc)
        if end_dt:
            end_limit = datetime.strptime(end_dt, '%Y%m%d %H:%M:%S UTC').replace(tzinfo=timezone.utc)
            if chunk_end > end_limit + timedelta(hours=1):
                break
        elif chunk_end > now + timedelta(hours=2):
            break

        time.sleep(1)  # pacing between chunks

    all_bars.sort(key=lambda x: x['t'])
    return all_bars


if __name__ == '__main__':
    import argparse
    import time

    parser = argparse.ArgumentParser(description='IB Tick Fetcher')
    parser.add_argument('--paper', action='store_true', help='Paper account (port 4002)')
    args = parser.parse_args()

    port = 4002 if args.paper else 4001
    ib = IB()
    ib.connect(HOST, port, clientId=CLIENT_ID, timeout=15)
    ib.reqMarketDataType(3)

    print(f"Fetching ticks from IB (port {port})...")
    t0 = time.time()
    ticks = fetch_ticks(ib, '20260227 14:30:00 UTC')
    elapsed = time.time() - t0
    print(f"Fetched {len(ticks)} ticks in {elapsed:.1f}s")

    if ticks:
        first_dt = datetime.fromtimestamp(ticks[0]['t'] / 1000, tz=timezone.utc)
        last_dt = datetime.fromtimestamp(ticks[-1]['t'] / 1000, tz=timezone.utc)
        total_vol = sum(t['size'] for t in ticks)
        print(f"  First: {first_dt.strftime('%H:%M:%S')}  price={ticks[0]['price']}  size={ticks[0]['size']}")
        print(f"  Last:  {last_dt.strftime('%H:%M:%S')}  price={ticks[-1]['price']}  size={ticks[-1]['size']}")
        print(f"  Total volume: {total_vol:,}")

    ib.disconnect()
