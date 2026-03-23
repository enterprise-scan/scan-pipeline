"""
IB Volume Bar Pipeline - Build volume bars from IB data.

Supports two data formats:
  Ticks:    {t, price, size}  — accumulate size backwards
  5s bars:  {t, o, h, l, c, v} — accumulate v backwards, use OHLC from bars
"""

from typing import Dict, List, Optional

DEFAULT_VOLUMES = [360000]
BASE_COLS = ['open', 'high', 'low', 'close',
             'high_ts', 'high_src_open', 'high_src_close', 'high_src_vol', 'high_spike_vol',
             'low_ts', 'low_src_open', 'low_src_close', 'low_src_vol', 'low_spike_vol']


def create_volume_bar_backwards(data: List[Dict], volume_threshold: int) -> Optional[Dict]:
    """Create one volume bar by working backwards from end of data.
    Auto-detects tick format (price/size) vs bar format (o/h/l/c/v)."""
    if not data:
        return None

    is_tick = 'price' in data[0]
    accumulated_volume = 0
    bar_items = []

    for i in range(len(data) - 1, -1, -1):
        item = data[i]
        bar_items.append(item)
        accumulated_volume += item['size'] if is_tick else item.get('v', item.get('volume', 0))
        if accumulated_volume >= volume_threshold:
            break

    if not bar_items:
        return None

    # Helper to find neighbor volumes in full data array
    def _get_neighbor_vols(item, vol_key):
        item_ts = item['t']
        for idx in range(len(data)):
            if data[idx]['t'] == item_ts:
                prev_vol = data[idx - 1].get(vol_key, 0) if idx > 0 else 0
                next_vol = data[idx + 1].get(vol_key, 0) if idx < len(data) - 1 else 0
                return prev_vol, next_vol
        return 0, 0

    if is_tick:
        open_price = bar_items[-1]['price']
        close_price = bar_items[0]['price']
        high_item = max(bar_items, key=lambda x: x['price'])
        low_item = min(bar_items, key=lambda x: x['price'])
        high_price = high_item['price']
        low_price = low_item['price']
        high_ts = high_item['t']
        low_ts = low_item['t']
        high_src_open = high_item['price']
        high_src_close = high_item['price']
        high_src_vol = high_item['size']
        low_src_open = low_item['price']
        low_src_close = low_item['price']
        low_src_vol = low_item['size']
        high_prev_vol, high_next_vol = _get_neighbor_vols(high_item, 'size')
        low_prev_vol, low_next_vol = _get_neighbor_vols(low_item, 'size')
    else:
        open_price = bar_items[-1].get('o', bar_items[-1].get('open', 0))
        close_price = bar_items[0].get('c', bar_items[0].get('close', 0))
        high_item = max(bar_items, key=lambda x: x.get('h', x.get('high', 0)))
        low_item = min(bar_items, key=lambda x: x.get('l', x.get('low', 0)))
        high_price = high_item.get('h', high_item.get('high', 0))
        low_price = low_item.get('l', low_item.get('low', 0))
        high_ts = high_item['t']
        low_ts = low_item['t']
        high_src_open = high_item.get('o', high_item.get('open', 0))
        high_src_close = high_item.get('c', high_item.get('close', 0))
        high_src_vol = high_item.get('v', high_item.get('volume', 0))
        low_src_open = low_item.get('o', low_item.get('open', 0))
        low_src_close = low_item.get('c', low_item.get('close', 0))
        low_src_vol = low_item.get('v', low_item.get('volume', 0))
        vol_key = 'v' if 'v' in high_item else 'volume'
        high_prev_vol, high_next_vol = _get_neighbor_vols(high_item, vol_key)
        low_prev_vol, low_next_vol = _get_neighbor_vols(low_item, vol_key)

    high_spike_vol = max(0, high_src_vol - (high_prev_vol + high_next_vol) / 2)
    low_spike_vol = max(0, low_src_vol - (low_prev_vol + low_next_vol) / 2)

    return {
        'open': open_price,
        'high': high_price,
        'low': low_price,
        'close': close_price,
        'high_ts': high_ts,
        'high_src_open': high_src_open,
        'high_src_close': high_src_close,
        'high_src_vol': high_src_vol,
        'high_spike_vol': high_spike_vol,
        'low_ts': low_ts,
        'low_src_open': low_src_open,
        'low_src_close': low_src_close,
        'low_src_vol': low_src_vol,
        'low_spike_vol': low_spike_vol,
    }


def format_prefix(volume: int) -> str:
    """Format volume as prefix (e.g., 360000 -> '360k')."""
    return f"{volume // 1000}k" if volume >= 1000 else str(volume)


def compute_volume_bars(cut_data: List[Dict], volumes: List[int], step_ts: int) -> Optional[Dict]:
    """Compute volume bars for all thresholds."""
    if not cut_data:
        return None

    last_raw_ts = cut_data[-1]['t']

    result = {
        'timestamp': step_ts,
        'last_raw_ts': last_raw_ts,
    }

    for vol in volumes:
        prefix = format_prefix(vol)
        bar = create_volume_bar_backwards(cut_data, vol)
        if bar:
            for key, value in bar.items():
                result[f'{prefix}_{key}'] = value

    return result
