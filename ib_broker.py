"""
ib_broker.py — IB Gateway order execution module.

Imported by trade_map_plotter_v34_lite_action.py.
Sends Buy/Sell orders to Interactive Brokers via ib_insync.
Not a standalone script.

If ib_insync is not installed, the module loads but all functions
return immediately (graceful degradation — no crash).
"""

import logging
import os
import re
from datetime import datetime

_logger = logging.getLogger('ib_broker')
_logger.setLevel(logging.DEBUG)
_fh = logging.FileHandler('ib_broker_debug.log')
_fh.setFormatter(logging.Formatter('%(asctime)s %(message)s', datefmt='%H:%M:%S'))
_logger.addHandler(_fh)

try:
    from ib_insync import IB, Option, Stock, MarketOrder
    IB_AVAILABLE = True
except ImportError:
    IB_AVAILABLE = False


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONFIG = {
    'host':         '127.0.0.1',
    'port':         4002,       # 4002=paper, 4001=live
    'client_id':    10,
    'quantity':     1,          # contracts per trade
    'order_type':   'MKT',     # MKT only for now
    'max_quantity': 10,         # safety cap
}


# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

_ib = None
_connected = False
_positions = {
    'CALL': None,   # {'symbol': str, 'con_id': int, 'entry_step': int, 'entry_price': float, 'entry_time': str}
    'PUT':  None,
}
_contract_cache = {}    # {occ_symbol: qualified IB contract}
_trade_objects = {}     # {order_id: ib_insync Trade object}
_order_log_path = None
_fill_log_path = None
_fill_callback = None   # external callback: fn(side, action, occ_symbol, fill_price, pnl, step)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def connect(host=None, port=None, client_id=None, log_dir=None):
    """Connect to IB Gateway. Returns True on success."""
    global _ib, _connected, _positions, _contract_cache
    global _order_log_path, _fill_log_path

    if not IB_AVAILABLE:
        _log_print("ib_insync not installed — IB orders disabled")
        return False

    h = host or CONFIG['host']
    p = port or CONFIG['port']
    cid = client_id or CONFIG['client_id']

    _ib = IB()
    try:
        _ib.connect(h, p, clientId=cid)
    except Exception as e:
        _log_print(f"IB connect failed: {e}")
        _ib = None
        _connected = False
        return False

    _connected = True
    _contract_cache = {}
    _positions = {'CALL': None, 'PUT': None}

    # Set up log paths
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        _order_log_path = os.path.join(log_dir, 'ib-orders.log')
        _fill_log_path = os.path.join(log_dir, 'ib-fills.log')

    mode = "PAPER" if p == 4002 else "LIVE" if p == 4001 else f"port {p}"
    _log_print(f"IB connected ({mode}) — {h}:{p} clientId={cid}")
    return True


def execute(action, occ_symbol, step, side):
    """Execute a Buy or Sell order with position tracking.

    Position tracking ensures correct open/close pairs:
    - Buy only fires if no open position on that side
    - Sell only fires if there IS an open position on that side
    - Round-trip logged to ib-fills.log on close with P&L

    Args:
        action:     'Buy' or 'Sell'
        occ_symbol: OCC symbol like 'AAPL260212C00315000'
        step:       current step number
        side:       'CALL' or 'PUT'

    Returns:
        dict with order result or None on failure
    """
    _logger.debug(f"execute() called: action={action} occ={occ_symbol} step={step} side={side}")
    _logger.debug(f"  _connected={_connected} _ib={_ib} isConnected={_ib.isConnected() if _ib else 'N/A'}")
    _logger.debug(f"  position[{side}]={_positions[side]}")

    if not _connected or _ib is None:
        _log_print(f"step={step} | {side} | {action} | SKIPPED — not connected")
        _logger.debug(f"  SKIPPED: _connected={_connected} _ib={_ib}")
        return None

    if action not in ('Buy', 'Sell'):
        _logger.debug(f"  SKIPPED: action={action} not Buy/Sell")
        return None

    # --- Position pair validation ---
    pos = _positions[side]

    if action == 'Buy':
        if pos is not None:
            _log_print(f"step={step} | {side} | OPEN SKIPPED — already open since step {pos['entry_step']}")
            _logger.debug(f"  SKIPPED: {side} position already open: {pos}")
            return None
    elif action == 'Sell':
        if pos is None:
            _log_print(f"step={step} | {side} | CLOSE SKIPPED — no open position")
            _logger.debug(f"  SKIPPED: no open {side} position")
            return None

    # Parse OCC symbol
    parsed = _parse_occ(occ_symbol)
    if parsed is None:
        _log_print(f"step={step} | {side} | {action} | FAILED — cannot parse symbol: {occ_symbol}")
        _logger.debug(f"  FAILED: parse returned None for {occ_symbol}")
        return None

    _logger.debug(f"  parsed: {parsed}")

    # Qualify contract (cached)
    cached = occ_symbol in _contract_cache
    contract = _get_qualified_contract(occ_symbol, parsed)
    if contract is None:
        _log_print(f"step={step} | {side} | {action} | FAILED — contract qualification failed: {occ_symbol}")
        _logger.debug(f"  FAILED: qualify returned None (cached={cached})")
        return None

    _logger.debug(f"  contract: conId={contract.conId} cached={cached}")

    # Place order
    ib_action = 'BUY' if action == 'Buy' else 'SELL'
    qty = CONFIG['quantity']
    if qty > CONFIG['max_quantity']:
        qty = CONFIG['max_quantity']

    _log_print(f"step={step} | {side} | {'OPEN' if action == 'Buy' else 'CLOSE'} | "
               f"{ib_action} {qty}x {occ_symbol}")

    _logger.debug(f"  calling _place_order: {ib_action} {qty}x {occ_symbol}")
    result = _place_order(contract, ib_action, qty, side, parsed['strike'], occ_symbol)

    if result is None:
        _log_print(f"step={step} | {side} | {action} | FAILED — order placement failed")
        _logger.debug(f"  FAILED: _place_order returned None")
        _log_order(step, side, ib_action, occ_symbol, qty, None, 'FAILED', None, None)
        return None

    _logger.debug(f"  SUCCESS: orderId={result['order_id']} status={result['status']}")

    order_id = result['order_id']
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # --- Update position tracking (no fill yet) ---
    if action == 'Buy':
        _positions[side] = {
            'occ_symbol': occ_symbol,
            'entry_step': step,
            'entry_time': ts,
            'qty': qty,
            'order_id': order_id,
            'perm_id': result.get('perm_id'),
            'entry_price': None,
        }
        _log_print(f"step={step} | {side} OPENED | BUY {qty}x {occ_symbol} @pending")
        _logger.debug(f"  position[{side}] OPENED: {_positions[side]}")

    elif action == 'Sell':
        _log_print(f"step={step} | {side} CLOSE SENT | SELL {qty}x {occ_symbol} @pending")

    # Log order (fill pending)
    _log_order(step, side, ib_action, occ_symbol, qty,
               order_id, result['status'], None, None)

    # --- Attach fill callback (non-blocking, event-driven) ---
    trade = _trade_objects[order_id]

    def _on_filled(t):
        fill_price = t.orderStatus.avgFillPrice or (t.fills[-1].execution.price if t.fills else None)
        fill_str = f"@${fill_price:.2f}" if fill_price else "@no_fill"
        pnl_val = None

        if action == 'Buy':
            if _positions.get(side) and _positions[side].get('order_id') == order_id:
                _positions[side]['entry_price'] = fill_price
            _log_print(f"step={step} | {side} FILL | BUY {qty}x {occ_symbol} {fill_str}")

        elif action == 'Sell':
            entry_step = pos['entry_step'] if pos else None
            entry_price = pos.get('entry_price') if pos else None
            hold = (step - entry_step) if entry_step else 0

            pnl_str = ""
            if entry_price and fill_price:
                pnl_val = (fill_price - entry_price) * qty * 100
                pnl_str = f" | P&L=${pnl_val:+.2f}"

            _log_print(f"step={step} | {side} FILL | SELL {qty}x {occ_symbol} {fill_str} | "
                       f"held {hold} steps (open@step {entry_step} @${entry_price or 0:.2f}){pnl_str}")
            _log_fill(side, occ_symbol, entry_step, entry_price, step, fill_price)
            _positions[side] = None

        _logger.debug(f"  filledEvent: orderId={order_id} fill={fill_price}")

        # Notify external UI
        if _fill_callback:
            try:
                _fill_callback(side, action, occ_symbol, fill_price, pnl_val, step)
            except Exception as e:
                _logger.debug(f"  fill_callback error: {e}")

    trade.filledEvent += _on_filled

    return result


_stock_cache = {}  # {ticker: qualified Stock contract}

def execute_stock(action, ticker, qty, step, side):
    """Execute a stock Buy or Sell order on the same IB connection.

    CALL side: Buy=BUY 1 share, Sell=SELL 1 share
    PUT side:  Buy=SELL 1 share (short), Sell=BUY 1 share (cover)

    Args:
        action:  'Buy' or 'Sell' (from signal)
        ticker:  stock ticker like 'AAPL'
        qty:     number of shares
        step:    current step number
        side:    'CALL' or 'PUT'
    """
    if not _connected or _ib is None:
        _log_print(f"step={step} | STK {side} | {action} | SKIPPED — not connected")
        return None

    if action not in ('Buy', 'Sell'):
        return None

    # Qualify stock contract (cached)
    if ticker not in _stock_cache:
        contract = Stock(ticker, 'SMART', 'USD')
        try:
            qualified = _ib.qualifyContracts(contract)
            if not qualified:
                _log_print(f"step={step} | STK | FAILED — cannot qualify {ticker}")
                return None
            _stock_cache[ticker] = qualified[0]
        except Exception as e:
            _log_print(f"step={step} | STK | FAILED — {e}")
            return None
    contract = _stock_cache[ticker]

    # CALL: Buy→BUY, Sell→SELL
    # PUT:  Buy→SELL (short), Sell→BUY (cover)
    if side == 'PUT':
        ib_action = 'SELL' if action == 'Buy' else 'BUY'
    else:
        ib_action = 'BUY' if action == 'Buy' else 'SELL'

    try:
        order = MarketOrder(ib_action, qty)
        trade = _ib.placeOrder(contract, order)
        oid = trade.order.orderId
        _trade_objects[oid] = trade
        _log_print(f"step={step} | STK {side} | {ib_action} {qty}x {ticker} | orderId={oid} {trade.orderStatus.status}")
        return oid
    except Exception as e:
        _log_print(f"step={step} | STK {side} | ORDER FAILED — {e}")
        return None


def get_positions():
    """Return current open positions dict for external inspection."""
    return dict(_positions)


def set_fill_callback(fn):
    """Register a callback for fill notifications.
    fn(side, action, occ_symbol, fill_price, pnl, step)
    Called asynchronously when IB reports a fill (during pump())."""
    global _fill_callback
    _fill_callback = fn


def pump():
    """Pump the IB event loop to process pending events (fills, status changes).
    Call this periodically from your UI loop (e.g. root.after(200, pump))."""
    if _ib is not None and _connected:
        try:
            _ib.sleep(0)
        except Exception:
            pass


def disconnect():
    """Disconnect from IB Gateway cleanly."""
    global _ib, _connected

    if _ib is not None:
        try:
            _ib.disconnect()
        except Exception:
            pass
        _ib = None

    _connected = False
    _log_print("IB disconnected")


def is_connected():
    """Check if IB connection is active."""
    if not _connected or _ib is None:
        return False
    try:
        return _ib.isConnected()
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Internal functions
# ---------------------------------------------------------------------------

def _parse_occ(occ):
    """Parse OCC option symbol into contract parameters.
    AAPL260212C00315000 → {symbol, expiry, right, strike}
    """
    if occ is None or occ == 'NA':
        return None
    m = re.match(r'^([A-Z]{1,6})(\d{6})([CP])(\d{8})$', occ.strip())
    if not m:
        return None
    return {
        'symbol': m.group(1),
        'expiry': '20' + m.group(2),
        'right':  m.group(3),
        'strike': int(m.group(4)) / 1000.0,
    }


def _get_qualified_contract(occ_symbol, parsed):
    """Get a qualified IB contract, using cache if available."""
    if occ_symbol in _contract_cache:
        return _contract_cache[occ_symbol]

    contract = Option(
        symbol=parsed['symbol'],
        lastTradeDateOrContractMonth=parsed['expiry'],
        strike=parsed['strike'],
        right=parsed['right'],
        exchange='SMART',
        currency='USD',
        multiplier='100',
    )

    try:
        qualified = _ib.qualifyContracts(contract)
        if not qualified:
            return None
        _contract_cache[occ_symbol] = qualified[0]
        return qualified[0]
    except Exception as e:
        _log_print(f"Contract qualification error: {e}")
        return None


def _place_order(contract, ib_action, quantity, side, strike, occ_symbol):
    """Place a market order. Returns immediately without waiting for fill."""
    try:
        order = MarketOrder(ib_action, quantity)
        _logger.debug(f"  placeOrder calling: {ib_action} {quantity}x conId={contract.conId} {occ_symbol}")
        trade = _ib.placeOrder(contract, order)

        order_id = trade.order.orderId
        perm_id = trade.order.permId
        status = trade.orderStatus.status

        # Store trade object for later fill queries
        _trade_objects[order_id] = trade

        _logger.debug(f"  placeOrder result: orderId={order_id} permId={perm_id} status={status}")

        strike_str = f"{strike:g}"
        _log_print(f"ORDER SENT: {ib_action} {quantity}x {side} {strike_str} {occ_symbol} "
                   f"orderId={order_id} {status}")

        return {
            'order_id': order_id,
            'perm_id': perm_id,
            'status': status,
            'fill_price': None,
            'filled_qty': 0,
        }
    except Exception as e:
        _log_print(f"Order error: {e}")
        _logger.debug(f"  placeOrder EXCEPTION: {type(e).__name__}: {e}")
        return None



def _log_print(msg):
    """Print to console with timestamp."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[IB] {ts} | {msg}")


def _log_order(step, side, ib_action, occ_symbol, qty, order_id, status, fill_price, latency):
    """Append to ib-orders.log."""
    if _order_log_path is None:
        return
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    fill_str = f"{fill_price:.2f}" if fill_price is not None else "---"
    lat_str = f"{latency:.1f}s" if latency is not None else "---"
    oid_str = str(order_id) if order_id is not None else "---"
    line = f"{ts} | step={step:05d} | {side} | {ib_action:<4} | {occ_symbol} | qty={qty} | MKT | orderId={oid_str} | {status} | fill={fill_str} | {lat_str}\n"
    try:
        with open(_order_log_path, 'a') as f:
            f.write(line)
    except Exception:
        pass


def _log_fill(side, occ_symbol, buy_step, buy_price, sell_step, sell_price):
    """Append completed round-trip to ib-fills.log.

    P&L includes 100x options multiplier.
    """
    if _fill_log_path is None:
        return
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    buy_s = f"step={buy_step:05d}" if buy_step is not None else "step=-----"
    sell_s = f"step={sell_step:05d}" if sell_step is not None else "step=-----"
    buy_p = f"@{buy_price:.2f}" if buy_price is not None else "@---"
    sell_p = f"@{sell_price:.2f}" if sell_price is not None else "@---"
    pnl = ""
    if buy_price is not None and sell_price is not None:
        qty = CONFIG['quantity']
        pnl_val = (sell_price - buy_price) * qty * 100
        pnl = f" | PNL=${pnl_val:+.2f}"
    hold = ""
    if buy_step is not None and sell_step is not None:
        hold = f" | Hold={sell_step - buy_step} steps"
    line = f"{ts} | {side} | {occ_symbol} | Buy {buy_s} {buy_p} | Sell {sell_s} {sell_p}{pnl}{hold}\n"
    try:
        with open(_fill_log_path, 'a') as f:
            f.write(line)
    except Exception:
        pass
