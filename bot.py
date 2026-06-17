import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional
import websockets
from telegram import Bot
from telegram.constants import ParseMode

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")
DERIV_WS_URL = "wss://ws.binaryws.com/websockets/v3?app_id=1089"

INSTRUMENTS = {
    "V10_1S":    {"name": "Volatility 10 (1s)",   "symbol": "1HZ10V"},
    "V25_1S":    {"name": "Volatility 25 (1s)",   "symbol": "1HZ25V"},
    "V50_1S":    {"name": "Volatility 50 (1s)",   "symbol": "1HZ50V"},
    "V75_1S":    {"name": "Volatility 75 (1s)",   "symbol": "1HZ75V"},
    "V100_1S":   {"name": "Volatility 100 (1s)",  "symbol": "1HZ100V"},
    "V10":       {"name": "Volatility 10",         "symbol": "R_10"},
    "V25":       {"name": "Volatility 25",         "symbol": "R_25"},
    "V50":       {"name": "Volatility 50",         "symbol": "R_50"},
    "V75":       {"name": "Volatility 75",         "symbol": "R_75"},
    "V100":      {"name": "Volatility 100",        "symbol": "R_100"},
    "BOOM300":   {"name": "Boom 300",              "symbol": "BOOM300N"},
    "BOOM500":   {"name": "Boom 500",              "symbol": "BOOM500N"},
    "BOOM600":   {"name": "Boom 600",              "symbol": "BOOM600N"},
    "BOOM900":   {"name": "Boom 900",              "symbol": "BOOM900N"},
    "BOOM1000":  {"name": "Boom 1000",             "symbol": "BOOM1000N"},
    "CRASH300":  {"name": "Crash 300",             "symbol": "CRASH300N"},
    "CRASH500":  {"name": "Crash 500",             "symbol": "CRASH500N"},
    "CRASH600":  {"name": "Crash 600",             "symbol": "CRASH600N"},
    "CRASH900":  {"name": "Crash 900",             "symbol": "CRASH900N"},
    "CRASH1000": {"name": "Crash 1000",            "symbol": "CRASH1000N"},
    "STEP":      {"name": "Step Index",            "symbol": "stpRNG"},
    "JUMP10":    {"name": "Jump 10",               "symbol": "JD10"},
    "JUMP25":    {"name": "Jump 25",               "symbol": "JD25"},
    "JUMP50":    {"name": "Jump 50",               "symbol": "JD50"},
    "JUMP75":    {"name": "Jump 75",               "symbol": "JD75"},
    "JUMP100":   {"name": "Jump 100",              "symbol": "JD100"},
}

TIMEFRAMES = {"M1": 60, "M5": 300, "H1": 3600}
CANDLES_NEEDED = 50
MIN_CONFLUENCE = 3

@dataclass
class Candle:
    time: int
    open: float
    high: float
    low: float
    close: float

    @property
    def body_mid(self): return (self.open + self.close) / 2
    @property
    def range(self): return self.high - self.low
    @property
    def is_bullish(self): return self.close > self.open
    @property
    def is_bearish(self): return self.close < self.open

@dataclass
class Signal:
    instrument: str
    direction: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    rr: float
    confluence: int
    reasons: list[str]
    timeframe: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

async def fetch_candles(symbol: str, granularity: int, count: int = CANDLES_NEEDED) -> list[Candle]:
    candles = []
    try:
        async with websockets.connect(DERIV_WS_URL, ping_interval=None) as ws:
            req = {
                "ticks_history": symbol,
                "adjust_start_time": 1,
                "count": count,
                "end": "latest",
                "granularity": granularity,
                "style": "candles"
            }
            await ws.send(json.dumps(req))
            resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
            if "candles" in resp:
                for c in resp["candles"]:
                    candles.append(Candle(
                        time=c["epoch"], open=float(c["open"]),
                        high=float(c["high"]), low=float(c["low"]), close=float(c["close"])
                    ))
            else:
                logger.warning(f"Pas de candles {symbol} g={granularity}: {resp.get('error')}")
    except Exception as e:
        logger.error(f"fetch_candles {symbol}: {e}")
    return candles

def analyze_crt(candles: list[Candle]) -> tuple[Optional[str], list[str]]:
    if len(candles) < 3:
        return None, []
    mother, trigger, current = candles[-3], candles[-2], candles[-1]
    if trigger.high > mother.high and current.close < mother.high:
        return "BUY", ["CRT: manipulation haussière (trigger > high mère)"]
    if trigger.low < mother.low and current.close > mother.low:
        return "SELL", ["CRT: manipulation baissière (trigger < low mère)"]
    return None, []

def detect_order_block(candles: list[Candle]) -> tuple[Optional[str], list[str]]:
    if len(candles) < 5:
        return None, []
    price = candles[-1].close
    for i in range(len(candles) - 4, max(len(candles) - 20, 0), -1):
        c = candles[i]
        next_move = candles[i+1:i+4]
        if not next_move:
            continue
        if c.is_bearish and all(x.close > x.open for x in next_move):
            if c.low <= price <= c.high:
                return "BUY", [f"SMC: Bullish OB [{c.low:.4f}–{c.high:.4f}]"]
        if c.is_bullish and all(x.close < x.open for x in next_move):
            if c.low <= price <= c.high:
                return "SELL", [f"SMC: Bearish OB [{c.low:.4f}–{c.high:.4f}]"]
    return None, []

def detect_fvg(candles: list[Candle]) -> tuple[Optional[str], list[str]]:
    if len(candles) < 4:
        return None, []
    price = candles[-1].close
    for i in range(len(candles) - 3, max(len(candles) - 15, 1), -1):
        c1, c3 = candles[i-1], candles[i+1]
        if c3.low > c1.high and c1.high <= price <= c3.low:
            return "BUY", [f"SMC: Bullish FVG [{c1.high:.4f}–{c3.low:.4f}]"]
        if c3.high < c1.low and c3.high <= price <= c1.low:
            return "SELL", [f"SMC: Bearish FVG [{c3.high:.4f}–{c1.low:.4f}]"]
    return None, []

def analyze_fibonacci(candles: list[Candle]) -> tuple[Optional[str], list[str]]:
    if len(candles) < 20:
        return None, []
    recent = candles[-20:]
    swing_high = max(c.high for c in recent)
    swing_low = min(c.low for c in recent)
    price = candles[-1].close
    if swing_high == swing_low:
        return None, []
    rang = swing_high - swing_low
    avg_open = sum(c.open for c in recent) / len(recent)
    fib_618 = swing_high - 0.618 * rang
    fib_786 = swing_high - 0.786 * rang
    if fib_786 <= price <= fib_618 and price > avg_open:
        return "BUY", [f"Fibo: zone 61.8–78.6% achat [{fib_786:.4f}–{fib_618:.4f}]"]
    fib_618s = swing_low + 0.618 * rang
    fib_786s = swing_low + 0.786 * rang
    if fib_618s <= price <= fib_786s and price < avg_open:
        return "SELL", [f"Fibo: zone 61.8–78.6% vente [{fib_618s:.4f}–{fib_786s:.4f}]"]
    return None, []

def get_h1_bias(candles_h1: list[Candle]) -> Optional[str]:
    if len(candles_h1) < 6:
        return None
    recent = candles_h1[-6:]
    highs = [c.high for c in recent]
    lows = [c.low for c in recent]
    if highs[-1] > highs[-3] > highs[-5] and lows[-1] > lows[-3] > lows[-5]:
        return "BUY"
    if lows[-1] < lows[-3] < lows[-5] and highs[-1] < highs[-3] < highs[-5]:
        return "SELL"
    return None

def compute_sl_tp(direction: str, candles_m1: list[Candle], entry: float):
    recent = candles_m1[-10:]
    if direction == "BUY":
        sl = min(c.low for c in recent) - 0.0002 * entry
        dist = entry - sl
    else:
        sl = max(c.high for c in recent) + 0.0002 * entry
        dist = sl - entry
    tp1 = entry + 1.5 * dist if direction == "BUY" else entry - 1.5 * dist
    tp2 = entry + 2.5 * dist if direction == "BUY" else entry - 2.5 * dist
    return sl, tp1, tp2, 2.5

async def analyze_instrument(key: str, info: dict) -> Optional[Signal]:
    symbol = info["symbol"]
    candles_h1, candles_m5, candles_m1 = await asyncio.gather(
        fetch_candles(symbol, TIMEFRAMES["H1"]),
        fetch_candles(symbol, TIMEFRAMES["M5"]),
        fetch_candles(symbol, TIMEFRAMES["M1"]),
    )
    if not candles_m1 or len(candles_m1) < 10:
        return None
    h1_bias = get_h1_bias(candles_h1)
    dir_crt,  r_crt  = analyze_crt(candles_m5)
    dir_ob,   r_ob   = detect_order_block(candles_m5)
    dir_fvg,  r_fvg  = detect_fvg(candles_m5)
    dir_fibo, r_fibo = analyze_fibonacci(candles_m5)
    directions = [dir_crt, dir_ob, dir_fvg, dir_fibo]
    reasons = r_crt + r_ob + r_fvg + r_fibo
    buy_count = directions.count("BUY")
    sell_count = directions.count("SELL")
    if buy_count > sell_count:
        direction, confluence = "BUY", buy_count
    elif sell_count > buy_count:
        direction, confluence = "SELL", sell_count
    else:
        return None
    if h1_bias and h1_bias != direction:
        return None
    if h1_bias == direction:
        confluence += 1
        reasons.append(f"H1 biais confirme {direction}")
    if confluence < MIN_CONFLUENCE:
        return None
    entry = candles_m1[-1].close
    sl, tp1, tp2, rr = compute_sl_tp(direction, candles_m1, entry)
    return Signal(
        instrument=info["name"], direction=direction,
        entry=entry, sl=sl, tp1=tp1, tp2=tp2, rr=rr,
        confluence=confluence, reasons=reasons[:4],
        timeframe="H1→M5→M1",
    )

def format_signal(sig: Signal) -> str:
    emoji = "🟢" if sig.direction == "BUY" else "🔴"
    arrow = "📈" if sig.direction == "BUY" else "📉"
    stars = "⭐" * sig.confluence
    reasons_text = "\n".join(f"  • {r}" for r in sig.reasons)
    return (
        f"{emoji} *{sig.direction} — {sig.instrument}*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{arrow} *Multi-TF :* {sig.timeframe}\n"
        f"⭐ *Confluence :* {stars} ({sig.confluence}/6)\n\n"
        f"💰 *Entrée :* `{sig.entry:.5f}`\n"
        f"🛑 *Stop Loss :* `{sig.sl:.5f}`\n"
        f"🎯 *TP1 (1:1.5) :* `{sig.tp1:.5f}`\n"
        f"🎯 *TP2 (1:2.5) :* `{sig.tp2:.5f}`\n\n"
        f"📊 *Confluences :*\n{reasons_text}\n\n"
        f"🕐 _{sig.timestamp.strftime('%H:%M UTC')}_ | ⚠️ _Risque 1% max_"
    )

async def run_bot():
    bot = Bot(token=TELEGRAM_TOKEN)
    logger.info("✅ Bot Synthétiques démarré")
    await bot.send_message(
        chat_id=CHAT_ID,
        text=(
            "🤖 *Bot Indices Synthétiques Deriv*\n"
            "━━━━━━━━━━━━━━━━\n"
            "📊 V10/V25/V50/V75/V100 (1s & standard)\n"
            "💥 Boom 300/500/600/900/1000\n"
            "💥 Crash 300/500/600/900/1000\n"
            "🦘 Jump 10/25/50/75/100 · Step Index\n"
            "🧠 CRT + OB + FVG + Fibonacci\n"
            "⏱ Multi-TF H1→M5→M1 | Cycle 5min"
        ),
        parse_mode=ParseMode.MARKDOWN,
    )
    last_signal: dict[str, tuple[str, float]] = {}
    while True:
        logger.info("🔍 Analyse en cours...")
        tasks = [analyze_instrument(k, v) for k, v in INSTRUMENTS.items()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for key, result in zip(INSTRUMENTS.keys(), results):
            if isinstance(result, Exception) or result is None:
                continue
            sig = result
            prev = last_signal.get(key)
            if prev:
                prev_dir, prev_entry = prev
                if prev_dir == sig.direction and abs(sig.entry - prev_entry) / prev_entry < 0.001:
                    continue
            last_signal[key] = (sig.direction, sig.entry)
            try:
                await bot.send_message(chat_id=CHAT_ID, text=format_signal(sig), parse_mode=ParseMode.MARKDOWN)
                logger.info(f"✅ {key} {sig.direction} confluence={sig.confluence}")
            except Exception as e:
                logger.error(f"Telegram error: {e}")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(run_bot())
