import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import websockets
from telegram import Bot
from telegram.constants import ParseMode

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

INSTRUMENTS = [
    "R_10", "R_25", "R_50", "R_75", "R_100",
    "1HZ10V", "1HZ25V", "1HZ50V", "1HZ75V", "1HZ100V",
    "BOOM300N", "BOOM500", "BOOM600", "BOOM900", "BOOM1000",
    "CRASH300N", "CRASH500", "CRASH600", "CRASH900", "CRASH1000",
    "JD10", "JD25", "JD50", "JD75", "JD100",
]

INSTRUMENT_NAMES = {
    "R_10": "Volatility 10", "R_25": "Volatility 25", "R_50": "Volatility 50",
    "R_75": "Volatility 75", "R_100": "Volatility 100",
    "1HZ10V": "Volatility 10 (1s)", "1HZ25V": "Volatility 25 (1s)",
    "1HZ50V": "Volatility 50 (1s)", "1HZ75V": "Volatility 75 (1s)",
    "1HZ100V": "Volatility 100 (1s)",
    "BOOM300N": "Boom 300", "BOOM500": "Boom 500", "BOOM600": "Boom 600",
    "BOOM900": "Boom 900", "BOOM1000": "Boom 1000",
    "CRASH300N": "Crash 300", "CRASH500": "Crash 500", "CRASH600": "Crash 600",
    "CRASH900": "Crash 900", "CRASH1000": "Crash 1000",
    "JD10": "Jump 10", "JD25": "Jump 25", "JD50": "Jump 50",
    "JD75": "Jump 75", "JD100": "Jump 100",
}

DERIV_WS_URL = "wss://ws.binaryws.com/websockets/v3?app_id=1089"
candles_store = {sym: {"m5": [], "m15": []} for sym in INSTRUMENTS}
last_signal_time = {}


def compute_ema(closes, period):
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema


def detect_order_block(candles):
    if len(candles) < 3:
        return None
    c1, c2, c3 = candles[-3], candles[-2], candles[-1]
    if c2["close"] < c2["open"] and c3["close"] > c3["open"]:
        body_ratio = (c3["close"] - c3["open"]) / (c3["high"] - c3["low"] + 1e-10)
        if body_ratio > 0.6:
            return {"type": "bullish", "ob_high": c2["high"], "ob_low": c2["low"]}
    if c2["close"] > c2["open"] and c3["close"] < c3["open"]:
        body_ratio = (c3["open"] - c3["close"]) / (c3["high"] - c3["low"] + 1e-10)
        if body_ratio > 0.6:
            return {"type": "bearish", "ob_high": c2["high"], "ob_low": c2["low"]}
    return None


def detect_fvg(candles):
    if len(candles) < 3:
        return None
    c1, c2, c3 = candles[-3], candles[-2], candles[-1]
    if c3["low"] > c1["high"]:
        return {"type": "bullish", "fvg_high": c3["low"], "fvg_low": c1["high"]}
    if c3["high"] < c1["low"]:
        return {"type": "bearish", "fvg_high": c1["low"], "fvg_low": c3["high"]}
    return None


def detect_crt(candles):
    if len(candles) < 3:
        return None
    c1, c2, c3 = candles[-3], candles[-2], candles[-1]
    c1_range = c1["high"] - c1["low"]
    if c1_range == 0:
        return None
    if c2["low"] < c1["low"] and c3["close"] > (c1["low"] + c1_range * 0.5):
        return {"type": "bullish"}
    if c2["high"] > c1["high"] and c3["close"] < (c1["high"] - c1_range * 0.5):
        return {"type": "bearish"}
    return None


def get_swing_points(candles, lookback=20):
    subset = candles[-lookback:] if len(candles) >= lookback else candles
    return max(c["high"] for c in subset), min(c["low"] for c in subset)


def analyze(symbol, m5_candles, m15_candles):
    if len(m5_candles) < 20 or len(m15_candles) < 10:
        return None
    m15_closes = [c["close"] for c in m15_candles]
    ema21_m15 = compute_ema(m15_closes, 21)
    if ema21_m15 is None:
        return None
    htf_bias = "bullish" if m15_closes[-1] > ema21_m15 else "bearish"
    ob = detect_order_block(m5_candles)
    fvg = detect_fvg(m5_candles)
    crt = detect_crt(m5_candles)
    confluence = sum(1 for s in [ob, fvg, crt] if s and s["type"] == htf_bias)
    if confluence < 2:
        return None
    current_price = m5_candles[-1]["close"]
    swing_high, swing_low = get_swing_points(m5_candles)
    price_range = swing_high - swing_low
    if price_range == 0:
        return None
    entry = current_price
    if htf_bias == "bullish":
        sl = swing_low - price_range * 0.02
        tp1 = entry + price_range * 0.382
        tp2 = entry + price_range * 0.618
        rr = (tp1 - entry) / (entry - sl) if (entry - sl) > 0 else 0
    else:
        sl = swing_high + price_range * 0.02
        tp1 = entry - price_range * 0.382
        tp2 = entry - price_range * 0.618
        rr = (entry - tp1) / (sl - entry) if (sl - entry) > 0 else 0
    if rr < 1.5:
        return None
    return {
        "symbol": symbol, "name": INSTRUMENT_NAMES.get(symbol, symbol),
        "direction": htf_bias, "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
        "rr": round(rr, 2), "confluence": confluence,
        "ob": ob is not None and ob["type"] == htf_bias,
        "fvg": fvg is not None and fvg["type"] == htf_bias,
        "crt": crt is not None and crt["type"] == htf_bias,
        "htf_bias": htf_bias,
    }


def format_signal(sig):
    arrow = "🟢 BUY" if sig["direction"] == "bullish" else "🔴 SELL"
    icons = []
    if sig["ob"]: icons.append("🧱 OB")
    if sig["fvg"]: icons.append("🌫 FVG")
    if sig["crt"]: icons.append("🔄 CRT")
    p = lambda x: f"{x:.5f}" if x < 10 else f"{x:.2f}"
    return (
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *{sig['name']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{arrow}\n\n"
        f"🎯 *Entry :* `{p(sig['entry'])}`\n"
        f"🔻 *SL :* `{p(sig['sl'])}`\n"
        f"✅ *TP1 :* `{p(sig['tp1'])}`\n"
        f"🚀 *TP2 :* `{p(sig['tp2'])}`\n\n"
        f"⚖️ *R:R :* `{sig['rr']}`\n\n"
        f"🔍 *Confluence ({sig['confluence']}/3) :*\n"
        f"{'  '.join(icons)}\n\n"
        f"📈 *Biais HTF M15 :* {'Haussier 🟢' if sig['htf_bias'] == 'bullish' else 'Baissier 🔴'}\n"
        f"⏰ `{datetime.now(timezone.utc).strftime('%H:%M UTC')}`\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )


async def send_signal(bot, signal):
    key = signal["symbol"]
    now = datetime.now(timezone.utc).timestamp()
    if key in last_signal_time and now - last_signal_time[key] < 900:
        return
    last_signal_time[key] = now
    await bot.send_message(chat_id=CHAT_ID, text=format_signal(signal), parse_mode=ParseMode.MARKDOWN)
    logger.info(f"Signal sent: {signal['symbol']} {signal['direction']}")


async def fetch_candles(ws, symbol, granularity, count=50):
    req = {"ticks_history": symbol, "adjust_start_time": 1, "count": count,
           "end": "latest", "granularity": granularity, "style": "candles"}
    await ws.send(json.dumps(req))
    while True:
        resp = json.loads(await ws.recv())
        if resp.get("msg_type") == "candles":
            return [{"open": float(c["open"]), "high": float(c["high"]),
                     "low": float(c["low"]), "close": float(c["close"]),
                     "epoch": c["epoch"]} for c in resp.get("candles", [])]
        if "error" in resp:
            logger.warning(f"Deriv error {symbol}: {resp['error']}")
            return []


async def scan_all(bot):
    logger.info("Starting scan...")
    try:
        async with websockets.connect(DERIV_WS_URL, ping_interval=20) as ws:
            for symbol in INSTRUMENTS:
                try:
                    m5 = await fetch_candles(ws, symbol, 300, 50)
                    await asyncio.sleep(0.3)
                    m15 = await fetch_candles(ws, symbol, 900, 30)
                    await asyncio.sleep(0.3)
                    if not m5 or not m15:
                        continue
                    signal = analyze(symbol, m5, m15)
                    if signal:
                        await send_signal(bot, signal)
                except Exception as e:
                    logger.error(f"Error {symbol}: {e}")
    except Exception as e:
        logger.error(f"WS error: {e}")
    logger.info("Scan complete.")


async def main():
    if not TELEGRAM_TOKEN or not CHAT_ID:
        raise ValueError("Missing TELEGRAM_TOKEN or CHAT_ID")
    bot = Bot(token=TELEGRAM_TOKEN)
    await bot.send_message(
        chat_id=CHAT_ID,
        text=(
            "🤖 *Deriv Synthetic Signal Bot*\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "✅ Bot démarré\n"
            f"📊 {len(INSTRUMENTS)} instruments surveillés\n"
            "⏱ Scan toutes les 5 minutes\n"
            "🔍 SMC + CRT\n"
            "━━━━━━━━━━━━━━━━━━━━"
        ),
        parse_mode=ParseMode.MARKDOWN,
    )
    while True:
        await scan_all(bot)
        await asyncio.sleep(300)


if __name__ == "__main__":
    asyncio.run(main())
