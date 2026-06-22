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
last_signal_time = {}
COOLDOWN_SECONDS = 4 * 3600
FIBO_LOW = 0.618
FIBO_HIGH = 0.709
MIN_RR = 2.0
ATR_PERIOD = 14
ATR_MIN_MULTIPLIER = 0.5
pending_signals = {}
FOLLOW_UP_DELAY = 1800


def compute_ema(closes, period):
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema


def get_bias(candles, period=21):
    closes = [c["close"] for c in candles]
    ema = compute_ema(closes, period)
    if ema is None:
        return None
    return "bullish" if closes[-1] > ema else "bearish"


def compute_atr(candles, period=14):
    if len(candles) < period + 1:
        return None, None
    trs = []
    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i-1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if len(trs) < period:
        return None, None
    current_atr = sum(trs[-period:]) / period
    avg_atr = sum(trs) / len(trs)
    return current_atr, avg_atr


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
        return {"type": "bullish", "size": c3["low"] - c1["high"]}
    if c3["high"] < c1["low"]:
        return {"type": "bearish", "size": c1["low"] - c3["high"]}
    return None


def detect_crt(candles):
    if len(candles) < 3:
        return None
    c1, c2, c3 = candles[-3], candles[-2], candles[-1]
    c1_range = c1["high"] - c1["low"]
    if c1_range == 0:
        return None
    if c2["low"] < c1["low"] and c3["close"] > (c1["low"] + c1_range * 0.5):
        return {"type": "bullish", "strength": round((c3["close"] - c1["low"]) / c1_range, 2)}
    if c2["high"] > c1["high"] and c3["close"] < (c1["high"] - c1_range * 0.5):
        return {"type": "bearish", "strength": round((c1["high"] - c3["close"]) / c1_range, 2)}
    return None


def get_swing_points(candles, lookback=20):
    subset = candles[-lookback:] if len(candles) >= lookback else candles
    return max(c["high"] for c in subset), min(c["low"] for c in subset)


def fibo_zone(swing_high, swing_low, direction):
    price_range = swing_high - swing_low
    if direction == "bullish":
        zone_high = swing_high - price_range * FIBO_LOW
        zone_low = swing_high - price_range * FIBO_HIGH
    else:
        zone_low = swing_low + price_range * FIBO_LOW
        zone_high = swing_low + price_range * FIBO_HIGH
    return zone_low, zone_high


def compute_signal_score(ob, fvg, crt, h4_bias, atr_ratio):
    score = 0
    if ob and ob["type"] == h4_bias:
        score += 1
    if fvg and fvg["type"] == h4_bias:
        score += 1
        if fvg.get("size", 0) > 0:
            score += 0.5
    if crt and crt["type"] == h4_bias:
        score += 1
        if crt.get("strength", 0) > 0.75:
            score += 0.5
    if atr_ratio and atr_ratio > 1.2:
        score += 0.5
    return min(5, round(score))


def score_to_stars(score):
    return "⭐" * score + "☆" * (5 - score)


def analyze(symbol, m5, h1, h4):
    if len(h4) < 21:
        return None
    h4_bias = get_bias(h4)
    if h4_bias is None:
        return None
    if len(h1) < 21:
        return None
    h1_bias = get_bias(h1)
    if h1_bias is None or h1_bias != h4_bias:
        return None
    if len(m5) < 20:
        return None
    current_atr, avg_atr = compute_atr(m5, ATR_PERIOD)
    if current_atr is None or avg_atr is None:
        return None
    if current_atr < avg_atr * ATR_MIN_MULTIPLIER:
        return None
    atr_ratio = current_atr / avg_atr if avg_atr > 0 else 1.0
    ob = detect_order_block(m5)
    fvg = detect_fvg(m5)
    crt = detect_crt(m5)
    confluence = sum(1 for s in [ob, fvg, crt] if s and s["type"] == h4_bias)
    if confluence < 2:
        return None
    current_price = m5[-1]["close"]
    m5_high, m5_low = get_swing_points(m5)
    m5_range = m5_high - m5_low
    if m5_range == 0:
        return None
    zone_low, zone_high = fibo_zone(m5_high, m5_low, h4_bias)
    if not (zone_low <= current_price <= zone_high):
        return None
    entry = current_price
    if ob and ob["type"] == h4_bias:
        sl = ob["ob_low"] * 0.999 if h4_bias == "bullish" else ob["ob_high"] * 1.001
    else:
        sl = m5_low - m5_range * 0.02 if h4_bias == "bullish" else m5_high + m5_range * 0.02
    h1_high, h1_low = get_swing_points(h1, lookback=len(h1))
    h1_range = h1_high - h1_low
    if h1_range == 0:
        return None
    if h4_bias == "bullish":
        tp1 = entry + h1_range * 0.618
        tp2 = entry + h1_range * 1.0
        sl_dist = entry - sl
    else:
        tp1 = entry - h1_range * 0.618
        tp2 = entry - h1_range * 1.0
        sl_dist = sl - entry
    if sl_dist <= 0:
        return None
    rr1 = abs(tp1 - entry) / sl_dist
    rr2 = abs(tp2 - entry) / sl_dist
    if rr1 < MIN_RR:
        return None
    score = compute_signal_score(ob, fvg, crt, h4_bias, atr_ratio)
    return {
        "symbol": symbol,
        "name": INSTRUMENT_NAMES.get(symbol, symbol),
        "direction": h4_bias,
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
        "rr1": round(rr1, 2), "rr2": round(rr2, 2),
        "confluence": confluence,
        "ob": ob is not None and ob["type"] == h4_bias,
        "fvg": fvg is not None and fvg["type"] == h4_bias,
        "crt": crt is not None and crt["type"] == h4_bias,
        "h4_bias": h4_bias, "h1_bias": h1_bias,
        "fibo_low": round(zone_low, 5),
        "fibo_high": round(zone_high, 5),
        "score": score,
        "atr_ratio": round(atr_ratio, 2),
    }


def format_signal(sig):
    arrow = "🟢 BUY" if sig["direction"] == "bullish" else "🔴 SELL"
    bias_icon = "🟢" if sig["h4_bias"] == "bullish" else "🔴"
    icons = []
    if sig["ob"]: icons.append("🧱 OB")
    if sig["fvg"]: icons.append("🌫 FVG")
    if sig["crt"]: icons.append("🔄 CRT")
    p = lambda x: f"{x:.5f}" if x < 10 else f"{x:.2f}"
    return (
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *{sig['name']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{arrow}\n"
        f"🏆 *Score :* {score_to_stars(sig['score'])} ({sig['score']}/5)\n\n"
        f"🎯 *Entry :* `{p(sig['entry'])}`\n"
        f"🔻 *SL :* `{p(sig['sl'])}`\n"
        f"✅ *TP1 :* `{p(sig['tp1'])}` _(R:R {sig['rr1']})_\n"
        f"🚀 *TP2 :* `{p(sig['tp2'])}` _(R:R {sig['rr2']})_\n\n"
        f"📐 *Zone Fibo 61.8–70.9% :*\n"
        f"  🔼 High : `{p(sig['fibo_high'])}`\n"
        f"  🔽 Low : `{p(sig['fibo_low'])}`\n\n"
        f"🔍 *Confluence ({sig['confluence']}/3) :*\n"
        f"{'  '.join(icons)}\n\n"
        f"📈 *Multi-TF :*\n"
        f"  H4 {bias_icon} → H1 {bias_icon} → M5 ✅\n\n"
        f"📊 *ATR :* `{sig['atr_ratio']}x` moyenne\n"
        f"⏰ `{datetime.now(timezone.utc).strftime('%H:%M UTC')}`\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )


async def fetch_current_price(symbol):
    try:
        async with websockets.connect(DERIV_WS_URL, ping_interval=20) as ws:
            req = {"ticks": symbol, "subscribe": 0}
            await ws.send(json.dumps(req))
            while True:
                resp = json.loads(await ws.recv())
                if resp.get("msg_type") == "tick":
                    return float(resp["tick"]["quote"])
                if "error" in resp:
                    return None
    except Exception:
        return None


async def check_follow_ups(bot):
    now = datetime.now(timezone.utc).timestamp()
    to_remove = []
    for key, pending in list(pending_signals.items()):
        signal = pending["signal"]
        sent_at = pending["sent_at"]
        if now - sent_at < FOLLOW_UP_DELAY:
            continue
        current_price = await fetch_current_price(signal["symbol"])
        if current_price is None:
            to_remove.append(key)
            continue
        direction = signal["direction"]
        entry = signal["entry"]
        sl = signal["sl"]
        tp1 = signal["tp1"]
        tp2 = signal["tp2"]
        p = lambda x: f"{x:.5f}" if x < 10 else f"{x:.2f}"
        if direction == "bullish":
            if current_price >= tp2:
                result = "tp2"
            elif current_price >= tp1:
                result = "tp1"
            elif current_price <= sl:
                result = "sl"
            else:
                result = "open"
        else:
            if current_price <= tp2:
                result = "tp2"
            elif current_price <= tp1:
                result = "tp1"
            elif current_price >= sl:
                result = "sl"
            else:
                result = "open"
        if result == "tp2":
            emoji, label = "🚀", "TP2 ATTEINT"
        elif result == "tp1":
            emoji, label = "✅", "TP1 ATTEINT"
        elif result == "sl":
            emoji, label = "❌", "SL TOUCHÉ"
        else:
            emoji, label = "⏳", "EN COURS"
        msg = (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{emoji} *Suivi — {signal['name']}*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"*Résultat :* {label}\n\n"
            f"Prix actuel : `{p(current_price)}`\n"
            f"Entry : `{p(entry)}`\n"
            f"SL : `{p(sl)}` | TP1 : `{p(tp1)}` | TP2 : `{p(tp2)}`\n"
            f"⏰ `{datetime.now(timezone.utc).strftime('%H:%M UTC')}`\n"
            f"━━━━━━━━━━━━━━━━━━━━"
        )
        await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN)
        logger.info(f"Follow-up: {signal['symbol']} → {label}")
        to_remove.append(key)
    for key in to_remove:
        pending_signals.pop(key, None)


async def send_signal(bot, signal):
    key = (signal["symbol"], signal["direction"])
    now = datetime.now(timezone.utc).timestamp()
    if key in last_signal_time and now - last_signal_time[key] < COOLDOWN_SECONDS:
        return
    last_signal_time[key] = now
    await bot.send_message(chat_id=CHAT_ID, text=format_signal(signal), parse_mode=ParseMode.MARKDOWN)
    pending_signals[signal["symbol"]] = {"signal": signal, "sent_at": now}
    logger.info(f"Signal: {signal['symbol']} {signal['direction']} Score={signal['score']}")


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
    await check_follow_ups(bot)
    try:
        async with websockets.connect(DERIV_WS_URL, ping_interval=20) as ws:
            for symbol in INSTRUMENTS:
                try:
                    m5 = await fetch_candles(ws, symbol, 300, 50)
                    await asyncio.sleep(0.3)
                    h1 = await fetch_candles(ws, symbol, 3600, 30)
                    await asyncio.sleep(0.3)
                    h4 = await fetch_candles(ws, symbol, 14400, 30)
                    await asyncio.sleep(0.3)
                    if not m5 or not h1 or not h4:
                        continue
                    signal = analyze(symbol, m5, h1, h4)
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
            "🔍 OB + FVG + CRT | Fibo 61.8–70.9%\n"
            "📈 H4 → H1 → M5\n"
            "⚖️ R:R min 2.0 | Cooldown 4h\n"
            "🏆 Score qualité 1–5\n"
            "📬 Suivi résultat après 30 min\n"
            "━━━━━━━━━━━━━━━━━━━━"
        ),
        parse_mode=ParseMode.MARKDOWN,
    )
    while True:
        await scan_all(bot)
        await asyncio.sleep(300)


if __name__ == "__main__":
    asyncio.run(main())
