"""
Deriv Synthetic Signal Bot v2
25 instruments synthétiques Deriv
Zone 61.8% + 70.5% + 78.6% + FVG + OB
SL sous/sur le swing high/low
"""
import os
import asyncio
import json
import logging
from datetime import datetime, timezone
import websockets
from telegram import Bot
from telegram.constants import ParseMode

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
CHECK_INTERVAL = 300

INSTRUMENTS = {
    "R_10":     {"name": "Volatility 10",       "emoji": "🔵", "min_range": 0.5},
    "R_25":     {"name": "Volatility 25",       "emoji": "🟣", "min_range": 1.0},
    "R_50":     {"name": "Volatility 50",       "emoji": "🟡", "min_range": 2.0},
    "R_75":     {"name": "Volatility 75",       "emoji": "🟠", "min_range": 3.0},
    "R_100":    {"name": "Volatility 100",      "emoji": "🔴", "min_range": 5.0},
    "1HZ10V":   {"name": "Volatility 10 (1s)", "emoji": "🔵", "min_range": 0.5},
    "1HZ25V":   {"name": "Volatility 25 (1s)", "emoji": "🟣", "min_range": 1.0},
    "1HZ50V":   {"name": "Volatility 50 (1s)", "emoji": "🟡", "min_range": 2.0},
    "1HZ75V":   {"name": "Volatility 75 (1s)", "emoji": "🟠", "min_range": 3.0},
    "1HZ100V":  {"name": "Volatility 100 (1s)","emoji": "🔴", "min_range": 5.0},
    "BOOM300N": {"name": "Boom 300",            "emoji": "🚀", "min_range": 5.0},
    "BOOM500":  {"name": "Boom 500",            "emoji": "🚀", "min_range": 8.0},
    "BOOM600":  {"name": "Boom 600",            "emoji": "🚀", "min_range": 8.0},
    "BOOM900":  {"name": "Boom 900",            "emoji": "🚀", "min_range": 10.0},
    "BOOM1000": {"name": "Boom 1000",           "emoji": "🚀", "min_range": 10.0},
    "CRASH300N":{"name": "Crash 300",           "emoji": "💥", "min_range": 5.0},
    "CRASH500": {"name": "Crash 500",           "emoji": "💥", "min_range": 8.0},
    "CRASH600": {"name": "Crash 600",           "emoji": "💥", "min_range": 8.0},
    "CRASH900": {"name": "Crash 900",           "emoji": "💥", "min_range": 10.0},
    "CRASH1000":{"name": "Crash 1000",          "emoji": "💥", "min_range": 10.0},
    "JD10":     {"name": "Jump 10",             "emoji": "⚡", "min_range": 5.0},
    "JD25":     {"name": "Jump 25",             "emoji": "⚡", "min_range": 10.0},
    "JD50":     {"name": "Jump 50",             "emoji": "⚡", "min_range": 20.0},
    "JD75":     {"name": "Jump 75",             "emoji": "⚡", "min_range": 30.0},
    "JD100":    {"name": "Jump 100",            "emoji": "⚡", "min_range": 50.0},
}

DERIV_WS_URL = "wss://ws.binaryws.com/websockets/v3?app_id=1089"
FIB_LEVELS = [0.618, 0.705, 0.786]
FIB_LABELS = {0.618: "61.8%", 0.705: "70.5%", 0.786: "78.6%"}
TOLERANCE_PCT = 0.50
MIN_SCORE = 3
MIN_RR = 2.0
SL_BUFFER_PCT = 0.002

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
active_signals = {}

def compute_ema(closes, period):
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema

def get_trend(candles):
    if not candles or len(candles) < 5:
        return "neutral"
    closes = [c["close"] for c in candles]
    ema21 = compute_ema(closes, 21)
    if ema21 is None:
        return "neutral"
    return "bull" if closes[-1] > ema21 else "bear"

def get_structure(candles):
    if not candles or len(candles) < 3:
        return "neutral"
    bull = sum(1 for c in candles[:3] if c["close"] > c["open"])
    return "bull" if bull >= 2 else "bear"

def detect_swing(candles, min_range):
    if len(candles) < 10:
        return None
    h = max(c["high"] for c in candles)
    l = min(c["low"] for c in candles)
    if h - l < min_range:
        return None
    return h, l

def detect_fvg(candles, direction, zh, zl):
    fvgs = []
    if len(candles) < 3:
        return fvgs
    for i in range(len(candles) - 2):
        c0, c2 = candles[i+2], candles[i]
        if direction == "bull" and c2["low"] > c0["high"]:
            mid = (c2["low"] + c0["high"]) / 2
            if zl <= mid <= zh:
                fvgs.append({"type": "Bull FVG", "top": round(c2["low"], 5), "bot": round(c0["high"], 5)})
        elif direction == "bear" and c2["high"] < c0["low"]:
            mid = (c2["high"] + c0["low"]) / 2
            if zl <= mid <= zh:
                fvgs.append({"type": "Bear FVG", "top": round(c0["low"], 5), "bot": round(c2["high"], 5)})
    return fvgs[-2:] if fvgs else []

def detect_ob(candles, direction, zh, zl):
    obs = []
    if len(candles) < 4:
        return obs
    for i in range(1, len(candles) - 2):
        ob, nxt = candles[i], candles[i-1]
        if abs(ob["close"] - ob["open"]) < 1e-10:
            continue
        in_zone = zl <= ob["low"] <= zh or zl <= ob["high"] <= zh
        if not in_zone:
            continue
        if direction == "bull" and ob["close"] < ob["open"] and nxt["close"] > ob["high"]:
            obs.append({"type": "Bull OB", "top": round(ob["high"], 5), "bot": round(ob["low"], 5)})
        elif direction == "bear" and ob["close"] > ob["open"] and nxt["close"] < ob["low"]:
            obs.append({"type": "Bear OB", "top": round(ob["high"], 5), "bot": round(ob["low"], 5)})
    return obs[-2:] if obs else []

def compute_score(obs, fvgs, h4_trend, h1_struct, atr_ratio):
    score = 0
    if obs: score += 1
    if fvgs: score += 1
    if obs and fvgs: score += 1
    if h4_trend == h1_struct: score += 0.5
    if atr_ratio and atr_ratio > 1.2: score += 0.5
    return min(5, round(score))

def score_to_stars(score):
    return "⭐" * score + "☆" * (5 - score)

def detect_setup(symbol, price, m5, h1, h4, min_range, name, emoji):
    swing = detect_swing(m5, min_range)
    if not swing:
        return None
    high, low = swing
    rng = high - low
    h4_trend = get_trend(h4) if h4 else get_trend(m5)
    h1_struct = get_structure(h1) if h1 else "neutral"
    direction = h4_trend
    if direction == "neutral":
        direction = "bull" if m5[0]["close"] > m5[-1]["close"] else "bear"
    tol = rng * TOLERANCE_PCT / 100
    buffer = rng * SL_BUFFER_PCT
    for ratio in FIB_LEVELS:
        if direction == "bull":
            level = round(high - rng * ratio, 5)
        else:
            level = round(low + rng * ratio, 5)
        if abs(price - level) > tol:
            continue
        zh = level + rng * 0.01
        zl = level - rng * 0.01
        fvgs = detect_fvg(m5, direction, zh, zl)
        obs = detect_ob(m5, direction, zh, zl)
        confluence = (1 if fvgs else 0) + (1 if obs else 0)
        if confluence < 1:
            continue
        trs = []
        for i in range(1, len(m5)):
            tr = max(m5[i]["high"] - m5[i]["low"],
                     abs(m5[i]["high"] - m5[i-1]["close"]),
                     abs(m5[i]["low"] - m5[i-1]["close"]))
            trs.append(tr)
        atr_ratio = None
        if len(trs) >= 14:
            current_atr = sum(trs[-14:]) / 14
            avg_atr = sum(trs) / len(trs)
            atr_ratio = round(current_atr / avg_atr, 2) if avg_atr > 0 else None
        score = compute_score(obs, fvgs, h4_trend, h1_struct, atr_ratio)
        if score < MIN_SCORE:
            continue
        if direction == "bull":
            sl = round(low - buffer, 5)
            t1d = rng * 0.382
            t2d = rng * 0.618
            tp1 = round(level + t1d, 5)
            tp2 = round(level + t2d, 5)
            bias = "BUY"
            sig_emoji = "🟢"
        else:
            sl = round(high + buffer, 5)
            t1d = rng * 0.382
            t2d = rng * 0.618
            tp1 = round(level - t1d, 5)
            tp2 = round(level - t2d, 5)
            bias = "SELL"
            sig_emoji = "🔴"
        sl_dist = abs(level - sl)
        if sl_dist == 0:
            continue
        rr1 = round(abs(tp1 - level) / sl_dist, 1)
        rr2 = round(abs(tp2 - level) / sl_dist, 1)
        if rr1 < MIN_RR:
            continue
        return {
            "symbol": symbol, "name": name, "asset_emoji": emoji,
            "direction": direction, "bias": bias, "emoji": sig_emoji,
            "fib_label": FIB_LABELS[ratio],
            "price": round(price, 5), "entry": level,
            "sl": sl, "tp1": tp1, "tp2": tp2,
            "rr1": rr1, "rr2": rr2,
            "swing_high": round(high, 5), "swing_low": round(low, 5),
            "h4": h4_trend.upper(), "h1": h1_struct.upper(),
            "fvgs": fvgs, "obs": obs,
            "confluence": confluence, "score": score,
            "atr_ratio": atr_ratio,
            "zh": round(zh, 5), "zl": round(zl, 5),
        }
    return None

def format_signal(s):
    dl = "LONG" if s["direction"] == "bull" else "SHORT"
    stars = score_to_stars(s["score"])
    cl = {1: "Bonne confluence", 2: "Confluence maximale ✅"}.get(s["confluence"], "Signal")
    fvg_txt = f"\n├ {s['fvgs'][-1]['type']}: `{s['fvgs'][-1]['bot']}`–`{s['fvgs'][-1]['top']}`" if s["fvgs"] else ""
    ob_txt = f"\n├ {s['obs'][-1]['type']}: `{s['obs'][-1]['bot']}`–`{s['obs'][-1]['top']}`" if s["obs"] else ""
    p = lambda x: f"{x:.5f}" if x < 10 else f"{x:.2f}"
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    sl_note = "sous Swing Low 🔽" if s["direction"] == "bull" else "au-dessus Swing High 🔼"
    return (
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{s['emoji']} *SIGNAL {s['name']} — {s['bias']}* {s['asset_emoji']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏆 *Score :* {stars} ({s['score']}/5)\n"
        f"⏰ `{now}`\n\n"
        f"📐 *Zone Fibo {s['fib_label']}*\n"
        f"├ Zone : `{p(s['zl'])}`–`{p(s['zh'])}`\n"
        f"├ 🔼 Swing High : `{p(s['swing_high'])}`\n"
        f"└ 🔽 Swing Low : `{p(s['swing_low'])}`\n\n"
        f"📈 *Multi-TF*\n"
        f"├ H4 : *{s['h4']}*\n"
        f"└ H1 : *{s['h1']}*\n\n"
        f"🔍 *Confluence SMC* {stars}\n"
        f"├ {cl}{ob_txt}{fvg_txt}\n"
        f"└ Zone {s['fib_label']} ✅\n\n"
        f"🎯 *ORDRE {dl}*\n"
        f"├ Entrée : `{p(s['entry'])}`\n"
        f"├ SL : `{p(s['sl'])}` 🛑 ({sl_note})\n"
        f"├ TP1 : `{p(s['tp1'])}` ✅ R:R 1:{s['rr1']}\n"
        f"└ TP2 : `{p(s['tp2'])}` 🚀 R:R 1:{s['rr2']}\n\n"
        f"📊 *ATR :* `{s['atr_ratio']}x` moyenne\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )

async def fetch_candles(ws, symbol, granularity, count=60):
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
            log.warning(f"Deriv error {symbol}: {resp['error']}")
            return []

async def fetch_current_price(symbol):
    try:
        async with websockets.connect(DERIV_WS_URL, ping_interval=20) as ws:
            req = {"ticks_history": symbol, "adjust_start_time": 1,
                   "count": 1, "end": "latest", "granularity": 60, "style": "candles"}
            await ws.send(json.dumps(req))
            while True:
                resp = json.loads(await ws.recv())
                if resp.get("msg_type") == "candles":
                    candles = resp.get("candles", [])
                    return float(candles[-1]["close"]) if candles else None
                if "error" in resp:
                    return None
    except Exception:
        return None

async def check_active_signals(bot):
    to_remove = []
    for symbol, data in list(active_signals.items()):
        sig = data["signal"]
        current_price = await fetch_current_price(symbol)
        if current_price is None:
            continue
        direction = sig["direction"]
        tp1, tp2, sl = sig["tp1"], sig["tp2"], sig["sl"]
        p = lambda x: f"{x:.5f}" if x < 10 else f"{x:.2f}"
        result = None
        if direction == "bull":
            if current_price >= tp2: result = ("tp2", "🚀", "TP2 ATTEINT")
            elif current_price >= tp1: result = ("tp1", "✅", "TP1 ATTEINT")
            elif current_price <= sl: result = ("sl", "❌", "SL TOUCHÉ")
        else:
            if current_price <= tp2: result = ("tp2", "🚀", "TP2 ATTEINT")
            elif current_price <= tp1: result = ("tp1", "✅", "TP1 ATTEINT")
            elif current_price >= sl: result = ("sl", "❌", "SL TOUCHÉ")
        if result:
            _, emoji, label = result
            msg = (
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"{emoji} *Suivi — {sig['name']}*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"*Résultat :* {label}\n\n"
                f"Prix actuel : `{p(current_price)}`\n"
                f"Entry : `{p(sig['entry'])}`\n"
                f"SL : `{p(sl)}` | TP1 : `{p(tp1)}` | TP2 : `{p(tp2)}`\n"
                f"⏰ `{datetime.now(timezone.utc).strftime('%H:%M UTC')}`\n"
                f"━━━━━━━━━━━━━━━━━━━━"
            )
            await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN)
            to_remove.append(symbol)
    for symbol in to_remove:
        active_signals.pop(symbol, None)

async def scan_all(bot):
    log.info("Scan en cours...")
    await check_active_signals(bot)
    try:
        async with websockets.connect(DERIV_WS_URL, ping_interval=20) as ws:
            for symbol, cfg in INSTRUMENTS.items():
                try:
                    m5 = await fetch_candles(ws, symbol, 300, 60)
                    await asyncio.sleep(0.3)
                    h1 = await fetch_candles(ws, symbol, 3600, 30)
                    await asyncio.sleep(0.3)
                    h4 = await fetch_candles(ws, symbol, 14400, 30)
                    await asyncio.sleep(0.3)
                    if not m5:
                        continue
                    price = m5[-1]["close"]
                    setup = detect_setup(symbol, price, m5, h1 or [], h4 or [],
                                         cfg["min_range"], cfg["name"], cfg["emoji"])
                    if setup:
                        await bot.send_message(chat_id=CHAT_ID, text=format_signal(setup),
                                               parse_mode=ParseMode.MARKDOWN)
                        active_signals[symbol] = {"signal": setup}
                        log.info(f"Signal: {symbol} {setup['bias']} Score={setup['score']}")
                except Exception as e:
                    log.error(f"Erreur {symbol}: {e}")
    except Exception as e:
        log.error(f"WS erreur: {e}")
    log.info("Scan terminé.")

async def main():
    if not TELEGRAM_TOKEN or not CHAT_ID:
        raise ValueError("TELEGRAM_TOKEN ou CHAT_ID manquant")
    bot = Bot(token=TELEGRAM_TOKEN)
    await bot.send_message(
        chat_id=CHAT_ID,
        text=(
            "🤖 *Deriv Synthetic Signal Bot v2*\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 {len(INSTRUMENTS)} instruments surveillés\n"
            "🔍 Zone 61.8% + 70.5% + 78.6% + FVG + OB\n"
            "📈 H4 → H1 → M5\n"
            f"⭐ Score min {MIN_SCORE}/5 | R:R min {MIN_RR}\n"
            "🛑 SL sous/sur Swing High/Low\n"
            "📬 Suivi TP/SL en temps réel\n"
            "⏱ Scan toutes les 5 min\n"
            "━━━━━━━━━━━━━━━━━━━━"
        ),
        parse_mode=ParseMode.MARKDOWN,
    )
    while True:
        await scan_all(bot)
        await asyncio.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
