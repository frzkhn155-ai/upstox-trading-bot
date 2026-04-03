"""
╔══════════════════════════════════════════════════════════════════════════════╗
║        REAL-TIME AI TRADING ASSISTANT  —  Dual-Provider Failover            ║
║                                                                              ║
║  PRIMARY  : Groq      — llama-3.3-70b-versatile  (free, 14400 req/day)      ║
║  FALLBACK : NVIDIA    — llama-3.3-70b-instruct   (free, 1000 req/day)       ║
║                                                                              ║
║  Automatic failover logic:                                                   ║
║    • Groq 429 (rate limit)  → switch to NVIDIA immediately                  ║
║    • Groq 5xx / timeout     → switch to NVIDIA immediately                  ║
║    • NVIDIA also fails      → skip scan, retry Groq next cycle              ║
║    • Groq recovers          → switch back automatically after GROQ_RETRY_S  ║
║                                                                              ║
║  Cost: ₹0  (both providers are free tier)                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

SETUP (one-time, 5 minutes):

  GROQ (primary):
    1. Go to https://console.groq.com → sign up → API Keys → Create key
    2. Paste below as GROQ_API_KEY
    3. Add as GitHub secret:  GROQ_API_KEY

  NVIDIA (fallback):
    1. Go to https://build.nvidia.com → sign in with email
    2. Search "llama-3.3-70b" → click "Get API Key"
    3. Paste below as NVIDIA_API_KEY
    4. Add as GitHub secret:  NVIDIA_API_KEY
"""

import os as _os
import threading
import time
import json
import requests as _req
from datetime import datetime, timedelta
from typing import Optional

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

# ── API Keys (read from env on GitHub Actions, fallback to hardcoded) ─────────
GROQ_API_KEY   = _os.environ.get("GROQ_API_KEY",   "YOUR_GROQ_API_KEY_HERE")
NVIDIA_API_KEY = _os.environ.get("NVIDIA_API_KEY", "YOUR_NVIDIA_API_KEY_HERE")

AI_ENABLED     = True    # set False to disable entirely

# ── Models ────────────────────────────────────────────────────────────────────
GROQ_MODEL     = "llama-3.3-70b-versatile"     # best free Groq model
NVIDIA_MODEL   = "meta/llama-3.3-70b-instruct" # same weights, NVIDIA hosted

# ── Behaviour knobs ───────────────────────────────────────────────────────────
AI_SCAN_INTERVAL_SECONDS    = 35     # slightly offset from 30s bot cycle
AI_MAX_TOKENS               = 400    # keep responses short for mobile screen
AI_TEMPERATURE              = 0.2    # low = more decisive, less hallucination
AI_AUTO_EXIT_ENABLED        = True   # let AI exit confirmed reversals
AI_MIN_HOLD_MINUTES         = 15     # don't auto-exit within first N minutes
AI_EXIT_PROFIT_ONLY         = False  # False = AI can exit losses too
AI_LOSS_EXIT_THRESHOLD_PCT  = 5.0    # only auto-exit loss if > 5% down
AI_SILENT_WHEN_NO_POSITIONS = True   # skip API call when nothing is open

# ── Soft AI Exit (profit protection) ─────────────────────────────────────────
AI_SOFT_EXIT_ENABLED   = True
AI_TRAIL_TRIGGER_PCT   = 15.0   # % PnL → tighten trail on WATCH
AI_TRAIL_TIGHT_PCT     = 3.0    # trail % to lock in once triggered
AI_FORCE_EXIT_PCT      = 25.0   # % PnL → exit if verdict != HOLD

# ── Failover settings ─────────────────────────────────────────────────────────
GROQ_RETRY_SECONDS  = 300   # after switching to NVIDIA, retry Groq after 5 min
NVIDIA_DAILY_LIMIT  = 950   # NVIDIA free = 1000/day, stop at 950 to stay safe
GROQ_DAILY_LIMIT    = 14000 # Groq free = 14400/day, stop at 14000 to stay safe

# ── API endpoints ─────────────────────────────────────────────────────────────
_GROQ_URL   = "https://api.groq.com/openai/v1/chat/completions"
_NVIDIA_URL = "https://integrate.api.nvidia.com/v1/chat/completions"

# ── State ─────────────────────────────────────────────────────────────────────
_ai_thread:         Optional[threading.Thread] = None
_ai_last_run:       Optional[datetime]         = None
_ai_call_count:     int                        = 0   # total calls today
_groq_call_count:   int                        = 0   # Groq calls today
_nvidia_call_count: int                        = 0   # NVIDIA calls today
_ai_error_streak:   int                        = 0

# Failover state
_using_nvidia:        bool               = False  # currently on fallback?
_groq_failed_at:      Optional[datetime] = None   # when Groq last failed
_nvidia_fail_streak:  int                = 0      # NVIDIA consecutive errors

_AI_LOCK = threading.Lock()


# ══════════════════════════════════════════════════════════════════════════════
# SYSTEM PROMPT
# ══════════════════════════════════════════════════════════════════════════════

_SYSTEM_PROMPT = """You are a real-time trading assistant for an Upstox F&O bot.
The bot trades NSE options (CE for LONG, PE for SHORT) using:
  • Klinger Oscillator (KO) — bullish when positive/rising, bearish when negative/falling
  • Heikin-Ashi (HA) candles — flip from green→red signals bearish reversal, red→green bullish
  • Bollinger Bands (squeeze = low volatility breakout setup)
  • R3/S3 pivot levels (breakout above R3 = CE, breakdown below S3 = PE)
  • ORB (Opening Range Breakout at 09:20)

Snapshot fields explained:
  u_ltp         — underlying stock's live price (not the option). Use to judge reversal strength.
  opt_ltp       — option premium live price.
  market.dir    — overall Nifty50 direction (UP/DOWN). A trade AGAINST market direction is higher risk.
  market.nifty  — Nifty50 ltp + % change.
  news          — top ET Markets headlines (≤5). Mention any that are relevant to held stocks.
  earn_warn     — true if this stock reports earnings within 3 days. Always flag this as elevated risk.

You receive this snapshot every 30 seconds. Your job:
1. EXPLAIN: In 1 sentence, what just happened (signal taken, blocked, alert fired).
2. WARN: Flag any position that looks risky — mention u_ltp trend, market direction, or news if relevant.
3. DECIDE: For each active position output one of:
   HOLD   — conditions still favourable
   WATCH  — deteriorating, monitor next scan
   EXIT   — confirmed reversal (HA flip + Klinger confirms), recommend exit now

OUTPUT FORMAT (strict — bot parses this):
SUMMARY: <one sentence>
POSITIONS:
  <SYMBOL>: <HOLD|WATCH|EXIT> — <reason in ≤15 words>
WATCHLIST:
  <SYMBOL>: <ALERT|OK> — <reason>
END

Rules:
- Never say EXIT unless HA flip AND Klinger both confirm.
- Never say EXIT for a position held less than 15 minutes (too noisy).
- For LOSING positions: say EXIT if reversal confirmed AND loss > 5%.
  Cutting a confirmed loss early beats holding through a full reversal.
- For losses under 5%: say WATCH — too small to act on, could be noise.
- If earnings_warning=true for a held stock: say WATCH minimum, note the risk.
- If market direction opposes the trade direction: note it but don't override Klinger.
- Keep total response under 300 words.
- Use ₹ for prices. Be direct. No filler words."""


# ══════════════════════════════════════════════════════════════════════════════
# MARKET CONTEXT FETCHER  — Nifty/BankNifty + news + earnings (cached 5 min)
# ══════════════════════════════════════════════════════════════════════════════

_MARKET_CACHE:        dict              = {}
_MARKET_CACHE_TIME:   Optional[datetime] = None
_MARKET_CACHE_TTL                       = 300

_EARNINGS_CACHE:      dict = {}
_EARNINGS_CACHE_DATE: str  = ""


def _fetch_market_context() -> dict:
    global _MARKET_CACHE, _MARKET_CACHE_TIME, _EARNINGS_CACHE, _EARNINGS_CACHE_DATE
    now = datetime.now()
    if (_MARKET_CACHE_TIME and
            (now - _MARKET_CACHE_TIME).total_seconds() < _MARKET_CACHE_TTL):
        return _MARKET_CACHE
    ctx = {}
    try:
        sess = _req.Session()
        sess.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept":     "application/json",
            "Referer":    "https://www.nseindia.com",
        })
        sess.get("https://www.nseindia.com", timeout=5)
        indices = {}
        for idx_name, key in [("NIFTY 50", "50"), ("NIFTY BANK", "BANK")]:
            try:
                r = sess.get(
                    "https://www.nseindia.com/api/quote-index",
                    params={"index": idx_name},
                    timeout=8,
                )
                if r.status_code == 200:
                    d   = r.json().get("data", {})
                    chg = round(d.get("percChange", 0), 2)
                    indices[key] = {
                        "ltp": round(d.get("last", 0), 2),
                        "chg": chg,
                        "dir": "UP" if chg >= 0 else "DOWN",
                    }
            except Exception:
                pass
        if indices:
            ctx["indices"] = indices
    except Exception:
        pass
    _MARKET_CACHE      = ctx
    _MARKET_CACHE_TIME = now
    return ctx


# ══════════════════════════════════════════════════════════════════════════════
# SNAPSHOT BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def _build_snapshot(bot_globals: dict) -> dict:
    active    = bot_globals.get("ACTIVE_POSITIONS", {})
    watchlist = bot_globals.get("HA_WATCHLIST",     {})
    mctx      = _fetch_market_context()

    positions = {}
    for pid, pos in active.items():
        sym    = pos.get("symbol", "")
        entry  = pos.get("entry_price", 0)
        ikey   = pos.get("instrument_key", "")
        ukey   = pos.get("underlying_key",  "")
        held   = round((datetime.now() - pos.get("timestamp", datetime.now())
                        ).total_seconds() / 60, 1)
        positions[sym] = {
            "direction":  pos.get("direction", ""),
            "entry":      entry,
            "held_min":   held,
            "strategy":   pos.get("strategy", ""),
        }

    recent_log = bot_globals.get("_AI_RECENT_LOG", [])
    orders     = bot_globals.get("GAP_ORDER_COUNT", 0) + \
                 bot_globals.get("BREAKOUT_ORDER_COUNT", 0)

    snap = {
        "time":      datetime.now().strftime("%H:%M:%S"),
        "market":    mctx.get("indices", {}),
        "positions": positions,
        "watchlist": {k: v.get("signal", "") for k, v in watchlist.items()},
        "pnl":       round(bot_globals.get("DAILY_PNL", 0), 2),
        "orders":    f"{orders}/{bot_globals.get('MAX_ORDERS_PER_DAY', 10)}",
        "stopped":   bot_globals.get("TRADING_STOPPED", False),
        "log":       recent_log,
    }
    if mctx.get("news"):
        snap["news"] = mctx["news"]
    return snap


# ══════════════════════════════════════════════════════════════════════════════
# PROVIDER ROUTER  — Groq primary, NVIDIA fallback
# ══════════════════════════════════════════════════════════════════════════════

def _build_payload(model: str, snapshot: dict) -> dict:
    """Build the common OpenAI-compatible payload for either provider."""
    return {
        "model":       model,
        "messages":    [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content":
                f"Current bot snapshot:\n{json.dumps(snapshot, indent=2, default=str)}"},
        ],
        "max_tokens":  AI_MAX_TOKENS,
        "temperature": AI_TEMPERATURE,
    }


def _call_groq(snapshot: dict) -> Optional[str]:
    """Call Groq API. Returns response text, or None on any failure."""
    global _groq_call_count, _ai_error_streak, _using_nvidia, _groq_failed_at

    if not GROQ_API_KEY or GROQ_API_KEY == "YOUR_GROQ_API_KEY_HERE":
        return None
    if _groq_call_count >= GROQ_DAILY_LIMIT:
        print(f"\n⚠️  AI: Groq daily limit ({GROQ_DAILY_LIMIT}) reached — switching to NVIDIA")
        _using_nvidia   = True
        _groq_failed_at = datetime.now()
        return None

    try:
        resp = _req.post(
            _GROQ_URL,
            headers={"Authorization": f"Bearer {GROQ_API_KEY}",
                     "Content-Type":  "application/json"},
            json=_build_payload(GROQ_MODEL, snapshot),
            timeout=20,
        )
        if resp.status_code == 200:
            _groq_call_count += 1
            _ai_error_streak  = 0
            _using_nvidia     = False   # Groq working — stay on primary
            return resp.json()["choices"][0]["message"]["content"].strip()

        elif resp.status_code == 429:
            print("\n⚠️  AI: Groq rate-limited (429) — switching to NVIDIA fallback")
            _using_nvidia   = True
            _groq_failed_at = datetime.now()
            _ai_error_streak += 1
            return None

        else:
            print(f"\n⚠️  AI: Groq error {resp.status_code} — switching to NVIDIA fallback")
            _using_nvidia   = True
            _groq_failed_at = datetime.now()
            _ai_error_streak += 1
            return None

    except Exception as e:
        print(f"\n⚠️  AI: Groq request failed ({e}) — switching to NVIDIA fallback")
        _using_nvidia   = True
        _groq_failed_at = datetime.now()
        _ai_error_streak += 1
        return None


def _call_nvidia(snapshot: dict) -> Optional[str]:
    """Call NVIDIA NIM API (safety fallback). Returns response text or None."""
    global _nvidia_call_count, _nvidia_fail_streak

    if not NVIDIA_API_KEY or NVIDIA_API_KEY == "YOUR_NVIDIA_API_KEY_HERE":
        print("\n⚠️  AI: NVIDIA_API_KEY not set — cannot use fallback.")
        print("      Get free key at https://build.nvidia.com")
        return None
    if _nvidia_call_count >= NVIDIA_DAILY_LIMIT:
        print(f"\n⚠️  AI: NVIDIA daily limit ({NVIDIA_DAILY_LIMIT}) reached — both providers exhausted")
        return None

    try:
        resp = _req.post(
            _NVIDIA_URL,
            headers={"Authorization": f"Bearer {NVIDIA_API_KEY}",
                     "Content-Type":  "application/json"},
            json=_build_payload(NVIDIA_MODEL, snapshot),
            timeout=25,
        )
        if resp.status_code == 200:
            _nvidia_call_count  += 1
            _nvidia_fail_streak  = 0
            return resp.json()["choices"][0]["message"]["content"].strip()

        elif resp.status_code == 429:
            print(f"\n⚠️  AI: NVIDIA also rate-limited (429) — skipping scan")
            _nvidia_fail_streak += 1
            return None

        else:
            print(f"\n⚠️  AI: NVIDIA error {resp.status_code} — skipping scan")
            _nvidia_fail_streak += 1
            return None

    except Exception as e:
        print(f"\n⚠️  AI: NVIDIA request failed ({e}) — skipping scan")
        _nvidia_fail_streak += 1
        return None


def _call_ai(snapshot: dict) -> Optional[str]:
    """
    Main router — tries Groq first, falls back to NVIDIA automatically.
    Also retries Groq after GROQ_RETRY_SECONDS if it previously failed.
    """
    global _using_nvidia, _groq_failed_at, _ai_call_count

    # ── Try to restore Groq if it failed a while ago ──────────────────────────
    if _using_nvidia and _groq_failed_at:
        secs_since_fail = (datetime.now() - _groq_failed_at).total_seconds()
        if secs_since_fail >= GROQ_RETRY_SECONDS:
            print(f"\n🔄 AI: {GROQ_RETRY_SECONDS}s elapsed — retrying Groq (primary)")
            _using_nvidia = False

    # ── Route to correct provider ─────────────────────────────────────────────
    if not _using_nvidia:
        result = _call_groq(snapshot)
        if result:
            _ai_call_count += 1
            return result
        # Groq failed → try NVIDIA this scan
        if _using_nvidia:
            result = _call_nvidia(snapshot)
            if result:
                _ai_call_count += 1
                provider_label = "NVIDIA (fallback)"
                print(f"   ↳ Response from {provider_label}")
            return result
    else:
        # Already on NVIDIA
        result = _call_nvidia(snapshot)
        if result:
            _ai_call_count += 1
        return result

    return None


def _provider_status() -> str:
    """One-line string showing current provider and call counts."""
    if _using_nvidia:
        return (f"NVIDIA fallback | Groq: {_groq_call_count}/{GROQ_DAILY_LIMIT} | "
                f"NVIDIA: {_nvidia_call_count}/{NVIDIA_DAILY_LIMIT}")
    return (f"Groq primary | Groq: {_groq_call_count}/{GROQ_DAILY_LIMIT} | "
            f"NVIDIA: {_nvidia_call_count}/{NVIDIA_DAILY_LIMIT} (standby)")


# ══════════════════════════════════════════════════════════════════════════════
# RESPONSE PARSER
# ══════════════════════════════════════════════════════════════════════════════

def _parse_response(text: str) -> dict:
    result  = {"summary": "", "positions": {}, "watchlist": {}, "raw": text}
    section = None
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("SUMMARY:"):
            result["summary"] = line[len("SUMMARY:"):].strip()
        elif line.startswith("POSITIONS:"):
            section = "positions"
        elif line.startswith("WATCHLIST:"):
            section = "watchlist"
        elif line == "END":
            section = None
        elif section in ("positions", "watchlist") and ":" in line:
            sym_part, _, rest = line.lstrip().partition(":")
            sym_part = sym_part.strip().upper()
            rest     = rest.strip()
            verdict  = ""
            reason   = rest
            for v in ("EXIT", "WATCH", "HOLD", "ALERT", "OK"):
                if rest.upper().startswith(v):
                    verdict = v
                    reason  = rest[len(v):].lstrip(" —-").strip()
                    break
            if sym_part:
                result[section][sym_part] = {"verdict": verdict, "reason": reason}
    return result


# ══════════════════════════════════════════════════════════════════════════════
# AUTO-EXIT ENFORCER
# ══════════════════════════════════════════════════════════════════════════════

def _trail_stoploss(symbol: str, pos_id: str, current_price: float,
                    bot_globals: dict) -> None:
    peak_prices = bot_globals.get("POSITION_PEAK_PRICES")
    if peak_prices is None:
        return
    old_peak = peak_prices.get(pos_id, current_price)
    new_peak  = max(old_peak, current_price)
    peak_prices[pos_id] = new_peak
    tight_trail_price = round(new_peak * (1 - AI_TRAIL_TIGHT_PCT / 100), 2)
    print(
        f"\n🤖 AI SOFT-TRAIL: {symbol} | PnL >{AI_TRAIL_TRIGGER_PCT}% + WATCH\n"
        f"   Peak locked at ₹{new_peak:.2f} | "
        f"Tight trail stop → ₹{tight_trail_price:.2f} "
        f"({AI_TRAIL_TIGHT_PCT}% below peak)"
    )


def _enforce_exits(parsed: dict, bot_globals: dict, trader) -> None:
    if not AI_AUTO_EXIT_ENABLED or trader is None:
        return
    active  = bot_globals.get("ACTIVE_POSITIONS", {})
    exit_fn = bot_globals.get("exit_position")
    get_ltp = getattr(trader, "get_ltp", None)
    if not exit_fn or not get_ltp:
        return
    now = datetime.now()

    for pos_id, pos in list(active.items()):
        symbol  = pos.get("symbol", "")
        verdict = parsed["positions"].get(symbol, {}).get("verdict", "")

        # ── Soft exit ──────────────────────────────────────────────────────────
        if AI_SOFT_EXIT_ENABLED and exit_fn and get_ltp:
            option_key  = pos.get("instrument_key", "")
            _soft_price = get_ltp(option_key) if option_key else None
            _entry      = pos.get("entry_price", 0)
            if _soft_price and _entry:
                _pnl = (_soft_price - _entry) / _entry * 100
                if _pnl > AI_TRAIL_TRIGGER_PCT and verdict == "WATCH":
                    _trail_stoploss(symbol, pos_id, _soft_price, bot_globals)
                if _pnl > AI_FORCE_EXIT_PCT and verdict != "HOLD":
                    _reason = parsed["positions"].get(symbol, {}).get(
                        "reason", "AI soft-exit: profit protection"
                    )
                    print(
                        f"\n{'='*70}\n"
                        f"🤖 AI SOFT-EXIT (profit lock): {symbol}\n"
                        f"{'='*70}\n"
                        f"   PnL    : {_pnl:+.1f}% > {AI_FORCE_EXIT_PCT}% threshold\n"
                        f"   Verdict: {verdict}\n"
                        f"   Reason : {_reason}\n"
                        f"   Price  : ₹{_soft_price:.2f} | Entry: ₹{_entry:.2f}\n"
                        f"{'='*70}"
                    )
                    try:
                        exit_fn(trader, pos_id, pos, _soft_price,
                                reason="AI_SOFT_EXIT_PROFIT_LOCK")
                    except Exception as _e:
                        print(f"   ⚠️ AI soft-exit error: {_e}")
                    continue

        if verdict != "EXIT":
            continue

        # ── Safety gate 1: minimum hold time ──────────────────────────────────
        entry_time = pos.get("timestamp", now)
        held_mins  = (now - entry_time).total_seconds() / 60 if entry_time else 0
        if held_mins < AI_MIN_HOLD_MINUTES:
            print(f"\n🤖 AI: EXIT for {symbol} but held only "
                  f"{held_mins:.1f}min < {AI_MIN_HOLD_MINUTES}min — skipping")
            continue

        # ── Safety gate 2: loss threshold ─────────────────────────────────────
        option_key    = pos.get("instrument_key", "")
        current_price = get_ltp(option_key) if option_key else None
        entry         = pos.get("entry_price", 0)
        if current_price and entry:
            pnl_pct = (current_price - entry) / entry * 100
            is_loss = pnl_pct < 0
            if is_loss:
                if AI_EXIT_PROFIT_ONLY:
                    print(f"\n🤖 AI: EXIT for {symbol} blocked — loss "
                          f"({pnl_pct:.1f}%) + AI_EXIT_PROFIT_ONLY=True")
                    continue
                elif abs(pnl_pct) < AI_LOSS_EXIT_THRESHOLD_PCT:
                    print(f"\n🤖 AI: EXIT for {symbol} — loss {pnl_pct:.1f}% "
                          f"< threshold {AI_LOSS_EXIT_THRESHOLD_PCT}% — waiting")
                    continue

        if not current_price:
            print(f"\n🤖 AI: EXIT for {symbol} — could not fetch LTP, skipping")
            continue

        # ── All gates passed — auto-exit ───────────────────────────────────────
        reason_text = parsed["positions"][symbol].get("reason", "AI reversal signal")
        pnl_pct     = (current_price - entry) / entry * 100 if entry else 0
        pnl_icon    = "📈" if pnl_pct >= 0 else "📉"
        print(
            f"\n{'='*70}\n"
            f"🤖 AI AUTO-EXIT: {symbol}\n"
            f"{'='*70}\n"
            f"   Reason : {reason_text}\n"
            f"   Price  : ₹{current_price:.2f} | Entry: ₹{entry:.2f} | "
            f"{pnl_icon} P&L: {pnl_pct:+.1f}%\n"
            f"   Held   : {held_mins:.1f}min\n"
            f"{'='*70}"
        )
        try:
            exit_fn(trader, pos_id, pos, current_price, reason="AI_AUTO_EXIT")
        except Exception as e:
            print(f"   ⚠️ AI auto-exit error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# PRINT FORMATTED OUTPUT
# ══════════════════════════════════════════════════════════════════════════════

def _print_ai_response(parsed: dict, call_num: int) -> None:
    now      = datetime.now().strftime("%H:%M:%S")
    provider = "NVIDIA" if _using_nvidia else "Groq"
    print(f"\n{'─'*70}")
    print(f"🤖 AI ASSISTANT  [{now}]  (call #{call_num})  [{provider}]")
    print(f"{'─'*70}")
    if parsed["summary"]:
        print(f"📋 {parsed['summary']}")
    if parsed["positions"]:
        print("\n📊 POSITIONS:")
        for sym, info in parsed["positions"].items():
            verdict = info.get("verdict", "?")
            reason  = info.get("reason", "")
            icon    = {"HOLD": "✅", "WATCH": "⚠️ ", "EXIT": "🚨"}.get(verdict, "❓")
            print(f"   {icon} {sym}: {verdict} — {reason}")
    if parsed["watchlist"]:
        print("\n👁️  WATCHLIST:")
        for sym, info in parsed["watchlist"].items():
            verdict = info.get("verdict", "?")
            reason  = info.get("reason", "")
            icon    = {"ALERT": "🔔", "OK": "✅"}.get(verdict, "❓")
            print(f"   {icon} {sym}: {verdict} — {reason}")
    print(f"   📡 Provider: {_provider_status()}")
    print(f"{'─'*70}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════════════════════════

def _ai_loop(bot_globals_fn, trader_fn) -> None:
    global _ai_last_run, _ai_error_streak

    print("\n🤖 AI Assistant started — Dual provider (Groq + NVIDIA fallback)")
    print(f"   Groq model    : {GROQ_MODEL}")
    print(f"   NVIDIA model  : {NVIDIA_MODEL}")
    print(f"   Scan interval : {AI_SCAN_INTERVAL_SECONDS}s")
    print(f"   Auto-exit     : {'ENABLED' if AI_AUTO_EXIT_ENABLED else 'DISABLED'}")
    print(f"   Groq retry    : every {GROQ_RETRY_SECONDS}s after failure")

    groq_ok   = GROQ_API_KEY   not in ("YOUR_GROQ_API_KEY_HERE",   "", None)
    nvidia_ok = NVIDIA_API_KEY not in ("YOUR_NVIDIA_API_KEY_HERE", "", None)
    print(f"   Groq key      : {'✅ set' if groq_ok   else '❌ NOT SET — get at console.groq.com'}")
    print(f"   NVIDIA key    : {'✅ set' if nvidia_ok else '⚠️  not set — fallback disabled'}")

    time.sleep(90)   # let bot initialise first

    while True:
        try:
            if _ai_error_streak >= 5:
                wait = min(300, _ai_error_streak * 30)
                print(f"\n⚠️  AI: {_ai_error_streak} errors — waiting {wait}s")
                time.sleep(wait)
                _ai_error_streak = 0

            bg     = bot_globals_fn()
            trader = trader_fn()

            active = bg.get("ACTIVE_POSITIONS", {})
            wl     = bg.get("HA_WATCHLIST",     {})
            if AI_SILENT_WHEN_NO_POSITIONS and not active and not wl:
                time.sleep(AI_SCAN_INTERVAL_SECONDS)
                continue

            snapshot = _build_snapshot(bg)
            raw      = _call_ai(snapshot)

            if raw:
                parsed = _parse_response(raw)
                _print_ai_response(parsed, _ai_call_count)
                if active:
                    _enforce_exits(parsed, bg, trader)

            _ai_last_run = datetime.now()

        except Exception as e:
            _ai_error_streak += 1
            print(f"\n⚠️  AI loop error: {e}")

        time.sleep(AI_SCAN_INTERVAL_SECONDS)


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def start_ai_assistant(bot_globals_fn, trader_fn) -> None:
    global _ai_thread
    if not AI_ENABLED:
        print("🤖 AI Assistant: disabled (AI_ENABLED=False)")
        return
    if _ai_thread and _ai_thread.is_alive():
        print("🤖 AI Assistant: already running")
        return
    _ai_thread = threading.Thread(
        target=_ai_loop,
        args=(bot_globals_fn, trader_fn),
        daemon=True,
        name="AIAssistant",
    )
    _ai_thread.start()


def ai_status() -> str:
    groq_ok   = GROQ_API_KEY   not in ("YOUR_GROQ_API_KEY_HERE",   "", None)
    nvidia_ok = NVIDIA_API_KEY not in ("YOUR_NVIDIA_API_KEY_HERE", "", None)
    provider  = "NVIDIA(fallback)" if _using_nvidia else "Groq(primary)"
    return (
        f"AI Assistant: {'ACTIVE ✓' if (groq_ok or nvidia_ok) else 'KEYS MISSING ⚠️'} | "
        f"Provider: {provider} | "
        f"Calls: {_ai_call_count} "
        f"(Groq:{_groq_call_count} NVIDIA:{_nvidia_call_count}) | "
        f"Auto-exit: {'ON' if AI_AUTO_EXIT_ENABLED else 'OFF'}"
    )
