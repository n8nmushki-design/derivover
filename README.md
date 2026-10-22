# derivover

Deriv Digit Over/Under bot (new Deriv API: App ID + `pat_` token + OTP).

Scans **Over 0** and **Under 9** at the same time: every tick of every market is checked against both filters, and the first market that passes either one is traded (one trade open at a time). Each strategy keeps its own barrier ladder. See the docstring in `under9.py` for the full strategy.

## Features

- Dual scanner: Over 0 + Under 9 work together; best eligible market is traded
- Automatic session restart after stop-loss or take-profit (configurable)
- Live dashboard (desktop + phone) with digit scanner, ladder, trades, events
- Cloudflare free-tier cron worker to keep free hosts awake
- Self-keepalive on Render free tier

## Run locally

```bash
pip install -r requirements.txt        # websocket-client (+ flask if you want the dashboard)
export DERIV_APP_ID="your_app_id"
export DERIV_TOKEN="pat_your_token"    # needs the trade scope
python3 under9.py
```

On Pydroid (Android), set the two values with `os.environ[...] = "..."` at the top of a private launcher script, not in `under9.py`.

## Config (top of `under9.py`)

| Setting | Default | Notes |
|--------|---------|--------|
| `ACCOUNT_TYPE` | `"demo"` | `"demo"` or `"real"` – never falls back to the other type |
| `STAKE` | `1.0` | Per trade |
| `STOP_LOSS` | `3.0` | Session loss that stops trading |
| `TAKE_PROFIT` | `7.5` | Session profit that stops trading |
| `AUTO_RESTART_AFTER_SL_TP` | `True` | After SL/TP, wait then start a new session (P/L + ladders reset) |
| `RESTART_DELAY_SECS` | `45` | Pause between sessions |
| `MAX_TRADES` | `50` | Hard stop for the process (no auto-restart) |

## Deploy on Render (web service)

- Build: `pip install -r requirements.txt`
- Start: `gunicorn under9:app --bind 0.0.0.0:$PORT --workers 1 --timeout 120`  
  (keep `--workers 1` so only one bot trades)
- Env vars: `DERIV_APP_ID`, `DERIV_TOKEN`, optionally `STATUS_KEY`
- Open your Render URL for the dashboard
- `/health` is public (for uptime / Cloudflare cron). `/api/state` needs the key when `STATUS_KEY` is set

Render free tier sleeps after ~15 min without traffic. The bot pings its own `/health` every 10 min when `RENDER_EXTERNAL_URL` is set (`KEEPALIVE=0` turns that off).

## Cloudflare free cron (recommended on free hosts)

Keeps the service awake without relying only on Render’s self-ping.

1. Cloudflare dashboard → **Workers & Pages** → Create Worker
2. Paste the contents of [`cloudflare-cron/worker.js`](cloudflare-cron/worker.js)
3. Settings → Variables → add secret **`BOT_URL`** = `https://your-app.onrender.com` (no trailing slash)
4. Settings → Triggers → Cron Triggers → Add: `*/5 * * * *` (every 5 minutes)
5. Deploy. Open the Worker URL once to smoke-test; check Logs for `200` responses

Optional CLI: see `cloudflare-cron/wrangler.toml`.

Free tier allows ~100k requests/day — a 5-minute ping is trivial.

## Token / free tier

- Use a **demo** account and a token with the **trade** scope while testing.
- Deriv App ID + `pat_…` token are required (new API). If login fails, the dashboard shows the error; common causes: wrong App ID, token missing trade scope, or no account of the configured type (`demo`/`real`).
- Free hosting (Render free, etc.) is supported; pair it with the Cloudflare cron above.


## Auto-trade controls (dashboard)

The dashboard **Auto Trade** bar posts to `POST /api/control` (same `STATUS_KEY` as `/api/state` when set):

| Action | Effect |
|--------|--------|
| **Stop** | Stops new trades (`stop_reason=user`). Use Resume to continue. |
| **Pause** | Keeps scanning digits but does not open new trades. |
| **Resume** | Clears pause, or restarts after SL/TP / stop. |
| **Force Trigger** | Clears cooldown; next eligible market/strategy entry. |
| **Manual Trade** | Pick market + strategy; optional skip digit filter (one-shot). |
| **Reset Stats** | Clears trade counters and resets session P/L baseline to current balance. |

Also in the UI (client-side): market filters (All / Active / 1s / R_), trade filters + search + CSV export, log export, and auto-scroll for events.


Example:

```bash
curl -X POST "$URL/api/control" -H "Content-Type: application/json" \
  -H "X-Status-Key: $STATUS_KEY" -d '{"action":"pause"}'
```

## Supabase (optional history)

Full SQL is split into **6 paste-ready parts** in [`supabase/README.md`](supabase/README.md).


Persist sessions and trades (demo and real kept separate):

1. Run [`supabase/schema.sql`](supabase/schema.sql) in the Supabase SQL Editor  
2. Set on the host:

```bash
SUPABASE_URL=https://xxxx.supabase.co
SUPABASE_SERVICE_ROLE_KEY=eyJ...   # service_role only
```

See [`supabase/README.md`](supabase/README.md) for details.

## Safety

- Credentials are read from the environment only. Never commit them.
- `ACCOUNT_TYPE = "demo"` by default. If no account of that type exists, the bot stops instead of using another account.
- Stake, stop-loss, take-profit and max trades are constants at the top of `under9.py`. Test on demo first.

## Files

- `under9.py` – bot + Flask dashboard server
- `dashboard.html` – live UI
- `requirements.txt` – flask, websocket-client, gunicorn
- `cloudflare-cron/` – free keep-alive Worker
