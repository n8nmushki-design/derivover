# derivover

Deriv Digit Over/Under bot (new Deriv API: App ID + `pat_` token + OTP).
Starts with **Over 0**, switches to **Under 9** after 5 consecutive scans with no eligible market. See the docstring in `under9.py` for the full strategy.

## Run

```bash
pip install -r requirements.txt        # only websocket-client is needed to run the bot
export DERIV_APP_ID="your_app_id"
export DERIV_TOKEN="pat_your_token"    # needs the trade scope
python3 under9.py
```

On Pydroid (Android), set the two values with `os.environ[...] = "..."` at the top of a private launcher script, not in `under9.py`.

## Deploy on Render (web service)

- Build command: `pip install -r requirements.txt`
- Start command: `gunicorn under9:app --bind 0.0.0.0:$PORT --workers 1 --timeout 120`
  (keep `--workers 1`, otherwise several copies of the bot would trade at once)
- Environment variables: `DERIV_APP_ID`, `DERIV_TOKEN`, and optionally `STATUS_KEY`
- The bot starts by itself in a background thread when the service starts.
- Open your Render URL to see the dashboard: real or demo account, balance, session profit or loss, every trade, the digit scanner, the strategy ladder and a list of events and errors. It works on desktop and phone.
- Set `STATUS_KEY` in Render to protect the dashboard. The page then asks for that key once and remembers it on that device. Without it, anyone with the link can see your balance and trades (the page warns you).
- `/health` is a tiny public check for uptime monitors. `/api/state` is the data behind the dashboard (also at `/status`) and needs the key when `STATUS_KEY` is set.
- Files needed in the repo: `under9.py`, `dashboard.html`, `requirements.txt`.
- Render's free tier sleeps a service after ~15 minutes without web traffic, which stops the bot. On Render the bot pings its own `/health` every 10 minutes to stay awake (set `KEEPALIVE=0` to turn that off). For extra safety, also ping `/health` every 5 minutes from an uptime monitor, or use a paid instance.
- The bot starts in the web process itself and is restarted automatically if its thread is missing (for example after `--preload` forks a worker), so exactly one bot trades. A bot stopped by stop-loss, take-profit or the trade limit stays stopped until you restart the service.
- A restart (deploy, crash, sleep) resets the session's profit/loss counters, so stop-loss and take-profit start fresh.

## Safety

- Credentials are read from the environment only. Never commit them.
- `ACCOUNT_TYPE = "demo"` by default. If no account of that type exists, the bot stops instead of using another account.
- Stake, stop-loss, take-profit and max trades are constants at the top of `under9.py`. Test on demo first.
