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
- `/health` is public. `/status` shows mode, trades, balance and last error; if `STATUS_KEY` is set, open `/status?key=<STATUS_KEY>`.
- Render's free tier sleeps a service after ~15 minutes without web traffic, which stops the bot. Ping `/health` every 5 minutes from an uptime monitor, or use a paid instance.
- A restart (deploy, crash, sleep) resets the session's profit/loss counters, so stop-loss and take-profit start fresh.

## Safety

- Credentials are read from the environment only. Never commit them.
- `ACCOUNT_TYPE = "demo"` by default. If no account of that type exists, the bot stops instead of using another account.
- Stake, stop-loss, take-profit and max trades are constants at the top of `under9.py`. Test on demo first.
