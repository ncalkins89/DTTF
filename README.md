# DTTF — Drive to the Finals

NBA playoff PRA pick'em dashboard. Tracks player projections, series odds, and urgency scores to help decide who to pick each day.

## Running locally

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python3 scripts/update_db.py   # load today's data
python3 src/dashboard.py       # opens at http://127.0.0.1:8050
```

Create a `.env` file in the project root:
```
ODDS_API_KEY=your_key_here
```

## Deploying updates to the server

Push your changes to GitHub, then SSH in and pull:

```bash
ssh -i ~/.ssh/dttf-key.pem ubuntu@<SERVER_IP>
cd DTTF && git pull && sudo systemctl restart dttf
```

App is live at `http://<SERVER_IP>`.

## Server setup (one-time)

See setup instructions in chat history. Summary:
- Oracle Cloud Always Free — VM.Standard.E2.1.Micro, Ubuntu 22.04
- Served via gunicorn + nginx on port 80
- Systemd service (`dttf.service`) keeps it running and restarts on reboot
- APScheduler inside the app refreshes data every 30 min automatically
