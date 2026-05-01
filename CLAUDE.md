# DTTF – Claude Code Instructions

## Oracle (production server)

```
ssh -i /Users/nathancalkins/Code/dttf/ssh-key-2026-04-22.key ubuntu@147.224.51.47
```

App lives at `/home/ubuntu/DTTF/`. Service name: `dttf`.

```bash
# Deploy: pull + restart
ssh -i /Users/nathancalkins/Code/dttf/ssh-key-2026-04-22.key ubuntu@147.224.51.47 \
  "cd /home/ubuntu/DTTF && git pull origin master && sudo systemctl restart dttf"

# Tail logs
ssh -i /Users/nathancalkins/Code/dttf/ssh-key-2026-04-22.key ubuntu@147.224.51.47 \
  "sudo journalctl -u dttf -f"
```

## Deploy workflow

All changes: edit locally → `git push origin master` → SSH pull on Oracle → `sudo systemctl restart dttf`.
Never edit files directly on Oracle.

## Database

Production DB: `/home/ubuntu/DTTF/data/dttf.db` (Oracle).
Local DB: `data/dttf.db` — may be stale. For data diagnostics always check Oracle.
