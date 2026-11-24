#!/bin/zsh
set -eu

SCRIPT_DIR="${0:A:h}"
REPO_DIR="${SCRIPT_DIR:h}"
MIRROR_DIR="${HBMS_MIRROR_DIR:-/Users/henry/Desktop/HBMS Website}"
LOG_DIR="$REPO_DIR/logs"
LOG_FILE="$LOG_DIR/refresh_site.log"
LOCK_DIR="/tmp/hbms-website-refresh.lock"
LOCK_MAX_AGE_SECONDS="${HBMS_LOCK_MAX_AGE_SECONDS:-14400}"
SCREENER_TIMEOUT_SECONDS="${HBMS_SCREENER_TIMEOUT_SECONDS:-5400}"

mkdir -p "$LOG_DIR"
exec >> "$LOG_FILE" 2>&1

echo ""
echo "===== $(date '+%Y-%m-%d %H:%M:%S %Z') refresh start ====="

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  # The lock directory is deliberately empty, so it is safe to clear only
  # locks that are far older than one refresh cycle. This prevents a killed
  # git/python child from blocking every scheduled update indefinitely.
  lock_mtime="$(stat -f %m "$LOCK_DIR" 2>/dev/null || echo 0)"
  now_epoch="$(date +%s)"
  lock_age=$((now_epoch - lock_mtime))
  if [ "$lock_mtime" -gt 0 ] && [ "$lock_age" -gt "$LOCK_MAX_AGE_SECONDS" ] && rmdir "$LOCK_DIR" 2>/dev/null; then
    echo "Removed stale refresh lock (${lock_age}s old)."
    mkdir "$LOCK_DIR" || exit 1
  else
    echo "Another refresh is already running; exiting."
    exit 0
  fi
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

cd "$REPO_DIR" || exit 1

export PATH="/opt/anaconda3/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export SCREENER_WORKERS="${SCREENER_WORKERS:-16}"
# Never allow an interactive credential prompt to leave the scheduled job hanging.
export GIT_TERMINAL_PROMPT=0

echo "Repo: $REPO_DIR"
echo "Python: $(command -v python3 || echo missing)"
echo "Git: $(command -v git || echo missing)"

# Check the interpreter before three costly screener retries. Homebrew's
# Python 3.14 does not contain the site's scientific Python dependencies.
if ! python3 -c 'import numpy, pandas, yfinance, requests, scipy, sklearn'; then
  echo "Screener dependencies unavailable in $(command -v python3)."
  exit 1
fi

python3 - <<'PY'
import socket
hosts = ["github.com", "query1.finance.yahoo.com", "hbmsresearch.com"]
failed = []
for host in hosts:
    try:
        socket.gethostbyname(host)
    except OSError as exc:
        failed.append(f"{host}: {exc}")
if failed:
    print("DNS preflight failed:")
    for item in failed:
        print("  " + item)
    raise SystemExit(2)
print("DNS preflight OK")
PY

git fetch origin main
if ! git pull --ff-only origin main; then
  echo "Git sync failed; aborting before the screener runs on a stale branch."
  exit 1
fi

before_generated="$(python3 - <<'PY'
import json
from pathlib import Path
p = Path("data.json")
print(json.loads(p.read_text()).get("generated", "") if p.exists() else "")
PY
)"
echo "Before generated: ${before_generated:-none}"

attempt=1
while [ "$attempt" -le 3 ]; do
  echo "Screener attempt $attempt/3"
  if python3 - "$SCREENER_TIMEOUT_SECONDS" <<'PY'
import subprocess
import sys

timeout_seconds = int(sys.argv[1])
try:
    result = subprocess.run([sys.executable, "screener.py", "--no-upload"], timeout=timeout_seconds)
except subprocess.TimeoutExpired:
    print(f"Screener timed out after {timeout_seconds // 60} minutes.")
    raise SystemExit(124)
raise SystemExit(result.returncode)
PY
  then
    break
  fi
  if [ "$attempt" -eq 3 ]; then
    echo "Screener failed after 3 attempts."
    exit 1
  fi
  sleep $((attempt * 60))
  attempt=$((attempt + 1))
done

after_generated="$(python3 - <<'PY'
import json
from pathlib import Path
p = Path("data.json")
print(json.loads(p.read_text()).get("generated", "") if p.exists() else "")
PY
)"
echo "After generated: ${after_generated:-none}"

python3 - <<'PY'
import datetime, json, re
from pathlib import Path

today = datetime.date.today()
payload = json.loads(Path("data.json").read_text(encoding="utf-8"))
stats = json.loads(Path("ma_stock_stats.json").read_text(encoding="utf-8"))
if payload.get("generated", "")[:10] != str(today):
    raise SystemExit("Refusing to publish: dashboard generation date is stale")
if stats.get("generated", "")[:10] != str(today):
    raise SystemExit("Refusing to publish: MA statistics generation date is stale")
if payload.get("total", 0) < 450 or len(stats.get("stocks", [])) < 450:
    raise SystemExit("Refusing to publish: stock universe is incomplete")
market = payload.get("market") or {}
for key in ("sp500_history", "pe_ratio_history", "sp500_monthly_price_history"):
    if not market.get(key):
        raise SystemExit(f"Refusing to publish: {key} is empty")
fresh = 0
for stock in stats["stocks"]:
    ticker = str(stock.get("ticker", ""))
    filename = re.sub(r"[^A-Z0-9-]", "_", ticker.upper()) + ".json"
    try:
        rows = json.loads((Path("ma_stock_history") / filename).read_text(encoding="utf-8"))["rows"]
        last = datetime.date.fromisoformat(rows[-1][0])
        fresh += (today - last).days <= 7
    except (OSError, ValueError, KeyError, IndexError, TypeError):
        pass
if fresh < len(stats["stocks"]) * 0.90:
    raise SystemExit(f"Refusing to publish: only {fresh}/{len(stats['stocks'])} MA histories are fresh")
print(f"Publication checks passed: {payload['total']} stocks, {fresh} fresh MA histories")
PY

if [ -d "$MIRROR_DIR" ] && [ -w "$MIRROR_DIR" ]; then
  echo "Copying refreshed data into local mirror: $MIRROR_DIR"
  cp data.json "$MIRROR_DIR/data.json"
  cp ma_stock_stats.json "$MIRROR_DIR/ma_stock_stats.json"
  mkdir -p "$MIRROR_DIR/ma_stock_history"
  rsync -a --delete ma_stock_history/ "$MIRROR_DIR/ma_stock_history/"
else
  echo "Local mirror not available/writable, skipping: $MIRROR_DIR"
fi

git add data.json ma_stock_stats.json ma_stock_history
if git diff --cached --quiet; then
  echo "No website data changes to commit."
else
  git commit -m "Refresh website data"
  # GitHub occasionally stalls on the ~60 MB data payload over HTTP/2.  Force
  # HTTP/1.1 and bound the publish step so one failed push cannot block every
  # subsequent scheduled refresh behind this job's lock.
  python3 - <<'PY'
import subprocess
import sys

try:
    completed = subprocess.run(
        ["git", "-c", "http.version=HTTP/1.1", "-c", "http.postBuffer=524288000", "push", "origin", "main"],
        timeout=600,
    )
except subprocess.TimeoutExpired:
    print("Git push timed out after 10 minutes.")
    raise SystemExit(1)
raise SystemExit(completed.returncode)
PY
fi

python3 - <<'PY'
import json, ssl, time, urllib.request
url = f"https://hbmsresearch.com/data.json?auto_refresh_check={int(time.time())}"
try:
    with urllib.request.urlopen(url, timeout=45, context=ssl._create_unverified_context()) as r:
        payload = json.loads(r.read())
except Exception as exc:
    print(f"Live verification skipped/failed: {exc}")
else:
    print("Live generated:", payload.get("generated"))
    print("Live total:", payload.get("total"))
    market = payload.get("market") or {}
    rows = market.get("vix_history") or []
    if rows:
        last = rows[-1]
        print("Live VIX/VIX3M:", last.get("date"), last.get("vix"), last.get("vix3m"))
PY

echo "===== $(date '+%Y-%m-%d %H:%M:%S %Z') refresh done ====="
