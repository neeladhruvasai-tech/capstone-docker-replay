"""Cloud smoke test: paced historical events, two consumers, NOT live market data."""
import csv
import os
import signal
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

SOURCE = Path(os.getenv('REPLAY_SOURCE', 'data/raw/intraday/intraday_combined.csv'))
OUT = Path(os.getenv('REPLAY_OUTPUT', 'results/cloud_smoke'))
INTERVAL = float(os.getenv('REPLAY_INTERVAL_SECONDS', '0.2'))
BATCH_SECONDS = float(os.getenv('BATCH_INTERVAL_SECONDS', '5'))
MAX_EVENTS = int(os.getenv('MAX_EVENTS', '300'))
if INTERVAL < 0 or BATCH_SECONDS <= 0 or MAX_EVENTS < 1:
    raise ValueError('Invalid replay configuration')
OUT.mkdir(parents=True, exist_ok=True)
stop = False

def shutdown(*_):
    global stop
    stop = True

signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

with SOURCE.open(newline='') as f:
    rows = list(csv.DictReader(f))
rows.sort(key=lambda r: (r['Market_Timestamp'], r['Ticker']))
rows = rows[:MAX_EVENTS]
last_stream = {}
batch_pending = defaultdict(list)
last_batch = time.monotonic()
fields = ['pipeline', 'ticker', 'market_timestamp', 'received_at_utc', 'prediction_ready_at_utc', 'predicted_close', 'processing_latency_ms', 'receipt_to_ready_ms', 'source']

with (OUT / 'predictions.csv').open('w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    for event in rows:
        if stop:
            break
        if INTERVAL:
            time.sleep(INTERVAL)
        ticker = event['Ticker']
        received_wall = datetime.now(timezone.utc).isoformat()
        received_ns = time.perf_counter_ns()
        # Streaming persistence prediction uses current observed close; no future target is known yet.
        price = float(event['Close'])
        last_stream[ticker] = price
        ready_ns = time.perf_counter_ns()
        ready_wall = datetime.now(timezone.utc).isoformat()
        writer.writerow(dict(pipeline='stream', ticker=ticker, market_timestamp=event['Market_Timestamp'], received_at_utc=received_wall, prediction_ready_at_utc=ready_wall, predicted_close=price, processing_latency_ms=round((ready_ns-received_ns)/1e6, 6), receipt_to_ready_ms=round((ready_ns-received_ns)/1e6, 6), source='paced_historical_replay'))
        batch_pending[ticker].append((event, received_wall, received_ns))
        if time.monotonic() - last_batch >= BATCH_SECONDS:
            for t, pending in batch_pending.items():
                if not pending:
                    continue
                e, rwall, rns = pending[-1]
                start = time.perf_counter_ns()
                value = float(e['Close'])
                ready = time.perf_counter_ns()
                writer.writerow(dict(pipeline='batch', ticker=t, market_timestamp=e['Market_Timestamp'], received_at_utc=rwall, prediction_ready_at_utc=datetime.now(timezone.utc).isoformat(), predicted_close=value, processing_latency_ms=round((ready-start)/1e6, 6), receipt_to_ready_ms=round((ready-rns)/1e6, 6), source='paced_historical_replay'))
            batch_pending.clear()
            last_batch = time.monotonic()
            f.flush()
    # Final flush, including partial batch window
    for t, pending in batch_pending.items():
        if pending:
            e, rwall, rns = pending[-1]
            start = time.perf_counter_ns()
            value = float(e['Close'])
            ready = time.perf_counter_ns()
            writer.writerow(dict(pipeline='batch', ticker=t, market_timestamp=e['Market_Timestamp'], received_at_utc=rwall, prediction_ready_at_utc=datetime.now(timezone.utc).isoformat(), predicted_close=value, processing_latency_ms=round((ready-start)/1e6, 6), receipt_to_ready_ms=round((ready-rns)/1e6, 6), source='paced_historical_replay'))
print(f'Completed smoke test; output: {OUT / "predictions.csv"}', flush=True)
