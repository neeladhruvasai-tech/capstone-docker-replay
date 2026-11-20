FROM python:3.11-slim
WORKDIR /app
COPY replay.py synthetic.csv ./
ENV REPLAY_SOURCE=/app/synthetic.csv REPLAY_OUTPUT=/output MAX_EVENTS=60 REPLAY_INTERVAL_SECONDS=0.02 BATCH_INTERVAL_SECONDS=0.25
CMD ["python", "-u", "replay.py"]
