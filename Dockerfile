FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip uninstall -y pyrogram || true
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY . /app

ENV PYTHONUNBUFFERED=1


CMD ["python", "music.py"]



