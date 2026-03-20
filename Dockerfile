FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# تم إضافة xauth هنا لحل مشكلة الشاشة الوهمية
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    tini \
    ca-certificates \
    chromium \
    xvfb \
    xauth \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip setuptools wheel \
 && pip install -r /app/requirements.txt

COPY . /app

RUN mkdir -p /data

EXPOSE 8080

ENTRYPOINT ["/usr/bin/tini", "--"]

# تشغيل البوت عبر الشاشة الوهمية
CMD ["xvfb-run", "-a", "python", "music.py"]
