FROM python:3.12-slim AS runtime

LABEL org.opencontainers.image.title="Booky2.0" \
      org.opencontainers.image.description="Discord audiobook request bot for Bookshelf"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    LOG_LEVEL=INFO \
    DISCORD_TOKEN="" \
    BOOKSHELF_URL="" \
    BOOKSHELF_API_KEY=""

WORKDIR /app

RUN addgroup --system --gid 10001 booky \
    && adduser --system --uid 10001 --ingroup booky --home /app booky

COPY requirements.txt ./
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt

COPY --chown=booky:booky bot.py ./

USER booky
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD ["python", "-c", "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:' + os.getenv('PORT', '8080') + '/healthz', timeout=3)"]

STOPSIGNAL SIGTERM
CMD ["python", "bot.py"]
