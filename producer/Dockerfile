# Ingest Gateway (Producer).
# Контекст сборки – корень репозитория (docker-compose.yml): в образ входят общий модуль common/ и JSON Schema.
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY producer/requirements.txt .
RUN pip install -r requirements.txt && useradd --system --no-create-home app
COPY common/ common/
COPY docs/schemas/ docs/schemas/
COPY producer/src/ src/
USER app
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=5 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=2)"
CMD ["python", "src/gateway.py"]
