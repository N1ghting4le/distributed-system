# Dispatcher API (REST, чтение из PostgreSQL). Контекст сборки – корень репозитория (docker-compose.yml).
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY services/dispatcher-api/requirements.txt .
RUN pip install -r requirements.txt && useradd --system --no-create-home app
COPY services/dispatcher-api/src/ src/
USER app
EXPOSE 8081
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=5 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8081/health', timeout=2)"
CMD ["python", "src/dispatcher_api.py"]
