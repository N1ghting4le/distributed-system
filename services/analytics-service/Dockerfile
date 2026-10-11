# Schedule Analytics Service (Consumer).
# Контекст сборки – корень репозитория (docker-compose.yml): в образ входят общий модуль common/ и JSON Schema.
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY services/analytics-service/requirements.txt .
RUN pip install -r requirements.txt && useradd --system --no-create-home app
COPY common/ common/
COPY docs/schemas/ docs/schemas/
COPY services/analytics-service/src/ src/
USER app
CMD ["python", "src/analytics_service.py"]
