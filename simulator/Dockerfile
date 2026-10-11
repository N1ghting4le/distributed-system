# Fleet Simulator – эмулятор бортового оборудования (источник нагрузки). Контекст сборки – корень репозитория.
# Аргументы эмулятора передаются после имени образа: docker compose run --rm simulator --count 1000
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY simulator/requirements.txt .
RUN pip install -r requirements.txt && useradd --system --no-create-home app
COPY simulator/src/ src/
USER app
ENTRYPOINT ["python", "src/fleet_simulator.py"]
CMD ["--vehicles", "300", "--duration", "120"]
