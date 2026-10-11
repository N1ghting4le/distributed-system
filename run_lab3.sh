#!/usr/bin/env bash
# Эксперименты ЛР 3 (около 5 мин). Перед запуском: RabbitMQ и PostgreSQL запущены, схема 001–003 применена.
# Использование: ./run_lab3.sh [python]   – по умолчанию .venv/Scripts/python (Windows) или .venv/bin/python
set -e
cd "$(dirname "$0")"
PY=${1:-$( [ -x .venv/bin/python ] && echo .venv/bin/python || echo .venv/Scripts/python )}
export PYTHONIOENCODING=utf-8
RUN="$PY tools/run_lab3.py"
FAST="--env RETRY_DELAYS_MS=1000,2000,4000"   # задержки сокращены для экспериментов (в эксплуатации 5, 30, 120 с)

# 1. Нормальная обработка: 100 корректных сообщений, сбоев нет
$RUN --name exp1 --count 100 $FAST

# 2. Временный отказ: имитированный сбой 30 % попыток; серия 1000 сообщений с вероятностью 10, 30 и 50 %
$RUN --name exp2 --count 100 $FAST --env FAULT_RATE=0.3
for P in 0.1 0.3 0.5; do
  $RUN --name exp2_p${P#0.} --count 1000 --vehicles 300 $FAST --env FAULT_RATE=$P
done
# 2б. Реальный временный отказ: PostgreSQL остановлен на 10 с во время отправки (PREFETCH=1 – накопление видно в очереди)
$RUN --name exp2_outage --count 100 --rate 10 --db-outage 2:10 --env RETRY_DELAYS_MS=2000,5000,10000 --env DB_TIMEOUT_S=2 --env PREFETCH=1

# 3. Постоянный отказ: «ядовитое» сообщение (FAULT_POISON) и два битых сообщения среди 100 корректных
$RUN --name exp3 --count 100 $FAST --poison --malformed

# 4. Повторная доставка: каждый пакет отправлен 3 раза + потеря 20 % ack после фиксации
$RUN --name exp4 --count 100 $FAST --resend 3 --env FAULT_ACK_LOSS_RATE=0.2
