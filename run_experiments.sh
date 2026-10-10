#!/usr/bin/env bash
# Эксперименты ЛР 2. Перед запуском: RabbitMQ и PostgreSQL запущены (README, «Запуск»), схема применена.
# Использование: ./run_experiments.sh [python]   – по умолчанию .venv/Scripts/python (Windows) или .venv/bin/python
set -e
cd "$(dirname "$0")"
PY=${1:-$( [ -x .venv/bin/python ] && echo .venv/bin/python || echo .venv/Scripts/python )}
export PYTHONIOENCODING=utf-8
RUN="$PY tools/run_experiment.py"

# Эксперимент 1: 100 событий всех типов, все три обработчика – потерь быть не должно
$RUN --name exp1 --all --count 100 --vehicles 48 --incident-prob 0.05 --seed 3 --delay-ms 0 --prefetch 10 --repeat 3 \
     --types vehicle.position,ticket.validation,vehicle.incident

# Эксперимент 2: 1, 2, 4 экземпляра Track Service, 1000 координат, PROCESS_DELAY_MS=20, по 3 повтора
for P in 1 10; do
  for N in 1 2 4 8; do
    $RUN --name exp2_p${P}_n${N} --consumers $N --count 1000 --delay-ms 20 --prefetch $P --repeat 3
  done
done
# то же без имитации обработки: где узкое место, когда обработка «бесплатная»
for N in 1 2 4 8; do
  $RUN --name exp2_d0_n${N} --consumers $N --count 2000 --delay-ms 0 --prefetch 1 --repeat 3
done

# Эксперимент 3: 100, 1000, 10000 координат, 2 экземпляра Track Service, PROCESS_DELAY_MS=20
for C in 100 1000 10000; do
  $RUN --name exp3_c${C} --consumers 2 --count $C --delay-ms 20 --prefetch 1 --repeat 3
done
