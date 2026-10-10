# Fleet Simulator

`fleet_simulator.py` – эмулятор бортового оборудования. Машины выполняют рейсы учебного расписания
(`routes.json`, строится `tools/build_reference.py` из `database/reference/routes.json`): движение по трассе,
стоянка 30 с на остановках с открытыми дверями, оборот на конечной. Трекер раз в `--period` с передает
`vehicle.position`, валидаторы на остановках – `ticket.validation`, водитель и диагностика – `vehicle.incident`;
с заданной вероятностью машина сходит с трассы на 350–550 м, доля `--bad-ratio` координат искажается
(скорость, выход за город, устаревшая отметка). События отправляются пакетами по HTTP в Ingest Gateway;
при 503 или таймауте пакет повторяется с теми же `seq`.

```bash
python simulator/fleet_simulator.py --vehicles 48 --duration 120           # реальное время
python simulator/fleet_simulator.py --count 10000 --types vehicle.position  # N событий без ограничения темпа
python simulator/fleet_simulator.py --help
```
