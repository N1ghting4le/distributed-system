"""Сборка справочников из database/reference/routes.json.

Для каждого маршрута и направления (A – прямое, B – обратное) вычисляются длина трассы и плановое
смещение прибытия на каждую остановку от начала рейса: время в пути по скорости маршрута плюс стоянка.
Результат – два файла с одинаковыми числами:
  database/init/002_reference.sql – справочники для PostgreSQL (маршруты, остановки, трассы, смещения);
  simulator/routes.json           – те же трассы и смещения для Fleet Simulator.
Запуск: python tools/build_reference.py
"""
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / 'database' / 'reference' / 'routes.json'


def distance_m(a, b):
    """Расстояние между точками [lat, lon] в локальной равнопромежуточной проекции (точность < 0,1 % в пределах города)."""
    k = math.cos(math.radians((a[0] + b[0]) / 2))
    return math.hypot((b[0] - a[0]) * 110_540, (b[1] - a[1]) * 111_320 * k)


def build_direction(points, stop_ids, speed_ms, dwell_s, stops):
    """points – вершины трассы, stop_ids[i] – id остановки в вершине i или None."""
    cum = [0.0]
    for a, b in zip(points, points[1:]):
        cum.append(cum[-1] + distance_m(a, b))
    result, offset, prev = [], 0, None
    for i, sid in enumerate(stop_ids):
        if sid is None:
            continue
        if prev is not None:
            offset += dwell_s + round((cum[i] - cum[prev]) / speed_ms)
        name, lat, lon = stops[sid]
        result.append({'stop_id': sid, 'name': name, 'lat': lat, 'lon': lon, 'shape_index': i, 'offset_s': offset})
        prev = i
    return {'length_m': round(cum[-1]), 'duration_s': offset + dwell_s, 'shape': points, 'stops': result}


def sql_str(s):
    return "'" + str(s).replace("'", "''") + "'"


def main():
    src = json.loads(SRC.read_text(encoding='utf-8'))
    stops, dwell = src['stops'], src['service']['dwell_s']
    sim = {'service': src['service'], 'city_bbox': [53.82, 27.40, 53.98, 27.72], 'routes': []}
    sql = ['-- Сгенерировано tools/build_reference.py из database/reference/routes.json, не редактировать вручную.',
           'SET search_path TO transit;', '']
    sql.append('INSERT INTO routes (route_id, transport_type, name) VALUES')
    sql.append(',\n'.join(f"    ({sql_str(r['route_id'])}, {sql_str(r['transport_type'])}, {sql_str(r['name'])})"
                          for r in src['routes']) + '\nON CONFLICT (route_id) DO NOTHING;\n')
    sql.append('INSERT INTO stops (stop_id, name, lat, lon) VALUES')
    sql.append(',\n'.join(f'    ({sql_str(k)}, {sql_str(v[0])}, {v[1]}, {v[2]})' for k, v in stops.items())
               + '\nON CONFLICT (stop_id) DO NOTHING;\n')
    rs_rows, shape_rows = [], []
    for r in src['routes']:
        points = [stops[p][1:] if isinstance(p, str) else p for p in r['shape']]
        ids = [p if isinstance(p, str) else None for p in r['shape']]
        speed = r['speed_kmh'] / 3.6
        dirs = {'A': build_direction(points, ids, speed, dwell, stops),
                'B': build_direction(points[::-1], ids[::-1], speed, dwell, stops)}
        sim['routes'].append({k: r[k] for k in ('route_id', 'transport_type', 'name', 'vehicle_prefix', 'validators')}
                             | {'directions': dirs})
        for d, info in dirs.items():
            shape_rows.append(f"    ({sql_str(r['route_id'])}, '{d}', '{json.dumps(info['shape'])}')")
            for seq, s in enumerate(info['stops'], 1):
                rs_rows.append(f"    ({sql_str(r['route_id'])}, '{d}', {seq}, {sql_str(s['stop_id'])}, {s['offset_s']})")
            print(f"{r['route_id']}-{d}: {len(info['stops'])} остановок, {info['length_m']} м, рейс {info['duration_s']} с")
    sql.append('INSERT INTO route_stops (route_id, direction, stop_seq, stop_id, planned_offset_s) VALUES')
    sql.append(',\n'.join(rs_rows) + '\nON CONFLICT DO NOTHING;\n')
    sql.append('INSERT INTO route_shapes (route_id, direction, points) VALUES')
    sql.append(',\n'.join(shape_rows) + '\nON CONFLICT DO NOTHING;\n')
    sql.append(f"-- расписание на текущие сутки (интервал {src['service']['headway_min']} мин); сервисы вызывают функцию при запуске")
    sql.append(f"SELECT build_timetable((now() AT TIME ZONE 'Europe/Minsk')::date, {src['service']['headway_min']});")
    (ROOT / 'database' / 'init' / '002_reference.sql').write_text('\n'.join(sql) + '\n', encoding='utf-8')
    (ROOT / 'simulator' / 'routes.json').write_text(json.dumps(sim, ensure_ascii=False, indent=1), encoding='utf-8')


if __name__ == '__main__':
    main()
