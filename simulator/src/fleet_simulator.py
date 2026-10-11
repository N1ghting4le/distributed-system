"""Fleet Simulator – эмулятор бортового оборудования парка (источник данных).

Каждая машина выполняет рейсы учебного расписания (simulator/src/routes.json): едет по трассе маршрута,
стоит на остановках с открытыми дверями, после конечной берет ближайший рейс обратного направления.
Трекер раз в PERIOD секунд передает координаты (vehicle.position), валидаторы на остановках – проходы
пассажиров (ticket.validation), водитель и бортовая диагностика – инциденты (vehicle.incident).
События отправляются в Ingest Gateway пакетами по HTTP POST /api/v1/events; при ответе 503 или таймауте
пакет повторяется с теми же seq, поэтому шлюз присвоит повтору те же message_id.

Режимы:
  --count N   сформировать N событий «из недавнего прошлого» и отправить с темпом --rate (0 – без ограничения);
  --resend K  каждый пакет отправить K раз (устройство не получило ответ шлюза и повторило отправку) – ЛР 3;
  без --count  работа в реальном времени: каждые PERIOD секунд – очередной такт парка.

Примеры:
  python simulator/src/fleet_simulator.py --count 100
  python simulator/src/fleet_simulator.py --count 10000 --types vehicle.position
  python simulator/src/fleet_simulator.py --vehicles 300 --duration 120
"""
import argparse
import asyncio
import hashlib
import json
import logging
import math
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

GATEWAY_URL = os.getenv('GATEWAY_URL', 'http://localhost:8080')
ROUTES_FILE = Path(os.getenv('ROUTES_FILE', Path(__file__).with_name('routes.json')))

TYPES = ('vehicle.position', 'ticket.validation', 'vehicle.incident')
DEVICE_INCIDENTS = (  # тип, важность, кто обнаружил, вес
    ('panic_button', 'critical', 'driver', 1), ('accident', 'critical', 'driver', 1),
    ('breakdown', 'high', 'onboard', 3), ('engine_overheat', 'high', 'onboard', 2))
TICKET_TYPES = ('e_ticket', 'travel_card', 'bank_card', 'qr')
REJECT_REASONS = ('expired', 'insufficient_funds', 'blacklisted', 'read_error')

logging.basicConfig(level=logging.INFO, format='%(asctime)s [simulator] %(levelname)s %(message)s')
log = logging.getLogger('simulator')
logging.getLogger('httpx').setLevel(logging.WARNING)


def iso(t):
    return t.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def bearing(a, b):
    k = math.cos(math.radians(a[0]))
    return round(math.degrees(math.atan2((b[1] - a[1]) * k, b[0] - a[0]))) % 360


def seg_len(a, b):
    k = math.cos(math.radians((a[0] + b[0]) / 2))
    return math.hypot((b[0] - a[0]) * 110_540, (b[1] - a[1]) * 111_320 * k)


def along(points, i, j, frac):
    """Точка на ломаной points[i..j] на доле frac ее длины."""
    lens = [seg_len(points[k], points[k + 1]) for k in range(i, j)]
    target = frac * sum(lens)
    for k, ln in enumerate(lens):
        if target <= ln or k == len(lens) - 1:
            f = 0 if ln == 0 else min(1, target / ln)
            a, b = points[i + k], points[i + k + 1]
            return [a[0] + (b[0] - a[0]) * f, a[1] + (b[1] - a[1]) * f], bearing(a, b)
        target -= ln


class Vehicle:
    def __init__(self, number, route, direction, rnd, seq_base):
        self.number, self.route, self.direction, self.rnd = number, route, direction, rnd
        self.vehicle_id = f"{route['vehicle_prefix']}-{number}"
        self.tracker = f'trk-{number}'
        self.validators = [f'{number}-{k}' for k in range(1, route['validators'] + 1)]
        self.seq = {s: seq_base + rnd.randrange(1000) for s in [self.tracker] + [f'val-{x}' for x in self.validators]}
        self.trip_start = None
        self.delay_s = 0
        self.deviation = 0          # оставшиеся такты схода с трассы
        self.dev_shift = (0.0, 0.0)

    def next_seq(self, source):
        self.seq[source] += 1
        return self.seq[source]

    def pick_delay(self):
        r = self.rnd.random()
        if r < 0.75:
            return max(-60, min(240, self.rnd.gauss(60, 60)))
        return self.rnd.uniform(240, 600) if r < 0.92 else self.rnd.uniform(600, 1200)


class Fleet:
    def __init__(self, cfg, vehicles, t0, rnd, args):
        self.cfg, self.rnd, self.args = cfg, rnd, args
        self.tz = timezone(timedelta(hours=cfg['service']['utc_offset_hours']))
        self.headway = cfg['service']['headway_min'] * 60
        self.dwell = cfg['service']['dwell_s']
        seq_base = int(time.time() * 1000)  # счетчик устройства монотонен и между запусками эмулятора
        self.vehicles = []
        on_line = {}  # (маршрут, направление) → сколько машин уже выпущено: j-я машина берет рейс j интервалов назад
        for i in range(vehicles):
            route = cfg['routes'][i % len(cfg['routes'])]
            v = Vehicle(10000 + i, route, 'AB'[(i // len(cfg['routes'])) % 2], rnd, seq_base)
            dur = route['directions'][v.direction]['duration_s']
            j = on_line.get((route['route_id'], v.direction), 0)
            on_line[(route['route_id'], v.direction)] = j + 1
            v.delay_s = v.pick_delay()
            v.trip_start = self.grid_floor(t0) - timedelta(seconds=self.headway * (j % max(1, int(dur // self.headway))))
            self.vehicles.append(v)

    def grid_floor(self, t):
        local = t.astimezone(self.tz)
        midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
        return midnight + timedelta(seconds=(local - midnight).total_seconds() // self.headway * self.headway)

    def trip_id(self, v):
        return f"{v.route['route_id']}-{v.direction}-{v.trip_start.astimezone(self.tz):%H%M}"

    def state(self, v, t):
        """Положение машины в момент t: (точка, курс, скорость, двери открыты, id остановки или None)."""
        while True:
            d = v.route['directions'][v.direction]
            e = (t - v.trip_start).total_seconds() - v.delay_s
            if e < d['duration_s']:
                break
            end = v.trip_start + timedelta(seconds=v.delay_s + d['duration_s'] + 120)  # отстой на конечной
            v.direction = 'B' if v.direction == 'A' else 'A'
            v.trip_start = self.grid_floor(end) + timedelta(seconds=self.headway)
            v.delay_s = v.pick_delay() * 0.5
        stops, shape = d['stops'], d['shape']
        if e < 0:  # ожидает отправления на начальной остановке
            s0 = stops[0]
            return [s0['lat'], s0['lon']], bearing(shape[0], shape[1]), 0.0, False, None
        for k, s in enumerate(stops):
            nxt = stops[k + 1] if k + 1 < len(stops) else None
            if e < s['offset_s'] + self.dwell or nxt is None:
                j = s['shape_index']
                head = bearing(shape[j], shape[j + 1]) if j + 1 < len(shape) else bearing(shape[j - 1], shape[j])
                return [s['lat'], s['lon']], head, 0.0, True, s['stop_id']
            if e < nxt['offset_s']:
                travel = nxt['offset_s'] - s['offset_s'] - self.dwell
                frac = (e - s['offset_s'] - self.dwell) / travel
                p, head = along(shape, s['shape_index'], nxt['shape_index'], frac)
                length = sum(seg_len(shape[m], shape[m + 1]) for m in range(s['shape_index'], nxt['shape_index']))
                speed = length / travel * 3.6 * (1.3 - 0.6 * abs(frac - 0.5))  # разгон и торможение
                return p, head, speed, False, None

    def tick_events(self, v, t):
        """События одной машины за такт: координаты, валидации на остановке, редкий инцидент."""
        a, rnd = self.args, self.rnd
        p, head, speed, doors, stop_id = self.state(v, t)
        lat, lon = p[0] + rnd.gauss(0, 0.00004), p[1] + rnd.gauss(0, 0.00006)  # шум GPS ~5 м
        if v.deviation == 0 and speed > 0 and rnd.random() < a.deviation_prob:
            v.deviation = rnd.randint(3, 6)
            rad = math.radians(head + 90)
            off = rnd.uniform(350, 550)
            v.dev_shift = (off * math.cos(rad) / 110_540, off * math.sin(rad) / (111_320 * math.cos(math.radians(lat))))
        if v.deviation:
            lat, lon = lat + v.dev_shift[0], lon + v.dev_shift[1]
            v.deviation -= 1
        trip = self.trip_id(v)
        payload = {'vehicle_id': v.vehicle_id, 'route_id': v.route['route_id'], 'trip_id': trip,
                   'lat': round(lat, 6), 'lon': round(lon, 6), 'speed_kmh': round(max(0.0, speed), 1),
                   'heading': head, 'doors_open': doors}
        ts = t
        if rnd.random() < a.bad_ratio:  # ошибочные данные: проходят схему, но отвергаются Track Service
            kind = rnd.choice(('speed', 'outside', 'stale'))
            if kind == 'speed':
                payload['speed_kmh'] = round(rnd.uniform(150, 250), 1)
            elif kind == 'outside':
                payload['lat'] = round(lat + 0.5, 6)
            else:
                ts = t - timedelta(minutes=15)
        yield self.event('vehicle.position', v.tracker, v.next_seq(v.tracker), ts, payload)
        if doors and stop_id:
            n = self.poisson(a.validations_per_stop)
            for _ in range(n):
                val = rnd.choice(v.validators)
                rejected = rnd.random() < 0.05
                card = f'{rnd.getrandbits(64):016x}'
                yield self.event('ticket.validation', f'val-{val}', v.next_seq(f'val-{val}'),
                                 t + timedelta(seconds=rnd.uniform(0, 9)), {
                                     'validator_id': f'VAL-{val}', 'vehicle_id': v.vehicle_id,
                                     'route_id': v.route['route_id'], 'trip_id': trip, 'stop_id': stop_id,
                                     'ticket_type': rnd.choice(TICKET_TYPES),
                                     'ticket_ref': hashlib.sha256(card.encode()).hexdigest()[:16],
                                     'result': 'rejected' if rejected else 'accepted',
                                     'reject_reason': rnd.choice(REJECT_REASONS) if rejected else None})
        if rnd.random() < a.incident_prob:
            kind, severity, by, _ = rnd.choices(DEVICE_INCIDENTS, weights=[x[3] for x in DEVICE_INCIDENTS])[0]
            details = {'button': 'cabin'} if kind == 'panic_button' else {}
            if kind == 'engine_overheat':
                details = {'coolant_c': rnd.randint(108, 125)}
            yield self.event('vehicle.incident', v.tracker, v.next_seq(v.tracker), t, {
                'incident_type': kind, 'severity': severity, 'detected_by': by, 'vehicle_id': v.vehicle_id,
                'route_id': v.route['route_id'], 'trip_id': trip, 'lat': round(lat, 6), 'lon': round(lon, 6),
                'details': details})

    def poisson(self, lam):
        n, p, limit = 0, 1.0, math.exp(-lam)
        while True:
            p *= self.rnd.random()
            if p <= limit:
                return n
            n += 1

    @staticmethod
    def event(kind, source, seq, t, payload):
        return {'type': kind, 'schema_version': 1, 'source_id': source, 'seq': seq, 'timestamp': iso(t),
                'payload': payload}

    def run(self, t0, period, types):
        """Бесконечный поток событий парка начиная с t0; машины разнесены по фазе внутри такта."""
        n = len(self.vehicles)
        tick = 0
        while True:
            base = t0 + timedelta(seconds=tick * period)
            for i, v in enumerate(self.vehicles):
                t = base + timedelta(seconds=period * i / n)
                for ev in self.tick_events(v, t):
                    if ev['type'] in types:
                        yield ev
            tick += 1


class Sender:
    def __init__(self, args):
        self.args = args
        self.client = httpx.AsyncClient(base_url=GATEWAY_URL, timeout=args.timeout)
        self.stats = {'sent': 0, 'accepted': 0, 'rejected': 0, 'retries': 0, 'batches': 0, 'resent': 0}
        self.by_type = {k: 0 for k in TYPES}

    async def post(self, batch):
        """Отправка пакета с повтором при 503 и сетевых ошибках (как бортовой буфер устройства)."""
        delay = 0.5
        for attempt in range(self.args.max_retries + 1):
            try:
                r = await self.client.post('/api/v1/events', json=batch)
                if r.status_code == 202:
                    self.stats['accepted'] += len(batch)
                    for ev in batch:
                        self.by_type[ev['type']] += 1
                    return
                if r.status_code == 400:
                    self.stats['rejected'] += len(batch)
                    log.error('batch rejected by gateway: %s', r.text[:300])
                    return
                log.warning('gateway answered %s, retry in %.1fs', r.status_code, delay)
            except httpx.HTTPError as exc:
                log.warning('gateway unavailable (%s), retry in %.1fs', type(exc).__name__, delay)
            self.stats['retries'] += 1
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10)
        log.error('batch of %d events dropped after %d retries', len(batch), self.args.max_retries)

    async def worker(self, queue):
        while (batch := await queue.get()) is not None:
            await self.post(batch)
            self.stats['batches'] += 1
            self.stats['sent'] += len(batch)
            for _ in range(self.args.resend - 1):  # те же source_id и seq → шлюз присвоит те же message_id
                await self.post(batch)
                self.stats['resent'] += len(batch)


async def main():
    ap = argparse.ArgumentParser(description='Fleet Simulator')
    ap.add_argument('--vehicles', type=int, default=1200, help='машин на линии (ЛР 1: 1200 в час пик)')
    ap.add_argument('--period', type=float, default=10, help='период отправки координат, с')
    ap.add_argument('--count', type=int, help='сколько событий сформировать (без параметра – реальное время)')
    ap.add_argument('--duration', type=float, help='длительность работы в реальном времени, с')
    ap.add_argument('--rate', type=float, default=0, help='событий в секунду в режиме --count (0 – без ограничения)')
    ap.add_argument('--types', default=','.join(TYPES), help='типы событий через запятую')
    ap.add_argument('--batch', type=int, default=50, help='событий в одном HTTP-запросе (не более 50)')
    ap.add_argument('--concurrency', type=int, default=4, help='параллельных HTTP-запросов')
    ap.add_argument('--validations-per-stop', type=float, default=1.4, help='среднее число валидаций за такт на остановке')
    ap.add_argument('--incident-prob', type=float, default=0.002, help='вероятность инцидента за такт')
    ap.add_argument('--deviation-prob', type=float, default=0.002, help='вероятность начала схода с трассы за такт')
    ap.add_argument('--bad-ratio', type=float, default=0.0, help='доля ошибочных координат')
    ap.add_argument('--seed', type=int, help='зерно генератора случайных чисел')
    ap.add_argument('--timeout', type=float, default=10, help='таймаут HTTP, с')
    ap.add_argument('--max-retries', type=int, default=20)
    ap.add_argument('--resend', type=int, default=1, help='сколько раз отправить каждый пакет (дубликаты, ЛР 3)')
    args = ap.parse_args()
    types = set(args.types.split(','))
    assert types <= set(TYPES), f'неизвестные типы: {types - set(TYPES)}'
    cfg = json.loads(ROUTES_FILE.read_text(encoding='utf-8'))
    rnd = random.Random(args.seed)
    now = datetime.now(timezone.utc)
    sender = Sender(args)
    queue = asyncio.Queue(maxsize=args.concurrency * 2)
    workers = [asyncio.create_task(sender.worker(queue)) for _ in range(args.concurrency)]
    started = time.perf_counter()

    if args.count:
        # события «из недавнего прошлого»: такты заканчиваются к текущему моменту
        share = {'vehicle.position': 1.0, 'ticket.validation': 0.2, 'vehicle.incident': 0.002}
        per_tick = args.vehicles * sum(share[k] for k in types)
        t0 = now - timedelta(seconds=math.ceil(args.count / per_tick) * args.period)
        fleet = Fleet(cfg, args.vehicles, t0, rnd, args)
        gen = fleet.run(t0, args.period, types)
        log.info('started: count=%d vehicles=%d types=%s rate=%s gateway=%s', args.count, args.vehicles,
                 ','.join(sorted(types)), args.rate or 'max', GATEWAY_URL)
        batch, produced = [], 0
        for ev in gen:
            batch.append(ev)
            produced += 1
            if len(batch) == args.batch or produced == args.count:
                await queue.put(batch)
                batch = []
                if args.rate > 0:
                    lag = produced / args.rate - (time.perf_counter() - started)
                    if lag > 0:
                        await asyncio.sleep(lag)
            if produced == args.count:
                break
    else:
        fleet = Fleet(cfg, args.vehicles, now, rnd, args)
        gen = fleet.run(now, args.period, types)
        log.info('started realtime: vehicles=%d period=%ss types=%s gateway=%s', args.vehicles, args.period,
                 ','.join(sorted(types)), GATEWAY_URL)
        batch, produced, last_flush = [], 0, time.perf_counter()
        deadline = started + args.duration if args.duration else None
        try:
            for ev in gen:
                t = datetime.fromisoformat(ev['timestamp'])
                wait = (t - datetime.now(timezone.utc)).total_seconds()
                if wait > 0 and batch and (time.perf_counter() - last_flush > 0.2 or len(batch) >= args.batch):
                    await queue.put(batch)
                    batch, last_flush = [], time.perf_counter()
                if wait > 0:
                    await asyncio.sleep(wait)
                if deadline and time.perf_counter() > deadline:
                    break
                batch.append(ev)
                produced += 1
                if len(batch) >= args.batch:
                    await queue.put(batch)
                    batch, last_flush = [], time.perf_counter()
        except (KeyboardInterrupt, asyncio.CancelledError):
            log.info('stopping')
        if batch:
            await queue.put(batch)
    for _ in workers:
        await queue.put(None)
    await asyncio.gather(*workers)
    await sender.client.aclose()
    elapsed = time.perf_counter() - started
    s = sender.stats
    log.info('DONE generated=%d sent=%d resent=%d accepted=%d rejected=%d retries=%d elapsed=%.3fs rate=%.1f ev/s '
             'by_type=%s', produced, s['sent'], s['resent'], s['accepted'], s['rejected'], s['retries'], elapsed,
             s['accepted'] / elapsed, json.dumps(sender.by_type))


if __name__ == '__main__':
    asyncio.run(main())
