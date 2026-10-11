# Эксперименты ЛР 6: исследование системы

## Эксперимент 1. Производительность (2 реплики Track Service)

| Событий | Время, с | Throughput, соб./с | Track, сообщ./с | Analytics, сообщ./с | Latency Track ср./p95, с | Ошибок | DLQ | Потерь |
|---|---|---|---|---|---|---|---|---|
| 1000 | 15,0 | 66,8 | 60,4 | 78,3 | 7,0 / 12,7 | 0 | 0 | 0 |
| 5000 | 65,9 | 75,9 | 65,5 | 102,4 | 29,9 / 55,9 | 0 | 0 | 0 |
| 10000 | 132,7 | 75,4 | 65,5 | 102,1 | 59,9 / 112,2 | 0 | 0 | 0 |

| Интенсивность, соб./с | Время, с | Throughput | Latency Track ср./p95 | Latency Analytics ср./p95 |
|---|---|---|---|---|
| 25 | 118,7 | 25,3 | 0,37 / 0,67 | 0,25 / 0,46 |
| 50 | 59,6 | 50,3 | 0,37 / 0,68 | 0,26 / 0,48 |
| 100 | 39,5 | 75,9 | 5,12 / 9,38 | 0,28 / 0,58 |
| 200 | 39,2 | 76,4 | 12,51 / 23,18 | 7,17 / 13,55 |

## Эксперимент 2. Масштабирование (5000 координат)

### delay_20

| Реплик | Время, с | Система, соб./с | Track, сообщ./с | Analytics, сообщ./с | Latency Track ср./p95 | CPU Track / PG / RabbitMQ | fsync/с |
|---|---|---|---|---|---|---|---|
| 1 | 149,1 | 33,5 | 33,6 | 96,3 | 71,7 / 134,8 | 0,18 / 0,24 / 1,02 | 134,5 |
| 2 | 76,7 | 65,2 | 65,3 | 98,1 | 35,4 / 65,9 | 0,36 / 0,40 / 1,12 | 258,5 |
| 4 | 53,8 | 92,9 | 122,8 | 93,1 | 16,8 / 30,7 | 0,58 / 0,56 / 1,32 | 354,5 |

### delay_0

| Реплик | Время, с | Система, соб./с | Track, сообщ./с | Analytics, сообщ./с | Latency Track ср./p95 | CPU Track / PG / RabbitMQ | fsync/с |
|---|---|---|---|---|---|---|---|
| 1 | 58,9 | 84,8 | 85,0 | 92,4 | 26,9 / 48,8 | 0,46 / 0,50 / 1,25 | 339,7 |
| 2 | 57,0 | 87,8 | 129,7 | 87,8 | 16,3 / 28,8 | 0,60 / 0,58 / 1,37 | 329,2 |
| 4 | 54,7 | 91,3 | 237,3 | 91,4 | 6,5 / 11,1 | 0,64 / 0,59 / 1,33 | 306,7 |

## Эксперимент 3. Ошибки

| Тип | p | Отправлено | Успешно | Отклонено | Retry | DLQ | Время, с | Latency ср./max |
|---|---|---|---|---|---|---|---|---|
| transient | 0 | 2000 | 2000 | 0 | 0 | 0 | 33,1 | 15,34 / 29,37 |
| transient | 0.1 | 2000 | 2000 | 0 | 222 | 0 | 40,5 | 17,33 / 39,25 |
| transient | 0.2 | 2000 | 1999 | 0 | 505 | 1 | 45,9 | 19,99 / 44,07 |
| transient | 0.3 | 2000 | 1980 | 0 | 887 | 20 | 49,7 | 22,43 / 46,88 |
| bad_data | 0 | 2000 | 2000 | 0 | 0 | 0 | 30,5 | 14,26 / 27,76 |
| bad_data | 0.1 | 2000 | 1798 | 202 | 0 | 0 | 30,8 | 14,36 / 28,06 |
| bad_data | 0.2 | 2000 | 1586 | 414 | 0 | 0 | 30,6 | 14,28 / 27,88 |
| bad_data | 0.3 | 2000 | 1406 | 594 | 0 | 0 | 30,4 | 14,21 / 27,69 |

## consumer

```json
{
 "sent": 9858,
 "processed": 18444,
 "errors": 0,
 "retries": 0,
 "dlq": 0,
 "lost": 0,
 "time_s": 151.86,
 "thr_events": 64.9,
 "thr_ops": 121.5,
 "ingest_rate": 65.0,
 "track_thr": 56.4,
 "analytics_thr": 65.2,
 "lat_track_avg": 2.137,
 "lat_track_p95": 3.57,
 "lat_track_max": 4.476,
 "lat_an_avg": 1.166,
 "lat_an_p95": 3.125,
 "gateway_rejected": 0,
 "http_retries": 0,
 "redelivered": 50,
 "duplicates": 0,
 "rejected": 0,
 "events": {
  "t_fail": 45.84,
  "victim": "analytics-service-6bcd5b6b8c-wlmcn",
  "node": "transit-worker",
  "pid": "85451",
  "restart_detected_s": 1.29,
  "ready_s": 4.2,
  "first_processed_s": 2.67,
  "last_terminated": {
   "containerID": "containerd://9526f2b9c4f70d2d62db46e30a7aee9a5efb5e85f7bf0e218e35deefcbb38b94",
   "exitCode": 137,
   "finishedAt": "2026-10-11T18:00:33Z",
   "reason": "Error",
   "startedAt": "2026-10-11T17:59:40Z"
  },
  "recovered_t": 50.040000000000006
 },
 "recovery": {
  "analytics-service": {
   "arrived_during": 190,
   "backlog_after": 203,
   "processed_after": 7411
  },
  "track-service": {
   "arrived_during": 167,
   "backlog_after": 65,
   "processed_after": 6298
  },
  "incident-service": {
   "arrived_during": 0,
   "backlog_after": 0,
   "processed_after": 30
  }
 },
 "outage": [
  45.84,
  50.040000000000006
 ],
 "baseline_queue": 102,
 "queue_peak": 221,
 "normal_at": 66.4,
 "error_reasons": [],
 "log_lines": 46810,
 "log_attempts": {}
}
```

## broker

```json
{
 "sent": 12150,
 "processed": 22559,
 "errors": 0,
 "retries": 0,
 "dlq": 0,
 "lost": 0,
 "time_s": 327.95,
 "thr_events": 37.0,
 "thr_ops": 68.8,
 "ingest_rate": 67.3,
 "track_thr": 50.6,
 "analytics_thr": 64.3,
 "lat_track_avg": 20.642,
 "lat_track_p95": 29.617,
 "lat_track_max": 30.673,
 "lat_an_avg": 12.335,
 "lat_an_p95": 23.555,
 "gateway_rejected": 0,
 "http_retries": 24,
 "redelivered": 145,
 "duplicates": 5,
 "rejected": 0,
 "events": {
  "t_fail": 45.0,
  "broker_not_ready_s": 2.17,
  "broker_ready_s": 15.78,
  "gateway_ok_s": 16.05,
  "consumers_back_s": 294.49,
  "first_processed_s": 21.1,
  "recovered_t": 339.49,
  "pods_after": [
   {
    "name": "analytics-service-6bcd5b6b8c-wlmcn",
    "app": "analytics-service",
    "restarts": 2,
    "ready": true
   },
   {
    "name": "dispatcher-api-84579f68bf-hbtkv",
    "app": "dispatcher-api",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "gateway-866c7ff975-29jhp",
    "app": "gateway",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "incident-service-7889867f97-8zvnt",
    "app": "incident-service",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "postgres-0",
    "app": "postgres",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "rabbitmq-0",
    "app": "rabbitmq",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "track-service-d4b69b554-5kpwt",
    "app": "track-service",
    "restarts": 2,
    "ready": true
   },
   {
    "name": "track-service-d4b69b554-zk8sm",
    "app": "track-service",
    "restarts": 2,
    "ready": true
   }
  ]
 },
 "recovery": {
  "analytics-service": {
   "arrived_during": 9740,
   "backlog_after": 0,
   "processed_after": 0
  },
  "incident-service": {
   "arrived_during": 32,
   "backlog_after": 0,
   "processed_after": 0
  },
  "track-service": {
   "arrived_during": 8345,
   "backlog_after": 0,
   "processed_after": 0
  }
 },
 "outage": [
  45.0,
  339.49
 ],
 "baseline_queue": 158,
 "queue_peak": 3367,
 "normal_at": 339.9,
 "error_reasons": [
  {
   "consumer": "track-service",
   "outcome": "duplicate",
   "reason": "",
   "n": 3
  },
  {
   "consumer": "analytics-service",
   "outcome": "duplicate",
   "reason": "",
   "n": 2
  }
 ],
 "log_lines": 57712,
 "log_attempts": {}
}
```

## network

```json
{
 "sent": 13303,
 "processed": 24868,
 "errors": 1,
 "retries": 1,
 "dlq": 0,
 "lost": 0,
 "time_s": 249.56,
 "thr_events": 53.3,
 "thr_ops": 99.6,
 "ingest_rate": 66.2,
 "track_thr": 46.3,
 "analytics_thr": 66.0,
 "lat_track_avg": 37.672,
 "lat_track_p95": 53.754,
 "lat_track_max": 163.269,
 "lat_an_avg": 0.842,
 "lat_an_p95": 2.062,
 "gateway_rejected": 0,
 "http_retries": 0,
 "redelivered": 0,
 "duplicates": 0,
 "rejected": 0,
 "events": {
  "t_fail": 40.52,
  "victim": "track-service-5769fc487b-6rvhw",
  "node": "transit-worker",
  "rules": "iptables -I OUTPUT -d 10.96.88.133 -j DROP && iptables -I OUTPUT -d 10.96.247.50 -j DROP",
  "iptables_rc": 0,
  "iptables_err": "",
  "partition_s": 90,
  "not_ready_s": 5.15,
  "broker_dropped_consumer_s": null,
  "t_restore": 135.92,
  "restore_rc": 0,
  "ready_after_restore_s": 0.92,
  "victim_first_processed_after_restore_s": 8.17,
  "consumers_back_after_restore_s": 8.86,
  "recovered_t": 135.92,
  "victim_after": {
   "name": "track-service-5769fc487b-6rvhw",
   "ready": true,
   "restarts": 0,
   "last_terminated": null
  }
 },
 "recovery": {
  "analytics-service": {
   "arrived_during": 6668,
   "backlog_after": 0,
   "processed_after": 4628
  },
  "incident-service": {
   "arrived_during": 12,
   "backlog_after": 0,
   "processed_after": 15
  },
  "track-service": {
   "arrived_during": 5666,
   "backlog_after": 2860,
   "processed_after": 6945
  }
 },
 "outage": [
  40.52,
  135.92
 ],
 "baseline_queue": 180,
 "queue_peak": 3230,
 "normal_at": 252.9,
 "error_reasons": [
  {
   "consumer": "track-service",
   "outcome": "retry",
   "reason": "TimeoutError",
   "n": 1
  }
 ],
 "log_lines": 63307,
 "log_attempts": {
  "track-service:retry": {
   "TimeoutError": 1
  }
 }
}
```

## storage

```json
{
 "sent": 9964,
 "processed": 18499,
 "errors": 0,
 "retries": 0,
 "dlq": 0,
 "lost": 0,
 "time_s": 189.18,
 "thr_events": 52.7,
 "thr_ops": 97.8,
 "ingest_rate": 66.3,
 "track_thr": 45.6,
 "analytics_thr": 58.0,
 "lat_track_avg": 34.061,
 "lat_track_p95": 43.33,
 "lat_track_max": 81.849,
 "lat_an_avg": 25.916,
 "lat_an_p95": 42.962,
 "gateway_rejected": 0,
 "http_retries": 0,
 "redelivered": 0,
 "duplicates": 1,
 "rejected": 0,
 "events": {
  "t_fail": 40.0,
  "outage_s": 30,
  "pg_stopped_s": 0.44,
  "consumers_not_ready_s": 4.97,
  "t_restore": 70.0,
  "pg_ready_after_restore_s": 8.21,
  "consumers_ready_after_restore_s": 9.86,
  "first_processed_after_restore_s": 8.98,
  "recovered_t": 78.98
 },
 "recovery": {
  "analytics-service": {
   "arrived_during": 2686,
   "backlog_after": 2672,
   "processed_after": 7892
  },
  "incident-service": {
   "arrived_during": 4,
   "backlog_after": 4,
   "processed_after": 24
  },
  "track-service": {
   "arrived_during": 2317,
   "backlog_after": 2395,
   "processed_after": 6834
  }
 },
 "outage": [
  40.0,
  70.0
 ],
 "baseline_queue": 255,
 "queue_peak": 5168,
 "normal_at": 192.6,
 "error_reasons": [
  {
   "consumer": "track-service",
   "outcome": "duplicate",
   "reason": "",
   "n": 1
  }
 ],
 "log_lines": 48777,
 "log_attempts": {
  "track-service:retry": {
   "ConnectionDoesNotExistError": 1,
   "InternalClientError": 1,
   "CannotConnectNowError": 14,
   "ConnectionRefusedError": 25,
   "TimeoutError": 2
  },
  "analytics-service:retry": {
   "ConnectionRefusedError": 17,
   "TimeoutError": 2
  },
  "incident-service:retry": {
   "ConnectionRefusedError": 9
  }
 }
}
```

## storage2

```json
{
 "sent": 7810,
 "processed": 14588,
 "errors": 2,
 "retries": 2,
 "dlq": 0,
 "lost": 0,
 "time_s": 173.52,
 "thr_events": 45.0,
 "thr_ops": 84.1,
 "ingest_rate": 64.7,
 "track_thr": 41.1,
 "analytics_thr": 49.7,
 "lat_track_avg": 41.78,
 "lat_track_p95": 50.791,
 "lat_track_max": 96.434,
 "lat_an_avg": 37.632,
 "lat_an_p95": 53.539,
 "gateway_rejected": 0,
 "http_retries": 0,
 "redelivered": 0,
 "duplicates": 0,
 "rejected": 0,
 "events": {
  "t_fail": 30.01,
  "outage_s": 40,
  "pg_stopped_s": 0.51,
  "consumers_not_ready_s": 3.61,
  "t_restore": 70.01,
  "pg_ready_after_restore_s": 5.9,
  "consumers_ready_after_restore_s": 7.81,
  "first_processed_after_restore_s": 7.42,
  "recovered_t": 77.43,
  "dlq_before_replay": {
   "track.positions.dlq": 0,
   "analytics.positions.dlq": 0,
   "analytics.validations.dlq": 0,
   "incidents.dlq": 2
  },
  "replay": {
   "incidents.dlq": [
    "3c5eee03-2968-5880-b403-fc0d8d2bf0b0  attempts=4 class=transient by=incident-service/incident-service-65d548bd98-954tx at=2026-10-11T18:18:04.094+00:00",
    "    ConnectionRefusedError: [Errno 111] Connect call failed ('10.96.247.50', 5432)",
    "replayed 2 message(s)"
   ]
  },
  "t_replay": 180.89
 },
 "recovery": null,
 "outage": [
  30.01,
  70.01
 ],
 "baseline_queue": 250,
 "queue_peak": 6441,
 "normal_at": 170.1,
 "error_reasons": [
  {
   "consumer": "analytics-service",
   "outcome": "retry",
   "reason": "ConnectionRefusedError: [Errno 111] Connect call failed ('10.96.247.50",
   "n": 1
  },
  {
   "consumer": "track-service",
   "outcome": "retry",
   "reason": "ConnectionRefusedError: [Errno 111] Connect call failed ('10.96.247.50",
   "n": 1
  }
 ],
 "log_lines": 38470,
 "log_attempts": {
  "track-service:retry": {
   "ConnectionDoesNotExistError": 2,
   "CannotConnectNowError": 11,
   "ConnectionResetError": 1,
   "ConnectionRefusedError": 31,
   "TimeoutError": 2
  },
  "analytics-service:retry": {
   "InterfaceError": 1,
   "InternalClientError": 1,
   "CannotConnectNowError": 8,
   "ConnectionRefusedError": 19,
   "TimeoutError": 2
  },
  "incident-service:retry": {
   "ConnectionRefusedError": 12
  },
  "incident-service:dlq": {
   "transient": 2
  }
 }
}
```

## broker_fixed

```json
{
 "sent": 12078,
 "processed": 22419,
 "errors": 0,
 "retries": 0,
 "dlq": 0,
 "lost": 0,
 "time_s": 221.52,
 "thr_events": 54.5,
 "thr_ops": 101.2,
 "ingest_rate": 67.1,
 "track_thr": 46.6,
 "analytics_thr": 64.6,
 "lat_track_avg": 34.152,
 "lat_track_p95": 48.498,
 "lat_track_max": 50.191,
 "lat_an_avg": 12.332,
 "lat_an_p95": 24.636,
 "gateway_rejected": 0,
 "http_retries": 24,
 "redelivered": 150,
 "duplicates": 3,
 "rejected": 0,
 "events": {
  "t_fail": 45.0,
  "broker_not_ready_s": 2.96,
  "broker_ready_s": 17.14,
  "gateway_ok_s": 24.58,
  "consumers_back_s": 29.51,
  "first_processed_s": 25.17,
  "recovered_t": 74.51,
  "pods_after": [
   {
    "name": "analytics-service-79c4d5d755-mjmx9",
    "app": "analytics-service",
    "restarts": 2,
    "ready": true
   },
   {
    "name": "dispatcher-api-795dc49c98-l97c5",
    "app": "dispatcher-api",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "gateway-9d85848ff-fdbtj",
    "app": "gateway",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "incident-service-76bbb9d87d-vr2mk",
    "app": "incident-service",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "postgres-0",
    "app": "postgres",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "rabbitmq-0",
    "app": "rabbitmq",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "track-service-5d97d68bc-5vl8t",
    "app": "track-service",
    "restarts": 2,
    "ready": true
   },
   {
    "name": "track-service-5d97d68bc-l7dwx",
    "app": "track-service",
    "restarts": 2,
    "ready": false
   }
  ]
 },
 "recovery": {
  "analytics-service": {
   "arrived_during": 642,
   "backlog_after": 463,
   "processed_after": 9436
  },
  "incident-service": {
   "arrived_during": 0,
   "backlog_after": 0,
   "processed_after": 25
  },
  "track-service": {
   "arrived_during": 563,
   "backlog_after": 488,
   "processed_after": 8159
  }
 },
 "outage": [
  45.0,
  74.51
 ],
 "baseline_queue": 234,
 "queue_peak": 4338,
 "normal_at": 75.5,
 "error_reasons": [
  {
   "consumer": "track-service",
   "outcome": "duplicate",
   "reason": "",
   "n": 2
  },
  {
   "consumer": "analytics-service",
   "outcome": "duplicate",
   "reason": "",
   "n": 1
  }
 ],
 "log_lines": 57508,
 "log_attempts": {}
}
```

## broker_fixed2

```json
{
 "sent": 11945,
 "processed": 22321,
 "errors": 0,
 "retries": 0,
 "dlq": 0,
 "lost": 0,
 "time_s": 210.33,
 "thr_events": 56.8,
 "thr_ops": 106.1,
 "ingest_rate": 66.3,
 "track_thr": 49.2,
 "analytics_thr": 63.0,
 "lat_track_avg": 22.036,
 "lat_track_p95": 32.012,
 "lat_track_max": 33.742,
 "lat_an_avg": 12.82,
 "lat_an_p95": 26.653,
 "gateway_rejected": 0,
 "http_retries": 24,
 "redelivered": 112,
 "duplicates": 112,
 "rejected": 0,
 "events": {
  "t_fail": 45.0,
  "broker_not_ready_s": 1.97,
  "broker_ready_s": 16.01,
  "gateway_ok_s": 23.02,
  "consumers_back_s": 23.23,
  "first_processed_s": 26.36,
  "recovered_t": 68.23,
  "pods_after": [
   {
    "name": "analytics-service-65454bf7cd-4gqnj",
    "app": "analytics-service",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "dispatcher-api-795dc49c98-l97c5",
    "app": "dispatcher-api",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "gateway-9d85848ff-fdbtj",
    "app": "gateway",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "incident-service-6675bcf-wp5ht",
    "app": "incident-service",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "postgres-0",
    "app": "postgres",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "rabbitmq-0",
    "app": "rabbitmq",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "track-service-788965dc5d-bbxbt",
    "app": "track-service",
    "restarts": 0,
    "ready": true
   },
   {
    "name": "track-service-788965dc5d-n64ph",
    "app": "track-service",
    "restarts": 0,
    "ready": true
   }
  ]
 },
 "recovery": {
  "analytics-service": {
   "arrived_during": 0,
   "backlog_after": 0,
   "processed_after": 9464
  },
  "incident-service": {
   "arrived_during": 0,
   "backlog_after": 0,
   "processed_after": 37
  },
  "track-service": {
   "arrived_during": 0,
   "backlog_after": 0,
   "processed_after": 8296
  }
 },
 "outage": [
  45.0,
  68.23
 ],
 "baseline_queue": 212,
 "queue_peak": 3494,
 "normal_at": 69.2,
 "error_reasons": [
  {
   "consumer": "track-service",
   "outcome": "duplicate",
   "reason": "",
   "n": 75
  },
  {
   "consumer": "analytics-service",
   "outcome": "duplicate",
   "reason": "",
   "n": 37
  }
 ],
 "log_lines": 57153,
 "log_attempts": {}
}
```

## Проверка оптимизации: synchronous_commit

| synchronous_commit | Время, с | Track, сообщ./с | Система, соб./с | fsync/с | Latency Track ср. |
|---|---|---|---|---|---|
| on | 52,5 | 235,5 | 95,3 | 316,7 | 6,47 |
| off | 46,8 | 272,3 | 106,9 | 4,9 | 5,08 |
