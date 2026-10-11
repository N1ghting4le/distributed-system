# Развертывание в Kubernetes (ЛР 5)

Система из ЛР 4 развернута в локальном кластере kind (Kubernetes 1.32, узлы – контейнеры Docker):
узел управления и два рабочих узла. Манифесты – каталог [`k8s/`](../../k8s), развертывание одной командой
`kubectl apply -k k8s/`. Схема – [`docs/diagrams/k8s.mmd`](../diagrams/k8s.mmd).

## Объекты

| Компонент | Объект | Реплик | Доступ |
|---|---|---|---|
| RabbitMQ | StatefulSet `rabbitmq` + PVC `data-rabbitmq-0` (1 Gi) | 1 | Service `rabbitmq:5672`; NodePort 31672 → localhost:15672 |
| PostgreSQL | StatefulSet `postgres` + PVC `data-postgres-0` (2 Gi), ConfigMap `postgres-init` | 1 | Service `postgres:5432`; NodePort 30433 → localhost:5433 |
| Ingest Gateway (Producer) | Deployment `gateway` | 1 | Service `gateway:8080` (NodePort 30080 → localhost:8080) |
| Track Service | Deployment `track-service` | 1 → 2 → 4 | – (потребитель очереди) |
| Schedule Analytics Service | Deployment `analytics-service`, стратегия Recreate | 1 | – |
| Incident Service | Deployment `incident-service` | 1 | – |
| Dispatcher API | Deployment `dispatcher-api` | 1 | Service `dispatcher-api:8081` (NodePort 30081 → localhost:8081) |
| Fleet Simulator | Job `simulator-*` (по требованию) | – | обращается к `http://gateway:8080` |

Брокер и БД – StatefulSet: стабильное имя Pod (`rabbitmq-0` → узел `rabbit@rabbitmq-0`) и собственный том,
который переживает пересоздание Pod. Track Service без состояния – масштабируется `kubectl scale`;
Schedule Analytics Service хранит состояние машин в памяти и остается в одном экземпляре.

## Конфигурация

- ConfigMap `transit-config` – имена Services, порты, параметры обработки (PREFETCH, MAX_ATTEMPTS,
  RETRY_DELAYS_MS, TRACK_PROCESS_DELAY_MS = 20 мс, HEALTH_PORT = 8000 и т. д.), подключается через `envFrom`.
- Secret `transit-secret` – учетные данные RabbitMQ и PostgreSQL и строки подключения `RABBIT_URL`, `DB_DSN`
  (содержат пароль), подключаются через `secretKeyRef`.
- `CONSUMER_ID` = имя Pod (Downward API `metadata.name`) – экземпляр в `processed_messages`.
- Адреса – только имена Services (DNS кластера), IP-адреса Pod нигде не используются.

## Пробы и порядок запуска

- initContainer `wait-deps` у обработчиков ждет TCP-доступности `rabbitmq:5672` и `postgres:5432`
  (аналог `depends_on: service_healthy`).
- Обработчики: HTTP-сервер проб в `common/reliability.serve_health` (переменная HEALTH_PORT):
  `/live` – цикл событий отвечает (livenessProbe), `/ready` – соединение с брокером открыто и БД отвечает
  на `SELECT 1` (readinessProbe).
- Шлюз – `/health` (соединение с брокером), API – `/health` (БД), брокер – `rabbitmq-diagnostics`,
  БД – `pg_isready`.
- `terminationGracePeriodSeconds: 30`: по SIGTERM обработчик закрывает соединения, неподтвержденные
  сообщения брокер возвращает в очередь.

## Результаты

[`experiments.md`](experiments.md): развертывание 24 с (62 с при первом скачивании образов), Track Service
34,0 / 65,5 / 128,1 сообщ./с при 1 / 2 / 4 репликах, замена удаленного Pod готова через 5,7 с,
перезапуск контейнера после kill -9 – через 1,8 с, потерь сообщений нет.
