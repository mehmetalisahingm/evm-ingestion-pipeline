Herhangi bir sorun veya soru için mehmetalixdxd@gmail.com
Herhangi bir sorun veya soru için mehmetalixdxd@gmail.com
Herhangi bir sorun veya soru için mehmetalixdxd@gmail.com
For any issues or questions, please contact mehmetalixdxd@gmail.com
For any issues or questions, please contact mehmetalixdxd@gmail.com
For any issues or questions, please contact mehmetalixdxd@gmail.com

# EVM Ingestion Pipeline

BSC gibi EVM tabanlı blockchain ağlarından canlı blok ve log verilerini alan, eksik blokları tamamlayan ve ham verileri Kafka kuyruğuna gönderen asenkron veri toplama servisi.

Bu proje staj kapsamında geliştirilen **EVM Tabanlı Canlı Veri Akış ve İzolasyon Hattı** projesinin Sprint 1 çalışmasıdır.

---

## Sprint 1 Amacı

Sprint 1 kapsamında tüm mikroservislerin tamamlanması değil, aşağıdaki çekirdek veri hattının çalışan ve izlenebilir hâle getirilmesi amaçlanmıştır:

```text
EVM RPC Provider
        ↓
Ingestion Service
        ↓
Kafka: evm.raw
```

Bu sprintte ClickHouse tabloları hazırlanmıştır ancak Ingestion Service doğrudan ClickHouse’a veri yazmaz.

---

## Mevcut Veri Akışı

```text
BSC WebSocket
├── newHeads aboneliği
│   └── HTTP RPC ile tam blok ve transaction bilgileri alınır
│       └── Block Queue
│           └── Kafka: evm.raw
│               └── Redis checkpoint güncellenir
│
└── logs aboneliği
    └── Log Queue
        └── Kafka: evm.raw
```

Servis başlatıldığında Redis üzerindeki son başarılı checkpoint okunur.

Checkpoint ile güncel blok arasında eksik blok varsa HTTP RPC kullanılarak backfill işlemi yapılır. Eksik bloklar sırayla Kafka’ya gönderildikten sonra canlı WebSocket akışına geçilir.

---

## Tamamlanan Özellikler

- BSC HTTP RPC bağlantısı
- BSC WebSocket bağlantısı
- Canlı blok başlıklarını dinleme
- Canlı blockchain loglarını dinleme
- HTTP RPC üzerinden tam blok verisi alma
- Transaction detaylarını blok verisiyle birlikte alma
- Kafka `evm.raw` topic’ine ham veri gönderme
- Kafka idempotent producer
- Redis checkpoint yönetimi
- Gap detection
- HTTP backfill
- WebSocket bağlantı kopmalarında yeniden bağlanma
- Exponential backoff
- Jitter
- HTTP 429 rate-limit kontrolü
- `Retry-After` desteği
- Bloklar ve loglar için ayrı bounded queue
- Backpressure
- Ardışık blok kontrolü
- Health endpoint
- Readiness endpoint
- Prometheus metrics endpoint
- Docker healthcheck
- ClickHouse DDL scriptleri
- `ReplacingMergeTree(version)` tabloları

---

## Kullanılan Teknolojiler

- Python 3.14
- asyncio
- aiohttp
- websockets
- aiokafka
- redis-py
- prometheus-client
- Apache Kafka
- Redis
- ClickHouse
- Docker
- Docker Compose
- BSC JSON-RPC

---

## Proje Yapısı

```text
evm-ingestion-pipeline/
├── docker/
│   └── clickhouse/
│       └── init/
│           └── 0001_create_tables.sql
│
├── ingestion-service/
│   ├── app/
│   │   ├── common/
│   │   │   └── backoff.py
│   │   ├── kafka/
│   │   │   └── producer.py
│   │   ├── monitoring/
│   │   │   ├── metrics.py
│   │   │   └── server.py
│   │   ├── rpc/
│   │   │   ├── http_client.py
│   │   │   └── websocket_client.py
│   │   ├── services/
│   │   │   └── ingestion.py
│   │   ├── storage/
│   │   │   ├── checkpoint.py
│   │   │   └── redis_client.py
│   │   ├── main.py
│   │   └── settings.py
│   │
│   ├── scripts/
│   │   ├── dev/
│   │   │   └── set_test_checkpoint.py
│   │   └── manual/
│   │       ├── checkpoint_test.py
│   │       ├── http_429_test.py
│   │       ├── ingestion_test.py
│   │       ├── kafka_payload_test.py
│   │       ├── kafka_tests.py
│   │       ├── logs_test.py
│   │       ├── redis_test.py
│   │       ├── rpc_test.py
│   │       └── websocket_test.py
│   │
│   ├── Dockerfile
│   ├── requirements.txt
│   └── .dockerignore
│
├── .env
├── .env.example
├── .gitignore
├── docker-compose.yml
└── README.md
```

---

## Ortam Değişkenleri

Proje kökünde `.env` dosyası bulunmalıdır.

Örnek yapı:

```env
# EVM Network
CHAIN_NAME=bsc
CHAIN_ID=56

# RPC Providers
HTTP_RPC_URL=https://bsc-dataseed.bnbchain.org
WS_RPC_URL=wss://your-bsc-websocket-endpoint

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=evm.raw

# Redis
REDIS_URL=redis://localhost:6379/0

# Queue Limits
BLOCK_QUEUE_MAX_SIZE=500
LOG_QUEUE_MAX_SIZE=5000

# Monitoring
MONITORING_PORT=8000
```

Gerçek RPC anahtarları yalnızca `.env` dosyasında tutulmalıdır.

`.env` dosyası Git deposuna gönderilmemelidir.

---

## Docker ile Çalıştırma

Proje köküne geçin:

```powershell
cd C:\Users\mehmet\Desktop\evm-ingestion-pipeline
```

Tüm servisleri başlatın:

```powershell
docker compose up -d --build
```

Container durumlarını kontrol edin:

```powershell
docker compose ps
```

Beklenen servisler:

```text
evm-kafka
evm-redis
evm-clickhouse
evm-ingestion
```

Ingestion Service loglarını takip edin:

```powershell
docker compose logs -f ingestion
```

Log takibinden çıkmak için:

```text
Ctrl + C
```

Bu işlem containerı durdurmaz, yalnızca log ekranını kapatır.

Tüm servisleri durdurmak için:

```powershell
docker compose down
```

Volume verilerini de tamamen silmek için:

```powershell
docker compose down -v
```

`-v` seçeneği Redis checkpoint ve ClickHouse verilerini de siler.

---

## Yerel Python ile Çalıştırma

Ingestion Service yerel Python ile çalıştırılacaksa Docker içerisindeki ingestion containerı çalışmamalıdır.

Önce yalnızca altyapı servislerini başlatın:

```powershell
cd C:\Users\mehmet\Desktop\evm-ingestion-pipeline

docker compose down
docker compose up -d kafka redis clickhouse
```

Ingestion Service klasörüne geçin:

```powershell
cd ingestion-service
```

Sanal ortamı etkinleştirin:

```powershell
.\.venv\Scripts\Activate.ps1
```

Bağımlılıkları yükleyin:

```powershell
py -m pip install -r requirements.txt
```

Servisi başlatın:

```powershell
py -m app.main
```

Servisi durdurmak için:

```text
Ctrl + C
```

> Docker içerisindeki ingestion servisi çalışırken ayrıca `py -m app.main` çalıştırılmamalıdır. İki servis aynı Redis checkpoint anahtarını kullanacağı için blok sırası çakışabilir.

---

## Monitoring Endpoint’leri

### Health

```text
http://localhost:8000/health
```

Beklenen cevap:

```json
{"status":"ok"}
```

### Readiness

```text
http://localhost:8000/ready
```

Beklenen cevap:

```json
{"status":"ready"}
```

### Prometheus Metrics

```text
http://localhost:8000/metrics
```

Projeye ait önemli metrikler:

```text
evm_blocks_published_total
evm_logs_published_total
evm_backfill_blocks_total
evm_block_queue_size
evm_log_queue_size
evm_checkpoint_block
```

Yalnızca EVM metriklerini PowerShell üzerinden göstermek için:

```powershell
curl.exe -s http://localhost:8000/metrics |
Select-String "^evm_(blocks_published_total|logs_published_total|backfill_blocks_total|block_queue_size|log_queue_size|checkpoint_block)\s"
```

---

## Kafka

Kullanılan topic:

```text
evm.raw
```

Docker içerisindeki Kafka adresi:

```text
kafka:19092
```

Bilgisayar üzerinden erişim adresi:

```text
localhost:9092
```

Topic listesini görüntülemek için:

```powershell
docker compose exec kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --list `
  --bootstrap-server localhost:19092
```

Kafka’ya gelen üç ham mesajı görüntülemek için:

```powershell
docker compose exec kafka `
  /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:19092 `
  --topic evm.raw `
  --property print.key=true `
  --property print.timestamp=true `
  --max-messages 3
```

Kafka mesaj anahtarı olarak `chain_id` kullanılır.

BSC için örnek mesaj anahtarı:

```text
56
```

Kafka’ya iki tür ham event gönderilir:

```text
raw_block
raw_log
```

---

## Redis Checkpoint

Redis üzerinde kullanılan checkpoint anahtarı:

```text
evm:56:ingestion:checkpoint
```

Checkpoint değerini görüntülemek için:

```powershell
docker compose exec redis redis-cli GET evm:56:ingestion:checkpoint
```

Checkpoint yalnızca blok Kafka’ya başarıyla gönderildikten sonra güncellenir.

Bu sayede servis kapandığında veya bağlantı kesildiğinde son başarılı bloktan devam edebilir.

Checkpoint geriye doğru güncellenmez.

---

## Gap Detection ve Backfill

Servis başlangıcında:

1. Redis checkpoint okunur.
2. HTTP RPC üzerinden ağdaki güncel blok numarası alınır.
3. Checkpoint ile güncel blok arasında boşluk olup olmadığı kontrol edilir.
4. Eksik bloklar HTTP RPC üzerinden sırayla alınır.
5. Eksik bloklar Kafka’ya gönderilir.
6. Başarılı gönderimden sonra checkpoint ilerletilir.
7. Backfill tamamlandıktan sonra canlı WebSocket akışı başlatılır.

Canlı akış sırasında da blok numaraları arasında boşluk tespit edilirse HTTP backfill uygulanır.

---

## Queue ve Backpressure

Bloklar ve loglar için ayrı kuyruklar kullanılır:

```text
Block Queue: 500 event
Log Queue: 5000 event
```

Queue sınırına ulaşıldığında producer yeni veri ekleyebilmek için bekler.

Bu davranış kontrolsüz bellek kullanımını önler ve sisteme backpressure kazandırır.

---

## HTTP Rate-Limit Yönetimi

HTTP RPC sağlayıcısı `429 Too Many Requests` döndürürse:

1. `Retry-After` header değeri kontrol edilir.
2. Geçerli bir değer varsa o süre kadar beklenir.
3. Değer yoksa exponential backoff uygulanır.
4. Aynı anda oluşabilecek tekrar isteklerini dağıtmak için jitter eklenir.
5. Belirlenen retry sayısından sonra hata üst katmana aktarılır.

---

## WebSocket Dayanıklılığı

WebSocket bağlantısında:

- Ping ve pong heartbeat kullanılır.
- Bağlantı kopması algılanır.
- Exponential backoff uygulanır.
- Jitter eklenir.
- Belirlenen retry sayısına kadar yeniden bağlantı denenir.
- Başarılı bağlantıdan sonra abonelik yeniden oluşturulur.

Kullanılan abonelikler:

```text
newHeads
logs
```

---

## ClickHouse

Sprint 1 kapsamında aşağıdaki tablolar hazırlanmıştır:

```text
evm.blocks
evm.transactions
evm.logs
```

Tabloları görüntülemek için:

```powershell
docker compose exec clickhouse clickhouse-client `
  --user $env:CLICKHOUSE_USER `
  --password $env:CLICKHOUSE_PASSWORD `
  --query "SHOW TABLES FROM evm"
```

Alternatif olarak container içerisindeki varsayılan yapılandırmayla:

```powershell
docker compose exec clickhouse clickhouse-client `
  --query "SHOW TABLES FROM evm"
```

Tablolar `ReplacingMergeTree(version)` kullanır.

Bu yapı aynı kaydın yeni bir versiyonu geldiğinde append mantığıyla yazılmasını ve sorgu sırasında en güncel versiyonun seçilebilmesini sağlar.

Sprint 1’de ClickHouse’a veri yazılmaz.

Mevcut akış:

```text
BSC RPC
→ Ingestion Service
→ Kafka evm.raw
```

ClickHouse yazımı sonraki sprintlerde geliştirilecek Normalizer ve Batch Writer servisleri tarafından yapılacaktır.

---

## Manuel Kontrol Scriptleri

Manuel scriptler ana uygulama çalışırken otomatik olarak çalışmaz.

Bileşenleri ayrı ayrı test etmek için kullanılır.

Ingestion Service klasöründe:

```powershell
cd ingestion-service
```

HTTP RPC testi:

```powershell
py -m scripts.manual.rpc_test
```

WebSocket testi:

```powershell
py -m scripts.manual.websocket_test
```

Log aboneliği testi:

```powershell
py -m scripts.manual.logs_test
```

Redis testi:

```powershell
py -m scripts.manual.redis_test
```

Kafka payload testi:

```powershell
py -m scripts.manual.kafka_payload_test
```

Checkpoint testi:

```powershell
py -m scripts.manual.checkpoint_test
```

429 testi:

```powershell
py -m scripts.manual.http_429_test
```

---

## Demo Checkpoint Scripti

Sunum veya geliştirme sırasında binlerce eski bloğun backfill edilmesini önlemek için checkpoint güncel bloğa yaklaştırılabilir:

```powershell
py -m scripts.dev.set_test_checkpoint
```

Bu script Redis checkpoint değerini bilinçli olarak değiştirir.

Production ortamında kullanılmamalıdır.

---

## Temel Kontrol Komutları

Docker Compose yapılandırmasını doğrulamak için:

```powershell
docker compose config --quiet
```

Container durumlarını görmek için:

```powershell
docker compose ps
```

Ingestion loglarında kritik hata aramak için:

```powershell
docker compose logs --tail=200 ingestion |
Select-String "Traceback|ERROR|Exception|ModuleNotFound|Blok sırası bozuldu"
```

Python dosyalarının sözdizimini kontrol etmek için:

```powershell
cd ingestion-service
py -m compileall -q app scripts
```

Ana modül import kontrolü:

```powershell
py -c "from app.services.ingestion import run_ingestion; print('Import başarılı')"
```

---

## Sunumda Çalıştırma

Proje kökünde:

```powershell
cd C:\Users\mehmet\Desktop\evm-ingestion-pipeline

docker compose down
docker compose up -d kafka redis clickhouse
```

Checkpoint’i güncel bloğa yaklaştırın:

```powershell
cd ingestion-service
.\.venv\Scripts\Activate.ps1
py -m scripts.dev.set_test_checkpoint
cd ..
```

Ingestion servisini başlatın:

```powershell
docker compose up -d --build ingestion
```

Containerları kontrol edin:

```powershell
docker compose ps
```

Canlı logları gösterin:

```powershell
docker compose logs -f ingestion
```

Kafka mesajlarını gösterin:

```powershell
docker compose exec kafka `
  /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:19092 `
  --topic evm.raw `
  --property print.key=true `
  --max-messages 3
```

Redis checkpoint’i gösterin:

```powershell
docker compose exec redis redis-cli GET evm:56:ingestion:checkpoint
```

Monitoring endpoint’lerini gösterin:

```text
http://localhost:8000/health
http://localhost:8000/ready
http://localhost:8000/metrics
```

Sunum sonunda:

```powershell
docker compose down
```

---

## Sprint 1 Durumu

Sprint 1 kapsamında hedeflenen çekirdek hat çalışmaktadır:

```text
BSC WebSocket + HTTP RPC
          ↓
    Ingestion Service
          ↓
      Kafka evm.raw
```

Ek olarak aşağıdaki güvenilirlik ve izlenebilirlik özellikleri uygulanmıştır:

```text
Redis checkpoint
Gap detection
HTTP backfill
Bounded queue
Backpressure
429 rate-limit kontrolü
Exponential backoff
Jitter
WebSocket reconnect
Health endpoint
Readiness endpoint
Prometheus metrics
```

---

## Sonraki Sprintler

Sonraki aşamalarda aşağıdaki servislerin geliştirilmesi planlanmaktadır:

- Normalizer Service
- Deterministik `event_id`
- Veri doğrulama
- Dead Letter Queue
- Re-org Service
- Canonical event yönetimi
- Batch Writer Service
- 2.000 kayıt veya 3 saniye batch yazımı
- ClickHouse veri yazımı
- Retry Writer
- Reconciliation Service
- Prometheus ve Grafana dashboardları

---

## Git Branch

Sprint 1 geliştirmeleri aşağıdaki branch üzerinden yürütülür:

```text
feature/sprint-1-ingestion
```

Aktif branch’i kontrol etmek için:

```powershell
git branch --show-current
```

Git durumunu kontrol etmek için:

```powershell
git status
```

---

## Güvenlik

- Gerçek RPC API anahtarları Git deposuna gönderilmemelidir.
- `.env` dosyası `.gitignore` içinde bulunmalıdır.
- `.env.example` yalnızca örnek değerler içermelidir.
- Repository’ye push işleminden önce secret kontrolü yapılmalıdır.
