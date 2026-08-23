# EVM Ingestion Pipeline

BSC gibi EVM tabanlı blockchain ağlarından canlı blok, transaction ve log verilerini alan; Kafka üzerinden işleyen; normalize eden; re-org/idempotency kontrolünden geçirip canonical event üreten; ClickHouse'a batch olarak yazan ve Prometheus + Grafana ile gözlemlenen asenkron veri hattı.

Bu proje staj kapsamında geliştirilen **EVM Tabanlı Canlı Veri Akış ve İzolasyon Hattı** projesidir.

Sprint 1, Sprint 2 ve Sprint 3 kapsamındaki ana veri hattı tamamlanmış ve uçtan uca doğrulanmıştır.

---

## Mimari

```text
BSC
 |
 | WebSocket newHeads
 | HTTP RPC (block + receipts)
 v
Ingestion Service
 |
 v
Kafka: evm.raw
 |
 v
Normalizer Service
 |
 +--------------------------> Kafka: evm.dlq
 |
 v
Kafka: evm.normalized
 |
 v
Re-org / Idempotency Service
 |
 +--------------------------> Kafka: evm.dlq
 |
 v
Kafka: canonical-events
 |
 v
Batch Writer Service
 |
 +--------------------------> Kafka: evm.retry_writer
 |
 +--------------------------> Kafka: evm.dlq
 |
 v
ClickHouse

BSC RPC --------------------+
                            |
ClickHouse -----------------+--> Reconciliation Service

Tüm servislerin /metrics endpoint'leri
                |
                v
            Prometheus
                |
                v
             Grafana
```

---

# Sprint 1 - Ingestion

Sprint 1 kapsamında blockchain'den canlı veri alan ingestion katmanı geliştirildi.

## Ingestion Service

Temel görevleri:

- BSC WebSocket üzerinden `newHeads` dinlemek
- HTTP RPC ile tam blok verisini almak
- `eth_getBlockReceipts` ile blok receipt'lerini almak
- Receipt'lerden log eventlerini çıkarmak
- Block + log verilerini `evm.raw` topic'ine göndermek
- Redis checkpoint tutmak
- Gap detection ve backfill yapmak
- HTTP 429 durumlarında retry/backoff uygulamak
- Health, readiness ve Prometheus metrics sunmak

### Log toplama yaklaşımı

İlk sürümde loglar ayrı WebSocket subscription ile dinleniyordu. RPC sağlayıcısında oluşan WebSocket `429` problemleri ve restart/backfill sırasında log kaybı riski nedeniyle mimari değiştirildi.

Güncel yaklaşım:

```text
newHeads
   |
   v
block number
   |
   +--> eth_getBlockByNumber
   |
   +--> eth_getBlockReceipts
             |
             v
          logs
```

Böylece canlı akış ve backfill aynı veri toplama yolunu kullanır.

### Checkpoint güvenliği

Checkpoint ancak ilgili block ve o bloğa ait loglar Kafka'ya başarıyla gönderildikten sonra ilerletilir.

BSC checkpoint key örneği:

```text
evm:56:ingestion:checkpoint
```

Kontrol:

```powershell
docker exec evm-redis redis-cli GET evm:56:ingestion:checkpoint
```

---

# Sprint 2 - Normalizer ve Re-org

## Normalizer Service

`evm.raw` topic'ini tüketir ve ham kayıtları standart event formatına dönüştürür.

Desteklenen event tipleri:

```text
block
transaction
log
```

Bir block mesajı:

```text
1 block event
+
N transaction event
```

üretir.

Bir raw log mesajı ise bir `log` event üretir.

### Normalized event temel alanları

```text
schema_version
event_id
event_type
chain_id
block_number
block_hash
normalized_at
payload
```

Mevcut schema version:

```text
1
```

### Deterministik Event ID

Event ID değerleri SHA-256 ile deterministik olarak üretilir.

```text
block:
SHA256(block|chain_id|block_hash)

transaction:
SHA256(transaction|chain_id|block_hash|transaction_hash)

log:
SHA256(log|chain_id|block_hash|transaction_hash|log_index)
```

Aynı blockchain event'i tekrar işlendiğinde aynı `event_id` üretilir.

### Normalizer performans optimizasyonu

Normalizer başlangıçta mesajları tek tek tüketiyor, her event için `send_and_wait()` bekliyor ve her raw mesajdan sonra ayrı offset commit ediyordu.

Güncel yapı:

```text
Kafka getmany(max 500)
        |
        v
500 mesaja kadar normalize et
        |
        v
Producer buffer'a queue et
        |
        v
Delivery ACK'lerini birlikte bekle
        |
        v
Batch için tek offset commit
```

Producer tarafında Kafka batching için `linger_ms` ve daha büyük batch boyutu kullanılır.

---

## Re-org / Idempotency Service

`evm.normalized` topic'ini tüketir ve eventlerin canonical durumunu belirler.

Block eventlerinde `parent_hash`, mevcut canonical head ile karşılaştırılır.

Normal zincir devamında:

```text
canonical=true
version=1
```

Re-org oluştuğunda eski canonical eventler:

```text
canonical=false
version=N+1
```

olarak yeniden yayınlanır.

Yeni canonical zincir eventleri ise `canonical=true` olarak yayınlanır.

### Duplicate kontrolü

Aynı `event_id` aynı canonical state ile tekrar gelirse:

- yeni version oluşturulmaz
- `canonical-events` topic'ine tekrar gönderilmez
- duplicate metriği artırılır

### Redis state

Önemli key örnekleri:

```text
reorg:head:{chain_id}
reorg:event:{event_id}
reorg:block:{chain_id}:{block_number}
reorg:block-hash:{chain_id}:{block_hash}
reorg:block-events:{chain_id}:{block_hash}
reorg:block-index:{chain_id}
reorg:pending:{chain_id}:{block_hash}
```

Re-org window:

```text
50 block
```

Pending TTL:

```text
120 saniye
```

### Re-org performans optimizasyonları

Re-org servisinde chain state'i nedeniyle block eventleri sıralı işlenir. Transaction ve log eventleri ise block sınırları korunarak kontrollü concurrency ile paralel işlenebilir.

Güncel yaklaşım:

```text
BLOCK
  |
  v
sıralı işle
  |
  +--> TX / LOG child eventleri
          |
          v
  kontrollü concurrency
          |
          v
  hepsi bitmeden sonraki BLOCK'a geçme
```

Ek olarak:

- Kafka mesajları batch olarak tüketilir
- Batch sonunda tek offset commit yapılır
- Aynı `event_id` için eşzamanlı race oluşmaması amacıyla lock kullanılır
- Aynı JSON mesajı gereksiz yere iki kez parse edilmez
- Son canonical block state'leri RAM cache'de tutulur
- Cache miss durumunda Redis fallback kullanılır
- Redis source of truth olmaya devam eder
- Cache yalnızca Redis transaction başarıyla tamamlandıktan sonra güncellenir

Bu optimizasyonlar özellikle yoğun log trafiğinde Redis round-trip sayısını ciddi biçimde azaltır.

---

# Sprint 3 - Storage, Reconciliation ve Observability

## Batch Writer Service

`canonical-events` topic'ini tüketir ve eventleri ClickHouse'a toplu biçimde yazar.

Flush koşulu:

```text
2000 kayıt
VEYA
3 saniye
```

Akış:

```text
canonical-events
      |
      v
Batch Buffer
      |
      +--> 2000 kayıt doldu
      |
      +--> 3 saniye geçti
      |
      v
ClickHouse
```

Event tipine göre hedef tablolar:

```text
block       -> evm.blocks
transaction -> evm.transactions
log         -> evm.logs
```

### Güvenilirlik

ClickHouse yazımı başarısız olduğunda:

```text
write
 |
 x hata
 |
 v
retry + exponential backoff
 |
 x retry exhausted
 |
 v
evm.retry_writer
```

Bozuk canonical mesajlar `evm.dlq` topic'ine gönderilir.

Kafka offset'i yalnızca:

- ClickHouse yazımı başarılıysa veya
- başarısız kayıt güvenli biçimde `evm.retry_writer` topic'ine aktarılmışsa

commit edilir.

Bu yapı at-least-once yaklaşımını korur.

---

## ClickHouse

Tablolar:

```text
evm.blocks
evm.transactions
evm.logs
```

Tablolarda:

```text
ReplacingMergeTree(version)
```

kullanılır.

Bu sayede aynı logical event'in farklı canonical/version durumları yönetilebilir.

ClickHouse HTTP arayüzü:

```text
http://localhost:8123/play
```

Örnek sorgular:

```sql
SHOW TABLES FROM evm;
```

```sql
SELECT *
FROM evm.blocks
ORDER BY block_number DESC
LIMIT 20;
```

```sql
SELECT *
FROM evm.transactions
ORDER BY block_number DESC
LIMIT 20;
```

```sql
SELECT *
FROM evm.logs
ORDER BY block_number DESC
LIMIT 20;
```

---

## Reconciliation Service

Blockchain'de olması gereken veri ile ClickHouse'a gerçekten yazılan veriyi karşılaştırır.

Varsayılan çalışma aralığı:

```text
30 saniyede bir
son 20 block
```

Kontrol edilen başlıca değerler:

- RPC chain head
- ClickHouse head
- Pipeline lag
- Beklenen block sayısı
- ClickHouse block sayısı
- Beklenen log sayısı
- ClickHouse log sayısı
- Eksik block numaraları
- Reconciliation discrepancy

Log sayısı blockchain tarafında `eth_getBlockReceipts` kullanılarak hesaplanır.

Örnek başarılı kontrol:

```text
Expected Blocks = Actual Blocks
Expected Logs   = Actual Logs
Missing Blocks  = 0
```

Pipeline lag, veri kaybı anlamına gelmez. Chain head ile ClickHouse head arasındaki işlenmemiş backlog miktarını gösterir.

---

## Prometheus

Servislerin `/metrics` endpoint'lerini belirli aralıklarla scrape eder ve zaman serisi olarak saklar.

Prometheus UI:

```text
http://localhost:9090
```

Scrape edilen servisler:

```text
ingestion:8000
normalizer:8001
reorg:8002
batch-writer:8003
reconciliation:8004
```

Örnek metricler:

```text
evm_blocks_published_total
evm_logs_published_total
evm_normalizer_events_total
reorg_canonical_events_total
evm_clickhouse_written_rows_total
evm_batch_flush_duration_seconds
evm_reconciliation_discrepancies_total
evm_reconciliation_pipeline_lag_blocks
```

---

## Grafana

Prometheus metriclerini dashboard ve grafiklere dönüştürür.

Grafana UI:

```text
http://localhost:3000
```

Provision edilen dashboard:

```text
EVM Ingestion Pipeline
```

Dashboard panelleri:

- Healthy Services
- Pipeline Lag (Blocks)
- ClickHouse Written Rows
- Missing Blocks
- Live Pipeline Throughput
- Queue / Buffer Fullness
- ClickHouse Batch Flush Duration
- Service Error Rates
- Reconciliation Logs
- Reconciliation Blocks

Grafana dashboard dosyası:

```text
grafana/dashboards/evm-pipeline.json
```

---

# Kafka Topic'leri

## `evm.raw`

```text
Producer: Ingestion Service
Consumer: Normalizer Service
Key: chain_id
```

## `evm.normalized`

```text
Producer: Normalizer Service
Consumer: Re-org Service
Key: chain_id
```

## `canonical-events`

```text
Producer: Re-org Service
Consumer: Batch Writer Service
Key: event_id
```

Ek alanlar:

```text
canonical
version
```

## `evm.retry_writer`

Batch Writer'ın retry denemeleri bittikten sonra hâlâ ClickHouse'a yazılamayan kayıtların güvenli biçimde tutulduğu fallback topic'tir.

## `evm.dlq`

Parse/validation gibi nedenlerle işlenemeyen mesajlar için kullanılır.

---

# At-Least-Once Yaklaşımı

Consumer servislerinde otomatik offset commit kullanılmaz.

```text
enable_auto_commit=False
```

Genel kural:

```text
mesajı al
   |
   v
işle / güvenli output üret
   |
   v
başarılı mı?
   |
   +-- hayır --> commit yok
   |
   +-- evet  --> offset commit
```

Deterministik `event_id`, Redis idempotency state'i ve ClickHouse `ReplacingMergeTree(version)` yapısı tekrar işlemelerin etkisini sınırlar.

---

# Proje Yapısı

```text
evm-ingestion-pipeline/
├── batch-writer-service/
│   ├── app/
│   │   ├── batch/
│   │   ├── clickhouse/
│   │   ├── kafka/
│   │   └── monitoring/
│   ├── Dockerfile
│   └── requirements.txt
│
├── reconciliation-service/
│   ├── app/
│   │   ├── clickhouse/
│   │   ├── monitoring/
│   │   ├── reconciliation/
│   │   └── rpc/
│   ├── Dockerfile
│   └── requirements.txt
│
├── ingestion-service/
├── normalizer-service/
├── reorg-service/
│
├── docker/
│   └── clickhouse/
│       └── init/
│           └── 0001_create_tables.sql
│
├── prometheus/
│   └── prometheus.yml
│
├── grafana/
│   ├── dashboards/
│   │   └── evm-pipeline.json
│   └── provisioning/
│       ├── dashboards/
│       └── datasources/
│
├── scripts/
├── .env.example
├── docker-compose.yml
└── README.md
```

---

# Kullanılan Teknolojiler

- Python 3.14
- asyncio
- aiohttp
- websockets
- aiokafka
- redis-py
- pydantic
- prometheus-client
- Apache Kafka 4.3.1
- Redis 7.4
- ClickHouse 26.5.6
- Prometheus 3.5.0
- Grafana
- Docker
- Docker Compose
- BSC JSON-RPC

---

# Ortam Değişkenleri

Gerçek RPC adresleri ve API anahtarları `.env` dosyasında tutulmalıdır.

Örnek:

```env
CHAIN_NAME=bsc
CHAIN_ID=56

HTTP_RPC_URL=
WS_RPC_URL=

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
REDIS_URL=redis://localhost:6379/0

BLOCK_QUEUE_MAX_SIZE=500
```

`.env` Git deposuna gönderilmemelidir.

`.env.example` gerçek secret içermemelidir.

---

# Docker ile Çalıştırma

Proje kökünde:

```powershell
docker compose up -d --build
```

Container durumları:

```powershell
docker compose ps
```

Ana servisler:

```text
evm-kafka
evm-redis
evm-clickhouse
evm-ingestion
evm-normalizer
evm-reorg
evm-batch-writer
evm-reconciliation
evm-prometheus
evm-grafana
```

Canlı servis logları:

```powershell
docker compose logs -f ingestion normalizer reorg batch-writer reconciliation
```

Sistemi durdurmak:

```powershell
docker compose down
```

Volume'ları da silmek:

```powershell
docker compose down -v
```

> `down -v` Redis, ClickHouse, Prometheus ve Grafana volume verilerini siler.

---

# Monitoring Endpoint'leri

## Ingestion

```text
http://localhost:8000/health
http://localhost:8000/ready
http://localhost:8000/metrics
```

## Normalizer

```text
http://localhost:8001/health
http://localhost:8001/ready
http://localhost:8001/metrics
```

## Re-org

```text
http://localhost:8002/health
http://localhost:8002/ready
http://localhost:8002/metrics
```

## Batch Writer

```text
http://localhost:8003/health
http://localhost:8003/ready
http://localhost:8003/metrics
```

## Reconciliation

```text
http://localhost:8004/health
http://localhost:8004/ready
http://localhost:8004/metrics
```

---

# Kafka Consumer Lag Kontrolü

Tüm consumer group'ları:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server kafka:19092 `
  --all-groups `
  --describe
```

Bu çıktı ile Normalizer, Re-org ve Batch Writer backlog değerleri takip edilebilir.

---

# Manuel Doğrulamalar

Proje geliştirme sırasında aşağıdaki senaryolar doğrulanmıştır:

- BSC canlı block akışı
- Receipt tabanlı canlı log akışı
- Normalizer block / transaction / log üretimi
- Deterministik event ID
- DLQ davranışı
- Re-org simulation
- Duplicate suppression
- Canonical event üretimi
- Batch Writer block / transaction / log yazımı
- 2000 kayıt ile size-based flush
- 3 saniye ile time-based flush
- ClickHouse kapalıyken retry ve `evm.retry_writer` fallback
- Hatalı canonical mesajın DLQ'ya gönderilmesi
- Reconciliation expected/actual block eşleşmesi
- Reconciliation expected/actual log eşleşmesi
- Prometheus'ta beş uygulama servisinin scrape edilmesi
- Grafana dashboard provisioning
- Normalizer ve Re-org consumer lag performans optimizasyonları

---

# Mevcut Durum

Uçtan uca veri hattı:

```text
BSC
 |
 v
Ingestion
 |
 v
evm.raw
 |
 v
Normalizer
 |
 v
evm.normalized
 |
 v
Re-org / Idempotency
 |
 v
canonical-events
 |
 v
Batch Writer
 |
 v
ClickHouse
```

çalışır durumdadır.

Reconciliation ile blockchain ve ClickHouse verisi karşılaştırılabilir; Prometheus ve Grafana ile throughput, queue fullness, batch süreleri, hata oranları, missing block ve pipeline lag takip edilebilir.

`evm.retry_writer` başarısız Batch Writer kayıtları için durable fallback olarak kullanılmaktadır. Ayrı bir retry-consumer servisi eklenmesi gerekirse bu topic üzerinden genişletilebilir.

---

# Güvenlik Notları

- `.env` `.gitignore` içinde tutulmalıdır.
- `.env.example` yalnızca örnek değerler içermelidir.
- RPC API key gibi secret değerler README veya source code içine yazılmamalıdır.
- Production ortamında varsayılan/local geliştirme credential'ları kullanılmamalıdır.
- Commit öncesinde secret kontrolü yapılmalıdır.
