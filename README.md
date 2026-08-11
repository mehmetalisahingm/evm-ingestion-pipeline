EVM Ingestion Pipeline

BSC gibi EVM tabanlı blockchain ağlarından canlı blok ve log verilerini alan, ham veriyi Kafka üzerinden işleyen, normalize eden, re-org/idempotency kontrolünden geçirerek canonical event üreten asenkron veri hattı.

Bu proje staj kapsamında geliştirilen EVM Tabanlı Canlı Veri Akış ve İzolasyon Hattı projesidir.

Şu anda Sprint 1 ve Sprint 2 kapsamındaki ana veri hattı çalışmaktadır.

Mevcut Veri Akışı

BSC WebSocket + HTTP RPC
          |
          v
   Ingestion Service
          |
          v
      Kafka: evm.raw
          |
          v
   Normalizer Service
          |
          +---------------------> Kafka: evm.dlq
          |
          v
 Kafka: evm.normalized
          |
          v
 Re-org / Idempotency Service
          |
          +---------------------> Kafka: evm.dlq
          |
          v
 Kafka: canonical-events
          |
          v
   Batch Writer [sonraki sprint]
          |
          v
      ClickHouse

Sprint 1

Sprint 1 kapsamında EVM ağından canlı veri toplayan ingestion katmanı tamamlandı.

Tamamlanan özellikler

BSC HTTP RPC bağlantısı

BSC WebSocket bağlantısı

newHeads ve logs abonelikleri

Tam blok ve transaction bilgilerinin HTTP RPC ile alınması

Kafka evm.raw topic'ine ham veri gönderimi

Kafka idempotent producer

Redis checkpoint yönetimi

Gap detection

HTTP backfill

HTTP 429 rate-limit kontrolü

Retry-After desteği

Exponential backoff ve jitter

WebSocket reconnect

Block ve log için ayrı bounded queue

Backpressure

Ardışık blok kontrolü

Health, readiness ve Prometheus metrics endpoint'leri

ClickHouse başlangıç DDL'leri

ReplacingMergeTree(version) tabloları

Sprint 2

Sprint 2 kapsamında Normalizer Service ile Re-org / Idempotency Service tamamlandı.

Normalizer Service

evm.raw topic'ini tüketir ve ham kayıtları standart event formatına dönüştürür.

Bir tam blok mesajı:

1 block event
+
N transaction event

üretir.

Log mesajı ise bir log event üretir.

Desteklenen event tipleri:

block
transaction
log

Normalize edilen her event aşağıdaki temel alanlara sahiptir:

schema_version
event_id
event_type
chain_id
block_number
block_hash
normalized_at
payload

Mevcut schema version:

1

Deterministik Event ID

Event ID değerleri SHA-256 ile deterministik olarak üretilir.

block:
SHA256(block|chain_id|block_hash)

transaction:
SHA256(transaction|chain_id|block_hash|transaction_hash)

log:
SHA256(log|chain_id|block_hash|transaction_hash|log_index)

Aynı blockchain event'i tekrar işlendiğinde aynı event_id oluşur.

Veri dönüşümleri

Normalizer aşağıdaki dönüşümleri uygular:

Hexadecimal sayıları integer'a dönüştürme

Integer değerleri gerektiğinde string olarak saklama

Hex string'leri normalize etme

Timestamp değerlerini UTC ISO formata çevirme

Boolean alanlarını doğrulama

Zorunlu alanları doğrulama

Geçersiz kayıtlar evm.dlq topic'ine gönderilir.

DLQ

Hatalı mesajlarda aşağıdaki bilgiler tutulur:

schema_version
source_service
source_topic
source_partition
source_offset
error_type
error_reason
failed_at
original_message

Offset yalnızca başarılı output veya DLQ gönderiminden sonra commit edilir.

Re-org ve Idempotency

Re-org Service evm.normalized topic'ini tüketir.

Block event'lerinde:

parent_hash

değeri son canonical block'un:

block_hash

değeriyle karşılaştırılır.

Normal devam eden zincirde event:

canonical=true
version=1

olarak yayınlanır.

Re-org oluştuğunda eski canonical event'ler yeni versiyonla:

canonical=false
version=N+1

olarak yayınlanır.

Yeni canonical zincir event'leri ise:

canonical=true

olarak yayınlanır.

Duplicate kontrolü

Aynı event_id aynı canonical state ile tekrar gelirse:

yeni version oluşturulmaz

canonical-events topic'ine tekrar gönderilmez

duplicate metriği artırılır

Redis state

Re-org state Redis üzerinde tutulur.

Önemli key örnekleri:

reorg:head:{chain_id}
reorg:event:{event_id}
reorg:block:{chain_id}:{block_number}
reorg:block-hash:{chain_id}:{block_hash}
reorg:block-events:{chain_id}:{block_hash}
reorg:block-index:{chain_id}
reorg:pending:{chain_id}:{block_hash}

Re-org window:

50 block

Block event gelmeden önce ulaşan transaction/log event'leri pending durumda tutulabilir.

Pending TTL:

120 saniye

Kafka Topic'leri

evm.raw

Producer:

Ingestion Service

Consumer:

Normalizer Service

Kafka key:

chain_id

evm.normalized

Producer:

Normalizer Service

Consumer:

Re-org Service

Kafka key:

chain_id

canonical-events

Producer:

Re-org Service

Kafka key:

event_id

Mesajlara eklenen alanlar:

canonical
version

evm.dlq

Normalizer veya Re-org tarafından işlenemeyen kayıtlar için kullanılır.

At-Least-Once Yaklaşımı

Normalizer ve Re-org consumer'larında:

enable_auto_commit=False

kullanılır.

Offset yalnızca mesaj başarılı biçimde işlendikten sonra manuel olarak commit edilir.

Bu yapı veri kaybı yerine gerektiğinde tekrar işlemeyi tercih eder.

Deterministik event_id, duplicate kontrolü ve version mantığı tekrar işlemenin etkisini sınırlar.

Proje Yapısı

evm-ingestion-pipeline/
├── docker/
│   └── clickhouse/
│       └── init/
│           └── 0001_create_tables.sql
│
├── ingestion-service/
│   ├── app/
│   ├── scripts/
│   ├── Dockerfile
│   └── requirements.txt
│
├── normalizer-service/
│   ├── app/
│   │   ├── kafka/
│   │   ├── monitoring/
│   │   └── normalizer/
│   ├── scripts/
│   │   └── manual/
│   │       └── normalizer_smoke_test.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── reorg-service/
│   ├── app/
│   │   ├── kafka/
│   │   ├── monitoring/
│   │   ├── reorg/
│   │   └── state/
│   ├── Dockerfile
│   └── requirements.txt
│
├── scripts/
│   └── manual/
│       └── reorg_simulation_test.py
│
├── .env.example
├── docker-compose.yml
└── README.md

Kullanılan Teknolojiler

Python 3.14

asyncio

aiohttp

websockets

aiokafka

redis-py

pydantic

prometheus-client

Apache Kafka

Redis

ClickHouse

Docker

Docker Compose

BSC JSON-RPC

Ortam Değişkenleri

Gerçek RPC adresleri ve API anahtarları yalnızca .env dosyasında tutulmalıdır.

Örnek:

CHAIN_NAME=bsc
CHAIN_ID=56

HTTP_RPC_URL=
WS_RPC_URL=

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=evm.raw

REDIS_URL=redis://localhost:6379/0

BLOCK_QUEUE_MAX_SIZE=500
LOG_QUEUE_MAX_SIZE=5000

.env Git deposuna gönderilmemelidir.

.env.example gerçek secret içermemelidir.

Docker ile Çalıştırma

Proje kökünde:

cd C:\Users\mehmet\Desktop\evm-ingestion-pipeline
docker compose up -d --build

Container durumları:

docker compose ps

Beklenen ana servisler:

evm-kafka
evm-redis
evm-clickhouse
evm-ingestion
evm-normalizer
evm-reorg

Canlı loglar:

docker compose logs -f ingestion normalizer reorg

Ctrl + C yalnızca log takibini kapatır. Container'ları durdurmaz.

Tüm sistemi durdurmak için:

docker compose down

Volume'ları da silmek için:

docker compose down -v

down -v Redis state ve ClickHouse verilerini de siler.

Monitoring Endpoint'leri

Ingestion Service

http://localhost:8000/health
http://localhost:8000/ready
http://localhost:8000/metrics

Normalizer Service

http://localhost:8001/health
http://localhost:8001/ready
http://localhost:8001/metrics

Re-org Service

http://localhost:8002/health
http://localhost:8002/ready
http://localhost:8002/metrics

Önemli Re-org metrikleri:

reorg_normalized_messages_total
reorg_canonical_events_total
reorg_duplicate_events_total
reorg_detected_total
reorg_dlq_messages_total
reorg_offset_commits_total
reorg_processing_errors_total
reorg_service_ready

Redis Ingestion Checkpoint

BSC için checkpoint key:

evm:56:ingestion:checkpoint

Kontrol:

docker exec evm-redis redis-cli GET evm:56:ingestion:checkpoint

Checkpoint yalnızca blok Kafka'ya başarıyla gönderildikten sonra ilerletilir.

Servis yeniden başladığında checkpoint ile güncel blok arasında boşluk varsa backfill başlatılır.

Geliştirme ortamında çok eski checkpoint milyonlarca blokluk backfill başlatabilir ve RPC kotasını tüketebilir. Test amacıyla checkpoint bilinçli olarak güncel bloğa taşınabilir. Production ortamında geçmiş veriler kontrolsüz biçimde atlanmamalıdır.

Re-org Head Kontrolü

BSC için mevcut Re-org head:

docker exec evm-redis redis-cli GET "reorg:head:56"

Başarılı canlı akışta block number ağdaki güncel block seviyesine yakın olmalıdır.

Kafka Kontrol Komutları

Topic listesi:

docker exec evm-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:19092 `
  --list

Beklenen topic'ler:

evm.raw
evm.normalized
evm.dlq
canonical-events

Canonical event görüntüleme:

docker exec evm-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:19092 `
  --topic canonical-events `
  --max-messages 5 `
  --timeout-ms 15000 `
  --formatter-property print.key=true `
  --formatter-property key.separator=" | "

Başarılı event örneğinde:

Kafka key = event_id
canonical = true
version = 1

Manuel Testler

Normalizer smoke test

.\normalizer-service\.venv\Scripts\python.exe `
  .\normalizer-service\scripts\manual\normalizer_smoke_test.py

Re-org simulation testi

.\reorg-service\.venv\Scripts\python.exe `
  .\scripts\manual\reorg_simulation_test.py

Re-org testi aşağıdaki senaryoyu doğrular:

Block A canonical=true v1
Transaction A canonical=true v1
Block B canonical=true v1
Transaction B canonical=true v1

alternatif Block C gelir

Block B canonical=false v2
Transaction B canonical=false v2
Block C canonical=true v1

Block C tekrar gönderilir

duplicate output üretilmez

Sprint 2 geliştirmesinde bu test başarıyla tamamlanmıştır.

Sprint 2 Doğrulama Durumu

Sprint 2 sonunda canlı veri hattı aşağıdaki noktaya kadar uçtan uca doğrulanmıştır:

BSC
 |
 v
Ingestion Service
 |
 v
evm.raw
 |
 v
Normalizer Service
 |
 v
evm.normalized
 |
 v
Re-org / Idempotency Service
 |
 v
canonical-events

Canlı canonical-events mesajlarında:

canonical=true
version=1
Kafka key=event_id

doğrulanmıştır.

Re-org simulation testinde:

reorg_detected_total = 1
duplicate_events_total = 1
DLQ = 0
processing_errors = 0

beklenen davranış doğrulanmıştır.

ClickHouse

ClickHouse container'ı ve başlangıç tabloları projede bulunmaktadır:

evm.blocks
evm.transactions
evm.logs

Tablolarda:

ReplacingMergeTree(version)

kullanılır.

Sprint 2 sonunda canonical-events henüz ClickHouse'a yazılmamaktadır.

Bu işlem Batch Writer Service tarafından sonraki sprintte geliştirilecektir.

Sonraki Sprint

Bir sonraki ana akış:

canonical-events
      |
      v
Batch Writer Service
      |
      v
ClickHouse

Planlanan başlıca işler:

Batch Writer Service

2.000 kayıt veya 3 saniye batch yazımı

ClickHouse'a yüksek performanslı insert

Başarılı yazımdan sonra Kafka offset commit

Idempotent veri yazımı

Retry Writer

Reconciliation Service

Incoming / Processed ID takibi

Final E2E testleri

Monitoring ve dokümantasyon iyileştirmeleri

Git Branch

Sprint 2 geliştirmeleri:

feature/sprint-2-processing

Aktif branch:

git branch --show-current

Değişiklik kontrolü:

git status --short
git diff --stat

Güvenlik

Gerçek RPC API anahtarları Git deposuna gönderilmemelidir.

.env .gitignore içinde tutulmalıdır.

.env.example yalnızca örnek değerler içermelidir.

Commit öncesinde secret kontrolü yapılmalıdır.