import json
from dataclasses import dataclass
from typing import Any
from app.kafka.producer import NormalizerKafkaProducer
from app.settings import settings
from .exceptions import NormalizationError, ParsingError
from .schemas import DLQMessage
from .service import normalize_raw_message

@dataclass(frozen=True)
class ProcessingResult:
    normalized_count: int
    sent_to_dlq: bool
    event_types: tuple[str, ...] = ()
    error_type: str | None = None


def decode_kafka_message(value:Any)->Any:

    if not isinstance(value, (bytes,bytearray)):
        raise ParsingError(
            "kafka mesajı byte veya bytarray olmalıdır"
        )

    try:
        text=bytes(value).decode("utf-8")
    except UnicodeDecodeError as error:
        raise ParsingError(
            "kafka mesajı utf8 e çözülemedi"
        )from error

    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        raise ParsingError(
            f" geçerli bir json mesajı değil {error.msg}"
        )from error

def get_original_message_for_dlq(value:Any)-> Any:
    if isinstance(value, (bytes, bytearray)):
        return {
            "raw_value": bytes(value).decode(
                "utf-8",
                errors="replace",
            )
        }

    return {
        "raw_value": repr(value)
    }

async def process_kafka_message(
    message:Any,
    producer:NormalizerKafkaProducer,

)-> ProcessingResult:
    original_message: Any = get_original_message_for_dlq(
        message.value
    )

    try:
        original_message = decode_kafka_message(
            message.value
        )
# asıl normalizasyon burada yapılır
        normalized_events = normalize_raw_message(
            original_message
        )

        for event in normalized_events:
            await producer.send_normalized(event)

        return ProcessingResult(
            normalized_count=len(normalized_events),
            sent_to_dlq=False,
            event_types=tuple(
            event["event_type"]
                for event in normalized_events
            ),
)

    except NormalizationError as error:
        dlq_message = DLQMessage(
            schema_version=settings.schema_version,
            source_topic=message.topic,
            source_partition=message.partition,
            source_offset=message.offset,
            error_type=type(error).__name__,
            error_reason=str(error),
            original_message=original_message,
        )

        await producer.send_dlq(
            dlq_message.model_dump(mode="json")
        )

        return ProcessingResult(
            normalized_count=0,
            sent_to_dlq=True,
            error_type=type(error).__name__,
        )
