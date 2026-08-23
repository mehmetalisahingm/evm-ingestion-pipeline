import json
from dataclasses import dataclass
from typing import Any

from app.kafka.producer import NormalizerKafkaProducer
from app.settings import settings

from .exceptions import (
    NormalizationError,
    ParsingError,
)
from .schemas import DLQMessage
from .service import normalize_raw_message


@dataclass(frozen=True)
class PreparedMessage:
    normalized_events: tuple[
        dict[str, Any],
        ...
    ] = ()

    dlq_message: dict[str, Any] | None = None
    event_types: tuple[str, ...] = ()
    error_type: str | None = None

    @property
    def normalized_count(self) -> int:
        return len(
            self.normalized_events
        )

    @property
    def sent_to_dlq(self) -> bool:
        return (
            self.dlq_message
            is not None
        )


@dataclass(frozen=True)
class ProcessingResult:
    normalized_count: int
    sent_to_dlq: bool
    event_types: tuple[str, ...] = ()
    error_type: str | None = None


def decode_kafka_message(
    value: Any,
) -> Any:
    if not isinstance(
        value,
        (bytes, bytearray),
    ):
        raise ParsingError(
            "Kafka mesajı bytes veya "
            "bytearray olmalıdır."
        )

    try:
        text = bytes(value).decode(
            "utf-8"
        )

    except UnicodeDecodeError as error:
        raise ParsingError(
            "Kafka mesajı UTF-8 olarak "
            "çözülemedi."
        ) from error

    try:
        return json.loads(text)

    except json.JSONDecodeError as error:
        raise ParsingError(
            "Geçerli bir JSON mesajı değil: "
            f"{error.msg}"
        ) from error


def get_original_message_for_dlq(
    value: Any,
) -> Any:
    if isinstance(
        value,
        (bytes, bytearray),
    ):
        return {
            "raw_value": bytes(
                value
            ).decode(
                "utf-8",
                errors="replace",
            )
        }

    return {
        "raw_value": repr(value)
    }


def prepare_kafka_message(
    message: Any,
) -> PreparedMessage:
    """
    Mesajı decode + normalize eder.

    Burada Kafka'ya hiçbir şey
    gönderilmez.

    Böylece yüzlerce mesaj önce
    hazırlanıp daha sonra producer'a
    topluca verilebilir.
    """

    original_message: Any = (
        get_original_message_for_dlq(
            message.value
        )
    )

    try:
        original_message = (
            decode_kafka_message(
                message.value
            )
        )

        normalized_events = (
            normalize_raw_message(
                original_message
            )
        )

        normalized_tuple = tuple(
            normalized_events
        )

        return PreparedMessage(
            normalized_events=(
                normalized_tuple
            ),
            event_types=tuple(
                event["event_type"]
                for event
                in normalized_tuple
            ),
        )

    except (
        NormalizationError,
        ParsingError,
    ) as error:
        dlq_message = DLQMessage(
            schema_version=(
                settings.schema_version
            ),
            source_topic=(
                message.topic
            ),
            source_partition=(
                message.partition
            ),
            source_offset=(
                message.offset
            ),
            error_type=(
                type(error).__name__
            ),
            error_reason=str(error),
            original_message=(
                original_message
            ),
        )

        return PreparedMessage(
            dlq_message=(
                dlq_message.model_dump(
                    mode="json"
                )
            ),
            error_type=(
                type(error).__name__
            ),
        )


async def queue_prepared_message(
    *,
    prepared: PreparedMessage,
    producer: NormalizerKafkaProducer,
) -> list[Any]:
    """
    Hazırlanan eventleri Kafka producer
    bufferına koyar.

    ACK burada tek tek beklenmez.
    Dönen future'lar daha sonra birlikte
    beklenir.
    """

    deliveries: list[Any] = []

    if prepared.sent_to_dlq:
        if prepared.dlq_message is None:
            return deliveries

        delivery = (
            await producer.queue_dlq(
                prepared.dlq_message
            )
        )

        deliveries.append(
            delivery
        )

        return deliveries

    for event in (
        prepared.normalized_events
    ):
        delivery = (
            await producer.queue_normalized(
                event
            )
        )

        deliveries.append(
            delivery
        )

    return deliveries


async def process_kafka_message(
    message: Any,
    producer: NormalizerKafkaProducer,
) -> ProcessingResult:
    """
    Eski tek-mesaj kullanımını korur.

    Ana batch döngüsüne geçtikten sonra
    prepare_kafka_message +
    queue_prepared_message kullanılacak.
    """

    prepared = prepare_kafka_message(
        message
    )

    deliveries = (
        await queue_prepared_message(
            prepared=prepared,
            producer=producer,
        )
    )

    await producer.wait_for_deliveries(
        deliveries
    )

    return ProcessingResult(
        normalized_count=(
            prepared.normalized_count
        ),
        sent_to_dlq=(
            prepared.sent_to_dlq
        ),
        event_types=(
            prepared.event_types
        ),
        error_type=(
            prepared.error_type
        ),
    )