import json
from pydantic import ValidationError
from app.batch.models import CanonicalEvent

class CanonicalEventParsingError(Exception):
    pass

def parse_canonical_event(
    value: bytes,
) -> CanonicalEvent:
    try:
        decoded = value.decode("utf-8")
        data = json.loads(decoded)

        return CanonicalEvent.model_validate(data)

    except UnicodeDecodeError as exc:
        raise CanonicalEventParsingError(
            "Mesaj UTF-8 olarak çözülemedi."
        ) from exc

    except json.JSONDecodeError as exc:
        raise CanonicalEventParsingError(
            "Mesaj geçerli JSON değil."
        ) from exc

    except ValidationError as exc:
        raise CanonicalEventParsingError(
            f"Canonical event doğrulanamadı: {exc}"
        ) from exc