from datetime import datetime,timezone
from typing import Any

import re


from .exceptions import ConversionError

def hex_to_int(value: Any, field_name: str) -> int:


    if isinstance(value, bool):
        raise ConversionError(
            f"{field_name} boolean olamaz: {value}"
        )


    if isinstance(value, int):
        if value < 0:
            raise ConversionError(
                f"{field_name} negatif olamaz: {value}"
            )

        return value


    if not isinstance(value, str):
        raise ConversionError(
            f"{field_name} string veya integer olmalıdır"
        )

    normalized_value = value.strip()

    if not normalized_value:
        raise ConversionError(
            f"{field_name} boş olamaz"
        )

    try:
        if normalized_value.lower().startswith("0x"):
            result = int(normalized_value, 16)
        else:
            result = int(normalized_value, 10)

    except ValueError as error:
        raise ConversionError(
            f"{field_name} geçerli bir sayı değil: {value}"
        ) from error

    if result < 0:
        raise ConversionError(
            f"{field_name} negatif olamaz: {value}"
        )

    return result


def integer_value_to_string(
    value:Any,
    field_name:str,

)-> str:

    return str(hex_to_int(value,field_name))

def normalize_hex_string(
    value:Any,
    field_name:str,
    allow_none:bool=False,
) -> str | None :

    if value is None:
        if allow_none:
            return None


        raise ConversionError(
            f"{field_name} alanı eksik"
        )

    if not isinstance(value,str):
        raise ConversionError(
            f"{field_name}string olmalıdır"

        )

    normalized_value=value.strip().lower()

    if not normalized_value:
        raise ConversionError(
            f"{field_name}boş olamaz"
        )

    if not normalized_value.startswith("0x"):
        raise ConversionError(
            f"{field_name} 0x ile başlamamalıdır"
        )
    if re.fullmatch(r"0x[0-9a-f]*", normalized_value) is None:
        raise ConversionError(
            f"{field_name} geçerli hexadecimal değer değil: {value}"
    )

    return normalized_value

def timestamp_to_utc_iso(
    value:Any,
    field_name:str ="timestamp",

)->str:

    timestamp=hex_to_int(value,field_name)

    try:
        date_time=datetime.fromtimestamp(
            timestamp,
            tz=timezone.utc,
        )
    except(OverflowError,OSError,ValueError) as error:
        raise ConversionError(
            f"{field_name} geçerli bir unix timestamp değil"
        ) from error

    return (
        date_time
        .isoformat(timespec="milliseconds")
        .replace("+00:00","Z")
    )

def normalize_boolean(
    value: Any,
    field_name:str,
)-> bool:

    if isinstance(value,bool):
        return value

    if value in (0 ,"0", "0x0"):
        return False

    if value in( 1,"1","0x1"):
        return True
    raise ConversionError(
        f"{field_name} boolean değerine dönüştürülemedi"
    )
