class NormalizationError(Exception):
    """temel hatalar için"""

class ValidationError(NormalizationError):
    """zorunlu alan veya geçersiz veri tipi için"""

class ConversionError(NormalizationError):
    """hexadecimal sayı veya timestamp dönüşümü başarısız ise """

class UnsupportedEventTypeError(NormalizationError):
    """desteklenmeyen event_type geldiğinde"""


class ParsingError(NormalizationError):
    """json veya utf 8 çözülemediğinde kafka mesjaı """
