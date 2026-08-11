class ReorgError(Exception):
    """re org sırasında oluşan temel hatalar için """


class EventValidationError(ReorgError):
    """normalized event beklemem formatta olmadıüınnda oluşur """


class MissingBlockStateError(ReorgError):
    """transactions veya log için beklenen blok bilgisi bbulunmadıysa oluşur"""


class DeepReorgError(ReorgError):
    """ re org  rediste tutulan blok penceresinden daha derinse oluşur"""


class ChainGapError(ReorgError):
    """Bloklar arasında eksik blok bulunduğunda oluşur."""
