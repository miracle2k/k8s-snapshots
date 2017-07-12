import traceback
from typing import Dict, List, Iterable


class StructuredError(Exception):
    def __init__(self, message=None, **data):
        self.message = message
        self.data = data

    def __str__(self):
        return f'{self.__class__.__qualname__}: {self.message} {self.data!r}'

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.message} ' \
               f'data={self.data!r}>'

    def __structlog__(self):
        return self._serializable_exc()

    def _exc_chain(self) -> Iterable[Exception]:
        chain = []  # reverse chronological order
        exc = self

        while exc is not None:
            chain.append(exc)
            exc = exc.__cause__

        return reversed(chain)

    def _serializable_exc(self) -> List[Dict]:
        def serialize_exc(exc: Exception) -> Dict:
            if isinstance(exc, StructuredError):
                return exc.to_dict()
            else:
                exc_type = exc.__class__
                exc_tb = exc.__traceback__
                return {
                    'type': exc_type.__qualname__,
                    'message': str(exc),
                    'readable': traceback.format_exception(
                        exc_type,
                        exc,
                        exc_tb,
                        chain=False
                    )
                }

        return [serialize_exc(exc) for exc in self._exc_chain()]

    def to_dict(self) -> Dict:
        return {
            'type': self.__class__.__qualname__,
            'message': self.message,
            'data': self.data,
            'readable': traceback.format_exception(
                self.__class__,
                self,
                self.__traceback__,
                chain=False
            )
        }


class ConfigurationError(StructuredError):
    """ Raised for invalid configuration """
    pass


class DeltasParseError(StructuredError):
    """
    Raised for invalid delta strings

    -   In configuration.
    -   In PV or PVC annotations.
    """
    pass


class VolumeNotFound(StructuredError):
    pass


class UnsupportedVolume(StructuredError):
    """ Raised for PersistentVolumes we can't snapshot """
    pass


class SnapshotCreateError(StructuredError):
    pass


class AnnotationError(StructuredError):
    pass


class AnnotationNotFound(AnnotationError):
    pass
