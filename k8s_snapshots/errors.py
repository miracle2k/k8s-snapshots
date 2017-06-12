import traceback
from typing import Dict


class StructuredError(Exception):
    def __init__(self, message=None, **data):
        self.message = message
        self.data = data

    def __str__(self):
        return f'{self.__class__.__qualname__}: {self.message} {self.data!r}'

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.message} ' \
               f'data={self.data!r}>'

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


class UnsupportedVolume(StructuredError):
    """
    Raised for PersistentVolumes we can't snapshot.
    """
    pass


class SnapshotCreateError(StructuredError):
    pass


class AnnotationError(StructuredError):
    pass


class AnnotationNotFound(AnnotationError):
    pass
