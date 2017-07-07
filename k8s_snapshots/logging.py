import attr


class Loggable:
    def __structlog__(self):
        if attr.has(self.__class__):
            return attr.asdict(self)

        if hasattr(self, 'to_dict') and callable(self.to_dict):
            return self.to_dict()

        return self
