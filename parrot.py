class ParrotDB:
    def __init__(self):
        self.data = {}

    def set(self, key: str, value: str) -> None:
        try:
            self.data[key] = value
        except KeyError:
            return None

    def get(self, key: str) -> str:
        return self.data.get(key)

    def unset(self, key: str) -> None:
        try:
            del self.data[key]
        except KeyError:
            return None

    def begin(self):
        pass


