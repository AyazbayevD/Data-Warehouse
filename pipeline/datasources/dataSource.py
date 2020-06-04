from abc import *


class DataSource(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def pk_handler(self):
        pass

    @abstractmethod
    def fk_handler(self):
        pass

    @abstractmethod
    def extract(self):
        pass
