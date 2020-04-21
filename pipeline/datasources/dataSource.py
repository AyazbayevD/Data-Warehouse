from abc import *


class DataSource(ABC):
    def __init__(self, name: str):
        # toDo change DataSource to uniquely indetify specific datasource
        self.name = name

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def pk_handler(self):
        pass

    @abstractmethod
    def fk_handler(self):
        pass

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def load(self, data: dict, schema: dict):
        pass

    @abstractmethod
    def drop_data(self):
        pass
