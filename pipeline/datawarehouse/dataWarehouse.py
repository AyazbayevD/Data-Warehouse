from abc import ABC, abstractmethod


class DataWarehouse(ABC):
    def __init__(self, conn):
        self.conn = conn

    @abstractmethod
    def load(self, data, schema):
        pass

    @abstractmethod
    def get_table(self, table_name: str):
        pass

    @abstractmethod
    def drop_table(self, table_name: str):
        pass

    @abstractmethod
    def add_person(self, info: dict):
        pass

    @abstractmethod
    def find_people(self, info: dict):
        pass

    @abstractmethod
    def add_mention(self, info: dict):
        pass
