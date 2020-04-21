from pipeline.datasources.dataSource import DataSource
import pandas as pd
import os


class CSVSource(DataSource):
    def __init__(self, filename: str):
        super(CSVSource, self).__init__(filename)

    def connect(self):
        return

    def extract(self):
        all_data = pd.read_csv(self.name)
        return {'CSV': all_data}

    def pk_handler(self):
        return {'CSV': []}

    def fk_handler(self):
        return {'CSV': []}

    def load(self, data: dict, schema: dict):
        # toDo
        pass

    def drop_data(self):
        os.remove(self.name)
