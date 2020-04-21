from pipeline.datasources.dataSource import DataSource
import pandas as pd
import os


class JSONSource(DataSource):
    def __init__(self, filename: str):
        super(JSONSource, self).__init__(filename)

    def connect(self):
        return

    def extract(self):
        all_data = pd.read_json(self.name)
        return all_data

    def pk_handler(self):
        #toDo
        return {}

    def fk_handler(self):
        #toDo
        return {}

    def load(self, data: dict, schema: dict):
        # toDo
        pass

    def drop_data(self):
        os.remove(self.name)
