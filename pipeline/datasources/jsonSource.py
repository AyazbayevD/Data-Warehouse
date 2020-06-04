from pipeline.datasources.dataSource import DataSource
import pandas as pd


class JSONSource(DataSource):
    def __init__(self, name: str, filename: str):
        self.filename = filename
        super(JSONSource, self).__init__(name)

    def extract(self):
        all_data = pd.read_csv(self.filename)
        return {f'{self.name}': all_data}

    def pk_handler(self):
        return {f'{self.name}': []}

    def fk_handler(self):
        return {f'{self.name}': []}
