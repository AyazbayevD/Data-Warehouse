from abc import ABC, abstractmethod
from pipeline.datasource import DataSource
import pandas as pd


class Task(ABC):
    def __init__(self, requires: dict):
        self.__output = None
        for task in requires.values():
            if not isinstance(task, Task):
                raise AttributeError
        self.requires = requires

    def launch(self):
        input = {}
        for task_name, task in self.requires.items():
            task.launch()
            input[task_name] = task.get_output()
        self.run(input)

    @abstractmethod
    def run(self, input: dict):
        pass

    def get_output(self):
        return self.__output


class Extract(Task):
    def __init__(self, datasources: list):
        super(Extract, self).__init__({})
        for dsource in datasources:
            if not isinstance(dsource, DataSource):
                raise AttributeError
        self.datasources = datasources

    def run(self, input):
        self.__output = {}
        for dsource in self.datasources:
            dsource.connect()
            self.__output[dsource.name] = dsource.extract()

    def get_output(self):
        return self.__output


class Transform(Task):
    # toDo id mapping
    # toDo deduplicating
    def __init__(self, requires: dict, dwh_data: dict):
        requred_task_names = ['Extract']
        if len(requires) != len(requred_task_names):
            raise AttributeError
        for task_name in requred_task_names:
            if task_name not in requires:
                raise AttributeError
        super(Transform, self).__init__(requires)
        self.dwh_data = dwh_data

    def run(self, input: dict):
        for dsource in input.keys():
            for dataset in dsource.keys():
                if dsource in self.dwh_data and dataset in self.dwh_data[dsource]:
                    df = pd.concat([self.dwh_data[dsource][dataset], input[dsource][dataset]])
                    df.dropna(how='all')
                    df.drop_duplicates()

    pass


class Load(Task):
    def __init__(self, requires: dict, dwh: DataSource):
        requred_task_names = ['Extract']
        if len(requires) != len(requred_task_names):
            raise AttributeError
        for task_name in requred_task_names:
            if task_name not in requires:
                raise AttributeError
        # toDo
        super(Load, self).__init__(requires)
        self.dwh = dwh

    def run(self, input):
        try:
            for dsources in input['Extract'].values():
                self.dwh.load(dsources)
        except KeyError as error:
            print('Incorrect input', error)

    def get_output(self):
        return self.__output
