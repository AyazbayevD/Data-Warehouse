from abc import ABC, abstractmethod

from pandas._libs.tslibs.nattype import NaTType

from pipeline.datasource import DataSource
import pandas as pd
import numpy as np


class Task(ABC):
    def __init__(self, requires: dict):
        self._output = {}
        for task in requires.values():
            if not isinstance(task, Task):
                raise AttributeError
        self.requires = requires

    def launch(self):
        input = {}
        for task_name, task in self.requires.items():
            if len(task.get_output()) == 0:
                task.launch()
            input[task_name] = task.get_output()
        self.run(input)

    @abstractmethod
    def run(self, input: dict):
        pass

    def get_output(self):
        return self._output


class Extract(Task):
    def __init__(self, datasources: list):
        super(Extract, self).__init__({})
        for dsource in datasources:
            if not isinstance(dsource, DataSource):
                raise AttributeError
        self.datasources = datasources

    def run(self, input):
        for dsource in self.datasources:
            dsource.connect()
            self._output[dsource.name] = dsource.extract()


class GetSchema(Task):
    def __init__(self, datasources: list):
        super(GetSchema, self).__init__({})
        for dsource in datasources:
            if not isinstance(dsource, DataSource):
                raise AttributeError
        self.datasources = datasources

    def run(self, input: dict):
        for dsource in self.datasources:
            dsource.connect()
            self._output[dsource.name] = {
                'depends': dsource.fk_handler(),
                'pkeys': dsource.pk_handler()
            }


class Transform(Task):
    # toDo deduplicating
    def __init__(self, requires: dict, dwh: DataSource):
        required_task_names = ['Extract', 'GetSchema']
        if len(requires) != len(required_task_names):
            raise AttributeError
        for task_name in required_task_names:
            if task_name not in requires or str(type(requires[task_name])) != f"<class 'pipeline.tasks.{task_name}'>":
                raise AttributeError
        super(Transform, self).__init__(requires)
        self.dwh = dwh

    def run(self, input: dict):
        # toDo
        data = input['Extract']
        for source_name in data.keys():
            source_data = data[source_name]
            self._output[source_name] = {}
            for dataset_name in source_data.keys():
                df = source_data[dataset_name]
                if type(df) != pd.DataFrame:
                    raise ValueError
                df.dropna(how='all')
                df.fillna(value='Not specified', inplace=True)
                self._output[source_name][dataset_name] = df


class Load(Task):
    def __init__(self, requires: dict, dwh: DataSource):
        required_task_names = ['Transform', 'GetSchema']
        if len(requires) != len(required_task_names):
            raise AttributeError
        for task_name in required_task_names:
            if task_name not in requires or str(type(requires[task_name])) != f"<class 'pipeline.tasks.{task_name}'>":
                raise AttributeError
        super(Load, self).__init__(requires)
        self.dwh = dwh

    def run(self, input):
        try:
            self.dwh.drop_data()
            self.dwh.load(input['Transform'])
        except KeyError as error:
            print('Incorrect input', error)

    def get_output(self):
        return self._output
