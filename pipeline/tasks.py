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


class GetSchema(Task):
    def __init__(self, datasources: list):
        super(GetSchema, self).__init__({})
        for dsource in datasources:
            if not isinstance(dsource, DataSource):
                raise AttributeError
        self.datasources = datasources

    def run(self, input: dict):
        self.__output = {}
        for dsource in self.datasources:
            dsource.connect()
            self.__output[dsource.name] = {
                'depends': dsource.fk_handler(),
                'pkeys': dsource.pk_handler()
            }

    def get_output(self):
        return self.__output


class Transform(Task):
    # toDo id mapping
    # toDo deduplicating
    def __init__(self, requires: dict, dwh: DataSource):
        required_task_names = ['Extract', 'GetSchema']
        if len(requires) != len(required_task_names):
            raise AttributeError
        for task_name in required_task_names:
            if task_name not in requires or str(type(requires[task_name])) != task_name:
                raise AttributeError
        super(Transform, self).__init__(requires)
        self.dwh = dwh

    def run(self, input: dict):
        # Dropping primary_key column and deduplication
        self.__output = {}
        dwh_data = self.dwh.extract()
        for dsource_name in input[Extract].keys():
            self.__output[dsource_name] = {}
            for dataset_name in input[Extract][dsource_name].keys():
                df = input[Extract][dsource_name][dataset_name]
                pkey_columns = input[GetSchema]['pkeys'][dataset_name]
                df.drop(columns=pkey_columns)
                if dsource_name in dwh_data and dataset_name in dwh_data[dsource_name]:
                    merged_df = pd.concat([dwh_data[dsource_name][dataset_name], df])
                    merged_df.dropna(how='all')
                    merged_df.drop_duplicates()
                    self.__output[dsource_name][dataset_name] = merged_df
                else:
                    self.__output[dsource_name][dataset_name] = df

    def get_output(self):
        return self.__output


class Load(Task):
    def __init__(self, requires: dict, dwh: DataSource):
        requred_task_names = ['Transform']
        if len(requires) != len(requred_task_names):
            raise AttributeError
        for task_name in required_task_names:
            if task_name not in requires or str(type(requires[task_name])) != task_name:
                raise AttributeError
        # toDo
        super(Load, self).__init__(requires)
        self.dwh = dwh

    def run(self, input):
        try:
            self.dwh.drop_data()
            for dsource_data in input[Transform].values():
                self.dwh.load(dsource_data)
        except KeyError as error:
            print('Incorrect input', error)

    def get_output(self):
        return self.__output
