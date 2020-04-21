from abc import ABC, abstractmethod

from pipeline.datasources.dataSource import DataSource
import pandas as pd


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