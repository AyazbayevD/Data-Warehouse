from pipeline.datasources.dataSource import DataSource
from pipeline.datawarehouse.dataWarehouse import DataWarehouse
from pipeline.tasks.task import Task


class Load(Task):
    def __init__(self, requires: dict, dwh: DataWarehouse):
        required_task_names = ['Transform']
        if len(requires) != len(required_task_names):
            raise AttributeError
        for task_name in required_task_names:
            if task_name not in requires:
                raise AttributeError
        super(Load, self).__init__(requires)
        self.dwh = dwh

    def run(self, input):
        self.dwh.load(input['Transform']['data'], input['Transform']['schema'])

    def get_output(self):
        return self._output
