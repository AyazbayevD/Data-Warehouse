from pipeline.datasources.dataSource import DataSource
from pipeline.tasks.task import Task


class Load(Task):
    def __init__(self, requires: dict, dwh: DataSource):
        required_task_names = ['Transform']
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
            self.dwh.load(input['Transform']['data'], input['Transform']['schema'])
        except KeyError as error:
            print('Incorrect input', error)

    def get_output(self):
        return self._output
