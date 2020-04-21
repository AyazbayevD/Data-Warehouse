from pipeline.datasources.dataSource import DataSource
from pipeline.tasks.task import Task


class GetSchema(Task):
    def __init__(self, datasources: list):
        super(GetSchema, self).__init__({})
        for dsource in datasources:
            if not isinstance(dsource, DataSource):
                raise AttributeError
        self.datasources = datasources

    def run(self, input: dict):
        for dsource in self.datasources:
            self._output[dsource.name] = {
                'dependencies': dsource.fk_handler(),
                'pkeys': dsource.pk_handler()
            }
