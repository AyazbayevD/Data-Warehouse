from pipeline.datasources.dataSource import DataSource
from pipeline.tasks.task import Task


class Extract(Task):
    def __init__(self, datasources: list):
        super(Extract, self).__init__({})
        for dsource in datasources:
            if not isinstance(dsource, DataSource):
                raise AttributeError
        self.datasources = datasources

    def run(self, input):
        for dsource in self.datasources:
            self._output[dsource.name] = dsource.extract()
