import pandas as pd

from pipeline.tasks.task import Task


class Transform(Task):
    # toDo deduplicating
    def __init__(self, requires: dict):
        required_task_names = ['Extract', 'GetSchema']
        if len(requires) != len(required_task_names):
            raise AttributeError
        for task_name in required_task_names:
            if task_name not in requires or str(type(requires[task_name])) != f"<class 'pipeline.tasks.{task_name}'>":
                raise AttributeError
        super(Transform, self).__init__(requires)

    def run(self, input: dict):
        data = input['Extract']
        schema = input['GetSchema']
        for source_name in data.keys():
            source_data = data[source_name]
            source_schema = schema[source_name]
            for dataset_name in source_data.keys():
                df = source_data[dataset_name]
                dataset_schema = source_schema['dependencies'][dataset_name]
                if type(df) != pd.DataFrame:
                    raise ValueError
                df['source_name'] = source_name
                dataset_schema.append({'ftable': 'sources_names', 'fcolumn': 'source_name'})
                df.dropna(how='all')
                df.fillna(value='Not specified', inplace=True)
        source_names = [{'source_name': source_name} for source_name in data.keys()]
        output_data = self._output['data'] = {}
        output_schema = self._output['schema'] = {}
        output_data['sources'] = {'sources_names': pd.DataFrame(source_names)}
        output_schema['sources'] = {'dependencies': {'sources_names': []}, 'pkeys': {'sources_name': []}}
        for source_name in data.keys():
            output_data[source_name] = data[source_name]
            output_schema[source_name] = schema[source_name]
