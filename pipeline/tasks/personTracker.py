import pandas as pd

from pipeline.datawarehouse.dataWarehouse import DataWarehouse
from pipeline.tasks.task import Task
import re


class PersonTracker(Task):
    def __init__(self, requires: dict, dwh: DataWarehouse):
        required_task_names = ['Extract']
        if len(requires) != len(required_task_names):
            raise AttributeError
        for task_name in required_task_names:
            if task_name not in requires:
                raise AttributeError
        self.dwh = dwh
        super(PersonTracker, self).__init__(requires)

    def is_phone_number(self, string: str):
        phone = re.match(r'(\+[0-9]+\s*)?(\([0-9]+\))?[\s0-9\-]+[0-9]+?[\d\- ]{7,10}$', string)
        if phone:
            return True
        return False

    def is_email(self, string: str):
        email = re.match(r'\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b.', string)
        if email:
            return True
        return False

    def is_first_name(self, string: str):
        fname_var = ['имя', 'name', 'first_name', 'first name']
        for var in fname_var:
            name = re.search(var, string)
            if name:
                return True
        return False

    def is_last_name(self, string: str):
        lname_var = ['фамилия', 'last name', 'surname', 'last_name']
        for var in lname_var:
            name = re.search(var, string)
            if name:
                return True
        return False

    def about_person(self, df: pd.DataFrame):
        """
        Function that examine does DataFrame consist information about person.
        To be more specific, function simply checks if there any columns in DataFrame
        that point to the person. For instance, columns like first_name and last_name hint
        that some person is mentioned

        :param df: DataFrame that will be examined by function
        :return: True: dataset use information about some people
        """
        consist = {
            'first_name': False,
            'last_name': False,
            'email': False,
            'phone_number': False
        }
        for col in df.columns:
            col_name = col.lower()
            if self.is_first_name(col_name):
                consist['first_name'] = True
            elif self.is_last_name(col_name):
                consist['last_name'] = True
            if type(df[col][0]) == str:
                if self.is_email(df[col][0]):
                    consist['email'] = True
                if self.is_phone_number(df[col][0]):
                    consist['phone_number'] = True
        if not (consist['first_name'] | consist['last_name'] | consist['email'] | consist['phone_number']):
            return False
        return True

    def update_mentions(self, info: dict, dataset_name: str):
        people = self.dwh.find_people(info)
        n, m = people.shape
        for i in range(n):
            cnt = 0
            for key in info.keys():
                if people[key][i] == info[key]:
                    cnt += 1
            confidence = cnt / len(info.keys()) / n
            self.dwh.add_mention({'person': people['_id'][i], 'mention': dataset_name, 'confidence': confidence})

    def run(self, input: dict):
        data = input['Extract']
        for source_name in data.keys():
            source_data = data[source_name]
            col_map = {
                'first_name': None,
                'last_name': None,
                'email': None,
                'phone_number': None
            }
            for dataset_name, dataset in source_data.items():
                status = self.about_person(dataset)
                if not status:
                    continue
                for col in dataset.columns:
                    col_name = col.lower()
                    if self.is_first_name(col_name):
                        col_map['first_name'] = col
                    elif self.is_last_name(col_name):
                        col_map['last_name'] = col
                    if type(dataset[col][0]) == str:
                        if self.is_email(dataset[col][0]):
                            col_map['email'] = True
                        if self.is_phone_number(dataset[col][0]):
                            col_map['phone_number'] = True
                n, m = dataset.shape
                for i in range(n):
                    person = {}
                    for k, v in col_map.items():
                        if v:
                            person[k] = dataset[v][i]
                        else:
                            person[k] = None
                    self.update_mentions(person, dataset_name)
