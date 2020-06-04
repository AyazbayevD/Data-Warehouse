from pipeline.datasources.dataSource import DataSource
import xlrd
import pandas as pd


class ExcelSource(DataSource):
    def __init__(self, name: str, filename):
        self.filename = filename
        super(ExcelSource, self).__init__(name)

    def connect(self):
        xls = xlrd.open_workbook(self.filename, on_demand=True)
        return xls

    def extract(self):
        all_data = {}
        xls = self.connect()
        sheet_names = xls.sheet_names()
        for sheet_name in sheet_names:
            all_data[sheet_name] = pd.read_excel(self.name, sheet_name=sheet_name)
        return all_data

    def pk_handler(self):
        xls = self.connect()
        res = dict(zip(xls.sheet_names(), [[] for _ in xls.sheet_names()]))
        return res

    def fk_handler(self):
        xls = self.connect()
        res = dict(zip(xls.sheet_names(), [[] for _ in xls.sheet_names()]))
        return res
