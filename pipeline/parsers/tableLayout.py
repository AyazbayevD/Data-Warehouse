from pipeline.parsers.htmlParser import HtmlParser, ParseError
import requests
from bs4 import BeautifulSoup
import pandas as pd


class TableLayoutParser(HtmlParser):
    def __init__(self, url: str):
        super(TableLayoutParser, self).__init__(url)

    def fix_string(self, string: str, sep=' '):
        res = ''
        last_word = ''
        for c in string:
            if c.isalpha() or c.isdigit():
                last_word += c
            elif len(last_word):
                res += last_word
                res += sep
                last_word = ''
        if len(last_word):
            res += last_word
        return res

    def fix_row(self, row: list):
        res = []
        for value in row:
            value = self.fix_string(value)
            if not len(value):
                continue
            res.append(value)
        return res

    def parse(self):
        page = requests.get(self.url)
        soup = BeautifulSoup(page.content, 'html.parser')
        try:
            table = soup.find('table')
            table_name = self.fix_string(table.find('a').find('h2').get_text(), sep='_')
            data_table = table.find('table')
            columns = [th.get_text() for th in table.find("tr").find_all("th")]
            columns = self.fix_row(columns)
            rows = data_table.find_all('tr')
            datasets = []
            for row in rows[1:]:
                row_data = self.fix_row([td.get_text() for td in row.find_all('td')])
                dataset = dict(zip(columns, row_data))
                datasets.append(dataset)
            df = pd.DataFrame(datasets)
            df.to_csv(f'temp_files/{table_name}.csv')
        except Exception:
            raise ParseError("incorrect layout")
