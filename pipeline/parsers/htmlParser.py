from abc import *


class HtmlParser(ABC):
    def __init__(self, url: str):
        self.url = url

    @abstractmethod
    def parse(self):
        pass


class ParseError(Exception):
    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        if self.message:
            return f'ParseError: {self.message}'
        return 'ParseError has been raised'
