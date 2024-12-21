from itertools import islice
from src.configs.logger import get_logger


class BaseService:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

    from itertools import islice

    def chunks(self, arr_range, arr_size=50):
        arr_range = iter(arr_range)
        return iter(lambda: tuple(islice(arr_range, arr_size)), ())
