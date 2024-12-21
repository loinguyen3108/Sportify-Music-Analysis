from datetime import date
from hashlib import md5
from typing import Any

from sqlalchemy import Column, Date, Text, func

from .base import Base


class CrawlerTracking(Base):
    __table_args__ = {'schema': 'public'}
    __tablename__ = 'crawler_tracking'

    function_name = Column(Text, nullable=False, primary_key=True)
    main_arg_name = Column(Text, nullable=False, primary_key=True)
    main_arg_value = Column(Text, nullable=False, primary_key=True)
    tracked_at = Column(Date, nullable=False,
                        primary_key=True, server_default=func.now())

    def __repr__(self):
        return f'{self.__class__.__name__}(function_name={self.function_name}, ' \
            f'main_arg_name={self.main_arg_name}, ' \
            f'main_arg_value={self.main_arg_value}, ' \
            f'tracked_at={self.tracked_at})'
