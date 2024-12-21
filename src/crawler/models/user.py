from sqlalchemy import Column, Text

from .base import Base, TimeTrackingMixin


class User(Base, TimeTrackingMixin):
    __table_args__ = {'schema': 'public'}
    __tablename__ = 'user'

    user_id = Column(Text, primary_key=True, nullable=False)
    url = Column(Text, nullable=False)
    image = Column(Text)
    name = Column(Text)

    def __repr__(self):
        return f'{self.__class__.__name__}(user_id={self.user_id})'
