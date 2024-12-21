from sqlalchemy import ARRAY, Column, Integer, SmallInteger, Text

from .base import Base, TimeTrackingMixin


class Artist(Base, TimeTrackingMixin):
    __table_args__ = {'schema': 'public'}
    __tablename__ = 'artist'

    artist_id = Column(Text, primary_key=True, nullable=False)
    url = Column(Text, nullable=False)
    followers_count = Column(Integer)
    genres = Column(ARRAY(Text))
    image = Column(Text)
    name = Column(Text, nullable=False)
    popularity = Column(SmallInteger)

    def __repr__(self):
        return f'{self.__class__.__name__}(artist_id={self.artist_id})'
