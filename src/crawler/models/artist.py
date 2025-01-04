from sqlalchemy import ARRAY, BigInteger, Column, Integer, SmallInteger, Text

from .base import Base, TimeTrackingMixin


class Artist(Base, TimeTrackingMixin):
    __table_args__ = {'schema': 'public'}
    __tablename__ = 'artist'

    artist_id = Column(Text, primary_key=True, nullable=False)
    url = Column(Text, nullable=False)
    followers_count = Column(Integer)
    genres = Column(ARRAY(Text))
    image = Column(Text)
    name = Column(Text)
    popularity = Column(SmallInteger)
    monthly_listeners = Column(BigInteger)

    def __repr__(self):
        return f'{self.__class__.__name__}(artist_id={self.artist_id})'
