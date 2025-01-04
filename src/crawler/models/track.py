from sqlalchemy import ARRAY, BigInteger, Boolean, Column, Integer, \
    SmallInteger, Text

from .base import Base, TimeTrackingMixin


class Track(Base, TimeTrackingMixin):
    __table_args__ = {'schema': 'public'}
    __tablename__ = 'track'

    track_id = Column(Text, primary_key=True, nullable=False)
    album_id = Column(Text, nullable=False)
    available_markets = Column(ARRAY(Text))
    disc_number = Column(Integer)
    duration_ms = Column(Integer, nullable=False)
    explicit = Column(Boolean, nullable=False)
    url = Column(Text, nullable=False)
    name = Column(Text, nullable=False)
    popularity = Column(SmallInteger)
    restrictions = Column(ARRAY(Text))
    track_number = Column(Integer, nullable=False)
    plays_count = Column(BigInteger)

    def __repr__(self):
        return f'{self.__class__.__name__}(track_id={self.track_id})'
