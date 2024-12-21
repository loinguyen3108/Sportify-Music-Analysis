from sqlalchemy import ARRAY, Column, SmallInteger, Text

from .base import Base, TimeTrackingMixin


class Album(Base, TimeTrackingMixin):
    __table_args__ = {'schema': 'public'}
    __tablename__ = 'album'

    album_id = Column(Text, primary_key=True, nullable=False)
    album_type = Column(Text, nullable=False)
    available_markets = Column(ARRAY(Text), nullable=False)
    url = Column(Text, nullable=False)
    cover_image = Column(Text)
    name = Column(Text, nullable=False)
    release_date = Column(Text, nullable=False)
    release_date_precision = Column(Text, nullable=False)
    restrictions = Column(ARRAY(Text))
    artist_ids = Column(ARRAY(Text), nullable=False)
    label = Column(Text)
    popularity = Column(SmallInteger)

    def __repr__(self):
        return f'{self.__class__.__name__}(album_id={self.album_id})'
