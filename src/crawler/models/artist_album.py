from sqlalchemy import Column, Text

from .base import Base, TimeTrackingMixin


class ArtistAlbum(Base, TimeTrackingMixin):
    __table_args__ = {'schema': 'public'}
    __tablename__ = 'artist_album'

    artist_id = Column(Text, primary_key=True, nullable=False)
    album_id = Column(Text, primary_key=True, nullable=False)

    def __repr__(self):
        return f'ArtistAlbum(artist_id={self.artist_id}, album_id={self.album_id})'
