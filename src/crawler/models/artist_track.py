from sqlalchemy import Column, Text

from .base import Base, TimeTrackingMixin


class ArtistTrack(Base, TimeTrackingMixin):
    __table_args__ = {'schema': 'public'}
    __tablename__ = 'artist_track'

    artist_id = Column(Text, primary_key=True, nullable=False)
    track_id = Column(Text, primary_key=True, nullable=False)

    def __repr__(self):
        return f'ArtistTrack(artist_id={self.artist_id}, track_id={self.track_id})'
