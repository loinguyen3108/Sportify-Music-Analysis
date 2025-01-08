from sqlalchemy import Column, DateTime, Text

from .base import Base, TimeTrackingMixin


class PlaylistTrack(Base, TimeTrackingMixin):
    __table_args__ = {'schema': 'public'}
    __tablename__ = 'playlist_track'

    playlist_id = Column(Text, primary_key=True, nullable=False)
    track_id = Column(Text, primary_key=True, nullable=False)
    added_at = Column(DateTime, nullable=False)
    added_by = Column(Text)
    url = Column(Text)
