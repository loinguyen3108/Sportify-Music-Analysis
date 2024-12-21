from sqlalchemy import Column, Integer, Text, Boolean

from .base import Base, TimeTrackingMixin


class Playlist(Base, TimeTrackingMixin):
    __table_args__ = {'schema': 'public'}
    __tablename__ = 'playlist'

    playlist_id = Column(Text, primary_key=True, nullable=False)
    collaborative = Column(Boolean, nullable=False)
    description = Column(Text)
    url = Column(Text, nullable=False)
    followers_count = Column(Integer)
    cover_image = Column(Text)
    name = Column(Text, nullable=False)
    user_id = Column(Text, nullable=False)
    public = Column(Boolean, nullable=False)
    snapshot_id = Column(Text, nullable=False)

    def __repr__(self):
        return f'{self.__class__.__name__}(playlist_id={self.playlist_id})'
