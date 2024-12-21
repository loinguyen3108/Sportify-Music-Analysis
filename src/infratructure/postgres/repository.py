from sqlalchemy import create_engine, pool, select
from sqlalchemy.orm import scoped_session, sessionmaker

from src.configs.logger import get_logger
from src.configs.crawler import PG_SPOTIFY_URI


class Repository:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.session = self._create_session()

    def _create_session(self):
        engine = create_engine(
            PG_SPOTIFY_URI,
            echo=False,
            echo_pool=False,
            poolclass=pool.NullPool,
            client_encoding='utf-8'
        )
        session_factory = sessionmaker(
            autoflush=False, bind=engine, expire_on_commit=False
        )
        return scoped_session(session_factory)

    def build_query(self, model, *args):
        if not model:
            raise ValueError('model is required')

        stmt = select(model)
        if args:
            stmt = stmt.filter(*args)

        return self.session.execute(stmt)

    def bulk_insert(self, entities):
        try:
            ret = self.session.bulk_save_objects(entities)
            self.session.commit()
            return ret
        except Exception as e:
            self.session.rollback()
            raise e

    def find_one(self, model_instance, *args):
        if not model_instance:
            raise ValueError('model_instance is required')

        return self.build_query(model_instance, *args).scalar_one_or_none()

    def insert(self, model_instance):
        try:
            self.session.add(model_instance)
            self.session.commit()
            return model_instance
        except Exception:
            self.session.rollback()
            raise

    def upsert(self, model_instance):
        try:
            ret = self.session.merge(model_instance)
            self.session.commit()
            return ret
        except Exception:
            self.session.rollback()
            raise
