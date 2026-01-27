from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging

from config import Config

config = Config()

# Настройка логгирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Создаем движок базы данных с pg8000
engine = create_engine(
    config.SQLALCHEMY_DATABASE_URL,
    echo=False,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    # Убираем connect_args, так как pg8000 не принимает ssl параметр
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class Review(Base):
    """Модель таблицы отзывов Wildberries"""
    __tablename__ = 'wb_reviews'

    id = Column(Integer, primary_key=True, autoincrement=True)
    nm_id = Column(String(50), nullable=False, index=True)
    product_valuation = Column(Integer, nullable=False)
    color = Column(String(100))
    review_text = Column(Text, nullable=False)
    answer = Column(Text)
    word_count = Column(Integer, nullable=False)
    has_answer = Column(Boolean, default=False)
    text_length = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<Review(nm_id={self.nm_id}, words={self.word_count}, rating={self.product_valuation})>"


def create_tables():
    """Создает все таблицы в базе данных"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Таблицы созданы успешно!")
        return True
    except Exception as e:
        logger.error(f"Ошибка создания таблиц: {e}")
        return False


def get_session():
    """Возвращает сессию для работы с БД"""
    return SessionLocal()