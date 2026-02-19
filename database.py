from sqlalchemy import create_engine, Column, Integer, String, Text, Boolean, BigInteger, SmallInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging

from config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

engine = create_engine(
    config.SQLALCHEMY_DATABASE_URL,
    echo=False,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class Review(Base):
    """Модель таблицы отзывов Wildberries"""
    __tablename__ = 'wb_reviews'

    id = Column(Integer, primary_key=True, autoincrement=True)
    nm_id = Column(BigInteger, nullable=True)                     # теперь может быть NULL
    product_valuation = Column(Integer, nullable=True)            # теперь может быть NULL
    color = Column(String(100), nullable=True)                    # может быть NULL
    review_text = Column(Text, nullable=True)                     # теперь может быть NULL
    answer = Column(Text, nullable=True)                          # может быть NULL
    word_count = Column(Integer, nullable=False)                  # вычисляется всегда, NOT NULL
    has_answer = Column(Boolean, default=False, nullable=False)   # всегда FALSE, если answer нет
    text_length = Column(Integer, nullable=True)                  # длина текста (может быть NULL)
    file_feedb_num = Column(SmallInteger, nullable=True)          # номер файла (может быть NULL)

    def __repr__(self):
        return f"<Review(nm_id={self.nm_id}, words={self.word_count}, rating={self.product_valuation})>"


class Product(Base):
    """Модель таблицы продуктов Wildberries"""
    __tablename__ = 'wb_products'

    id = Column(Integer, primary_key=True, autoincrement=True)
    imt_id = Column(BigInteger, nullable=True)                    # теперь может быть NULL
    nm_id = Column(BigInteger, nullable=True)                     # теперь может быть NULL
    imt_name = Column(String(200), nullable=True)                 # теперь может быть NULL
    subj_name = Column(String(200), nullable=True)
    subj_root_name = Column(String(200), nullable=True)
    nm_colors_names = Column(String(200), nullable=True)
    description = Column(Text, nullable=True)
    brand_name = Column(String(100), nullable=True)
    file_basket_num = Column(SmallInteger, nullable=True)         # номер файла (может быть NULL)

    def __repr__(self):
        return f"<Product(nm_id={self.nm_id}, name={self.imt_name[:30] if self.imt_name else ''}...)>"


def create_tables():
    """Создаёт все таблицы в базе данных"""
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