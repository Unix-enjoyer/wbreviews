# database.py
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, BigInteger, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging
import gc

from config_nd import Config

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


class Product(Base):
    """Модель таблицы продуктов Wildberries"""
    __tablename__ = 'wb_products'

    id = Column(Integer, primary_key=True, autoincrement=True)
    imt_id = Column(BigInteger, index=True)
    nm_id = Column(BigInteger, nullable=False, index=True)
    imt_name = Column(String(500), nullable=False)
    subj_name = Column(String(200), index=True)
    subj_root_name = Column(String(200), index=True)
    nm_colors_names = Column(String(500))
    vendor_code = Column(String(100), index=True)
    description = Column(Text)
    brand_name = Column(String(200), index=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<Product(nm_id={self.nm_id}, name={self.int_name[:30]}...)>"


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


def optimize_database_for_loading():
    """Оптимизирует БД для быстрой загрузки"""
    session = SessionLocal()
    try:
        # Оптимизация для bulk insert
        session.execute(text("SET synchronous_commit = OFF"))
        session.execute(text("SET maintenance_work_mem = '2GB'"))
        session.execute(text("SET work_mem = '64MB'"))
        session.execute(text("SET max_parallel_workers_per_gather = 0"))
        session.execute(text("SET checkpoint_timeout = '30min'"))
        session.commit()
        logger.info("БД оптимизирована для загрузки")
    except Exception as e:
        logger.error(f"Ошибка оптимизации БД: {e}")
        session.rollback()
    finally:
        session.close()


def restore_database_settings():
    """Восстанавливает настройки БД после загрузки"""
    session = SessionLocal()
    try:
        # Восстанавливаем стандартные настройки
        session.execute(text("SET synchronous_commit = ON"))
        session.execute(text("RESET maintenance_work_mem"))
        session.execute(text("RESET work_mem"))
        session.commit()
        logger.info("Настройки БД восстановлены")
    except Exception as e:
        logger.error(f"Ошибка восстановления настроек БД: {e}")
        session.rollback()
    finally:
        session.close()


def create_indexes_after_loading():
    """Создает индексы после загрузки данных"""
    session = SessionLocal()
    try:
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_products_nm_id ON wb_products(nm_id)",
            "CREATE INDEX IF NOT EXISTS idx_products_brand ON wb_products(brand_name)",
            "CREATE INDEX IF NOT EXISTS idx_products_category ON wb_products(subj_name)",
            "CREATE INDEX IF NOT EXISTS idx_products_vendor_code ON wb_products(vendor_code)"
        ]

        for idx in indexes:
            session.execute(text(idx))

        session.commit()
        logger.info("Индексы созданы успешно")
    except Exception as e:
        logger.error(f"Ошибка создания индексов: {e}")
        session.rollback()
    finally:
        session.close()