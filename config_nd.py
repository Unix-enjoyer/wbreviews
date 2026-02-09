# config_nd.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Загружаем .env из корня проекта
load_dotenv()


class Config:
    # Database Configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5433')
    DB_NAME = os.getenv('DB_NAME', 'wb_reviews')
    DB_USER = os.getenv('DB_USER', 'username')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')

    # SQLAlchemy connection URL
    SQLALCHEMY_DATABASE_URL = f"postgresql+pg8000://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    # basket-01.json...
    # basket-02.json
    # basket-03.json
    # basket-04.json
    # basket-05.json
    # basket-06.json
    # basket-07.json
    # basket-08.json
    # basket-09.json
    # basket-10.json
    # basket-11.json
    # basket-12.json+
    # JSON файлы для обработки
    JSON_FILES = ["basket-01.json"]
        #f"basket-{i:02d}.json" for i in range(1, 2)  # basket-01.json, basket-02.json, ...
    #] proceed ffile 1

    # Настройки загрузки
    CHUNK_SIZE_LINES = 10000  # Размер чанка для чтения файлов
    INSERT_BATCH_SIZE = 2000  # Размер батча для вставки в БД

    # Настройки памяти
    MEMORY_WARNING_THRESHOLD = 70  # Порог предупреждения (процент)
    MEMORY_CRITICAL_THRESHOLD = 85  # Критический порог (процент)

    # Паузы
    PAUSE_BETWEEN_FILES = 10  # Пауза между файлами в секундах

    # Бэкап
    CREATE_BACKUP = True  # Создавать резервную копию перед загрузкой

    # Корень проекта
    PROJECT_ROOT = Path.cwd()

    @property
    def json_files_absolute(self):
        """Абсолютные пути к JSON файлам"""
        return [self.PROJECT_ROOT / file_name for file_name in self.JSON_FILES]


config_nd = Config()