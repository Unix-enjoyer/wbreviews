import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Настройки подключения к PostgreSQL в Docker
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5433')
    DB_NAME = os.getenv('DB_NAME', 'wb_reviews')
    DB_USER = os.getenv('DB_USER', 'YOUR_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'YOUR_PASSWORD')

    # URL для подключения с pg8000
    SQLALCHEMY_DATABASE_URL = f"postgresql+pg8000://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # Настройки загрузки
    BATCH_SIZE = 50000
    INSERT_BATCH_SIZE = 2000
    JSON_FILE = "feedbacks-00.json"


config = Config()


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
    DB_USER = os.getenv('DB_USER', 'admin')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'MY_PASSWORD')

    # SQLAlchemy connection URL
    SQLALCHEMY_DATABASE_URL = f"postgresql+pg8000://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # Все файлы для обработки (кроме уже обработанного 00)
    JSON_FILES = [
        f"feedbacks-{i:02d}.json" for i in range(1, 18)  # с 01 по 17
    ]

    # Настройки загрузки
    BATCH_SIZE = 50000
    INSERT_BATCH_SIZE = 2000
    MAX_WORKERS = 8  # Используем все 8 ядер

    # Корень проекта - текущая директория
    PROJECT_ROOT = Path.cwd()

    @property
    def json_files_absolute(self):
        """Абсолютные пути к JSON файлам"""
        return [self.PROJECT_ROOT / file_name for file_name in self.JSON_FILES]


config = Config()