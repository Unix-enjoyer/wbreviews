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