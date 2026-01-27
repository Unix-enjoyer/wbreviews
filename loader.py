import json
import re
import logging
from datetime import datetime
from typing import Dict, Any, Generator
from tqdm import tqdm

from database import SessionLocal, Review, create_tables
from config import Config

config = Config()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('loader.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def count_words(text: str) -> int:
    """Подсчет слов в тексте (оптимизированная версия для русских текстов)"""
    if not text or not isinstance(text, str):
        return 0

    # Удаляем лишние пробелы и переносы
    text = text.strip()
    if not text:
        return 0

    # Считаем слова (русские и английские буквы, цифры)
    words = re.findall(r'[а-яА-ЯёЁa-zA-Z0-9]+', text)
    return len(words)


def stream_json_file(file_path: str, batch_size: int = 50000) -> Generator[list, None, None]:
    """
    Потоковое чтение JSON файла.
    Поддерживает форматы:
    1. JSON Lines (каждая строка - отдельный JSON)
    2. Большой массив JSON
    """
    batch = []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Пробуем прочитать как JSON Lines
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    try:
                        data = json.loads(line)
                        batch.append(data)

                        # Возвращаем батч при достижении размера
                        if len(batch) >= batch_size:
                            yield batch
                            batch = []

                    except json.JSONDecodeError as e:
                        logger.warning(f"Ошибка JSON в строке {line_num}: {e}")
                        continue

            # Возвращаем оставшиеся записи
            if batch:
                yield batch

    except Exception as e:
        logger.error(f"Ошибка чтения файла {file_path}: {e}")
        raise


def process_review_data(item: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Обрабатывает одну запись из JSON.
    """
    try:
        # Извлекаем поля В ТОЧНОМ соответствии с JSON


        # 1. nmId (идентификатор отзыва)
        nm_id = item.get('nmId')
        if not nm_id:
            nm_id = item.get('nmld')  # на случай опечатки в данных
        if not nm_id:
            return None  # Пропускаем записи без ID

        # 2. Текст отзыва (обязательное поле для фильтрации)
        review_text = item.get('text', '')
        if not review_text:
            return None

        # 3. Подсчитываем количество слов
        word_count = count_words(review_text)

        # 4. Фильтруем: только отзывы с более чем 20 словами
        if word_count <= 20:
            return None

        # 5. productValuation (рейтинг товара)
        product_valuation = item.get('productValuation')
        if product_valuation is None:
            product_valuation = 5  # значение по умолчанию

        # 6. color (цвет товара)
        color = item.get('color', '')

        # 7. answer (ответ на отзыв)
        answer = item.get('answer', '')

        # 8. Дополнительные вычисления
        has_answer = bool(answer and answer.strip())
        text_length = len(review_text)

        # Формируем запись для БД
        return {
            'nm_id': str(nm_id),
            'product_valuation': int(product_valuation),
            'color': str(color)[:100] if color else '',  # ограничиваем длину
            'review_text': review_text,
            'answer': answer,
            'word_count': word_count,
            'has_answer': has_answer,
            'text_length': text_length
        }

    except Exception as e:
        logger.warning(f"Ошибка обработки записи: {e}, данные: {item}")
        return None


def insert_batch(session, batch_data: list, batch_num: int) -> int:
    """Вставляет батч данных в БД"""
    if not batch_data:
        return 0

    try:
        # Используем bulk insert для скорости
        session.bulk_insert_mappings(Review, batch_data)
        session.commit()
        logger.info(f"Батч {batch_num}: вставлено {len(batch_data)} записей")
        return len(batch_data)

    except Exception as e:
        session.rollback()
        logger.error(f"Ошибка при вставке батча {batch_num}: {e}")

        # Пробуем вставить по одной записи для отладки
        successful = 0
        for item in batch_data:
            try:
                review = Review(**item)
                session.add(review)
                session.commit()
                successful += 1
            except Exception:
                session.rollback()
                continue

        logger.info(f"Вставлено по одной: {successful}/{len(batch_data)}")
        return successful


def main():
    """Основная функция загрузки данных"""

    logger.info(f"Начало загрузки данных из {config.JSON_FILE}")
    logger.info(f"Фильтр: отзывы с более чем 20 словами")

    # Создаем таблицы если их нет
    create_tables()

    # Создаем сессию БД
    db = SessionLocal()

    try:
        # Статистика
        stats = {
            'total_read': 0,
            'total_processed': 0,
            'total_inserted': 0,
            'start_time': datetime.now()
        }

        batch_counter = 0
        insert_batch_data = []

        # Создаем прогресс-бар
        with tqdm(desc="Обработка отзывов", unit=" записей", unit_scale=True) as pbar:

            # Читаем и обрабатываем файл
            for batch in stream_json_file(config.JSON_FILE, config.BATCH_SIZE):
                batch_counter += 1

                for item in batch:
                    stats['total_read'] += 1

                    # Обрабатываем запись
                    processed_item = process_review_data(item)
                    if processed_item:
                        stats['total_processed'] += 1
                        insert_batch_data.append(processed_item)

                    # Обновляем прогресс-бар каждые 10000 записей
                    if stats['total_read'] % 10000 == 0:
                        pbar.update(10000)
                        pbar.set_postfix({
                            'прочитано': f"{stats['total_read']:,}",
                            'обработано': f"{stats['total_processed']:,}",
                            'вставлено': f"{stats['total_inserted']:,}"
                        })

                    # Вставляем батч при достижении лимита
                    if len(insert_batch_data) >= config.INSERT_BATCH_SIZE:
                        inserted = insert_batch(db, insert_batch_data, batch_counter)
                        stats['total_inserted'] += inserted
                        insert_batch_data = []

                # Промежуточная статистика
                if batch_counter % 10 == 0:
                    logger.info(
                        f"Промежуточная статистика: прочитано {stats['total_read']:,}, обработано {stats['total_processed']:,}")

        # Вставляем оставшиеся данные
        if insert_batch_data:
            inserted = insert_batch(db, insert_batch_data, batch_counter + 1)
            stats['total_inserted'] += inserted

        # Завершаем статистику
        stats['end_time'] = datetime.now()
        stats['duration'] = stats['end_time'] - stats['start_time']

        # Выводим итоги
        logger.info("=" * 50)
        logger.info("ИТОГИ ЗАГРУЗКИ:")
        logger.info(f"Всего прочитано записей: {stats['total_read']:,}")
        logger.info(f"Отфильтровано (>20 слов): {stats['total_processed']:,}")
        logger.info(f"Успешно вставлено в БД: {stats['total_inserted']:,}")
        logger.info(f"Процент отфильтрованных: {stats['total_processed'] / stats['total_read'] * 100:.1f}%")
        logger.info(f"Время выполнения: {stats['duration']}")
        logger.info(f"Скорость: {stats['total_read'] / stats['duration'].total_seconds():.1f} записей/сек")
        logger.info("=" * 50)

        # Дополнительная проверка
        from sqlalchemy import func
        count = db.query(func.count(Review.id)).scalar()
        logger.info(f"Всего записей в таблице wb_reviews: {count:,}")

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        db.rollback()
        raise

    finally:
        db.close()
        logger.info("Загрузка завершена!")


def test_small_file():
    """Тестирует загрузку на первых 1000 записях"""
    import tempfile

    logger.info("Тестирование на первых 1000 записях...")

    # Создаем временный файл с первыми 1000 записями
    temp_file = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.json', delete=False)

    try:
        with open(config.JSON_FILE, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= 1000:
                    break
                temp_file.write(line)

        temp_file.close()

        # Меняем конфигурацию на тестовый файл
        original_file = config.JSON_FILE
        config.JSON_FILE = temp_file.name

        # Запускаем загрузку
        main()

        # Восстанавливаем оригинальный файл
        config.JSON_FILE = original_file

    finally:
        import os
        os.unlink(temp_file.name)


if __name__ == "__main__":
    # Запускаем основную загрузку
    main()

    # Или для тестирования:
    # test_small_file()