# fast_multi_loader.py
import json
import re
import logging
from datetime import datetime
from typing import Dict, Any, List
from tqdm import tqdm
import concurrent.futures
import multiprocessing
from pathlib import Path
import psutil

# Импорты из корня проекта
from database import SessionLocal, Review, create_tables
from config import config
from sqlalchemy import text

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fast_loader.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def count_words(text: str) -> int:
    """Подсчет слов в тексте (оптимизированная версия)"""
    if not text or not isinstance(text, str):
        return 0

    text = text.strip()
    if not text:
        return 0

    # Быстрый подсчет через split (быстрее regex)
    return len(text.split())


def process_chunk(chunk_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Обрабатывает чанк данных (для многопроцессорной обработки)"""
    processed = []

    for item in chunk_data:
        try:
            nm_id = item.get('nmId') or item.get('nmld') or ''
            review_text = item.get('text', '')

            if not nm_id or not review_text:
                continue

            word_count = count_words(review_text)
            if word_count <= 20:
                continue

            product_valuation = item.get('productValuation', 5)
            color = item.get('color', '')
            answer = item.get('answer', '')

            processed.append({
                'nm_id': str(nm_id),
                'product_valuation': int(product_valuation) if product_valuation else 5,
                'color': str(color)[:100] if color else '',
                'review_text': review_text,
                'answer': answer,
                'word_count': word_count,
                'has_answer': bool(answer and answer.strip()),
                'text_length': len(review_text)
            })
        except Exception:
            continue

    return processed


def read_file_in_chunks(file_path: str, chunk_size: int = 50000):
    """Читает файл чанками (генератор)"""
    chunk = []

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    data = json.loads(line)
                    chunk.append(data)

                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
                except json.JSONDecodeError:
                    continue

        if chunk:
            yield chunk


def fast_insert_batch(batch_data: List[Dict[str, Any]]):
    """Быстрая вставка батча через bulk insert"""
    if not batch_data:
        return 0

    session = SessionLocal()

    try:
        # Используем bulk insert для скорости
        session.bulk_insert_mappings(Review, batch_data)
        session.commit()
        return len(batch_data)
    except Exception as e:
        session.rollback()
        logger.error(f"Ошибка вставки батча: {e}")
        return 0
    finally:
        session.close()


def process_single_file_parallel(file_path: Path, num_workers: int = 8):
    """Обрабатывает один файл с использованием многопоточности"""
    logger.info(f"  Начало обработки {file_path.name}")

    if not file_path.exists():
        logger.error(f"  Файл не найден: {file_path}")
        return 0, 0, 0

    file_stats = {
        'total_read': 0,
        'total_processed': 0,
        'total_inserted': 0,
        'start_time': datetime.now()
    }

    # Определяем оптимальный размер чанка для этого файла
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    chunk_size = max(10000, min(100000, int(50000 * (file_size_mb / 1000))))

    logger.info(f"  Размер файла: {file_size_mb:.1f} MB, размер чанка: {chunk_size}")

    # Используем ProcessPoolExecutor для CPU-bound задач
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Читаем и обрабатываем файл
        future_to_chunk = {}

        # Читаем файл и отправляем чанки на обработку
        for chunk_num, chunk in enumerate(read_file_in_chunks(str(file_path), chunk_size), 1):
            file_stats['total_read'] += len(chunk)

            # Отправляем чанк на обработку в отдельном процессе
            future = executor.submit(process_chunk, chunk)
            future_to_chunk[future] = chunk_num

        # Собираем результаты и вставляем в БД
        batch_counter = 0
        insert_batch_data = []

        with tqdm(total=len(future_to_chunk), desc=f"Обработка {file_path.name}", unit=" чанков") as pbar:
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk_num = future_to_chunk[future]

                try:
                    processed_chunk = future.result()

                    if processed_chunk:
                        file_stats['total_processed'] += len(processed_chunk)
                        insert_batch_data.extend(processed_chunk)

                    # Вставляем батч при достижении лимита
                    if len(insert_batch_data) >= config.INSERT_BATCH_SIZE * 2:  # Увеличиваем для параллельной обработки
                        inserted = fast_insert_batch(insert_batch_data)
                        file_stats['total_inserted'] += inserted
                        insert_batch_data = []
                        batch_counter += 1

                        if batch_counter % 10 == 0:
                            logger.debug(f"Файл {file_path.name}: вставлено {file_stats['total_inserted']:,}")

                except Exception as e:
                    logger.error(f"Ошибка обработки чанка {chunk_num}: {e}")

                pbar.update(1)
                pbar.set_postfix({
                    'прочитано': f"{file_stats['total_read']:,}",
                    'обработано': f"{file_stats['total_processed']:,}",
                    'вставлено': f"{file_stats['total_inserted']:,}"
                })

        # Вставляем оставшиеся данные
        if insert_batch_data:
            inserted = fast_insert_batch(insert_batch_data)
            file_stats['total_inserted'] += inserted

    # Завершаем статистику
    file_stats['end_time'] = datetime.now()
    file_stats['duration'] = file_stats['end_time'] - file_stats['start_time']

    logger.info(f"  Файл {file_path.name} обработан:")
    logger.info(f"   Прочитано: {file_stats['total_read']:,}")
    logger.info(f"   Обработано (>20 слов): {file_stats['total_processed']:,}")
    logger.info(f"   Вставлено в БД: {file_stats['total_inserted']:,}")
    logger.info(f"   Время: {file_stats['duration']}")

    if file_stats['total_read'] > 0:
        logger.info(
            f"   Процент отфильтрованных: {file_stats['total_processed'] / file_stats['total_read'] * 100:.1f}%")

    return file_stats['total_read'], file_stats['total_processed'], file_stats['total_inserted']


def optimize_database_for_loading():
    """Оптимизирует БД для быстрой загрузки"""
    logger.info("   Оптимизация БД для загрузки...")

    session = SessionLocal()

    try:
        # Отключаем индексы для ускорения вставки
        session.execute(text("DROP INDEX IF EXISTS idx_nm_id"))
        session.execute(text("DROP INDEX IF EXISTS idx_product_valuation"))
        session.execute(text("DROP INDEX IF EXISTS idx_word_count"))
        session.execute(text("DROP INDEX IF EXISTS idx_color"))
        session.commit()

        logger.info("  Индексы отключены для ускорения загрузки")

        # Увеличиваем параметры для быстрой загрузки
        session.execute(text("SET synchronous_commit = OFF"))
        session.execute(text("SET maintenance_work_mem = '1GB'"))
        session.execute(text("SET work_mem = '64MB'"))
        session.commit()

    except Exception as e:
        logger.error(f"Ошибка оптимизации БД: {e}")
        session.rollback()
    finally:
        session.close()


def restore_database_indexes():
    """Восстанавливает индексы после загрузки"""
    logger.info("  Восстановление индексов...")

    session = SessionLocal()

    try:
        # Создаем индексы заново
        indexes = [
            "CREATE INDEX idx_nm_id ON wb_reviews(nm_id)",
            "CREATE INDEX idx_product_valuation ON wb_reviews(product_valuation)",
            "CREATE INDEX idx_word_count ON wb_reviews(word_count)",
            "CREATE INDEX idx_color ON wb_reviews(color)",
            "CREATE INDEX idx_has_answer ON wb_reviews(has_answer)"
        ]

        for index_sql in indexes:
            try:
                session.execute(text(index_sql))
                session.commit()
                logger.info(f"Создан индекс: {index_sql}")
            except Exception as e:
                logger.error(f"Ошибка создания индекса: {e}")
                session.rollback()

    except Exception as e:
        logger.error(f"Ошибка восстановления индексов: {e}")
    finally:
        session.close()


def check_system_resources():
    """Проверяет системные ресурсы"""
    logger.info("  Проверка системных ресурсов...")

    cpu_count = multiprocessing.cpu_count()
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()

    logger.info(f"   Ядер CPU: {cpu_count}")
    logger.info(f"   Загрузка CPU: {cpu_percent}%")
    logger.info(
        f"   Оперативная память: {memory.used / (1024 ** 3):.1f} GB / {memory.total / (1024 ** 3):.1f} GB ({memory.percent}%)")

    # Рекомендация по количеству процессов
    recommended_workers = max(4, cpu_count - 2)  # Оставляем 2 ядра для системы и БД
    logger.info(f"   Рекомендуется использовать: {recommended_workers} процессов")

    return recommended_workers


def main():
    """Основная функция загрузки всех файлов"""

    logger.info("=" * 60)
    logger.info("  МНОГОПОТОЧНАЯ ЗАГРУЗКА 17 ФАЙЛОВ")
    logger.info("=" * 60)

    # Проверяем ресурсы системы
    recommended_workers = check_system_resources()
    num_workers = min(config.MAX_WORKERS, recommended_workers)
    logger.info(f"Используем {num_workers} процессов для обработки")

    # Проверяем наличие файлов
    files_to_process = []
    for file_path in config.json_files_absolute:
        if file_path.exists():
            files_to_process.append(file_path)
            file_size = file_path.stat().st_size / (1024 * 1024)
            logger.info(f"  {file_path.name}: {file_size:.1f} MB")
        else:
            logger.warning(f"  Файл не найден: {file_path.name}")

    if not files_to_process:
        logger.error("  Не найдено ни одного файла для обработки")
        return

    logger.info(f"\n  Всего файлов для обработки: {len(files_to_process)}")

    # Создаем таблицы если их нет
    create_tables()

    # Оптимизируем БД для загрузки
    optimize_database_for_loading()

    # Общая статистика
    global_stats = {
        'total_files': len(files_to_process),
        'files_processed': 0,
        'total_read': 0,
        'total_processed': 0,
        'total_inserted': 0,
        'start_time': datetime.now()
    }

    # Обрабатываем каждый файл ПОСЛЕДОВАТЕЛЬНО, но с многопоточностью внутри файла
    for file_path in files_to_process:
        logger.info(f"\n{'=' * 50}")
        logger.info(f"  Обработка файла: {file_path.name}")
        logger.info(f"{'=' * 50}")

        try:
            read, processed, inserted = process_single_file_parallel(file_path, num_workers)

            global_stats['files_processed'] += 1
            global_stats['total_read'] += read
            global_stats['total_processed'] += processed
            global_stats['total_inserted'] += inserted

            # Проверяем системные ресурсы между файлами
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()

            if memory.percent > 85:
                logger.warning("  Высокое использование памяти! Делаем паузу...")
                import time
                time.sleep(10)

        except Exception as e:
            logger.error(f"  Критическая ошибка при обработке {file_path.name}: {e}")
            continue

    # Восстанавливаем индексы
    restore_database_indexes()

    # Итоговая статистика
    global_stats['end_time'] = datetime.now()
    global_stats['duration'] = global_stats['end_time'] - global_stats['start_time']

    logger.info("\n" + "=" * 60)
    logger.info("  ИТОГИ ЗАГРУЗКИ ВСЕХ ФАЙЛОВ:")
    logger.info("=" * 60)
    logger.info(f"Обработано файлов: {global_stats['files_processed']}/{global_stats['total_files']}")
    logger.info(f"Всего прочитано записей: {global_stats['total_read']:,}")
    logger.info(f"Отфильтровано (>20 слов): {global_stats['total_processed']:,}")
    logger.info(f"Успешно вставлено в БД: {global_stats['total_inserted']:,}")

    if global_stats['total_read'] > 0:
        percentage = global_stats['total_processed'] / global_stats['total_read'] * 100
        logger.info(f"Процент отфильтрованных: {percentage:.1f}%")

    logger.info(f"Общее время выполнения: {global_stats['duration']}")

    if global_stats['duration'].total_seconds() > 0:
        speed = global_stats['total_read'] / global_stats['duration'].total_seconds()
        logger.info(f"Средняя скорость: {speed:.1f} записей/сек")

    # Проверяем общее количество в БД
    session = SessionLocal()
    try:
        from sqlalchemy import func
        total_in_db = session.query(func.count(Review.id)).scalar()
        logger.info(f"Всего записей в таблице wb_reviews: {total_in_db:,}")
    finally:
        session.close()

    logger.info("=" * 60)
    logger.info("  Все файлы успешно обработаны!")


if __name__ == "__main__":
    main()