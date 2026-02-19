import json
import re
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from tqdm import tqdm
import concurrent.futures
import multiprocessing
from pathlib import Path
import psutil

from database import SessionLocal, Review, create_tables
from config import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fast_loader.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def extract_feedb_num(filename: str) -> Optional[int]:
    """Извлекает номер файла из имени вида feedbacks-XX.json.
    Возвращает None, если номер не найден."""
    match = re.search(r'feedbacks?-(\d+)', filename)
    if match:
        return int(match.group(1))
    return None


def count_words(text: Optional[str]) -> int:
    """Подсчёт слов в тексте (если текст None, возвращает 0)"""
    if not text:
        return 0
    text = str(text).strip()
    return len(text.split()) if text else 0


def safe_int(value: Any) -> Optional[int]:
    """Безопасное преобразование в целое число. Если преобразование невозможно, возвращает None."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def process_chunk(chunk_data: List[Dict[str, Any]], feedb_num: Optional[int]) -> List[Dict[str, Any]]:
    """
    Обрабатывает чанк данных. Если поле отсутствует в JSON или его значение None,
    в результирующий словарь добавляется запись с ключом и значением None.
    Это приведёт к вставке NULL в соответствующую колонку БД.
    """
    processed = []

    for item in chunk_data:
        try:
            # nm_id
            nm_id_raw = item.get('nmId') or item.get('nmld')
            nm_id_val = safe_int(nm_id_raw)

            # review_text
            review_text_raw = item.get('text')
            review_text = str(review_text_raw).strip() if review_text_raw is not None else None
            word_count = count_words(review_text)
            text_length = len(review_text) if review_text else None

            # Пропускаем записи, где текст короче 20 слов (word_count может быть 0, если текста нет)
            if word_count <= 20:
                continue

            # product_valuation
            product_valuation_raw = item.get('productValuation')
            product_valuation_val = safe_int(product_valuation_raw)

            # color
            color_val = item.get('color')
            if color_val is not None:
                color_val = str(color_val)[:100] if color_val else ''
            # если ключа нет или значение None, color_val останется None

            # answer и has_answer
            answer_val = item.get('answer')
            if answer_val is not None:
                answer_val = str(answer_val) if answer_val else ''
                has_answer_val = bool(answer_val.strip())
            else:
                # ключ отсутствует или значение None
                answer_val = None
                has_answer_val = False

            # Формируем запись, включая все поля (даже если None)
            record = {
                'nm_id': nm_id_val,
                'product_valuation': product_valuation_val,
                'color': color_val,
                'review_text': review_text,
                'answer': answer_val,
                'word_count': word_count,
                'has_answer': has_answer_val,
                'text_length': text_length,
                'file_feedb_num': feedb_num,  # может быть None
            }

            processed.append(record)

        except Exception as e:
            # Логируем неожиданные ошибки, но продолжаем обработку
            logger.debug(f"Ошибка обработки записи: {e}")
            continue

    return processed


def read_file_in_chunks(file_path: str, chunk_size: int = 50000):
    """Читает файл чанками (каждая строка — отдельный JSON-объект)"""
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
    """Быстрая вставка батча через bulk_insert_mappings"""
    if not batch_data:
        return 0

    session = SessionLocal()
    try:
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
    """Обрабатывает один файл с использованием пула процессов"""
    logger.info(f"  Начало обработки {file_path.name}")

    if not file_path.exists():
        logger.error(f"  Файл не найден: {file_path}")
        return 0, 0, 0

    feedb_num = extract_feedb_num(file_path.name)
    if feedb_num is None:
        logger.warning(f"  Не удалось определить номер файла для {file_path.name}, будет вставлен NULL")
    else:
        logger.info(f"  Номер файла: {feedb_num}")

    file_stats = {
        'total_read': 0,
        'total_processed': 0,
        'total_inserted': 0,
        'start_time': datetime.now()
    }

    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    chunk_size = max(10000, min(100000, int(50000 * (file_size_mb / 1000))))
    logger.info(f"  Размер файла: {file_size_mb:.1f} MB, размер чанка: {chunk_size}")

    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        future_to_chunk = {}

        for chunk_num, chunk in enumerate(read_file_in_chunks(str(file_path), chunk_size), 1):
            file_stats['total_read'] += len(chunk)
            future = executor.submit(process_chunk, chunk, feedb_num)
            future_to_chunk[future] = chunk_num

        batch_counter = 0
        insert_batch_data = []

        with tqdm(total=len(future_to_chunk), desc=f"Обработка {file_path.name}", unit=" чанков") as pbar:
            for future in concurrent.futures.as_completed(future_to_chunk):
                try:
                    processed_chunk = future.result()
                    if processed_chunk:
                        file_stats['total_processed'] += len(processed_chunk)
                        insert_batch_data.extend(processed_chunk)

                    if len(insert_batch_data) >= config.INSERT_BATCH_SIZE * 2:
                        inserted = fast_insert_batch(insert_batch_data)
                        file_stats['total_inserted'] += inserted
                        insert_batch_data = []
                        batch_counter += 1

                        if batch_counter % 10 == 0:
                            logger.debug(f"Файл {file_path.name}: вставлено {file_stats['total_inserted']:,}")

                except Exception as e:
                    logger.error(f"Ошибка обработки чанка: {e}")

                pbar.update(1)
                pbar.set_postfix({
                    'прочитано': f"{file_stats['total_read']:,}",
                    'обработано': f"{file_stats['total_processed']:,}",
                    'вставлено': f"{file_stats['total_inserted']:,}"
                })

        if insert_batch_data:
            inserted = fast_insert_batch(insert_batch_data)
            file_stats['total_inserted'] += inserted

    file_stats['end_time'] = datetime.now()
    file_stats['duration'] = file_stats['end_time'] - file_stats['start_time']

    logger.info(f"  Файл {file_path.name} обработан:")
    logger.info(f"   Прочитано: {file_stats['total_read']:,}")
    logger.info(f"   Обработано (>20 слов): {file_stats['total_processed']:,}")
    logger.info(f"   Вставлено в БД: {file_stats['total_inserted']:,}")
    logger.info(f"   Время: {file_stats['duration']}")

    if file_stats['total_read'] > 0:
        logger.info(f"   Процент отфильтрованных: {file_stats['total_processed'] / file_stats['total_read'] * 100:.1f}%")

    return file_stats['total_read'], file_stats['total_processed'], file_stats['total_inserted']


def check_system_resources():
    logger.info("  Проверка системных ресурсов...")
    cpu_count = multiprocessing.cpu_count()
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()

    logger.info(f"   Ядер CPU: {cpu_count}")
    logger.info(f"   Загрузка CPU: {cpu_percent}%")
    logger.info(f"   Оперативная память: {memory.used / (1024 ** 3):.1f} GB / {memory.total / (1024 ** 3):.1f} GB ({memory.percent}%)")

    recommended_workers = max(4, cpu_count - 2)
    logger.info(f"   Рекомендуется использовать: {recommended_workers} процессов")
    return recommended_workers


def main():
    logger.info("=" * 60)
    logger.info("  МНОГОПОТОЧНАЯ ЗАГРУЗКА ФАЙЛОВ (С ПОДДЕРЖКОЙ NULL)")
    logger.info("=" * 60)

    recommended_workers = check_system_resources()
    num_workers = min(config.MAX_WORKERS, recommended_workers)
    logger.info(f"Используем {num_workers} процессов для обработки")

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

    # Создаём таблицы
    if not create_tables():
        logger.error("Не удалось создать таблицы. Выход.")
        return

    global_stats = {
        'total_files': len(files_to_process),
        'files_processed': 0,
        'total_read': 0,
        'total_processed': 0,
        'total_inserted': 0,
        'start_time': datetime.now()
    }

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

            memory = psutil.virtual_memory()
            if memory.percent > 85:
                logger.warning("  Высокое использование памяти! Пауза 10 сек...")
                import time
                time.sleep(10)

        except Exception as e:
            logger.error(f"  Критическая ошибка при обработке {file_path.name}: {e}")
            continue

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