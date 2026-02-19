# optimized_loader_nd.py
import json
import logging
import os
import gc
import time
import sys
import signal
import psutil
import atexit
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

# Импорты из корня проекта
from database_nd import SessionLocal, Product, create_tables, optimize_database_for_loading, restore_database_settings, \
    create_indexes_after_loading
from config_nd import config_nd
from checkpoint_manager import CheckpointManager

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('products_loader.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Глобальные переменные
SHOULD_STOP = False
LAST_STATUS_TIME = time.time()
checkpoint_manager = None


def emergency_save_checkpoint(reason: str = "emergency"):
    """Экстренное сохранение контрольной точки"""
    global checkpoint_manager
    if checkpoint_manager:
        try:
            # Используем последние известные значения из менеджера
            checkpoint_manager.save_current_progress(reason)
        except Exception as e:
            print(f"  Критическая ошибка при сохранении контрольной точки: {e}")


def signal_handler(signum, frame):
    """Обработчик сигналов для graceful shutdown"""
    global SHOULD_STOP
    signal_name = {signal.SIGINT: "SIGINT (Ctrl+C)",
                   signal.SIGTERM: "SIGTERM"}.get(signum, str(signum))

    print(f"\n  Получен сигнал {signal_name}. Сохраняю контрольную точку...")
    logger.warning(f"Получен сигнал {signal_name}. Сохранение контрольной точки...")

    SHOULD_STOP = True

    # Немедленное сохранение контрольной точки
    emergency_save_checkpoint("user_interrupt")

    # Даем время для сохранения
    time.sleep(0.5)

    # Выходим
    print("  Завершение программы...")
    sys.exit(0)


def atexit_handler():
    """Обработчик завершения программы"""
    if SHOULD_STOP:
        emergency_save_checkpoint("program_exit")


# Регистрируем обработчики
atexit.register(atexit_handler)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class MemoryMonitor:
    """Мониторинг памяти"""

    @staticmethod
    def get_memory_usage() -> float:
        """Возвращает использование памяти в процентах"""
        try:
            return psutil.virtual_memory().percent
        except:
            return 0.0

    @staticmethod
    def check_memory_limit(limit_percent: float = 88.0) -> bool:
        """Проверяет, превышен ли лимит памяти"""
        usage = MemoryMonitor.get_memory_usage()
        if usage > limit_percent:
            print(f"  Память: {usage:.1f}% (превышен лимит {limit_percent}%)")
        return usage > limit_percent

    @staticmethod
    def free_memory():
        """Освобождает память"""
        gc.collect()


def parse_product_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Парсит один элемент продукта из JSON"""
    try:
        # Извлекаем поля согласно описанию датасета
        imt_id = item.get('imt_id')
        nm_id = item.get('nm_id')

        # Проверяем альтернативные названия полей
        if imt_id is None:
            imt_id = item.get('imtId') or item.get('imtID')

        if nm_id is None:
            nm_id = item.get('nmId') or item.get('nmID')

        # nm_id обязателен
        if nm_id is None:
            return None

        # Преобразование типов
        try:
            nm_id_int = int(nm_id)
        except (ValueError, TypeError):
            # Пробуем очистить строку
            import re
            cleaned = re.sub(r'[^\d]', '', str(nm_id))
            nm_id_int = int(cleaned) if cleaned else 0

        if nm_id_int == 0:
            return None

        # imt_id может быть None
        imt_id_int = None
        if imt_id is not None:
            try:
                imt_id_int = int(imt_id)
            except:
                import re
                cleaned = re.sub(r'[^\d]', '', str(imt_id))
                imt_id_int = int(cleaned) if cleaned else None

        # Извлекаем остальные поля
        imt_name = item.get('imt_name') or item.get('imtName') or ''
        subj_name = item.get('subj_name') or item.get('subjName') or ''
        subj_root_name = item.get('subj_root_name') or item.get('subjRootName') or ''
        nm_colors_names = item.get('nm_colors_names') or item.get('nmColorsNames') or ''
        vendor_code = item.get('vendor_code') or item.get('vendorCode') or ''
        description = item.get('description') or ''
        brand_name = item.get('brand_name') or item.get('brandName') or 'Неизвестный бренд'

        return {
            'imt_id': imt_id_int,
            'nm_id': nm_id_int,
            'imt_name': str(imt_name)[:500],
            'subj_name': str(subj_name)[:200],
            'subj_root_name': str(subj_root_name)[:200],
            'nm_colors_names': str(nm_colors_names)[:500],
            'vendor_code': str(vendor_code)[:100],
            'description': str(description),
            'brand_name': str(brand_name)[:200]
        }

    except Exception as e:
        logger.debug(f"Ошибка парсинга продукта: {e}")
        return None


def process_chunk(chunk_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Обрабатывает чанк"""
    processed = []
    for item in chunk_data:
        parsed = parse_product_item(item)
        if parsed:
            processed.append(parsed)
    return processed


def fast_insert_batch_products(batch_data: List[Dict[str, Any]]) -> int:
    """Быстрая вставка батча продуктов"""
    if not batch_data:
        return 0

    session = SessionLocal()
    try:
        session.bulk_insert_mappings(Product, batch_data)
        session.commit()
        return len(batch_data)
    except Exception as e:
        session.rollback()
        logger.error(f"Ошибка вставки батча: {e}")

        # Fallback: вставка по одному
        inserted = 0
        for item in batch_data:
            try:
                product = Product(**item)
                session.add(product)
                inserted += 1
                if inserted % 100 == 0:
                    session.commit()
            except Exception:
                continue

        try:
            session.commit()
        except:
            session.rollback()

        return inserted
    finally:
        session.close()


def print_status(current_file: str, lines_read: int, inserted_count: int,
                 memory_usage: float, start_time: datetime):
    """Печатает статус загрузки"""
    elapsed_time = datetime.now() - start_time
    elapsed_hours = elapsed_time.total_seconds() / 3600

    if lines_read > 0:
        speed_per_hour = lines_read / elapsed_hours if elapsed_hours > 0 else 0

        status_msg = (f"[СТАТУС] Файл: {Path(current_file).name} | "
                      f"Прочитано: {lines_read:,} записей | "
                      f"Вставлено: {inserted_count:,} | "
                      f"Память: {memory_usage:.1f}% | "
                      f"Время: {str(elapsed_time)[:7]} | "
                      f"Скорость: {speed_per_hour:,.0f} записей/час")

        print(status_msg)
        logger.info(status_msg)


def skip_to_line(file_path: Path, target_line: int) -> Tuple[int, bool]:
    """
    Пропускает указанное количество строк в файле.
    Возвращает количество фактически пропущенных строк и флаг успеха.
    """
    if target_line <= 0:
        return 0, True

    try:
        lines_skipped = 0
        with open(file_path, 'r', encoding='utf-8') as f:
            while lines_skipped < target_line:
                line = f.readline()
                if not line:  # Достигли конца файла
                    break
                lines_skipped += 1

        return lines_skipped, True

    except UnicodeDecodeError as e:
        logger.error(f"  Ошибка декодирования при пропуске строк: {e}")
        return 0, False
    except Exception as e:
        logger.error(f"  Ошибка при пропуске строк: {e}")
        return 0, False


def process_file_with_checkpoint(file_path: Path, checkpoint_manager: CheckpointManager,
                                 start_line: int = 0, start_inserted: int = 0) -> Tuple[int, int, int]:
    """Обрабатывает один файл с поддержкой контрольных точек (только по номерам строк)"""
    global SHOULD_STOP, LAST_STATUS_TIME

    logger.info(f"Начало обработки: {file_path.name}")
    logger.info(f"Стартовая позиция: строка {start_line:,}")

    if not file_path.exists():
        logger.error(f"Файл не найден: {file_path}")
        return 0, 0, 0

    # Статистика
    stats = {
        'total_read': 0,
        'total_processed': 0,
        'total_inserted': start_inserted,
        'start_time': datetime.now()
    }

    # Инициализируем мониторинг памяти
    memory_monitor = MemoryMonitor()

    # Обновляем прогресс в менеджере
    checkpoint_manager.update_progress(
        str(file_path),
        start_line,
        stats['total_inserted']
    )

    # Чтение файла с пропуском строк
    chunk = []
    chunk_size = 10000
    insert_batch = []
    last_checkpoint_line = start_line

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Пропускаем уже обработанные строки
            if start_line > 0:
                print(f"  Пропускаю {start_line:,} строк...")
                lines_skipped = 0
                while lines_skipped < start_line:
                    line = f.readline()
                    if not line:  # Достигли конца файла
                        print(f"  Достигнут конец файла при пропуске строк")
                        break
                    lines_skipped += 1
                    if lines_skipped % 100000 == 0:
                        print(f"  Пропущено {lines_skipped:,} строк...")

                print(f"  Пропущено {lines_skipped:,} строк")
                stats['total_read'] = start_line

            # Читаем оставшиеся строки
            for line in f:
                # Обновляем прогресс в менеджере каждые 1000 строк
                if stats['total_read'] % 1000 == 0:
                    checkpoint_manager.update_progress(
                        str(file_path),
                        stats['total_read'],
                        stats['total_inserted']
                    )

                # Проверяем память каждые 1000 строк
                if stats['total_read'] % 1000 == 0:
                    memory_usage = memory_monitor.get_memory_usage()

                    # Проверяем лимит памяти (88%)
                    if memory_monitor.check_memory_limit(88.0):
                        logger.error(f"Память превысила 88%: {memory_usage:.1f}%")
                        print(f"  Память превысила 88%! Сохранение контрольной точки...")

                        # Сохраняем контрольную точку
                        checkpoint_manager.save_checkpoint(
                            str(file_path),
                            stats['total_read'],
                            stats['total_inserted'],
                            "memory_limit_exceeded"
                        )

                        SHOULD_STOP = True
                        logger.error("Завершение из-за превышения памяти")
                        print("  Завершение программы из-за превышения памяти")
                        return stats['total_read'], stats['total_processed'], stats['total_inserted']

                # Проверяем флаг остановки
                if SHOULD_STOP:
                    logger.warning("Обработка прервана")
                    print("  Обработка прервана по запросу")
                    return stats['total_read'], stats['total_processed'], stats['total_inserted']

                stats['total_read'] += 1

                # Выводим статус каждые 50,000 записей
                if stats['total_read'] % 50000 == 0:
                    memory_usage = memory_monitor.get_memory_usage()
                    print_status(str(file_path), stats['total_read'], stats['total_inserted'],
                                 memory_usage, stats['start_time'])
                    LAST_STATUS_TIME = time.time()

                    # Авто-сохранение контрольной точки каждые 100,000 строк
                    if stats['total_read'] - last_checkpoint_line >= 100000:
                        checkpoint_manager.save_checkpoint(
                            str(file_path),
                            stats['total_read'],
                            stats['total_inserted'],
                            "auto_save"
                        )
                        last_checkpoint_line = stats['total_read']

                try:
                    data = json.loads(line.strip())
                    chunk.append(data)

                    # Обрабатываем чанк
                    if len(chunk) >= chunk_size:
                        processed = process_chunk(chunk)
                        stats['total_processed'] += len(processed)
                        insert_batch.extend(processed)
                        chunk = []

                        # Вставляем батч
                        if len(insert_batch) >= 2000:
                            inserted = fast_insert_batch_products(insert_batch)
                            stats['total_inserted'] += inserted
                            insert_batch = []

                            # Освобождаем память после вставки
                            memory_monitor.free_memory()

                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    logger.debug(f"Ошибка обработки строки: {e}")

        # Обработка последнего чанка
        if chunk:
            processed = process_chunk(chunk)
            stats['total_processed'] += len(processed)
            insert_batch.extend(processed)

        # Вставка оставшихся данных
        if insert_batch:
            inserted = fast_insert_batch_products(insert_batch)
            stats['total_inserted'] += inserted

    except UnicodeDecodeError as e:
        logger.error(f"  Ошибка декодирования UTF-8 в файле {file_path.name}: {e}")
        print(f"  Ошибка декодирования UTF-8!")
        print(f"   Файл: {file_path.name}")
        print(f"   Текущая строка: {stats['total_read']:,}")
        print(f"   Ошибка: {e}")

        # Сохраняем контрольную точку перед завершением
        checkpoint_manager.save_checkpoint(
            str(file_path),
            stats['total_read'],
            stats['total_inserted'],
            "unicode_error"
        )

        raise

    except Exception as e:
        logger.error(f"Ошибка чтения файла {file_path.name}: {e}")
        import traceback
        logger.error(traceback.format_exc())

        # Сохраняем контрольную точку при ошибке
        if not SHOULD_STOP:
            logger.warning("Сохранение контрольной точки из-за ошибки...")
            checkpoint_manager.save_checkpoint(
                str(file_path),
                stats['total_read'],
                stats['total_inserted'],
                "error"
            )
        raise

    # Финальный статус после обработки файла
    memory_usage = memory_monitor.get_memory_usage()
    print_status(str(file_path), stats['total_read'], stats['total_inserted'],
                 memory_usage, stats['start_time'])

    # Статистика файла
    stats['end_time'] = datetime.now()
    stats['duration'] = stats['end_time'] - stats['start_time']

    logger.info(f"Файл {file_path.name} обработан:")
    logger.info(f"  Прочитано: {stats['total_read']:,}")
    logger.info(f"  Обработано: {stats['total_processed']:,}")
    logger.info(f"  Вставлено: {stats['total_inserted']:,}")
    logger.info(f"  Время: {stats['duration']}")

    if stats['total_read'] > 0:
        efficiency = (stats['total_processed'] / stats['total_read']) * 100
        speed = stats['total_read'] / stats['duration'].total_seconds() if stats['duration'].total_seconds() > 0 else 0
        logger.info(f"  Эффективность: {efficiency:.1f}%")
        logger.info(f"  Скорость: {speed:.1f} строк/сек")

    return stats['total_read'], stats['total_processed'], stats['total_inserted']


def main():
    """Основная функция загрузки с поддержкой контрольных точек"""
    global SHOULD_STOP, checkpoint_manager

    # Заголовок программы
    print("=" * 80)
    print("ЗАГРУЗЧИК ПРОДУКТОВ WILDBERRIES (линейные контрольные точки)")
    print("Контрольные точки: Ctrl+C для сохранения и выхода")
    print("Мониторинг памяти: остановка при 88% использования")
    print("=" * 80)

    # Инициализируем менеджер контрольных точек
    checkpoint_manager = CheckpointManager()

    # Пытаемся загрузить контрольную точку
    checkpoint = checkpoint_manager.load_checkpoint()
    resume_from_checkpoint = checkpoint is not None

    # Определяем, с какого файла и строки начинать
    start_file_path = None
    start_line = 0
    start_inserted = 0

    if resume_from_checkpoint:
        start_file_path = Path(checkpoint['file_path'])
        start_line = checkpoint.get('line_number', 0)
        start_inserted = checkpoint.get('inserted_count', 0)
        logger.info(f"Продолжение с контрольной точки: {start_inserted:,} уже вставлено")
        print(f" Продолжение с контрольной точки:")
        print(f"    Файл: {start_file_path.name}")
        print(f"    Строка: {start_line:,}")
        print(f"    Вставлено: {start_inserted:,}")
    else:
        print(" Новая загрузка (контрольная точка не найдена)")

    # Проверяем доступные файлы
    files_to_process = []
    for file_path in config_nd.json_files_absolute:
        if file_path.exists():
            files_to_process.append(file_path)
            file_size = file_path.stat().st_size / (1024 * 1024)
            lines_estimate = file_size * 5000  # Примерная оценка: 5000 строк на 1 MB
            logger.info(f"  {file_path.name}: {file_size:.1f} MB (~{int(lines_estimate):,} строк)")
            print(f"   {file_path.name}: {file_size:.1f} MB (~{int(lines_estimate):,} строк)")
        else:
            logger.warning(f"✗ Файл не найден: {file_path.name}")
            print(f"✗ Файл не найден: {file_path.name}")

    if not files_to_process:
        logger.error("Не найдено файлов для обработки")
        print(" Не найдено файлов для обработки")
        return

    logger.info(f"Всего файлов: {len(files_to_process)}")
    print(f" Всего файлов: {len(files_to_process)}")

    # Создаем таблицы
    print("  Создание таблиц...")
    create_tables()

    # Оптимизируем БД
    print(" Оптимизация БД для загрузки...")
    optimize_database_for_loading()

    # Глобальная статистика
    global_stats = {
        'total_files': len(files_to_process),
        'files_processed': 0,
        'total_read': 0,
        'total_processed': 0,
        'total_inserted': start_inserted,
        'start_time': datetime.now()
    }

    # Определяем, с какого файла начинать обработку
    start_index = 0
    if resume_from_checkpoint and start_file_path:
        # Находим индекс файла в списке
        for i, file_path in enumerate(files_to_process):
            if file_path == start_file_path:
                start_index = i
                break

        if start_index < len(files_to_process):
            logger.info(f"Начинаем с файла {start_index + 1}: {start_file_path.name}")
            print(f" Начинаем с файла {start_index + 1}: {start_file_path.name}")
        else:
            print(" Файл из контрольной точки не найден в списке, начинаем с первого")
            start_index = 0
            start_line = 0  # Сбрасываем позицию

    # Обрабатываем файлы
    for file_index in range(start_index, len(files_to_process)):
        file_path = files_to_process[file_index]

        print(f"\n{'=' * 60}")
        print(f"  Файл {file_index + 1}/{len(files_to_process)}: {file_path.name}")
        print(f"{'=' * 60}")

        try:
            # Определяем стартовую позицию для этого файла
            current_start_line = start_line if (resume_from_checkpoint and file_index == start_index) else 0
            current_start_inserted = start_inserted if (resume_from_checkpoint and file_index == start_index) else 0

            read, processed, inserted = process_file_with_checkpoint(
                file_path,
                checkpoint_manager,
                start_line=current_start_line,
                start_inserted=current_start_inserted
            )

            global_stats['files_processed'] += 1
            global_stats['total_read'] += read
            global_stats['total_processed'] += processed
            global_stats['total_inserted'] += inserted

            # После успешной обработки файла сбрасываем контрольную точку для этого файла
            if not SHOULD_STOP:
                # Если это последний файл, очищаем контрольную точку полностью
                if file_index == len(files_to_process) - 1:
                    checkpoint_manager.clear_checkpoint()
                    logger.info(f" Все файлы обработаны, контрольная точка очищена")
                    print(f" Все файлы обработаны, контрольная точка очищена")
                else:
                    # Для следующего файла начинаем с начала
                    logger.info(f" Файл {file_path.name} полностью обработан")
                    print(f" Файл {file_path.name} полностью обработан")

            # Пауза между файлами
            if file_index < len(files_to_process) - 1 and not SHOULD_STOP:
                logger.info("Пауза 5 секунд перед следующим файлом...")
                print("  Пауза 5 секунд перед следующим файлом...")
                time.sleep(5)

        except Exception as e:
            logger.error(f"Ошибка обработки файла: {e}")
            import traceback
            logger.error(traceback.format_exc())
            print(f" Ошибка обработки файла: {e}")

            # Если ошибка декодирования
            if "UnicodeDecodeError" in str(e) or "codec" in str(e):
                print("\n  Ошибка декодирования UTF-8!")


            continue

        # Прерываем обработку если получен сигнал или превышена память
        if SHOULD_STOP:
            logger.warning("Загрузка прервана")
            print("\n Загрузка прервана")
            break

    # Восстанавливаем настройки БД
    print("\n🔧 Восстановление настроек БД...")
    restore_database_settings()

    if not SHOULD_STOP:
        create_indexes_after_loading()

    print(" Настройки БД восстановлены")

    # Итоги
    global_stats['end_time'] = datetime.now()
    global_stats['duration'] = global_stats['end_time'] - global_stats['start_time']

    print(f"\n{'=' * 60}")
    print("  ИТОГИ ЗАГРУЗКИ:")
    print(f"{'=' * 60}")
    print(f" Обработано файлов: {global_stats['files_processed']}/{global_stats['total_files']}")
    print(f" Всего прочитано: {global_stats['total_read']:,}")
    print(f" Успешно обработано: {global_stats['total_processed']:,}")
    print(f" Вставлено в БД: {global_stats['total_inserted']:,}")
    print(f"  Общее время: {global_stats['duration']}")

    # Логируем итоги
    logger.info("\nИТОГИ ЗАГРУЗКИ:")
    logger.info(f"Обработано файлов: {global_stats['files_processed']}/{global_stats['total_files']}")
    logger.info(f"Всего прочитано: {global_stats['total_read']:,}")
    logger.info(f"Успешно обработано: {global_stats['total_processed']:,}")
    logger.info(f"Вставлено в БД: {global_stats['total_inserted']:,}")
    logger.info(f"Общее время: {global_stats['duration']}")

    if global_stats['duration'].total_seconds() > 0:
        speed = global_stats['total_read'] / global_stats['duration'].total_seconds()
        print(f" Средняя скорость: {speed:.1f} строк/сек")
        logger.info(f"Средняя скорость: {speed:.1f} строк/сек")

    # Проверяем итоговое количество если не было прерывания
    if not SHOULD_STOP:
        print("\n Проверка итогового количества записей...")
        session = SessionLocal()
        try:
            from sqlalchemy import func
            total_products = session.query(func.count(Product.id)).scalar()
            print(f" Всего продуктов в таблице: {total_products:,}")
            logger.info(f"Всего продуктов в таблице: {total_products:,}")
        except Exception as e:
            print(f" Ошибка проверки количества записей: {e}")
        finally:
            session.close()

    print(f"\n{'=' * 60}")

    if SHOULD_STOP:
        print(" Загрузка была прервана.")
        print(" Контрольная точка сохранена (линейная позиция)")
        print(" Для продолжения запустите программу снова.")
        logger.warning("Загрузка была прервана. Для продолжения запустите программу снова.")
    else:
        print("🎉 Загрузка завершена успешно!")
        logger.info("Загрузка завершена успешно!")

    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n Загрузка прервана пользователем (Ctrl+C)")
        logger.warning("Загрузка прервана пользователем (Ctrl+C)")
        sys.exit(1)
    except Exception as e:
        print(f"\n Критическая ошибка: {e}")
        logger.error(f"Критическая ошибка: {e}")
        import traceback

        traceback.print_exc()
        logger.error(traceback.format_exc())
        sys.exit(1)