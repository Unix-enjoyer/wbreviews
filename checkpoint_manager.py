# checkpoint_manager.py
import json
import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
import sys

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Надежный менеджер контрольных точек с позицией начала строки"""

    def __init__(self, checkpoint_file='checkpoint.json'):
        self.checkpoint_file = checkpoint_file
        self.current_progress = {
            'file_path': None,
            'line_number': 0,  # Номер строки (0-based)
            'inserted_count': 0
        }

    def save_checkpoint(self,
                        file_path: str,
                        line_number: int,
                        inserted_count: int,
                        reason: str = "unknown"):
        """Сохраняет контрольную точку (только номер строки)"""
        checkpoint = {
            'file_path': str(file_path),
            'line_number': line_number,
            'inserted_count': inserted_count,
            'timestamp': datetime.now().isoformat(),
            'reason': reason,
            'version': 'line_based_v1'
        }

        try:
            # Атомарное сохранение через временный файл
            temp_file = f"{self.checkpoint_file}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint, f, indent=2, ensure_ascii=False)

            if os.path.exists(self.checkpoint_file):
                os.replace(temp_file, self.checkpoint_file)
            else:
                os.rename(temp_file, self.checkpoint_file)

            # Сохраняем в памяти
            self.current_progress.update({
                'file_path': file_path,
                'line_number': line_number,
                'inserted_count': inserted_count
            })

            logger.info(f"  Контрольная точка сохранена: строка {line_number:,} ({reason})")
            return True

        except Exception as e:
            logger.error(f"  Ошибка сохранения контрольной точки: {e}")
            return False

    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Загружает контрольную точку"""
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint = json.load(f)

                # Валидация контрольной точки
                if not self._validate_checkpoint(checkpoint):
                    logger.warning(" ️ Некорректная контрольная точка")
                    return None

                # Конвертируем старые контрольные точки в новый формат
                if 'byte_position' in checkpoint:
                    logger.info("  Конвертация старой контрольной точки")
                    # Для старых контрольных точек используем только номер строки
                    checkpoint['line_number'] = checkpoint.get('line_number', 0)
                    # Удаляем устаревшие поля
                    if 'byte_position' in checkpoint:
                        del checkpoint['byte_position']

                logger.info(f"  Загружена контрольная точка: {Path(checkpoint['file_path']).name}")
                logger.info(f"   Строка: {checkpoint['line_number']:,}")
                logger.info(f"   Вставлено: {checkpoint['inserted_count']:,}")

                self.current_progress.update(checkpoint)
                return checkpoint

        except json.JSONDecodeError as e:
            logger.error(f"  Ошибка чтения контрольной точки (некорректный JSON): {e}")
        except Exception as e:
            logger.error(f"  Ошибка загрузки контрольной точки: {e}")

        return None

    def _validate_checkpoint(self, checkpoint: Dict) -> bool:
        """Проверяет корректность контрольной точки"""
        required_fields = ['file_path', 'line_number']

        for field in required_fields:
            if field not in checkpoint:
                logger.error(f"Отсутствует обязательное поле: {field}")
                return False

        # Проверка типов
        if not isinstance(checkpoint['line_number'], int) or checkpoint['line_number'] < 0:
            logger.error(f"Некорректное значение line_number: {checkpoint['line_number']}")
            return False

        return True

    def clear_checkpoint(self):
        """Удаляет контрольную точку"""
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)

                self.current_progress = {
                    'file_path': None,
                    'line_number': 0,
                    'inserted_count': 0
                }

                logger.info("  Контрольная точка очищена")
                return True

        except Exception as e:
            logger.error(f"  Ошибка удаления контрольной точки: {e}")
            return False

    def update_progress(self,
                        file_path: str,
                        line_number: int,
                        inserted_count: int):
        """Обновляет прогресс в памяти"""
        self.current_progress.update({
            'file_path': file_path,
            'line_number': line_number,
            'inserted_count': inserted_count
        })

    def save_current_progress(self, reason: str = "manual"):
        """Сохраняет текущий прогресс из памяти"""
        if self.current_progress['file_path']:
            return self.save_checkpoint(
                self.current_progress['file_path'],
                self.current_progress['line_number'],
                self.current_progress['inserted_count'],
                reason
            )
        return False

    def find_line_start_position(self, file_path: Path, target_line: int) -> Tuple[int, bool]:
        """
        Находит позицию начала строки по номеру.
        Возвращает (line_number, is_correct)
        Если target_line больше общего числа строк, возвращает последнюю строку.
        """
        if not file_path.exists():
            return 0, False

        if target_line == 0:
            return 0, True

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                current_line = 0

                # Пропускаем строки до target_line - 1
                while current_line < target_line:
                    line = f.readline()
                    if not line:  # Достигли конца файла
                        break
                    current_line += 1

                # Возвращаем текущую позицию (начало следующей строки или конец файла)
                return current_line, True

        except UnicodeDecodeError as e:
            logger.error(f"  Ошибка декодирования при поиске строки {target_line}: {e}")
            return 0, False
        except Exception as e:
            logger.error(f"  Ошибка при поиске строки {target_line}: {e}")
            return 0, False