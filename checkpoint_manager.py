# checkpoint_manager.py
import json
import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Надежный менеджер контрольных точек"""

    def __init__(self, checkpoint_file='checkpoint.json'):
        self.checkpoint_file = checkpoint_file
        self.current_progress = {
            'file_path': None,
            'byte_position': 0,
            'line_number': 0,
            'inserted_count': 0,
            'current_chunk': 0,
            'current_batch': 0
        }

    def save_checkpoint(self,
                        file_path: str,
                        byte_position: int,
                        line_number: int,
                        inserted_count: int,
                        reason: str = "unknown"):
        """Сохраняет контрольную точку"""
        checkpoint = {
            'file_path': str(file_path),
            'byte_position': byte_position,
            'line_number': line_number,
            'inserted_count': inserted_count,
            'timestamp': datetime.now().isoformat(),
            'reason': reason,
            'program_version': '1.0'
        }

        try:
            # Сначала пишем во временный файл
            temp_file = f"{self.checkpoint_file}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint, f, indent=2, ensure_ascii=False)

            # Атомарно заменяем основной файл
            if os.path.exists(self.checkpoint_file):
                os.replace(temp_file, self.checkpoint_file)
            else:
                os.rename(temp_file, self.checkpoint_file)

            # Сохраняем в памяти
            self.current_progress.update({
                'file_path': file_path,
                'byte_position': byte_position,
                'line_number': line_number,
                'inserted_count': inserted_count
            })

            logger.info(f"✅ Контрольная точка сохранена: {Path(file_path).name}, строка {line_number:,} ({reason})")
            print(f"✅ Контрольная точка сохранена: строка {line_number:,} ({reason})")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения контрольной точки: {e}")
            print(f"❌ Ошибка сохранения контрольной точки: {e}")
            return False

    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Загружает контрольную точку"""
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint = json.load(f)

                # Валидация контрольной точки
                if not self._validate_checkpoint(checkpoint):
                    logger.warning("⚠️ Некорректная контрольная точка")
                    return None

                logger.info(f"✅ Загружена контрольная точка: {Path(checkpoint['file_path']).name}")
                logger.info(f"   Позиция: строка {checkpoint['line_number']:,}, байт {checkpoint['byte_position']:,}")
                logger.info(
                    f"   Вставлено: {checkpoint['inserted_count']:,}, причина: {checkpoint.get('reason', 'unknown')}")

                self.current_progress.update(checkpoint)
                return checkpoint

        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка чтения контрольной точки (некорректный JSON): {e}")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки контрольной точки: {e}")

        return None

    def _validate_checkpoint(self, checkpoint: Dict) -> bool:
        """Проверяет корректность контрольной точки"""
        required_fields = ['file_path', 'byte_position', 'line_number', 'inserted_count']

        for field in required_fields:
            if field not in checkpoint:
                logger.error(f"Отсутствует обязательное поле: {field}")
                return False

        # Проверка типов
        if not isinstance(checkpoint['byte_position'], int) or checkpoint['byte_position'] < 0:
            logger.error(f"Некорректное значение byte_position: {checkpoint['byte_position']}")
            return False

        if not isinstance(checkpoint['line_number'], int) or checkpoint['line_number'] < 0:
            logger.error(f"Некорректное значение line_number: {checkpoint['line_number']}")
            return False

        # Проверка существования файла
        file_path = Path(checkpoint['file_path'])
        if not file_path.exists():
            logger.warning(f"Файл контрольной точки не существует: {file_path}")
            # Файл мог быть перемещен, но это не фатальная ошибка

        return True

    def clear_checkpoint(self):
        """Удаляет контрольную точку"""
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
                backup_file = f"{self.checkpoint_file}.backup"
                if os.path.exists(backup_file):
                    os.remove(backup_file)

                self.current_progress = {
                    'file_path': None,
                    'byte_position': 0,
                    'line_number': 0,
                    'inserted_count': 0
                }

                logger.info("✅ Контрольная точка очищена")
                print("✅ Контрольная точка очищена")
                return True

        except Exception as e:
            logger.error(f"❌ Ошибка удаления контрольной точки: {e}")
            return False

    def backup_checkpoint(self):
        """Создает резервную копию контрольной точки"""
        try:
            if os.path.exists(self.checkpoint_file):
                import shutil
                backup_file = f"{self.checkpoint_file}.backup"
                shutil.copy2(self.checkpoint_file, backup_file)
                return True
        except Exception as e:
            logger.error(f"Ошибка создания бэкапа контрольной точки: {e}")
        return False

    def update_progress(self,
                        file_path: str,
                        byte_position: int,
                        line_number: int,
                        inserted_count: int):
        """Обновляет прогресс в памяти (для быстрого сохранения)"""
        self.current_progress.update({
            'file_path': file_path,
            'byte_position': byte_position,
            'line_number': line_number,
            'inserted_count': inserted_count
        })

    def save_current_progress(self, reason: str = "manual"):
        """Сохраняет текущий прогресс из памяти"""
        if self.current_progress['file_path']:
            return self.save_checkpoint(
                self.current_progress['file_path'],
                self.current_progress['byte_position'],
                self.current_progress['line_number'],
                self.current_progress['inserted_count'],
                reason
            )
        return False

    def get_progress_percentage(self, file_size: int = 0) -> float:
        """Возвращает процент выполнения"""
        if file_size > 0 and self.current_progress['byte_position'] > 0:
            return min(100.0, (self.current_progress['byte_position'] / file_size) * 100)
        return 0.0