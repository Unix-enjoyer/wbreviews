# checkpoint_utils.py
import json
import os
from pathlib import Path
from datetime import datetime


def show_checkpoint_info():
    """Показывает информацию о текущей контрольной точке"""
    checkpoint_file = 'checkpoint.json'

    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)

            print("=" * 60)
            print("  ИНФОРМАЦИЯ О КОНТРОЛЬНОЙ ТОЧКЕ")
            print("=" * 60)
            print(f"Файл: {Path(checkpoint['file_path']).name}")
            print(f"Полный путь: {checkpoint['file_path']}")
            print(f"Байтовая позиция: {checkpoint['byte_position']:,}")
            print(f"Номер строки: {checkpoint['line_number']:,}")
            print(f"Вставлено записей: {checkpoint['inserted_count']:,}")
            print(f"Сохранено: {checkpoint['timestamp']}")
            print(f"Причина: {checkpoint.get('reason', 'unknown')}")
            print("=" * 60)

            # Рассчитываем примерный прогресс
            try:
                file_path = Path(checkpoint['file_path'])
                if file_path.exists():
                    file_size = file_path.stat().st_size
                    progress_percent = (checkpoint['byte_position'] / file_size) * 100
                    print(f"Прогресс в файле: {progress_percent:.1f}%")
                    print(f"Размер файла: {file_size / (1024 ** 2):.1f} MB")
            except:
                pass

        except Exception as e:
            print(f"Ошибка чтения контрольной точки: {e}")
    else:
        print("Контрольная точка не найдена.")


def delete_checkpoint():
    """Удаляет контрольную точку"""
    checkpoint_file = 'checkpoint.json'

    if os.path.exists(checkpoint_file):
        try:
            os.remove(checkpoint_file)
            print("  Контрольная точка удалена")
        except Exception as e:
            print(f"  Ошибка удаления контрольной точки: {e}")
    else:
        print("Контрольная точка не найдена.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == 'show':
            show_checkpoint_info()
        elif command == 'delete':
            delete_checkpoint()
        elif command == 'help':
            print("Использование:")
            print("  python checkpoint_utils.py show    - показать информацию")
            print("  python checkpoint_utils.py delete  - удалить контрольную точку")
            print("  python checkpoint_utils.py help    - показать эту справку")
        else:
            print(f"Неизвестная команда: {command}")
            print("Используйте 'help' для справки")
    else:
        show_checkpoint_info()