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

            print("=" * 70)
            print("    ИНФОРМАЦИЯ О КОНТРОЛЬНОЙ ТОЧКЕ (линейная версия)")
            print("=" * 70)
            print(f"  Файл: {Path(checkpoint['file_path']).name}")
            print(f"  Полный путь: {checkpoint['file_path']}")
            print(f"  Номер строки: {checkpoint['line_number']:,}")
            print(f"  Вставлено записей: {checkpoint.get('inserted_count', 0):,}")
            print(f"   Сохранено: {checkpoint['timestamp']}")
            print(f"  Причина: {checkpoint.get('reason', 'unknown')}")
            print(f"  Версия: {checkpoint.get('version', 'unknown')}")

            # Рассчитываем примерный прогресс
            try:
                file_path = Path(checkpoint['file_path'])
                if file_path.exists():
                    file_size = file_path.stat().st_size
                    # Очень примерная оценка: 5000 строк на 1 MB
                    estimated_lines = file_size / 1024 * 5000
                    if estimated_lines > 0:
                        progress_percent = (checkpoint['line_number'] / estimated_lines) * 100
                        print(f"  Примерный прогресс: {progress_percent:.1f}%")
                    print(f"  Размер файла: {file_size / (1024 ** 2):.1f} MB")
            except:
                pass

            print("=" * 70)

            # Предложения действий
            print("\n  Действия:")
            print("1. Для продолжения загрузки: python optimized_loader_nd.py")
            print("2. Для удаления контрольной точки: python checkpoint_utils.py delete")
            print("3. Для просмотра снова: python checkpoint_utils.py show")

        except json.JSONDecodeError:
            print("  Ошибка: файл контрольной точки поврежден (некорректный JSON)")
        except Exception as e:
            print(f"  Ошибка чтения контрольной точки: {e}")
    else:
        print("  Контрольная точка не найдена.")
        print("\n  Запустите: python optimized_loader_nd.py для начала загрузки")


def delete_checkpoint():
    """Удаляет контрольную точку"""
    checkpoint_file = 'checkpoint.json'

    print("\n" + "=" * 60)
    print("     УДАЛЕНИЕ КОНТРОЛЬНОЙ ТОЧКИ")
    print("=" * 60)

    if os.path.exists(checkpoint_file):
        try:
            # Показываем информацию перед удалением
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)

            print(f"  Файл: {Path(checkpoint['file_path']).name}")
            print(f"  Последняя строка: {checkpoint['line_number']:,}")
            print(f"  Вставлено: {checkpoint.get('inserted_count', 0):,}")

            # Запрос подтверждения
            response = input("\n  Вы уверены? (y/N): ").strip().lower()

            if response == 'y':
                os.remove(checkpoint_file)
                print("\n  Контрольная точка удалена")
                print("  Теперь можно начать новую загрузку: python optimized_loader_nd.py")
            else:
                print("\n  Удаление отменено")

        except Exception as e:
            print(f"  Ошибка удаления контрольной точки: {e}")
    else:
        print("  Контрольная точка не найдена.")


def convert_old_checkpoint():
    """Конвертирует старую контрольную точку в новую линейную версию"""
    checkpoint_file = 'checkpoint.json'

    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)

            # Проверяем, нужно ли конвертировать
            if 'byte_position' in checkpoint and 'version' not in checkpoint:
                print("  Обнаружена старая контрольная точка")
                print(f"   Файл: {Path(checkpoint['file_path']).name}")
                print(f"   Байтовая позиция: {checkpoint.get('byte_position', 0):,}")
                print(f"   Строка: {checkpoint.get('line_number', 0):,}")

                response = input("\n  Конвертировать в линейную версию? (y/N): ").strip().lower()

                if response == 'y':
                    # Создаем новую контрольную точку только с номером строки
                    new_checkpoint = {
                        'file_path': checkpoint['file_path'],
                        'line_number': checkpoint.get('line_number', 0),
                        'inserted_count': checkpoint.get('inserted_count', 0),
                        'timestamp': checkpoint.get('timestamp', datetime.now().isoformat()),
                        'reason': checkpoint.get('reason', 'converted'),
                        'version': 'line_based_v1'
                    }

                    # Сохраняем
                    with open(checkpoint_file, 'w', encoding='utf-8') as f:
                        json.dump(new_checkpoint, f, indent=2, ensure_ascii=False)

                    print("  Контрольная точка конвертирована в линейную версию")
                else:
                    print("  Конвертация отменена")
            else:
                print("  Контрольная точка уже в новом формате или не требует конвертации")

        except Exception as e:
            print(f"  Ошибка конвертации контрольной точки: {e}")
    else:
        print("  Контрольная точка не найдена.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == 'show':
            show_checkpoint_info()
        elif command == 'delete':
            delete_checkpoint()
        elif command == 'convert':
            convert_old_checkpoint()
        elif command == 'help':
            print("Использование:")
            print("  python checkpoint_utils.py show     - показать информацию")
            print("  python checkpoint_utils.py delete   - удалить контрольную точку")
            print("  python checkpoint_utils.py convert  - конвертировать старую контрольную точку")
            print("  python checkpoint_utils.py help     - показать эту справку")
            print("\nПримеры:")
            print("  python checkpoint_utils.py          - показать информацию (по умолчанию)")
            print("  python checkpoint_utils.py delete   - удалить контрольную точку")
            print("  python checkpoint_utils.py convert  - конвертировать старую контрольную точку в линейную")
        else:
            print(f"  Неизвестная команда: {command}")
            print("  Используйте 'help' для справки")
    else:
        show_checkpoint_info()