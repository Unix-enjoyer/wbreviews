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
            print("  📋 ИНФОРМАЦИЯ О КОНТРОЛЬНОЙ ТОЧКЕ")
            print("=" * 70)
            print(f"📄 Файл: {Path(checkpoint['file_path']).name}")
            print(f"📍 Полный путь: {checkpoint['file_path']}")
            print(f"🔢 Байтовая позиция: {checkpoint['byte_position']:,}")
            print(f"📝 Номер строки: {checkpoint['line_number']:,}")
            print(f"✅ Вставлено записей: {checkpoint['inserted_count']:,}")
            print(f"⏱️  Сохранено: {checkpoint['timestamp']}")
            print(f"📌 Причина: {checkpoint.get('reason', 'unknown')}")

            # Рассчитываем примерный прогресс
            try:
                file_path = Path(checkpoint['file_path'])
                if file_path.exists():
                    file_size = file_path.stat().st_size
                    progress_percent = (checkpoint['byte_position'] / file_size) * 100
                    print(f"📊 Прогресс в файле: {progress_percent:.1f}%")
                    print(f"📦 Размер файла: {file_size / (1024 ** 2):.1f} MB")

                    # Оставшийся размер
                    remaining_bytes = file_size - checkpoint['byte_position']
                    remaining_mb = remaining_bytes / (1024 ** 2)
                    print(f"⏳ Осталось: {remaining_mb:.1f} MB")
            except:
                pass

            # Проверяем бэкап
            backup_file = 'checkpoint.json.backup'
            if os.path.exists(backup_file):
                print(f"💾 Доступен бэкап: Да")
            else:
                print(f"💾 Доступен бэкап: Нет")

            print("=" * 70)

            # Предложения действий
            print("\n💡 Действия:")
            print("1. Для продолжения загрузки: python optimized_loader_nd.py")
            print("2. Для удаления контрольной точки: python checkpoint_utils.py delete")
            print("3. Для просмотра снова: python checkpoint_utils.py show")

        except json.JSONDecodeError:
            print("❌ Ошибка: файл контрольной точки поврежден (некорректный JSON)")
        except Exception as e:
            print(f"❌ Ошибка чтения контрольной точки: {e}")
    else:
        print("ℹ️ Контрольная точка не найдена.")
        print("\n💡 Запустите: python optimized_loader_nd.py для начала загрузки")


def delete_checkpoint():
    """Удаляет контрольную точку"""
    checkpoint_file = 'checkpoint.json'

    print("\n" + "=" * 60)
    print("  🗑️  УДАЛЕНИЕ КОНТРОЛЬНОЙ ТОЧКИ")
    print("=" * 60)

    if os.path.exists(checkpoint_file):
        try:
            # Показываем информацию перед удалением
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)

            print(f"📄 Файл: {Path(checkpoint['file_path']).name}")
            print(f"📝 Последняя строка: {checkpoint['line_number']:,}")
            print(f"✅ Вставлено: {checkpoint['inserted_count']:,}")

            # Запрос подтверждения
            response = input("\n❓ Вы уверены? (y/N): ").strip().lower()

            if response == 'y':
                os.remove(checkpoint_file)

                # Удаляем бэкап если есть
                backup_file = 'checkpoint.json.backup'
                if os.path.exists(backup_file):
                    os.remove(backup_file)

                print("\n✅ Контрольная точка удалена")
                print("🚀 Теперь можно начать новую загрузку: python optimized_loader_nd.py")
            else:
                print("\nℹ️ Удаление отменено")

        except Exception as e:
            print(f"❌ Ошибка удаления контрольной точки: {e}")
    else:
        print("ℹ️ Контрольная точка не найдена.")


def repair_checkpoint():
    """Пытается восстановить контрольную точку из бэкапа"""
    checkpoint_file = 'checkpoint.json'
    backup_file = 'checkpoint.json.backup'

    print("\n" + "=" * 60)
    print("  🔧 ВОССТАНОВЛЕНИЕ КОНТРОЛЬНОЙ ТОЧКИ")
    print("=" * 60)

    if os.path.exists(backup_file):
        try:
            print("Найден бэкап контрольной точки...")

            with open(backup_file, 'r', encoding='utf-8') as f:
                backup_data = json.load(f)

            print(f"📄 Файл в бэкапе: {Path(backup_data['file_path']).name}")
            print(f"📝 Строка: {backup_data['line_number']:,}")

            response = input("\n❓ Восстановить из бэкапа? (y/N): ").strip().lower()

            if response == 'y':
                import shutil
                shutil.copy2(backup_file, checkpoint_file)
                print("✅ Контрольная точка восстановлена из бэкапа")
            else:
                print("ℹ️ Восстановление отменено")

        except Exception as e:
            print(f"❌ Ошибка восстановления: {e}")
    else:
        print("ℹ️ Бэкап контрольной точки не найден")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == 'show':
            show_checkpoint_info()
        elif command == 'delete':
            delete_checkpoint()
        elif command == 'repair':
            repair_checkpoint()
        elif command == 'help':
            print("Использование:")
            print("  python checkpoint_utils.py show     - показать информацию")
            print("  python checkpoint_utils.py delete   - удалить контрольную точку")
            print("  python checkpoint_utils.py repair   - восстановить из бэкапа")
            print("  python checkpoint_utils.py help     - показать эту справку")
            print("\nПримеры:")
            print("  python checkpoint_utils.py          - показать информацию (по умолчанию)")
            print("  python checkpoint_utils.py delete   - удалить контрольную точку")
        else:
            print(f"❌ Неизвестная команда: {command}")
            print("ℹ️ Используйте 'help' для справки")
    else:
        show_checkpoint_info()