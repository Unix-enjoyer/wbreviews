import json
import pandas as pd
from collections import Counter
import sys
import os

# Добавляем путь к корню проекта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

config = Config()


def analyze_json_structure(file_path: str, sample_size: int = 1000):
    """
    Анализирует структуру JSON файла и выводит статистику
    """
    all_keys = Counter()
    sample_records = []

    print(f"Анализ структуры файла: {file_path}")
    print("=" * 60)

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= sample_size:
                    break

                try:
                    record = json.loads(line.strip())
                    sample_records.append(record)

                    # Собираем все ключи
                    for key in record.keys():
                        all_keys[key] += 1

                except json.JSONDecodeError:
                    continue

        if not sample_records:
            print("Не удалось прочитать ни одной записи")
            return

        # Выводим статистику по ключам
        print("Статистика ключей:")
        print(f"{'Ключ':<25} {'Кол-во':<10} {'%':<10}")
        print("-" * 50)

        for key, count in all_keys.most_common():
            percentage = (count / len(sample_records)) * 100
            print(f"{key:<25} {count:<10} {percentage:.1f}%")

        print("\nПример записи:")
        print(json.dumps(sample_records[0], ensure_ascii=False, indent=2))

        # Анализ конкретных полей
        print("\nАнализ полей (первые 5 записей):")
        fields_to_check = ['nmId', 'productValuation', 'color', 'text', 'answer']

        for i, record in enumerate(sample_records[:5], 1):
            print(f"\nЗапись #{i}:")
            for field in fields_to_check:
                value = record.get(field)
                if value is not None:
                    if isinstance(value, str):
                        words = len(value.split())
                        print(f"  '{field}': '{value[:50]}...' ({words} слов)")
                    else:
                        print(f"  '{field}': {value}")
                else:
                    print(f"  '{field}': ОТСУТСТВУЕТ")

        # Создаем DataFrame для удобного просмотра
        df = pd.DataFrame(sample_records)
        print(f"\nDataFrame ({len(df)}x{len(df.columns)}):")
        print(df.head(10).to_string(index=False))

        # Сохраняем в CSV для просмотра в Excel
        df.head(100).to_csv('sample_data.csv', index=False, encoding='utf-8-sig')
        print("\nПример данных сохранен в 'sample_data.csv'")

        return df

    except Exception as e:
        print(f"Ошибка: {e}")
        return None


def check_word_counts(file_path: str, n_samples: int = 500):
    """
    Проверяет количество слов в отзывах
    """
    # Импортируем функцию подсчета слов
    from loader import count_words

    print(f"\nПроверка количества слов в {n_samples} отзывах:")

    word_counts = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= n_samples:
                break

            try:
                record = json.loads(line.strip())
                text = record.get('text', '')
                if text:
                    words = count_words(text)
                    word_counts.append(words)
            except:
                continue

    if word_counts:
        import statistics
        print(f"  Образцов: {len(word_counts)}")
        print(f"  Минимум: {min(word_counts)} слов")
        print(f"  Максимум: {max(word_counts)} слов")
        print(f"  Среднее: {statistics.mean(word_counts):.1f} слов")
        print(f"  Медиана: {statistics.median(word_counts)} слов")

        # Распределение
        print(f"\n  Распределение по количеству слов:")
        ranges = [(0, 10), (11, 20), (21, 50), (51, 100), (101, 1000)]
        for r_min, r_max in ranges:
            count = sum(1 for w in word_counts if r_min <= w <= r_max)
            percentage = (count / len(word_counts)) * 100
            print(f"    {r_min:3}-{r_max:3} слов: {count:4} ({percentage:5.1f}%)")

        # Сколько будет отфильтровано при пороге >20 слов
        filtered = sum(1 for w in word_counts if w > 20)
        print(
            f"\n  При фильтре >20 слов: {filtered}/{len(word_counts)} пройдут ({filtered / len(word_counts) * 100:.1f}%)")
    else:
        print("  Не удалось прочитать данные")


def check_data_quality(file_path: str, n_samples: int = 1000):
    """
    Проверяет качество данных
    """
    print(f"\nПроверка качества данных ({n_samples} записей):")

    stats = {
        'total': 0,
        'has_text': 0,
        'has_nmId': 0,
        'has_rating': 0,
        'has_color': 0,
        'has_answer': 0
    }

    with open(file_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= n_samples:
                break

            try:
                record = json.loads(line.strip())
                stats['total'] += 1

                if record.get('text'):
                    stats['has_text'] += 1
                if record.get('nmId'):
                    stats['has_nmId'] += 1
                if record.get('productValuation'):
                    stats['has_rating'] += 1
                if record.get('color'):
                    stats['has_color'] += 1
                if record.get('answer'):
                    stats['has_answer'] += 1

            except:
                continue

    print(f"  Всего записей: {stats['total']}")
    print(f"  С текстом отзыва: {stats['has_text']} ({stats['has_text'] / stats['total'] * 100:.1f}%)")
    print(f"  С ID (nmId): {stats['has_nmId']} ({stats['has_nmId'] / stats['total'] * 100:.1f}%)")
    print(f"  С рейтингом: {stats['has_rating']} ({stats['has_rating'] / stats['total'] * 100:.1f}%)")
    print(f"  С указанием цвета: {stats['has_color']} ({stats['has_color'] / stats['total'] * 100:.1f}%)")
    print(f"  С ответом: {stats['has_answer']} ({stats['has_answer'] / stats['total'] * 100:.1f}%)")


if __name__ == "__main__":
    # Указываем путь к файлу
    FILE_PATH = config.JSON_FILE

    print("Запуск анализа данных...")

    # 1. Анализ структуры
    df = analyze_json_structure(FILE_PATH, 1000)

    # 2. Проверка количества слов
    check_word_counts(FILE_PATH, 500)

    # 3. Проверка качества данных
    check_data_quality(FILE_PATH, 1000)

    print("\nАнализ завершен!")