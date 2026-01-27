import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text, create_engine
import subprocess
import time


def check_docker_container():
    """Проверяет, запущен ли Docker контейнер"""
    print("Проверка Docker контейнера...")

    try:
        # Проверяем список контейнеров
        result = subprocess.run(
            ["docker", "ps", "--format", "table {{.Names}}\t{{.Status}}\t{{.Ports}}"],
            capture_output=True,
            text=True,
            shell=True
        )

        if result.returncode == 0:
            print("Docker запущен")
            print("\nЗапущенные контейнеры:")
            print(result.stdout)

            # Проверяем конкретный контейнер
            result = subprocess.run(
                ["docker", "ps", "-f", "name=wb_reviews_db", "--format", "{{.Status}}"],
                capture_output=True,
                text=True,
                shell=True
            )

            if result.stdout.strip():
                print(f"\nКонтейнер wb_reviews_db: {result.stdout.strip()}")
                return True
            else:
                print("Контейнер wb_reviews_db не найден")
                return False
        else:
            print("Docker не запущен")
            return False

    except FileNotFoundError:
        print("Docker не установлен или не в PATH")
        return False


def check_port():
    """Проверяет, слушает ли PostgreSQL на порту 5433"""
    print("\nПроверка порта 5433...")

    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(('localhost', 5433))
        sock.close()

        if result == 0:
            print("Порт 5433 открыт и слушает")
            return True
        else:
            print("Порт 5433 закрыт или не слушает")
            return False
    except Exception as e:
        print(f"Ошибка проверки порта: {e}")
        return False


def test_postgres_connection():
    """Тестирует подключение к PostgreSQL"""
    print("\nТестирование подключения к PostgreSQL...")

    # Тестируем разные варианты подключения
    test_urls = [
        # pg8000 драйвер
        "postgresql+pg8000://admin:admin123@localhost:5433/wb_reviews",
        # psycopg2 драйвер (если установлен)
        "postgresql://admin:admin123@localhost:5433/wb_reviews",
    ]

    for i, url in enumerate(test_urls, 1):
        print(f"\nТест {i}: {url}")
        try:
            engine = create_engine(url, echo=False, pool_pre_ping=True)

            # Пробуем подключиться с таймаутом
            with engine.connect() as conn:
                # Проверяем версию PostgreSQL
                result = conn.execute(text("SELECT version()"))
                version = result.scalar()
                print(f" Успешное подключение!")
                print(f" Версия: {version}")

                # Проверяем текущую базу данных
                result = conn.execute(text("SELECT current_database()"))
                db = result.scalar()
                print(f"База данных: {db}")

                # Проверяем пользователя
                result = conn.execute(text("SELECT current_user"))
                user = result.scalar()
                print(f" Пользователь: {user}")

                return True

        except Exception as e:
            print(f" Ошибка: {str(e)[:100]}...")
            continue

    return False


def test_with_simple_connection():
    """Тестирует подключение через простой TCP сокет"""
    print("\nТестирование TCP подключения...")

    try:
        import socket
        import time

        # Подключаемся к PostgreSQL порту
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)

        print("   Подключаемся к localhost:5433...")
        sock.connect(('localhost', 5433))

        # Отправляем простой тестовый пакет (SSL negotiation)
        sock.send(b'\x00\x00\x00\x08\x04\xd2\x16\x2f')
        response = sock.recv(1024)

        if response:
            print(f"PostgreSQL отвечает на порту 5433")
            print(f"Ответ сервера (первые 50 байт): {response[:50].hex()}")
            sock.close()
            return True
        else:
            print("PostgreSQL не ответил")
            sock.close()
            return False

    except Exception as e:
        print(f"Ошибка TCP подключения: {e}")
        return False


def check_docker_logs():
    """Проверяет логи Docker контейнера"""
    print("\nПроверка логов контейнера...")

    try:
        result = subprocess.run(
            ["docker", "logs", "--tail", "20", "wb_reviews_db"],
            capture_output=True,
            text=True,
            shell=True
        )

        if result.returncode == 0:
            print("Логи контейнера (последние 20 строк):")
            print("-" * 50)
            print(result.stdout)
            print("-" * 50)

            # Проверяем наличие успешного старта
            if "database system is ready to accept connections" in result.stdout:
                print("PostgreSQL успешно запущен в контейнере")
                return True
            else:
                print("PostgreSQL может быть еще не готов")
                return False
        else:
            print("Не удалось получить логи")
            return False
    except Exception as e:
        print(f"Ошибка получения логов: {e}")
        return False


def restart_docker():
    """Перезапускает Docker контейнер"""
    print("\nПерезапуск Docker контейнера...")

    try:
        # Останавливаем контейнер
        print("Останавливаем контейнер...")
        subprocess.run(["docker-compose", "down"], capture_output=True, shell=True)

        # Ждем 3 секунды
        time.sleep(3)

        # Запускаем контейнер
        print("Запускаем контейнер...")
        result = subprocess.run(
            ["docker-compose", "up", "-d"],
            capture_output=True,
            text=True,
            shell=True
        )

        if result.returncode == 0:
            print("Контейнер перезапущен")

            # Ждем пока PostgreSQL запустится
            print("Ждем 10 секунд для запуска PostgreSQL...")
            time.sleep(10)

            return True
        else:
            print(f"Ошибка перезапуска: {result.stderr}")
            return False

    except Exception as e:
        print(f"Ошибка: {e}")
        return False


def main():
    print("=" * 60)
    print("ПРОВЕРКА ПОДКЛЮЧЕНИЯ К POSTGRESQL В DOCKER")
    print("=" * 60)

    # Шаг 1: Проверяем Docker
    docker_ok = check_docker_container()

    if not docker_ok:
        print("\nПопытка запуска контейнера...")
        restart_docker()
        docker_ok = check_docker_container()

    if not docker_ok:
        print("\nDocker не запущен. Запустите вручную:")
        print("   1. Откройте Docker Desktop")
        print("   2. Запустите: docker-compose up -d")
        return

    # Шаг 2: Проверяем порт
    port_ok = check_port()

    if not port_ok:
        print("\n️Порт 5433 недоступен. Возможные причины:")
        print("   - Контейнер остановлен")
        print("   - Порт занят другим приложением")
        print("   - Проблемы с сетью Docker")

        print("\nПерезапускаем контейнер...")
        restart_docker()
        port_ok = check_port()

    # Шаг 3: Проверяем логи
    check_docker_logs()

    # Шаг 4: Простое TCP подключение
    test_with_simple_connection()

    # Шаг 5: Тестируем SQLAlchemy подключение
    connection_ok = test_postgres_connection()

    if connection_ok:
        print("\n" + "=" * 60)
        print("ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ УСПЕШНО!")
        print("=" * 60)
        print("\n Теперь вы можете запустить загрузчик:")
        print("   python loader.py")
    else:
        print("\n" + "=" * 60)
        print("ПРОБЛЕМЫ С ПОДКЛЮЧЕНИЕМ")
        print("=" * 60)

        print("\n Действия по устранению:")
        print("1. Проверьте, что Docker Desktop запущен")
        print("2. Попробуйте перезапустить контейнер:")
        print("   docker-compose down")
        print("   docker-compose up -d")
        print("3. Проверьте, не занят ли порт 5433:")
        print("   netstat -an | findstr :5433")
        print("4. Проверьте логи контейнера:")
        print("   docker logs wb_reviews_db")
        print("\n5. Альтернативно, используйте SQLite:")
        print("   Измените USE_SQLITE = True в config.py")


if __name__ == "__main__":
    main()