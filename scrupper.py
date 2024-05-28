import psycopg2
import requests
from bs4 import BeautifulSoup
from loguru import logger
import os
from dotenv import load_dotenv
import threading
from psycopg2.extensions import connection

TYPE = ('.png', '.pdf')


def create_table(conn: connection) -> None:
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS text_on_page(
                    id serial PRIMARY KEY,
                    link text NOT NULL,
                    header text NOT NULL,
                    text text NOT NULL);"""
            )
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Ошибка при подключении к базе данных: {error}")


def log_function(func: callable) -> callable:
    def wrapper(*args, **kwargs):
        url = args[0]
        logger.info(f"Запуск функции с URL: {url}")
        return func(*args, **kwargs)
    return wrapper


def insert_in_table(url: str, lock: threading.Lock, conn: connection) -> BeautifulSoup | None:
    try:
        site = requests.get(url)
        soup = BeautifulSoup(site.text, 'html.parser')
    except Exception as e:
        logger.error(f"Ошибка при запросе к URL {url}: {e}")
        return None

    with lock:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT link FROM text_on_page WHERE link = %s""", (url,))
            if not cursor.fetchone():
                header = soup.find('body').find('header', class_='entry-header')
                header_on_page = ' '.join(header.text.split()).replace('\n', '') if header else ''
                prev_text = ' '.join(p.text for p in soup.find('body').find_all('p'))
                text_on_page = ' '.join(prev_text.split()).replace('\n', '')
                try:
                    cursor.execute("""INSERT INTO text_on_page (link, header, text) VALUES (%s, %s, %s);""",
                                   (url, header_on_page, text_on_page))
                    conn.commit()
                    logger.info(f"Добавлено в таблицу: {url}, {header_on_page}, {text_on_page}")
                except (Exception, psycopg2.Error) as error:
                    logger.error(f"Ошибка при обработке страницы {url}: {error}")
        return soup


@log_function
def get_and_commit_text_on_page(url: str, conn: connection, visited: set, lock: threading.Lock, thread_pool: list) -> None:
    with lock:
        if url in visited:
            return
        visited.add(url)

    soup = insert_in_table(url, lock, conn)

    if soup is None:
        return

    new_links = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.startswith('/'):
            new_url = f"https://pythonworld.ru{href}"
        elif href.startswith('https://pythonworld.ru'):
            new_url = href
        else:
            continue
        with lock:
            if new_url not in visited and not new_url.endswith(TYPE):
                new_links.append(new_url)

    for new_url in new_links:
        thread = threading.Thread(target=get_and_commit_text_on_page, args=(new_url, conn, visited, lock, thread_pool), daemon=True)
        thread.start()
        with lock:
            thread_pool.append(thread)


if __name__ == '__main__':
    logger.add('logs/log.log', rotation='14 day', level='DEBUG')

    load_dotenv()
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT')
    )

    create_table(conn)

    visited = set()

    lock = threading.Lock()

    thread_pool = []

    initial_url = 'https://pythonworld.ru/'

    main_thread = threading.Thread(target=get_and_commit_text_on_page,
                                   args=(initial_url, conn, visited, lock, thread_pool), daemon=True)

    main_thread.start()
    thread_pool.append(main_thread)

    for thread in thread_pool:
        thread.join()

    conn.close()

