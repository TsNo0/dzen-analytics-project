import requests
from bs4 import BeautifulSoup
import csv
import time
import random
import sys

# --- СПИСОК ХАБОВ ---
HUBS = [
    'python', 'java', 'javascript', 'cpp', 'csharp', 'go', 'php', 'rust',  # Языки
    'machine_learning', 'artificial_intelligence', 'bigdata', 'data_engineering',  # AI/Data
    'devops', 'infosecurity', 'network_technologies', 'sys_admin', 'linux',  # Администрирование
    'web_design', 'ui_ux', 'frontend',  # Дизайн
    'mobile_dev', 'android', 'ios',  # Мобайл
    'algorithms', 'mathematics',  # Наука
    'career', 'freelance', 'personnel', 'relocation',  # Карьера
    'startups', 'pm', 'agile',  # Менеджмент
    'crypto', 'blockchain', 'iot', 'arduino'  # Железо и крипта
]

PAGES_PER_HUB = 50
OUTPUT_FILE = 'habr_big_data_20k.csv'
BASE_URL = 'https://habr.com/ru/hubs/{}/articles/page{}/'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
}


def parse_metric(text_str):
    if not text_str: return 0
    clean = text_str.strip().replace('К', 'K').replace('М', 'M').replace(',', '.').replace('+', '')
    try:
        if 'K' in clean:
            return int(float(clean.replace('K', '')) * 1000)
        elif 'M' in clean:
            return int(float(clean.replace('M', '')) * 1000000)
        return int(float(clean))
    except:
        return 0


def main():
    columns = ['date', 'author', 'views', 'likes', 'comments', 'reading_time_min', 'complexity', 'tags', 'title',
               'hub_source', 'url_id']

    seen_ids = set()
    total_saved = 0

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(columns)

        print(f"🚀 ЗАПУСК INDUSTRIAL PARSER. Цель: 20 000+ статей.")
        print(f"📋 Очередь: {len(HUBS)} хабов.")

        for hub_idx, hub in enumerate(HUBS):
            print(f"\n[{hub_idx + 1}/{len(HUBS)}] 🌊 Входим в хаб: {hub.upper()} (Всего сохранено: {total_saved})")

            for page in range(1, PAGES_PER_HUB + 1):
                url = BASE_URL.format(hub, page)

                try:
                    response = requests.get(url, headers=HEADERS, timeout=10)

                    if response.status_code != 200:
                        # Если 404, значит страницы в этом хабе кончились
                        if response.status_code == 404:
                            break
                        time.sleep(2)
                        continue

                    soup = BeautifulSoup(response.text, 'html.parser')
                    articles = soup.find_all('article', class_='tm-articles-list__item')

                    if not articles:
                        break

                    new_items_on_page = 0
                    for article in articles:
                        try:
                            # Получаем ID статьи из ссылки
                            title_link = article.find('h2', class_='tm-title').find('a')
                            if not title_link: continue

                            article_url = title_link['href']
                            # Ссылка вида /ru/articles/123456/ -> берем 123456
                            article_id = article_url.split('/')[-2]

                            if article_id in seen_ids:
                                continue

                            seen_ids.add(article_id)

                            # --- СБОР МЕТРИК ---
                            title = title_link.text.strip()

                            user_link = article.find('a', class_='tm-user-info__username')
                            author = user_link.text.strip() if user_link else 'anonymous'

                            time_tag = article.find('time')
                            pub_date = time_tag['datetime'][:10] if time_tag else 'no_date'

                            complex_span = article.find('span', class_='tm-article-complexity__label')
                            complexity = complex_span.text.strip() if complex_span else "Normal"

                            read_time_span = article.find('span', class_='tm-article-reading-time__label')
                            read_time = int(read_time_span.text.split()[0]) if read_time_span else 0

                            likes_tag = article.find('div', class_='tm-votes-meter__value')
                            likes = parse_metric(likes_tag.text) if likes_tag else 0

                            counters = article.find_all('span', class_='tm-icon-counter__value')
                            views = 0
                            for c in counters:
                                val = parse_metric(c.text)
                                if val > views: views = val

                            comm_tag = article.find('span', class_='tm-article-comments-counter-link__value')
                            comments = parse_metric(comm_tag.text) if comm_tag else 0

                            hubs = article.find_all('a', class_='tm-publication-hub__link')
                            tags = ", ".join([h.text.strip().replace('*', '') for h in hubs])

                            writer.writerow(
                                [pub_date, author, views, likes, comments, read_time, complexity, tags, title, hub,
                                 article_id])
                            new_items_on_page += 1
                            total_saved += 1

                        except Exception:
                            continue

                    sys.stdout.write(f".")
                    sys.stdout.flush()

                    time.sleep(random.uniform(0.5, 1.5))

                except Exception as e:
                    continue

            print(f" Готово. Текущий итог: {total_saved} уникальных.")

    print(f"\nДанные успешно собрано, собрано {total_saved} статей. Файл: {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
