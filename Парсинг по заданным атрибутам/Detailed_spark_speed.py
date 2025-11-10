import os
import pandas as pd
import time
import datetime
import threading
from queue import Queue
import logging
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log'),
        logging.StreamHandler()
    ]
)

# Конфигурация
CHUNK_SIZE = 250
START_FROM_CHUNK = 0
OUTPUT_FOLDER = "parsed_data_okved"
THREADS = 5
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

task_queue = Queue()


class ChunkProgress:
    def __init__(self, total):
        self.total = total
        self.processed = 0
        self.lock = threading.Lock()
        self.start_time = time.time()

    def update(self):
        with self.lock:
            self.processed += 1
            return self.processed

    def get_progress(self):
        with self.lock:
            elapsed = time.time() - self.start_time
            speed = self.processed / elapsed if elapsed > 0 else 0
            remaining = (self.total - self.processed) / speed if speed > 0 else 0
            return {
                'processed': self.processed,
                'total': self.total,
                'speed': speed,
                'remaining': remaining
            }


progress_trackers = {}


def worker():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1290,1080')
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-logging")
    service = Service(executable_path=ChromeDriverManager().install())

    while not task_queue.empty():
        chunk_num, chunk = task_queue.get()

        if chunk_num not in progress_trackers:
            progress_trackers[chunk_num] = ChunkProgress(len(chunk))

        driver = webdriver.Chrome(service=service, options=options)
        data = []
        start_idx = chunk_num * CHUNK_SIZE
        end_idx = min((chunk_num + 1) * CHUNK_SIZE, len(df))

        logging.info(f"Поток {threading.current_thread().name} начал чанк {chunk_num} ({start_idx + 1}-{end_idx})")

        try:
            for link in chunk['Ссылка']:
                try:
                    driver.get(link)
                    time.sleep(1)

                    name_xpth = '/html/body/div[3]/div[2]/div[2]/div[1]/div/div/div[1]/div/div[1]/div[2]'
                    nameS_xpth = '//h1[@itemprop="name"]'
                    INN_xpth = '/html/body/div[3]/div[2]/div[2]/div[1]/div/div/div[1]/div/div[8]/div[2]/span'
                    adress_xpth = '/html/body/div[3]/div[2]/div[2]/div[1]/div/div/div[1]/div/div[8]/div[2]/span'
                    okvedN_xpth = '/html/body/div[3]/div[2]/div[2]/div[1]/div/div/div[5]/div[1]/div[2]/div/div[1]'
                    okvedTit_xpth = '/html/body/div[3]/div[2]/div[2]/div[1]/div/div/div[5]/div[1]/div[2]/div/div[2]'

                    # Парсинг данных
                    name = driver.find_element(By.XPATH, '/html/body/div[3]/div[2]/div[2]/div[1]/div/div/div[1]/div/div[1]/div[2]').text if driver.find_elements(
                        By.XPATH, '/html/body/div[3]/div[2]/div[2]/div[1]/div/div/div[1]/div/div[1]/div[2]') else "99999"
                    nameS = driver.find_element(By.XPATH,
                                               '//h1[@itemprop="name"]').text if driver.find_elements(
                        By.XPATH, '//h1[@itemprop="name"]') else "99999"
                    INN = driver.find_element(By.XPATH,
                                                '/html/body/div[3]/div[2]/div[2]/div[1]/div/div/div[1]/div/div[8]/div[2]/span').text if driver.find_elements(
                        By.XPATH, '/html/body/div[3]/div[2]/div[2]/div[1]/div/div/div[1]/div/div[8]/div[2]/span') else "99999"
                    adress=driver.find_element(By.XPATH, '//span[@itemprop="address"]').text if driver.find_elements(
                        By.XPATH, '//span[@itemprop="address"]') else "99999"
                    lon = driver.find_element(By.CSS_SELECTOR, 'div.js-company-map').get_attribute(
                        "data-place-longitude") if driver.find_elements(By.CSS_SELECTOR, 'div.js-company-map') else "99999"
                    lat = driver.find_element(By.CSS_SELECTOR, 'div.js-company-map').get_attribute(
                        "data-place-latitude") if driver.find_elements(By.CSS_SELECTOR, 'div.js-company-map') else "99999"
                    okvedN = driver.find_element(By.XPATH,
                                                 '/html/body/div[3]/div[2]/div[2]/div[1]/div/div/div[5]/div[1]/div[2]/div/div[1]').text if driver.find_elements(
                        By.XPATH,
                        '/html/body/div[3]/div[2]/div[2]/div[1]/div/div/div[5]/div[1]/div[2]/div/div[1]') else "99999"
                    okvedTit = driver.find_element(By.XPATH, okvedTit_xpth).text if driver.find_elements(
                        By.XPATH,okvedTit_xpth) else "99999"

                    data.append({
                        'Название': name,
                        'Название Кратко': nameS,
                        'ИНН': INN,
                        'адрес' : adress,
                        'lat': lat,
                        'lon': lon,
                        'Код оквэд': okvedN,
                        'Основной вид деятельности': okvedTit,
                        'Ссылка': link
                    })

                    # Обновление прогресса
                    processed = progress_trackers[chunk_num].update()
                    if processed % 50 == 0:
                        progress = progress_trackers[chunk_num].get_progress()
                        remaining_time = str(datetime.timedelta(seconds=int(progress['remaining'])))
                        logging.info(
                            f"Чанк {chunk_num}: {processed}/{progress['total']} "
                            f"({processed / progress['total']:.1%}) | "
                            f"Скорость: {progress['speed']:.1f} it/s | "
                            f"Осталось: {remaining_time}"
                        )

                except Exception as e:
                    logging.error(f"Ошибка в чанке {chunk_num}: {str(e)}")
                    continue

            output_file = os.path.join(OUTPUT_FOLDER,
                                       f'spark_data_detailed_chunk-{chunk_num}_{start_idx + 1}-{end_idx}.csv')
            pd.DataFrame(data).to_csv(output_file, index=False)
            logging.info(f"Чанк {chunk_num} сохранен в {output_file}")

        finally:
            driver.quit()
            task_queue.task_done()


if __name__ == "__main__":
    # Загрузка данных
    df = pd.read_excel('input.xlsx')
    chunks = [df[i:i + CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    # Добавляем задачи в очередь
    for chunk_num, chunk in enumerate(chunks[START_FROM_CHUNK:], start=START_FROM_CHUNK):
        task_queue.put((chunk_num, chunk))

    # Запуск потоков
    threads = []
    for i in range(THREADS):
        thread = threading.Thread(target=worker, name=f"Thread-{i + 1}")
        thread.start()
        threads.append(thread)

    # Ожидание завершения
    task_queue.join()
    for thread in threads:
        thread.join()

    logging.info("\nВсе чанки обработаны!")