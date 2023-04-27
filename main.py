import sqlite3
import logging
import argparse
import requests
from xml.etree import ElementTree


# Функция создания таблицы курсов валют в БД
def create_table(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS currency_rates
                      (date text, code integer, name text, rate real, PRIMARY KEY (date, code))''')


# Функция записи курсов валют в БД
def insert_rates(conn, date, rates):
    cursor = conn.cursor()
    for rate in rates:
        cursor.execute("INSERT OR REPLACE INTO currency_rates VALUES (?, ?, ?, ?)", (date, rate['code'], rate['name'], rate['value']))
    conn.commit()


# Функция получения курсов валют от ЦБ РФ по дате
def get_rates_on_date(date):
    response = requests.post('https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx/GetCursOnDateXML', data={'On_date': date})
    response.encoding = 'utf-8'
    xml_string = response.text
    root = ElementTree.fromstring(xml_string)
    rates = []
    for child in root.iter():
        if child.tag == 'ValuteCursOnDate':
            code = int(child.find('Vcode').text)
            name = child.find('Vname').text
            value = float(child.find('Vcurs').text.replace(',', '.'))
            rates.append({'code': code, 'name': name, 'value': value})
    return rates


# Функция настройки логирования
def setup_logging(log_file):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Создаем обработчик для вывода в файл
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    # Создаем обработчик для вывода в консоль
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Создаем форматирование для вывода в оба обработчика
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # Назначаем форматирование для обработчиков
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Добавляем обработчики к логгеру
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


if __name__ == '__main__':
    # Парсим аргументы командной строки
    parser = argparse.ArgumentParser(description='Load official currency rates from the Central Bank of Russia into the database')
    parser.add_argument('date', type=str, help='Date of currency rates in the format DD.MM.YYYY')
    parser.add_argument('codes', type=str, help='Comma-separated list of currency codes (numeric ISO 4217 codes)')
    args = parser.parse_args()

    # Создаем логгер
    log_file = f'{__file__.split(".")[0]}.log'
    logger = setup_logging(log_file)

    # Подключаемся к БД
    conn = sqlite3.connect('currency_rates.db')
    create_table(conn)
    if __name__ == '__main__':
        # Функция создания БД и таблицы курсов валют
        def create_database():
            conn = sqlite3.connect('currency_rates.db')
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS currency_rates
                              (date text, code integer, name text, rate real, PRIMARY KEY (date, code))''')
            conn.close()


        create_database()
    # Получаем курсы валют и записываем их в БД
    try:
        rates = get_rates_on_date(args.date)
        codes = [int(code.strip()) for code in args.codes.split(',')]
        rates = [rate for rate in rates if rate['code'] in codes]
        insert_rates(conn, args.date, rates)
        logger.info(f'Successfully loaded {len(rates)} currency rates for {args.date}')
    except Exception as e:
        logger.error(f'Error while loading currency rates: {str(e)}')

    # Закрываем соединение с БД
    conn.close()
