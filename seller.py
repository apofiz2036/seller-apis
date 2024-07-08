import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Функция получает список товаров магазина озон.
    
    Args:
    last_id (int): id последнего товара
    client_id (int): id клиента магазина озон
    seller_token (str): токен продавца магазина озон

    Returns:
    dict: JSON-объект содержащий список товаров
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Функция получения артикулов товаров магазина озон.
    
    Args:
    client_id (int): id клиента магазина озон
    seller_token (str): токен продавца магазина озон

    Returns:
    list: список артикулов товаров магазина озон
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Функция обновляет цены товаров в магазине озон.
    
    Args:
    prices (list): список словарей с ценой товара
    client_id (int): id клиента магазина озон
    seller_token (str): токен продавца магазина озон

    Returns:
    list: JSON-объект с обновлёнными ценами
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Функция обновляет остатки товаров в магазине озон
    
    Args:
    stocks (list): список словарей с остатками товара
    client_id (int): id клиента магазина озон
    seller_token (str): токен продавца магазина озон

    Returns:
    list: JSON-объект с обновлёнными остатками
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Функция загружает с сайта casio файл с остатками товаров (ostatki.xls). 
    Разархивирует файл, загружает содержимое файла в базу данных и удаляет файл.

    Returns:
    list: список словарей с остатками товара
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")

    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    '''Функция создаёт список словарей с остатками товара
    
    Args:
    watch_remnants (list): список словарей с остатками товара в магазине casio
    offer_ids (list:) список артикулов товаров магазина озон

    Returns:
    list: список словарей stocks содержащий код товара и остатки
    '''
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    '''Функция создаёт список словарей с ценой товара
    
    Args:
    watch_remnants (list): список словарей с остатками товара в магазине casio
    offer_ids (list): список артикулов товаров магазина озон

    Returns:
    list: список словарей содержащий артикул товара и цену
    '''    
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Функция преобразует формат отображения цены.
    
    Функция принимет значение price в формате строки и возвращает так же строку, но в виде числа
    
    Пример: цена "5'990.00 руб." будет преобразована в '5990'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Функция разделяет список lst на части по n элементов
    
    На вход функция получает спиок и целое число на которое нужно поделить список
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

  
async def upload_prices(watch_remnants, client_id, seller_token):
    '''Функция обновляет цены товаров в магазине озон
    
    Args:
    watch_remnants (list): список словарей с остатками товара в магазине casio
    client_id (int): id клиента в озон
    seller_token (str): токен продавца озон

    Returns:
    list: JSON-объект с с обновлёнными ценами
    '''
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    '''Функция обновляет остатки товаров в магазине озон
    
    Args: 
    watch_remnants (list): список словарей с остатками товара в магазине casio
    client_id (int): id клиента в озон
    seller_token (str): токен продавца озон

    Returns:
    tuple: кортеж из двух списков, not_empty - список словарей с остатками отличными от 0, и stocks - список словарей со всеми остатками
    '''
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    '''Для работы файлу необходимо передать:
    SELLER_TOKEN - токен продавца магазина озон
    CLIENT_ID - id клиента магазина озон

    Данный файл скачивает остатки товаров с магазина casio и обновляет их в магазине озон
    '''
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
