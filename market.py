import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Функция получает список товаров магазина яндкс маркет

    Args:
    page (int): страница с которой нужно начать получение товара
    campaign_id (int): id кампании
    access_token (str): токен доступа к яндекс маркету

    Returns:
    dict: словарь содержащий список товаров
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Функция обновляет остатки товаров в магазине яндекс маркет
    
    Args:
    stocks (list): список словарей с остатками товара в магазине
    campaign_id (int): id кампании
    access_token (str): токен доступа к яндекс маркету

    Returns:
    dict: JSON-объекст с обновлёнными остатками
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    '''Функция обновляет цены товаров в магазине яндекс маркет

    Args:
    prices (list): список словарей с ценой товара
    campaig_id (int): id кампании 
    acecess_token (str): токен доступа к яндекс маркету

    Returns:
    dict: JSON-объекст, содержащий информацию об обновлении цен.
    '''
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Функция получения артикулов товаров магазина яндекс маркет

    Args:
    campaign_id (int): id кампании
    market_token (str): токен продавца яндекс маркета

    Returns:
    list: список артикулов товаров магазина яндекс маркет
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    '''Функция создаёт список словарей с остатками товара

    Args:
    watch_remnants (list): список словарей с остатками товара в магазине casio
    offer_ids (list): список артикулов то варов магазина яндекс маркета
    warehouse_id (int): id магазина ядекс маркета

    Returns:
    list: список словарей содержащий код товара и остатки
    '''
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    '''Функция создаёт список словарей с ценой товара

    Args:
    watch_remnants (list): список словарей с остатками товара в магазине casio
    offer_ids (list): список артикулов товаров магазина яндекс маркет

    Returns:
    list: спиисок словарей содержащий артикул товара и цену
    '''    

    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices

 
async def upload_prices(watch_remnants, campaign_id, market_token):
    '''Функция обновляет цены товаров в магазине яндекс маркет

    Args:
    watch_remnants (list): список словарей с остатками товара в магазине casio
    campaign_id (int): id кампании
    market_token (str): токен продавца яндекс маркета

    Returns:
    list: список словарей с обновлёнными ценами
    '''
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    '''Функция обновляет остатки товаров в магазине яндекс маркет

    Args:
    watch_remnants (list): список словарей с остатками товара в магазине casio
    campaign_id (int): id кампании в яндекс маркете
    market_token (str): токен продавца яндекс маркета

    Returns:
    tuple: кортеж из двух списков, not_empty - список словарей с остатками отличными от 0, и stocks - список словарей со всеми остатками
    '''
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    '''Для работы файлу необходимо передать:
    MARKET_TOKEN - токен продавца яндекс маркета
    FBS_ID - id кампании FBS
    DBS_ID - id кампании DBS
    WAREHOUSE_FBS_ID - id склада FBS
    WAREHOUSE_DBS_ID - id склада DBS

    Данный файл скачивает остатки товаров с магазина casio и обновляет их в магазине яндекс маркет
    '''
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
