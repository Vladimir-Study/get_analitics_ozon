import asyncio
from support_func import insert_date, get_account_list
from pprint import pprint
from analytic_part_one import get_analytic_one_in_db
from analytic_part_two import get_analytic_two_in_db
import os.path

import aiohttp
import json

metric_one = ['hits_view_search', 'hits_view_pdp', 'hits_view', 'hits_tocart_search',
              'hits_tocart_pdp', 'hits_tocart', 'session_view_search', 'session_view_pdp', 'session_view',
              'conv_tocart_search', 'conv_tocart_pdp', 'conv_tocart', 'revenue'
              ]
metric_two = ['returns', 'cancellations', 'ordered_units', 'delivered_units', 'adv_view_pdp',
              'adv_view_search_category', 'adv_view_all', 'adv_sum_all', 'position_category', 'postings',
              'postings_premium'
              ]
dimension = ['sku', 'day', 'category1', 'brand']
actual_date = insert_date()


async def get_analytic(client_id: str, api_key: str, dimensions: list, metric: list, date_from, date_to,
                       file_name: str, limit: int = 100) -> None:
    url = 'https://api-seller.ozon.ru/v1/analytics/data'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/104.0.0.0 Safari/537.36',
        'Content-Type': 'application/json',
        'Client-Id': client_id,
        'Api-Key': api_key
    }
    data = {
        'date_from': date_from,
        'date_to': date_to,
        'metrics': metric,
        'dimension': dimensions,
        'limit': limit
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    url=url,
                    headers=headers,
                    json=data
            ) as resp:
                response = await resp.json()
                if resp.status == 200:
                    return response['result']['data']
                else:
                    print(response['message'])
                    return []
    except Exception as E:
        print(f'Exception in get analytics: {E}')


async def main() -> None:
    account_list = await get_account_list()
    for account in account_list:
        lst_analytics_one = await get_analytic(account['client_id'], account['api_key'], dimension, metric_one,
                                               actual_date['date_from'], actual_date['date_to'], 'analytic_part_one')
        await get_analytic_one_in_db(account['client_id'], lst_analytics_one)
        lst_analytics_two = await get_analytic(account['client_id'], account['api_key'], dimension, metric_two,
                                               actual_date['date_from'], actual_date['date_to'], 'analytic_part_two')
        await get_analytic_two_in_db(account['client_id'], lst_analytics_two)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
