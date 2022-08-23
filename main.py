import asyncio
from pprint import pprint

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
                with open(f'{file_name}.json', 'w', encoding='utf-8') as file:
                    json.dump(response['result']['data'], file, indent=4)
    except Exception as E:
        print(f'Exception in get analytics: {E}')


async def main() -> None:
    await get_analytic(dimension, metric_one,
                       '2022-08-21', '2022-08-22', 'analytic_part_one')
    await get_analytic(dimension, metric_two,
                       '2022-08-21', '2022-08-22', 'analytic_part_two')


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
