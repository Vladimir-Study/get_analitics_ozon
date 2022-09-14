import asyncio

import asyncpg
import random
from support_func import insert_date, get_account_list
from pprint import pprint
from analytic_part_one import get_analytic_one_in_db
from analytic_part_two import get_analytic_two_in_db
import os.path
from pprint import pprint
from datetime import datetime

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
        limit: int = 1000, offset: int = 0) -> None:
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
        'limit': limit,
        'offset': offset
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(
                url=url,
                headers=headers,
                json=data
        ) as resp:
            count = 0
            while count != 10:
                try:
                    response = await resp.json()
                    print(response)
                    if resp.status == 200:
                        if 'result' in response.keys():
                            return response['result']['data']
                        else:
                            return ['Repeat', f"Status code: {resp.status}. Message: {response['message']}"]
                    elif resp.status == 429:
                        print(count)
                        print(response['message'], f'Status code: {resp.status}', sep='\n')
                        count += 1
                        await asyncio.sleep(5)
                        if count < 10:
                            continue
                        else:
                            return ['Repeat', f"Status code: {resp.status}. Message: {response['message']}"]
                    else:
                        print(response['message'], f'Status code: {resp.status}', sep='\n')
                        return ['Repeat', f"Status code: {resp.status}. Message: {response['message']}"]
            except ContentTypeError:
                break
            except Exception as E:
                print(f'Exception in get analytics: {E}')


async def delete_dublicate() -> None:
    conn = await asyncpg.connect(
        host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
        port='6432',
        database='market_db',
        user=os.environ['DB_LOGIN'],
        password=os.environ['DB_PASSWORD'],
        ssl='require'
    )
    table_one = await conn.execute('DELETE FROM data_analytics_bydays1 WHERE ctid IN (SELECT ctid FROM (SELECT *, '
                       'ctid, row_number()OVER (PARTITION BY sku_id, date ORDER BY id DESC) FROM data_analytics_bydays1)s '
                       'WHERE row_number >= 2)'
                       )
    table_two = await conn.execute('DELETE FROM data_analytics_bydays2 WHERE ctid IN (SELECT ctid FROM (SELECT *, '
                       'ctid, row_number()OVER (PARTITION BY sku_id, date ORDER BY id DESC) FROM data_analytics_bydays2)s '
                       'WHERE row_number >= 2)'
                       )
    print('Deletion of duplicates in the table of orders is completed')
    print(f'Table one: {table_one}, table two: {table_two}')


async def main() -> None:
    account_list = await get_account_list()
    len_list_one = 0
    len_list_two = 0
    visited_account = []
    analitic_log = []
    try:
        for account in account_list:
            analitic_log.append({'date': datetime.today().strftime('%x-%X')})
            api_id_dict = {'api_id': account['client_id']}
            analitic_log[-1] = {**analitic_log[-1], **api_id_dict}
            if account['client_id'] not in visited_account:
                lst_analytics_one = [] 
                lst_analytics_two = []
                count_offset = 0
                len_orders = 1000
                repeat = 0
                while len_orders == 1000:
                    try:
                        print(count_offset)
                        part_one = await get_analytic(account['client_id'], account['api_key'], dimension, metric_one,
                                                               actual_date['date_from'], actual_date['date_to'], offset=count_offset*1000)
                        if len(part_one) == 2 and part_one[0] == 'Repeat':
                            repeat += 1
                            error = {'Error': part_one[1]}
                            analitic_log[-1] = {**analitic_log[-1], **error}
                            if repeat == 10:
                                break
                            print(f'Part one: offset = {count_offset}, repeat = {repeat}')
                            count_offset += 1
                            continue
                        else:
                            repeat = 0
                        if (len(part_one) >= 1 and part_one[0] != 'Repeat') or part_one == []:
                            lst_analytics_one = [*lst_analytics_one, *part_one]
                            print(f'Len one: {len(lst_analytics_one)}')
                            len_orders = len(part_one)
                        count_offset += 1
                    except Exception as E:
                        print(f'Error while one: {E}')
                if lst_analytics_one != []:
                    metric_dict_one = {'dataset1': len(lst_analytics_one)}
                    analitic_log[-1] = {**analitic_log[-1], **metric_dict_one}
                    await get_analytic_one_in_db(account['client_id'], lst_analytics_one)
                else:
                    metric_dict_one = {'dataset1': len(lst_analytics_one)}
                    analitic_log[-1] = {**analitic_log[-1], **metric_dict_one}
                    print('Count order part one = 0')
                len_list_one += len(lst_analytics_one)
                count_offset = 0
                len_orders = 1000
                repeat = 0
                while len_orders == 1000:
                    try:
                        print(count_offset)
                        part_two = await get_analytic(account['client_id'], account['api_key'], dimension, metric_two,
                                                               actual_date['date_from'], actual_date['date_to'], offset=count_offset*1000)
                        if len(part_two) == 2 and part_two[0] == 'Repeat':
                            repeat += 1
                            error = {'Error': part_one[1]}
                            analitic_log[-1] = {**analitic_log[-1], **error}
                            if repeat == 10:
                                break
                            print(f'Part two: offset = {count_offset}, repeat = {repeat}')
                            count_offset += 1
                            continue
                        else:
                            repeat = 0
                        if (len(part_two) >= 1 and part_two[0] != 'Repeat') or part_two == []:
                            lst_analytics_two = [*lst_analytics_two, *part_two]
                            print(f'Len two: {len(lst_analytics_two)}')
                            len_orders = len(part_two)
                        count_offset += 1
                    except Exception as E:
                        print(f'Error while two: {E}')
                if lst_analytics_two != []:
                    metric_dict_two = {'dataset2': len(lst_analytics_two)}
                    analitic_log[-1] = {**analitic_log[-1], **metric_dict_two}
                    await get_analytic_two_in_db(account['client_id'], lst_analytics_two)
                else:
                    metric_dict_two = {'dataset2': len(lst_analytics_two)}
                    analitic_log[-1] = {**analitic_log[-1], **metric_dict_two}
                    print('Count order part two = 0')
                len_list_two += len(lst_analytics_two)
                visited_account.append(account['client_id'])
                print(visited_account)
            continue
        print(f"Count one: {len_list_one}, two: {len_list_two}")
        pprint(analitic_log)
        await delete_dublicate()
    except Exception as E:
        print(f'Error: {E}')


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
