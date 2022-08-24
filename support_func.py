from datetime import datetime, timedelta
from pprint import pprint
import asyncio
import asyncpg
import os
from dotenv import load_dotenv
load_dotenv()


def insert_date() -> dict:
    date_to = datetime.now()
    date_from = date_to + timedelta(days=-1)
    return {'date_to': date_to.date().strftime('%Y-%m-%d'), 'date_from': date_from.date().strftime('%Y-%m-%d')}


async def get_account_list() -> list:
    returned_dict = []
    async with asyncpg.create_pool(
            host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
            port='6432',
            database='market_db',
            user=os.environ['db_login'],
            password=os.environ['db_password'],
            ssl='require'
    ) as pool:
        async with pool.acquire() as conn:
            records = await conn.fetch('select * from account_list where mp_id = 1')
            for record in records:
                flag = False
                for _ in returned_dict:
                    if record['client_id_api'] == _['client_id'] or record['client_id_api'] in ['', None, 'null']:
                        flag = True
                if not flag or returned_dict == []:
                    returned_dict.append({'client_id': record['client_id_api'], 'api_key': record['api_key']})
            return returned_dict


if __name__ == '__main__':
    pprint(insert_date())