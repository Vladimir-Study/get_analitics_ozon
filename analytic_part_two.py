import asyncio
import json
import asyncpg
import os
import random
from dotenv import load_dotenv
from datetime import datetime
from pprint import pprint
load_dotenv()


async def request_generation(pool, analytic_data, api_id) -> None:
    await pool.execute('''INSERT INTO data_analytics_bydays2(api_id, sku_id, sku_name, date, category_id, category_name, 
                        brand_id, brand_name, returns, cancellations, ordered_units, delivered_units, 
                        adv_view_pdp, adv_view_search_category, adv_view_all, adv_sum_all, position_category, 
                        postings, postings_premium)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        $13, $14, $15, $16, $17, $18, $19)''', str(api_id),
                       analytic_data['dimensions'][0]['id'], analytic_data['dimensions'][0]['name'],
                       datetime.strptime(analytic_data['dimensions'][1]['id'], '%Y-%m-%d'),
                       analytic_data['dimensions'][2]['id'], analytic_data['dimensions'][2]['name'],
                       analytic_data['dimensions'][3]['id'], analytic_data['dimensions'][3]['name'],
                       analytic_data['metrics'][0], analytic_data['metrics'][1], analytic_data['metrics'][2],
                       analytic_data['metrics'][3], analytic_data['metrics'][4], analytic_data['metrics'][5],
                       analytic_data['metrics'][6], analytic_data['metrics'][7], analytic_data['metrics'][8],
                       analytic_data['metrics'][9], analytic_data['metrics'][10]
                       )


async def get_analytic_two_in_db(client_id: str, lst_analytics: list):
    tasks = []
    async with asyncpg.create_pool(
            host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
            port='6432',
            database='market_db',
            user=os.environ['DB_LOGIN'],
            password=os.environ['DB_PASSWORD'],
            ssl='require'
    ) as pool:
        count = 0
        chunk = 1000
        for _ in lst_analytics:
            count += 1
            task = asyncio.create_task(request_generation(pool, _, client_id))
            tasks.append(task)
            if len(tasks) == chunk or count == len(lst_analytics):
                await asyncio.gather(*tasks)
                tasks = []
                print(count)


if __name__ == '__main__':
    pass
