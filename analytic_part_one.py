import asyncio
import json
import asyncpg
import os
from dotenv import load_dotenv
from datetime import datetime
from pprint import pprint
load_dotenv()


async def request_generation(pool, analytic_data, api_id) -> None:
    await pool.execute('''INSERT INTO data_analytics_bydays1(api_id, sku_id, sku_name, date, category_id, category_name, 
                        brand_id, brand_name, hits_view_search, hits_view_pdp, hits_view, hits_tocart_search, 
                        hits_tocart_pdp, hits_tocart, session_view_search, session_view_pdp, session_view, 
                        conv_tocart_search, conv_tocart_pdp, conv_tocart, revenue)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        $13, $14, $15, $16, $17, $18, $19, $20, $21)''', str(api_id),
                       analytic_data['dimensions'][0]['id'], analytic_data['dimensions'][0]['name'],
                       datetime.strptime(analytic_data['dimensions'][1]['id'], '%Y-%m-%d'),
                       analytic_data['dimensions'][2]['id'], analytic_data['dimensions'][2]['name'],
                       analytic_data['dimensions'][3]['id'], analytic_data['dimensions'][3]['name'],
                       analytic_data['metrics'][0], analytic_data['metrics'][1], analytic_data['metrics'][2],
                       analytic_data['metrics'][3], analytic_data['metrics'][4], analytic_data['metrics'][5],
                       analytic_data['metrics'][6], analytic_data['metrics'][7], analytic_data['metrics'][8],
                       analytic_data['metrics'][9], analytic_data['metrics'][10], analytic_data['metrics'][11],
                       analytic_data['metrics'][12]
                       )


async def get_analytic_one_in_db(client_id: str, lst_analytics: list):
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
        chunk = 10
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
