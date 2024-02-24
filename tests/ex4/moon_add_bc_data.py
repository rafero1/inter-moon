import os
import sys
from moon_client.moon_client import MoonClient
from timeit import default_timer as timer
from datetime import datetime
import config


def ellipsize(string: str, length: int) -> str:
    return string[:length] + '...' + string[-length:]


dir_path = os.path.dirname(os.path.realpath(__file__))
params = config.Config(dir_path + '/db.cfg')
factor = 1
try:
    factor = sys.argv[1]
except IndexError:
    print('Please provide a factor number as an argument. Example: python3 moon_add_sql_data.py 1')

queries = []
times = []
results = []

chosen_query = f"sales_data_{str(factor)}x.sql"

print(f"{datetime.now()}: Reading {chosen_query}.")
with open(os.path.join(chosen_query), 'r', encoding='utf-8') as file:
    for line in file:
        queries.append(line.replace('\n', ''))

moon_client = MoonClient(
    str(params['moon']['host']), int(params['moon']['port']))

for i, query in enumerate(queries):
    start = timer()
    result = moon_client.send_query(query)
    end = timer()

    result = result.decode('utf-8')

    response_time = (end - start) * 1000

    times.append(response_time)
    results.append(result)

    print(f"{datetime.now()}: {i+1}/{len(queries)} {ellipsize(result, 50)} in {response_time} ms.")
