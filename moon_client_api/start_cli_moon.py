import csv
from datetime import datetime
from moon_client.moon_client import MoonClient

def truncate_string(str_data, max_size):
    return (str_data[:max_size] + '..') if len(str_data) > max_size else str_data


moon_client = MoonClient('localhost', 9654)

queries = []
results = []
times = []

evaluation = int(input('Evaluation: \n0 - Twitter \n1 - Voter\n'))
if evaluation == 0:
    queries_file = 'queries_twitter.txt'
    print('\nChosen: Twitter\n')
elif evaluation == 1: 
    queries_file = 'queries_voter.txt'
    print('\nChosen: Voter\n\n')
else:
    raise Exception('Uncognized Evaluation')

logfile = input('Logfile: ')
logfile += '.csv'

with open(queries_file, 'r') as query_file:
    for line in query_file:
        queries.append(line.replace('\n', ''))

for query in queries:
    before = datetime.now()
    result = moon_client.send_query(query)
    after = datetime.now()
    result = result.decode('utf-8')
    response_time = after-before
    rt_millis = response_time.days * 86400000
    rt_millis += response_time.seconds * 1000
    rt_millis += round(response_time.microseconds/float(1000), 1)
    times.append(
        rt_millis
    )
    results.append(
        result
    )
    print(
        '{}\t {}\t {}'.format(
            truncate_string(query, 25),
            truncate_string(result, 12),
            rt_millis
        )
    )

with open(logfile, 'w') as csv_file:
    csv_writer = csv.writer(csv_file)
    for i in range(0, len(queries)):
        csv_writer.writerow([queries[i], results[i], times[i]])
