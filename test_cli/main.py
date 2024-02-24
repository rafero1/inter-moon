import os
import csv
from datetime import datetime
from timeit import default_timer as timer
from pydash.strings import snake_case
from dotenv import load_dotenv
from moon_client.moon_client import MoonClient
from helpers.floats import s_to_ms
from helpers.strings import truncate_string

load_dotenv()

# This script enables the user to choose a query file to send over for processing,
# and then logs the query, query result and timestamps to a file.


def main():
    # MOON client. Change this to configure the client's host and port addresses.
    moon_client = MoonClient(
        os.environ['MOON_HOST'], int(os.environ['MOON_PORT']))

    # 1. Choose query
    query_choices = [file for file in os.listdir(
        "queries") if os.path.isfile(os.path.join("queries", file))]
    query_choices.sort()
    print(f"{datetime.now()}: Found {len(query_choices)} query files.\n")

    try:
        query_choices.append('Custom SQL (write)')
        evaluation = int(input('Choose a query to begin, or any other value to quit:\n' +
                               "\n".join([f"{i}: {file}" for i, file in enumerate(query_choices)]) + "\n\nPick a number -> "))

        if evaluation < 0 or evaluation >= len(query_choices):
            raise ValueError
    except Exception as e:
        print("")
        print(f"{datetime.now()}: {str(e)}.")
        print(f"{datetime.now()}: Exiting.")
        return

    print("")

    custom_query_flag = False
    custom_query = ""
    chosen_query = ""
    if evaluation == len(query_choices)-1:
        custom_query_flag = True
        custom_query = input('SQL -> ')

    try:
        enable_logging = input('Enable Logging? (y/n)\n')

        if enable_logging != 'y' and enable_logging != 'n':
            raise ValueError
    except Exception as e:
        print("")
        print(f"{datetime.now()}: {str(e)}.")
        print(f"{datetime.now()}: Exiting.")
        return

    print("")

    # 2. Handle query execution
    try:
        queries = []
        times = []
        results = []

        if (not custom_query_flag):
            chosen_query = query_choices[evaluation]

            print(f"{datetime.now()}: Reading {chosen_query}.")
            with open(os.path.join("queries", chosen_query), 'r', encoding='utf-8') as file:
                for line in file:
                    queries.append(line.replace('\n', ''))
        else:
            queries.append(custom_query)

        print(f"{datetime.now()}: Starting experiment.")
        print(f"Query  |  Result  |  Response time (ms)")
        for query in queries:
            start = timer()
            result = moon_client.send_query(query)
            end = timer()

            result = result.decode('utf-8')

            response_time = s_to_ms(end - start, 5)

            times.append(response_time)
            results.append(result)

            print(
                '{}\t {}\t {}'.format(
                    truncate_string(query, 25),
                    truncate_string(result, 12),
                    response_time
                )
            )
    except Exception as e:
        print("")
        print(f"{datetime.now()}: {str(e)}.")
        print(f"{datetime.now()}: Exiting.")
        return

    # 3. Logging
    if (enable_logging == 'y'):
        now = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        logname = 'custom' if custom_query_flag else snake_case(
            chosen_query[:-4])
        logfile = f"{logname} {now}.csv"
        print(f"{datetime.now()}: Results written to {logfile}")

        with open(os.path.join("log", logfile), 'w+', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['query', 'results', 'response_time'])
            for i in range(0, len(queries)):
                csv_writer.writerow([queries[i], results[i], times[i]])

    print(f"{datetime.now()}: Done.")

    # 4. Restart
    main()


if __name__ == "__main__":
    main()
