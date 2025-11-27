import os, platform
import numpy as np
from time import perf_counter_ns as timeit
from decimal import Decimal

import psycopg2 as sql
from psycopg2.extras import DictCursor
import pymongo as mongo
import dotenv as de

def cast_decimal(record: tuple, reference: list) -> dict:
    out = {}
    for i in range(len(record)):
        out[reference[i]] = float(record[i]) if isinstance(record[i], Decimal) else record[i]
    return out

if __name__ == "__main__":
    isEnv = de.load_dotenv('./sys.env')
    setPG = False
    setMn = False
    PSDB = os.getenv('POSTGRES_DB', "postgres")
    PSUSER = os.getenv('POSTGRES_USER', "postgres")
    PSPASS = os.getenv('POSTGRES_PASSWORD', "postgres")
    PSHOST = os.getenv('POSTGRES_HOST', "localhost")
    PSPORT = os.getenv('POSTGRES_PORT', "5432")
    MONGOURI = os.getenv('MONGO_URI', "mongodb://localhost:27017")
    column = None
    result = None
    runs = 30

    if isEnv:
        print("Environment loaded successfully")
        
        livedb = sql.connect(database=PSDB, user=PSUSER, password=PSPASS, host=PSHOST, port=PSPORT)

        star_times = None
        non_star_time = None
        print("Performing Test for Data with Star Schema")
        with open("OLAP_star_test.sql", mode="r", encoding="utf8") as f:
            queries = f.read().split("------")
            star_times = np.zeros(shape=(len(queries),runs), dtype=np.int64)

            for i, query in enumerate(queries):
                for j in range(runs):
                    with livedb.cursor() as live:
                        try:
                            dt = timeit()
                            live.execute(query)
                            #column = live.description
                            result = live.fetchall()
                            dt = timeit() - dt
                            star_times[i][j] = dt
                        except (Exception, sql.DatabaseError) as error:
                            print(f"Error: {error}")

        report = np.array2string(star_times, separator=",")
        report += "\nmeans: " + np.mean(star_times, axis=0)
        report += "\nstds: " + np.std(star_times, axis=0)
        report += "\nvariance: " + np.std(star_times, axis=0)
        report += "\nAll numbers are in nanoseconds (ns)"
        report += "\nSystem detail: " + platform.processor()
        
        with open("Star_test.txt", "x") as f:
            f.write(report)
        
        with open("OLAP_none_test.sql", mode="r", encoding="utf8") as f:
            queries = f.read().split("------")
            non_star_time = np.zeros(shape=(len(queries),runs), dtype=np.int64)

            for i, query in enumerate(queries):
                for j in range(runs):
                    with livedb.cursor() as live:
                        try:
                            dt = timeit()
                            live.execute(query)
                            result = live.fetchall()
                            dt = timeit() - dt
                            star_times[i][j] = dt
                        except (Exception, sql.DatabaseError) as error:
                            print(f"Error: {error}")

        report = np.array2string(non_star_time, separator=",")
        report += "\nmeans: " + np.mean(non_star_time, axis=0)
        report += "\nstds: " + np.std(non_star_time, axis=0)
        report += "\nvariance: " + np.std(non_star_time, axis=0)
        report += "\nAll numbers are in nanoseconds (ns)"
        report += "\nSystem detail: " + platform.processor()
        with open("Non_Star_test.txt", "x") as f:
            f.write(report)

        livedb.close()
