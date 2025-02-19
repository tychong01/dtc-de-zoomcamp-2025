import os
import time
import pandas as pd
from sqlalchemy import create_engine

CHUNKSIZE = 100000
DBUSER = os.environ.get("POSTGRES_USER")
DBPWD = os.environ.get("POSTGRES_PASSWORD")
DBHOST = os.environ.get("POSTGRES_HOST")
DBPORT = os.environ.get("POSTGRES_PORT")
DBNAME = os.environ.get("POSTGRES_DB")

# print(pd.io.sql.get_schema(df_green_tripdata, name='green_taxi_data', con=engine))

def load_nytaxidata(engine, df, target_table):
    first_chunk = next(df)
    first_chunk.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace')
    first_chunk.to_sql(name=target_table, con=engine, if_exists='append', index=False)

    print(f"Created table and inserted first chunk: {first_chunk.shape[0]}")
    while True:
        try:
            start = time.time()
            chunk = next(df)
            chunk.to_sql(name=target_table, con=engine, if_exists='append', index=False)
            end = time.time()
            print(f"Inserted another chunk: {chunk.shape[0]}, took {end-start} seconds")
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == "__main__":
    engine = create_engine(f"postgresql://{DBUSER}:{DBPWD}@{DBHOST}:{DBPORT}/{DBNAME}")
    engine.connect()
    print(f"Processing Green Taxi data...")
    df_greentaxi = pd.read_csv(
        '/data/green_tripdata_2019-10.csv.gz',
        compression='gzip',
        iterator=True,
        chunksize=CHUNKSIZE,
        low_memory=False,
        parse_dates=['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    )
    load_nytaxidata(engine, df_greentaxi, 'green_taxi')
    print(f"Processing Taxi Zonedata...")
    df_taxizone = pd.read_csv(
        '/data/taxi_zone_lookup.csv',
        iterator=True,
        chunksize=CHUNKSIZE,
        low_memory=False
    )
    load_nytaxidata(engine, df_taxizone, 'taxi_zone')


