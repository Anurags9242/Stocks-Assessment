import os
import psycopg2
import pandas as pd
from multiprocessing import Pool
from dotenv import load_dotenv


load_dotenv()

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "example"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

FILE_PATH = os.getenv("DATA_PATH", "data/")

CHUNK_SIZE = 1000


def create_tables():
    "Create tables if not exists already"
    sql = f"""
    CREATE TABLE IF NOT EXISTS price (date DATE PRIMARY KEY, {",".join([f'stk_{str(i).zfill(3)} FLOAT' for i in range(1, 201)])});
    CREATE TABLE IF NOT EXISTS volume (date DATE PRIMARY KEY, {",".join([f'stk_{str(i).zfill(3)} FLOAT' for i in range(1, 201)])});
    """
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def process_file(file):
    """
    Reads file in chunks, transform it and load it into database"""
    print(f"Processing {file}")

    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                for chunk in pd.read_csv(file, chunksize=CHUNK_SIZE, parse_dates=["date"]):

                    # to remove duplicate rows
                    chunk = chunk.drop_duplicates()

                    date_price_df = chunk.pivot(index='date', columns='id', values='price').reset_index()
                    date_volume_df = chunk.pivot(index='date', columns='id', values='trade_volume').reset_index()

                    date_price_df.columns = ["date"] + [f"stk_{str(col).zfill(3)}" for col in date_price_df.columns[1:]]
                    date_volume_df.columns = ["date"] + [f"stk_{str(col).zfill(3)}" for col in
                                                         date_volume_df.columns[1:]]

                    # loop to update price data in database
                    for index, row in date_price_df.iterrows():
                        update_columns = []
                        insert_columns = ['date']
                        insert_placeholders = ['%s']
                        insert_values = [row['date']]

                        for col in date_price_df.columns:
                            if col == 'date':
                                continue

                            val = row[col]
                            if pd.notnull(val):
                                insert_columns.append(col)
                                insert_placeholders.append('%s')
                                insert_values.append(val)
                                update_columns.append(f"{col} = EXCLUDED.{col}")

                        if not update_columns:
                            continue

                        query = f"""
                            INSERT INTO price ({', '.join(insert_columns)})
                            VALUES ({', '.join(insert_placeholders)})
                            ON CONFLICT (date) DO UPDATE SET {', '.join(update_columns)}
                        """

                        # print(query, insert_values)
                        cur.execute(query, tuple(insert_values))

                    # loop to update volume data in database
                    for index, row in date_volume_df.iterrows():
                        update_columns = []
                        insert_columns = ['date']
                        insert_placeholders = ['%s']
                        insert_values = [row['date']]

                        for col in date_volume_df.columns:
                            if col == 'date':
                                continue

                            val = row[col]
                            if pd.notnull(val):
                                insert_columns.append(col)
                                insert_placeholders.append('%s')
                                insert_values.append(val)
                                update_columns.append(f"{col} = EXCLUDED.{col}")

                        if not update_columns:
                            continue

                        query = f"""
                            INSERT INTO volume ({', '.join(insert_columns)})
                            VALUES ({', '.join(insert_placeholders)})
                            ON CONFLICT (date) DO UPDATE SET {', '.join(update_columns)}
                        """
                        # print(query, insert_values)
                        cur.execute(query, tuple(insert_values))

                print(f"Finished processing {file}")

    except Exception as e:
        print(f"Error while processing {file}: {e}")


def calculate_daily_returns():
    "Recalculate returns using data in price and volume table"
    drop_query = "DROP TABLE IF EXISTS returns"

    stock_ids = [f"stk_{i:03d}" for i in range(1, 201)]

    investment_exprs = [
        f"p.{stk} * v.{stk} AS inv_{stk}" for stk in stock_ids
    ]

    return_exprs = [
        f"""((inv_{stk} - LAG(inv_{stk}) OVER (ORDER BY date)) /
        LAG(inv_{stk}) OVER (ORDER BY date)) AS {stk}""" for stk in stock_ids
    ]

    query = f"""
    CREATE TABLE returns AS
    WITH stock_investment AS (
    SELECT
        p.date, {', '.join(investment_exprs)}
    FROM public.price p
    JOIN public.volume v
        ON p.date = v.date
    )
    SELECT
    date, {', '.join(return_exprs)}
    FROM stock_investment
    ORDER BY date;
    """

    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            cur.execute(drop_query)
            cur.execute(query)


if __name__ == "__main__":
    print("Starting Ingestion..")

    create_tables()

    files = [os.path.join(FILE_PATH, f) for f in os.listdir(FILE_PATH) if os.path.isfile(os.path.join(FILE_PATH, f))]

    with Pool(4) as p:
        p.map(process_file, files)

    print("Ingestion completed.")

    calculate_daily_returns()

    print("Return calculated!")

