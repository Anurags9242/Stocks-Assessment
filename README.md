# Stock Data ETL Process

1. You are asked to create an ETL process to inject a big stock pricing data into a database with limited a resource(in terms of memory and CPU).  The data is in 10 csv files of total size  00GB in random order (all stocks for 1 date can be in multiple files),  and your ETL process will be run in a server with memory 32G and 4CPUs, and free disk space 100GB. The data structure of the data is in the following format: date:: datetime(%Y-%m-%d), id::int, price::float, trade_volume::int. The price values will be saved into a price table which has the following structure: date, stk_001,stk_002, stk_003,…,stk_200. Assume there are 200 stocks in the data and the ids for the stocks are from 1 to 200. And save the trade_volume to a volume table with the following column structure: date, stk_001, stk_002, …, stk_200. Find a most efficient solution in terms of run time and resources. 
2. Calculate stock returns using above data.

---

## Table of Contents

- [Overview](#overview)
- [Setup and Installation](#setup-and-installation)
  - [Prerequisites](#prerequisites)
  - [Create a Virtual Environment](#create-a-virtual-environment)
  - [Install Dependencies](#install-dependencies)
  - [Create a .env File](#create-a-env-file)
- [Running the ETL Process](#running-the-etl-process)
- [Expected Output](#expected-output)
- [Calculation of Stock Returns](#calculation-of-stock-returns)
- [Efficiency Considerations](#efficiency-considerations)
- [Assumptions](#assumptions)

---

## Overview

This ETL process performs the following operations:

- **Ingestion:**  
  Reads multiple CSV files in chunks using pandas and processes them in parallel using Python’s `multiprocessing.Pool` (utilizing 4 CPUs).
- **Data Transformation:**  
  Pivots the CSV data to convert rows into columns so that the data is stored in two tables (`price` and `volume`) with the following structure:  
  `date, stk_001, stk_002, …, stk_200`.
- **Database Operations:**  
  Uses PostgreSQL upserts (with `ON CONFLICT`) to insert/update data efficiently.
- **Calculation:**  
  Computes daily stock returns using SQL window functions (LAG) in a new table called `returns`.

---

## Setup and Installation

### Prerequisites

- Python 3.7 or higher.
- PostgreSQL instance.
- CSV files containing the stock data.

### Create a Virtual Environment

Create a Python virtual environment:
```bash
python3 -m venv venv
```

Activate the virtual environment:

- **On Linux/Mac:**
  ```bash
  source venv/bin/activate
  ```
- **On Windows:**
  ```bash
  venv\Scripts\activate
  ```

### Install Dependencies

Install the packages using pip:
```bash
pip install -r requirements.txt
```

### Create a .env File

Create a file named `.env` in the root directory of the project and include the following configuration parameters:

```env
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_HOST=your_database_host
DB_PORT=your_database_port
DATA_PATH=path_to_your_csv_files_directory
```

Replace the placeholder values with your actual database credentials and the file path to your CSV files.

---

## Running the ETL Process

After the virtual environment is set up and the dependencies are installed, run the ETL script:

```bash
python etl.py
```

The script will:
- Create the `price` and `volume` tables if they do not already exist.
- Process each CSV file in parallel using 4 processes.
- Insert or update data if already exist in the database.
- Calculate daily returns and create the `returns` table.

---

## Expected Output
Console should output following:

![img.png](images%2Fimg.png)

In PostgreSQL database, three tables will be created:
- **price:** Containing pivoted price data with columns: `date, stk_001, …, stk_200`.
- **volume:** Containing pivoted trade volume with columns: `date, stk_001, …, stk_200`.
- **returns:** Containing the daily stock returns calculated for each stock and table will have columns: `date, stk_001, …, stk_200`.

---

## Calculation of Stock Returns

The daily return for each stock is calculated using the formula:

```
daily_return = ((Today's Investment - Previous Day's Investment) / Previous Day's Investment)
```

Where:
- **Today's Investment** is computed as `price * trade_volume`.
- **Previous Day's Investment** is fetched using the SQL `LAG` window function.

---

## Efficiency Considerations

- **Chunk-Based Processing:**  
  I have used Chunks to read CSV files (default `CHUNK_SIZE = 1000`) so that memory usage is minimized even with large files.
  
- **Parallel Processing:**  
  I have used `multiprocessing.Pool` (with 4 processes) to leverage available CPU resources, so that file processing occurs concurrently.
  
- **Database Upserts:**  
  I have used `ON CONFLICT` in PostgreSQL to efficiently insert/update without requiring separate queries, reducing overhead.
  
- **SQL Window Functions:**  
  I have used `LAG` function to calculate returns in the database so that complex computations are offloaded from the application layer, thus enhancing the overall performance.

---

## Assumptions

- **Data Format:**  
  The CSV files are expected to have the following columns:  
  `date` (formatted as `%Y-%m-%d`), `id` (integer from 1 to 200), `price` (float), and `trade_volume` (integer).
  
- **Table Schema:**  
  The `price` and `volume` tables are structured with a primary key on `date`.
  
- **Duplicate Handling:**  
  Duplicate rows are removed from each chunk using `drop_duplicates()` to ensure data consistency.
  
- **Database Connection:**  
  The PostgreSQL database is accessible using the credentials in the `.env` file. 

