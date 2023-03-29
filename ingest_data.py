#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine


def main(params):
       user = params.user
       password = params.password
       host = params.host
       port = params.port
       db = params.db
       table_name = params.table_name
       url = params.url
       print(f'The url is {url}')
       csv_name = 'output.csv'

       os.system(f'wget -q -O - {url} | gzip -d > {csv_name}')

       engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')



       df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, low_memory=False)



       df = next(df_iter)


       df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
       df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)



       df.head(0).to_sql(con=engine, name = table_name, if_exists='replace')



       while True:
              
              t_start = time()
              
              df = next(df_iter)
              
              df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
              df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
              
              df.to_sql(con=engine, name = table_name, if_exists='append')
              
              t_end = time()
              
              print('insert another chunk...%.3f' %(t_end-t_start))

if __name__ == '__main__':
       parser = argparse.ArgumentParser(description='Ingest csv data to postgres')

       parser.add_argument('--user', help='user name for postgres')
       parser.add_argument('--password', help='password for postgres')
       parser.add_argument('--host', help='host for postgres')
       parser.add_argument('--port', help='port for postgres')
       parser.add_argument('--db', help='database for postgres')
       parser.add_argument('--table_name', help='name of the database where we will write the data')
       parser.add_argument('--url', help='url of the csv')

       args = parser.parse_args()

       main(args)