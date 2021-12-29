import time
from timeit import timeit

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc
import pandas as pd
import pyarrow.compute as pc
import os
import sqlalchemy
from sqlalchemy import create_engine


def read():
    changePath = 'D:\data\parquet\\'
    testPath = 'D:\data\parquet\\kw_flood.parquet'
    fileName = 'kw.parquet'
    file = 'D:\data\parquet\kw.parquet'
    chFileName = fileName.rsplit('.')[0]
    file_list = os.listdir(changePath)

    # ts = time.time()
    # df = pd.read_parquet(testPath)
    # print(df, str(round(time.time() - ts, 2)) + ' sec')
    #
    # ts = time.time()
    # dataframe = df.columns.values.tolist()
    # print(dataframe, str(round(time.time() - ts, 2)) + ' sec')
    #
    # ts = time.time()
    # pyarrow_table = pc.read_csv(file)
    # print(pyarrow_table.schema)

    parquet = pq.read_metadata(file, memory_map=True)
    print(parquet)

    parquet1 = pq.read_schema(file)
    print(parquet1)

    parquet2 = pq.read_table(file)
    print(parquet2)



    parquet_file = pq.ParquetFile(file)
    print(parquet_file.metadata)

    print(parquet_file.schema)

    print(parquet_file.read_row_group(0))









    # engine = create_engine("postgresql://postgres:postgres@localhost:5432/postgres")
    # engine.execute("DROP TABLE IF EXISTS public"+'.'+chFileName)  # drop table if exists
    #
    # df.to_sql(name=chFileName,
    #              con=engine,
    #              schema='public',
    #              if_exists='fail',  # {'fail', 'replace', 'append'), default 'fail'
    #              index=True,
    #              index_label='id',
    #              chunksize=2,
    #              dtype={
    #                  'id': sqlalchemy.types.INTEGER(),
    #                  'date': sqlalchemy.DateTime(),
    #                  'name': sqlalchemy.types.VARCHAR(100),
    #                  'age': sqlalchemy.types.INTEGER(),
    #                  'math_score': sqlalchemy.types.Float(precision=3),
    #                  'pass_yn': sqlalchemy.types.Boolean()
    #              })



    # for i in file_list:
    #     df = pd.read_parquet(i)








if __name__ == '__main__':
    read()