import time
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc
import os


def print_hi():
    time
    defaultPath = 'D:\data\csvSample\\'
    changePath = 'D:\data\parquet\\'

    file_list = os.listdir(defaultPath)
    print(file_list)

    for i in file_list:
        py_table = pc.read_csv(defaultPath+i, parse_options=pc.ParseOptions(delimiter="\t"))
        df = py_table.to_pandas()
        table = pa.Table.from_pandas(df)
        changeFile = i.rsplit('.')[0]
        pq.write_table(table, changePath+changeFile+".parquet")

        print('기존 파일 : '+i)
        print('기존 파일 데이터 : ', os.path.getsize(defaultPath+i))
        print('변환 파일 : '+i)
        print('변환 파일 데이터 : ', os.path.getsize(changePath+changeFile+".parquet"))



    print('변환 종료')



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
