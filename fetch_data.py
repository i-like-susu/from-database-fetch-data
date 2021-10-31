import pymysql as ps
import pandas as pd
import os
import re
from dataclasses import dataclass


@dataclass
class FetchData():
    host: str
    port: int
    user: str
    password: str
    db: str
    ''''''
    sql = '''
            select a.date,a.msisdn,b.model_name from(
            select date,msisdn,tacid
            from(
            select 
            date(start_time) date,msisdn,tacid
            from db_fact_psup_http_20210101
            group by date,msisdn,tacid
            union all
            select 
            date(start_time) date,msisdn,tacid
            from db_fact_psup_https_20210101
            group by date,msisdn,tacid
            union all
            select 
            date(start_time) date,msisdn,tacid
            from db_fact_psup_flow_20210101
            group by date,msisdn,tacid
            )c
            group by date,msisdn,tacid
            ) as a left join dim_terminal as b on a.tacid = b.tacid
         '''
    ''''''
    sql = 'select * from EMP'

    def __enter__(self):
        '''
            连接数据库对象
        '''
        self.connection = ps.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.db,
            charset='utf8mb4',
            autocommit=True,
        )
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, type, value, trace):
        self.cursor.close()
        self.connection.close()

    def get_time_range(self, start_time, end_time):
        '''
            返回一个以天为粒度日期范围列表，参数start_time = '20210101' end_time = '20210201'
        '''
        self.time_range = pd.date_range(start=start_time,
                                        end=end_time,
                                        freq='D')

        self.date_list = [day.strftime('%Y%m%d') for day in self.time_range]

        return self.date_list

    def from_sql_into_dataframe(self, data):
        '''
            将取出来的数据转换为DataFrame格式,data:sql取出来的数据
        '''
        # 获取列名
        col_tuple = self.cursor.description
        col_name = [col[0] for col in col_tuple]

        # 保存为dataframe格式
        data_frame = pd.DataFrame(list(data), columns=col_name)
        return data_frame

    def save_data_into_excel(self, data_frame, sheet):
        data_frame.to_excel(FetchData.excel_path,
                            sheet_name=sheet,
                            index=False,
                            engine='xlsxwriter')

    def write_excel(self):
        '''
            一条sql语句执行这个函数
        '''
        self.cursor.execute(FetchData.sql)
        data = self.cursor.fetchall()
        data_frame = self.from_sql_into_dataframe(data)
        excel_path = os.path.join(os.path.expanduser("~"),
                                  'Desktop') + '\\' + 'fetch_data.csv'
        data_frame.to_csv(excel_path, index=False)
        print('数据已经保存到桌面')

    def write_excel_by_date(self, date_list):
        '''
            按照日期执行的sql执行这个函数
        '''
        for day in date_list:
            subsql = re.sub('\d+', day, FetchData.sql)
            self.cursor.execute(subsql)
            data = self.cursor.fetchall()
            data_frame = self.from_sql_into_dataframe(data)
            excel_path = os.path.join(os.path.expanduser("~"),
                                      'Desktop') + '\\' + day + '.csv'
            data_frame.to_csv(excel_path, index=False)
            print(f'-----{day} 数据已经保存-----')


# with FetchData('10.120.7.196', 5258, 'gbase', 'ZXvmax_2017', 'zxvmax',
#                False) as fetch_data:
#     date_list = fetch_data.get_time_range('20211019', '20211026')
#     fetch_data.write_excel_by_date(date_list)
with FetchData('192.168.1.111', 3306, 'root', '123', 'study') as fetch_data:
    fetch_data.write_excel()
