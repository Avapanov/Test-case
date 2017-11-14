# импорт библиотек
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import datetime

#загружаем данные
data=pd.read_csv('raw_data.csv', parse_dates=['ts'])

#Функция, отрезает от данных 15 мин  
#возвращает отрезанный DataFrame, начальный индекс для следующих 15 мин и время начала цикла  
def cut_data(data, i_0):
    for j in range(i_0,len(data)):
        c=data['ts'][j]-data['ts'][i_0] 
        if c.seconds/60>=15:
            break
    return data[i_0:j], j+1 , data['ts'][i_0] 

# Функция, возвращающая DataFrame состоящий из пар признаков 'api_name','mtd' и колличеством ошибок 5XX
def table_15_min(data):
    data['s_cod']= data['cod']//100
    data=data.drop(['cod'], axis=1)
    data=data.drop(['ts'], axis=1)
    ex_table=pd.DataFrame()
    mtd=[]
    api=[]
    cod=[]
    num=5
    for (k1,k2), group in data.groupby(['api_name','mtd'])['s_cod']:    
        if num in group.values:
            val=0
            for i in group.values:
                if i==num:
                    val+=i
            cod.append(val/num)
        else:
            cod.append(0)   
        api.append(k1)
        mtd.append(k2)     
    ex_table['api_name']=api
    ex_table['http_method']=mtd
    ex_table['count_http_code_5xx']=cod
    return  ex_table 

# Функция суммирует таблицы с признаками 'api','mtd','s_cod','ts' по всем 15 мин отрезкам
def metriks(data):
    columns = ['api_name','http_method','count_http_code_5xx','timeframe_start']
    exit_table= pd.DataFrame(columns=columns) 
    i_0=0
    l=0
    while i_0<len(data):
        data_cut, i_0, ts = cut_data(data,i_0)
        tf=table_15_min(data_cut)
        exit_table=pd.concat([ exit_table, tf], ignore_index=True)
        exit_table['timeframe_start'][l:]=ts
        l+=len(tf)
    return exit_table

# Расчитываем метрику для всех значений
exit_table2=metriks(data)

#Зададим столбец 'is_anomali' для script_2
exit_table2['is_anomali']=np.nan

#записываем таблицу в csv фаил
exit_table2.to_csv('exit_table_script_1.csv')

# записываем таблицу в mysql
from pandas.io import sql
import MySQLdb
con = MySQLdb.connect()# НЕобходимо вставить путь к базе данных
sql.write_frame(exit_table2, con=con, name='table_Script_2', 
                if_exists='replace', flavor='mysql')
