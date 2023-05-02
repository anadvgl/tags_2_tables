import os
import pymysql
from sqlalchemy import create_engine, event, types
import pandas as pd
import json
from pandas import json_normalize
import getpass
import numpy as np
from typing import Optional
import requests
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")
from loguru import logger
import httpx

# pmysqlsql credentials
mysqluser = os.getenv('mysqluser')
mysqlpwd = os.getenv('mysqlpwd')

# connect to mysql
mysql_conn_str = f"mysql+pymysql://{mysqluser}:{mysqlpwd}@host:port/db"
mysql_engine = create_engine(mysql_conn_str)
mysql_engine.execute("SET FOREIGN_KEY_CHECKS=0")
mysql_engine.connect()
print("Connected to mysql at: " + datetime.now().strftime('%H:%M:%S'))

query = ''' select tagFQN, targetFQN from openmetadata_db.tag_usage '''

df = pd.read_sql_query(query, mysql_engine)

print("df created at: " + datetime.now().strftime('%H:%M:%S'))

# close mysql connection
mysql_engine.dispose()

# df processing
df["dots"] = df["targetFQN"].str.count('\.')
df_columns = df[df["dots"] == 4].reset_index(drop=True)

df_tables = df[df["dots"] == 3].reset_index(drop=True)
df_tables_tags = df_tables[['tagFQN', 'targetFQN']]
df_tables_tags.columns = ['tagFQN', 'tableFQN']
df_tables_tags['tableFQN'] = df_tables_tags['tableFQN'].str.lower()

df_columns['columnName'] = df_columns['targetFQN'].apply(lambda x: x.split('.')[4].strip())
df_columns['tableFQN'] = df_columns['targetFQN'].apply(lambda x: x.split('.')[0].strip()) + "." + df_columns['targetFQN'].apply(lambda x: x.split('.')[1].strip()) + "." + df_columns['targetFQN'].apply(lambda x: x.split('.')[2].strip()) + "." + df_columns['targetFQN'].apply(lambda x: x.split('.')[3].strip())

df_columns_tags = df_columns[["tagFQN", "tableFQN"]].drop_duplicates().reset_index(drop=True)
df_columns_tags['tableFQN'] = df_columns_tags['tableFQN'].str.lower()

col_index = df_columns_tags.set_index(['tagFQN', 'tableFQN']).index
tab_index = df_tables_tags.set_index(['tagFQN', 'tableFQN']).index
mask = ~col_index.isin(tab_index)

tags_to_add = df_columns_tags.loc[mask].reset_index(drop=True)
tables_list = tags_to_add["tableFQN"].unique()
all_tags = pd.concat([tags_to_add, df_tables_tags], axis=0).reset_index(drop=True)

need_to_add_tags_tables = all_tags[all_tags["tableFQN"].isin(tables_list)].reset_index(drop=True)
need_to_add_tags_tables = need_to_add_tags_tables[['tableFQN', 'tagFQN']]
need_to_add_tags_tables['tagRank'] = need_to_add_tags_tables.groupby("tableFQN")["tagFQN"].rank(method="first", ascending=True).reset_index(drop=True)

final_df = need_to_add_tags_tables.sort_values(by=['tableFQN', 'tagFQN'])
final_df["tagIndex"] = final_df["tagRank"] - 1
final_df['tagIndex'] = final_df['tagIndex'].round()
final_df['tagIndex'] = final_df['tagIndex'].astype(int)
final_df = final_df.reset_index(drop=True)
final_df = final_df[['tableFQN', 'tagFQN', 'tagIndex']]

print("Таблиц для простановки тегов всего: ", len(final_df))

system_filter_1 = 'dol.'
system_filter_2 = 'ms excel.'
filtered_df = final_df[~final_df['tableFQN'].str.startswith((system_filter_1, system_filter_2))].reset_index(drop=True)

tag_filter = 'Мобильная абонентская база.'
final_filtered_df = filtered_df[~filtered_df['tagFQN'].str.startswith(tag_filter)].reset_index(drop=True)
print("Таблиц для простановки тегов после фильтрации: ", len(final_filtered_df))

# omd credentials
userpwd = os.getenv("client_secret")

# connect to omd
def get_token(userpwd) -> str:
    data = {
        "grant_type": "client_credentials",
        "client_id": "***",
        "client_secret": userpwd,
    }
    response = httpx.post(
        "https://keycloak***/auth/realms/***/protocol/***-connect/token",
        data=data,
        verify=False,
        timeout=httpx.Timeout(300),
    )
    logger.info(f"Response status code: {response.status_code}")
    return response.json()["access_token"]

# tags inserting
if len(final_filtered_df) == 0:
    print("Нет таблиц для простановки тегов")
else:
    print("Таблиц для простановки тегов после фильтрации: ", len(final_filtered_df))
    tables_need_id = final_filtered_df['tableFQN'].to_frame().drop_duplicates().reset_index(drop=True)
    
    tables_id = []
    for fqn in tables_need_id['tableFQN']:
        token = get_token(userpwd)
        response = requests.get(
            f"https://open-metadata.***/api/v1/tables/name/{fqn}",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
            data={},
            verify=False,
        )
        result_id = response.json()
        tables_id.append(result_id)
        result_normalized = json_normalize(tables_id)
        result_normalized = result_normalized[["id"]]
        tables_need_id["tableId"] = result_normalized
    
    tables_need_id = tables_need_id.dropna().reset_index(drop=True)
    table_ids = dict(zip(tables_need_id.tableFQN,tables_need_id.tableId))
    
    final_filtered_df['tableId'] = final_filtered_df['tableFQN'].map(table_ids)
    final_filtered_df = final_filtered_df.dropna().reset_index(drop=True)
    
    i = 0
    out = []
    for tid in final_filtered_df.tableId.unique():
        print(final_filtered_df.loc[final_filtered_df['tableId'] == tid].tableFQN.unique())
        req = []
        for tag, index in zip(final_filtered_df.loc[final_filtered_df['tableId'] == tid]['tagFQN'],
                            final_filtered_df.loc[final_filtered_df['tableId'] == tid]['tagIndex']):
            print(tag)
            req.append("""{"op":"add", "path":""" + f'"/tags/{index}"'",""" + " """""value": {"tagFQN":""" + f'"{tag}' + """"}}""")
        res_req = str(req).replace("'","")
        response = requests.patch(
                    f"https://open-metadata.***/api/v1/tables/{tid}",
                    headers={"Content-Type": "application/json-patch+json", "Authorization": f"Bearer {token}"},
                    data=(res_req).encode('utf-8'),
                    verify=False,) 
        if response.status_code == 401:
            token = get_token(userpwd)
            response = requests.patch(
                        f"https://open-metadata.***/api/v1/tables/{tid}",
                        headers={"Content-Type": "application/json-patch+json", "Authorization": f"Bearer {token}"},
                        data=(res_req).encode('utf-8'),
                        verify=False,)  
        i += 1
        out.append(res_req)

    print("Количество проставленных тегов: ", i)
