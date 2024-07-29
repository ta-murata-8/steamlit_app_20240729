import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# セッションを作成
session = get_active_session()

# SQLクエリを定義
sql = f"SELECT * FROM STREAMLIT_APPS.PUBLIC.STREAMLIT_TABLE_20240729 LIMIT 30000;"

# データを取得
data = session.sql(sql).collect()

# データフレームに変換
data = pd.DataFrame(data)

# 緯度と経度を数値型に変換し、無効なデータを除外
data['LATITUDE'] = pd.to_numeric(data['LATITUDE'], errors='coerce')
data['LONGITUDE'] = pd.to_numeric(data['LONGITUDE'], errors='coerce')
data = data.dropna(subset=['LATITUDE', 'LONGITUDE'])

# 太陽光発電所のみをフィルタリング
solar_data = data[data['PRIMARY_FUEL'] == 'Solar']

# CAPACITY_MW 列のデータを数値型に変換
solar_data['CAPACITY_MW'] = pd.to_numeric(solar_data['CAPACITY_MW'], errors='coerce')

# サイドバーにフィルタリングオプションを追加
st.sidebar.title('Filters')
min_capacity = int(solar_data['CAPACITY_MW'].min())
max_capacity = int(solar_data['CAPACITY_MW'].max())
selected_capacity = st.sidebar.slider('Capacity (MW)', min_capacity, max_capacity, (min_capacity, max_capacity))

# フィルタリング
filtered_data = solar_data[(solar_data['CAPACITY_MW'] >= selected_capacity[0]) & 
                           (solar_data['CAPACITY_MW'] <= selected_capacity[1])]

# 地図表示
st.title('発電可能量 {} から {} MWの太陽光発電所'.format(selected_capacity[0], selected_capacity[1]))
st.map(filtered_data[['LATITUDE', 'LONGITUDE']])

# 発電容量の分布を表示
st.subheader('発電量ごとの分布')

# 発電容量の分布を集計
capacity_counts = filtered_data['CAPACITY_MW'].value_counts().sort_index()
capacity_counts.index = capacity_counts.index.astype(str)  # インデックスを文字列型に変換

# 棒グラフを表示
st.bar_chart(capacity_counts)

# 選択した範囲のデータの統計情報
st.subheader('選択した範囲のデータの統計情報')
st.write(filtered_data.describe())

# 接続を閉じる
session.close()
