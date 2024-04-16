import pandas as pd
import json
import os
import mysql.connector
from sqlalchemy import create_engine

# Connection for MySQL Database
config = {
      'user':'root', 'password':'1234',
      'host':'127.0.0.1', 'database':'phonepe_data'
  }
connection = mysql.connector.connect(**config)
cursor=connection.cursor()

# Extraction of Data
# Aggregated Transaction

path="D:/Project/PhonePe/pulse/data/aggregated/transaction/country/india/state/"
Agg_state_list=os.listdir(path)
Agg_state_list

clm={'State':[], 'Year':[],'Quarter':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}

for i in Agg_state_list:
    p_i=path+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              clm['Transaction_type'].append(Name)
              clm['Transaction_count'].append(count)
              clm['Transaction_amount'].append(amount)
              clm['State'].append(i)
              clm['Year'].append(j)
              clm['Quarter'].append(int(k.strip('.json')))
Agg_Trans=pd.DataFrame(clm)

Agg_Trans['State']=Agg_Trans['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Agg_Trans['State']=Agg_Trans['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','Dadra and Nagar Haveli Daman and Diu')
Agg_Trans['State']=Agg_Trans['State'].str.replace('-',' ')
Agg_Trans['State']=Agg_Trans['State'].str.title()

# Aggregated user

path2="D:/Project/PhonePe/pulse/data/aggregated/user/country/india/state/"
Agg_state_list=os.listdir(path)

clm2={'State':[], 'Year':[],'Quarter':[],'Brand':[], 'Transaction_count':[], 'Percentage':[]}

for i in Agg_state_list:
    p_i=path2+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D1=json.load(Data)
            try:
              for z in D1['data']['usersByDevice']:
                brand=z['brand']
                count=z['count']
                percentage=z['percentage']
                clm2['Brand'].append(brand)
                clm2['Transaction_count'].append(count)
                clm2['Percentage'].append(percentage)
                clm2['State'].append(i)
                clm2['Year'].append(j)
                clm2['Quarter'].append(int(k.strip('.json')))
            except:
              pass
Agg_user=pd.DataFrame(clm2)

Agg_user['State']=Agg_user['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Agg_user['State']=Agg_user['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','Dadra and Nagar Haveli Daman and Diu')
Agg_user['State']=Agg_user['State'].str.replace('-',' ')
Agg_user['State']=Agg_user['State'].str.title()

# Map Transaction

path3="D:/Project/PhonePe/pulse/data/map/transaction/hover/country/india/state/"
Map_state_list=os.listdir(path)

clm3={'State':[], 'Year':[],'Quarter':[],'Districts':[], 'Transaction_count':[], 'Transaction_amount':[]}

for i in Map_state_list:
    p_i=path3+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D2=json.load(Data)
            for z in D2['data']['hoverDataList']:
              Name=z['name']
              count=z['metric'][0]['count']
              amount=z['metric'][0]['amount']
              clm3['Districts'].append(Name)
              clm3['Transaction_count'].append(count)
              clm3['Transaction_amount'].append(amount)
              clm3['State'].append(i)
              clm3['Year'].append(j)
              clm3['Quarter'].append(int(k.strip('.json')))
Map_Trans=pd.DataFrame(clm3)

Map_Trans['State']=Map_Trans['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Map_Trans['State']=Map_Trans['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','Dadra and Nagar Haveli Daman and Diu')
Map_Trans['State']=Map_Trans['State'].str.replace('-',' ')
Map_Trans['State']=Map_Trans['State'].str.title()

# Map user

path4="D:/Project/PhonePe/pulse/data/map/user/hover/country/india/state/"
Map_state_list=os.listdir(path)

clm4={'State':[], 'Year':[],'Quarter':[],'Districts':[], 'RegisteredUsers':[], 'AppOpens':[]}

for i in Map_state_list:
    p_i=path4+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D3=json.load(Data)
            for z in D3['data']['hoverData'].items():
              district=z[0]
              registeredUsers=z[1]['registeredUsers']
              appOpens=z[1]['appOpens']
              clm4['Districts'].append(district)
              clm4['RegisteredUsers'].append(registeredUsers)
              clm4['AppOpens'].append(appOpens)
              clm4['State'].append(i)
              clm4['Year'].append(j)
              clm4['Quarter'].append(int(k.strip('.json')))
Map_user=pd.DataFrame(clm4)

Map_user['State']=Map_user['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Map_user['State']=Map_user['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','Dadra and Nagar Haveli Daman and Diu')
Map_user['State']=Map_user['State'].str.replace('-',' ')
Map_user['State']=Map_user['State'].str.title()

# Top Transaction

path5="D:/Project/PhonePe/pulse/data/top/transaction/country/india/state/"
Top_state_list=os.listdir(path)

clm5={'State':[], 'Year':[],'Quarter':[],'Pincodes':[], 'Transaction_count':[], 'Transaction_amount':[]}

for i in Top_state_list:
    p_i=path5+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D4=json.load(Data)
            for z in D4['data']['pincodes']:
              entityName=z['entityName']
              count=z['metric']['count']
              amount=z['metric']['amount']
              clm5['Pincodes'].append(entityName)
              clm5['Transaction_count'].append(count)
              clm5['Transaction_amount'].append(amount)
              clm5['State'].append(i)
              clm5['Year'].append(j)
              clm5['Quarter'].append(int(k.strip('.json')))
Top_Trans=pd.DataFrame(clm5)

Top_Trans['State']=Top_Trans['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Top_Trans['State']=Top_Trans['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','Dadra and Nagar Haveli Daman and Diu')
Top_Trans['State']=Top_Trans['State'].str.replace('-',' ')
Top_Trans['State']=Top_Trans['State'].str.title()

# Top user

path6="D:/Project/PhonePe/pulse/data/top/user/country/india/state/"
Top_state_list=os.listdir(path)

clm6={'State':[], 'Year':[],'Quarter':[],'Pincodes':[], 'RegisteredUsers':[]}

for i in Top_state_list:
    p_i=path6+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D5=json.load(Data)
            for z in D5['data']['pincodes']:
              name=z['name']
              registeredUsers=z['registeredUsers']
              clm6['Pincodes'].append(name)
              clm6['RegisteredUsers'].append(registeredUsers)
              clm6['State'].append(i)
              clm6['Year'].append(j)
              clm6['Quarter'].append(int(k.strip('.json')))
Top_user=pd.DataFrame(clm6)

Top_user['State']=Top_user['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Top_user['State']=Top_user['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','Dadra and Nagar Haveli Daman and Diu')
Top_user['State']=Top_user['State'].str.replace('-',' ')
Top_user['State']=Top_user['State'].str.title()

# Creating and Insertion of Tables in MySQL 

engine = create_engine("mysql+mysqlconnector://root:1234@localhost/phonepe_data")

Agg_Trans.to_sql('aggregated_transaction', con=engine, if_exists='fail', index=False)
Agg_user.to_sql('aggregated_user', con=engine, if_exists='fail', index=False)
Map_Trans.to_sql('map_transaction', con=engine, if_exists='fail', index=False)
Map_user.to_sql('map_user', con=engine, if_exists='fail', index=False)
Top_Trans.to_sql('top_transaction', con=engine, if_exists='fail', index=False)
Top_user.to_sql('top_user', con=engine, if_exists='fail', index=False)