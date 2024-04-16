#------------------------------------------------imported needed libraries---------------------------------------------- 
import streamlit as st
from st_pages import Page,show_pages,add_page_title
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import plotly.express as px
import numpy as np
import seaborn as sns

#---------------------------------------------MySQL SQLAlchemy Connection-----------------------------------------------
engine = create_engine('mysql+mysqlconnector://root:1234@localhost/phonepe_data',echo=False)
config = {
    'user':'root',    'host':'127.0.0.1',
    'password':'1234', 'database':'phonepe_data'
}
connection = mysql.connector.connect(**config)
cursor = connection.cursor(buffered=True)

cursor.execute("""select State from map_transaction;""")
state_result = cursor.fetchall()
state_df = pd.DataFrame(state_result,columns=['State'])
state_list = state_df['State'].unique()

#---------------------------------------------------Streamlit setup---------------------------------------------------------
st.set_page_config(page_title = 'Home', layout = 'wide')
st.title(":violet[Phonepe Pulse Data Visulization]")
st.write('Note : Data available from 2018 to 2023')
st.write('## :blue[State wise Insights]')
tab1,tab2 = st.tabs(['Transaction','User'])

#----------------------------------------------------Transaction Block--------------------------------------------------
with tab1:
    col1, col2, col3 = st.columns(3)
    with col1:
            sel = st.selectbox('**Type**',('Transaction Count','Transaction Amount'),key='sel') 
    with col2:     
            year = st.selectbox('**Year**',('All','2018','2019','2020','2021','2022','2023'),key ='year')  
    with col3:        
            quarter = st.selectbox('**Quarter**',('All','1','2','3','4'),key='quarter')
    
    if year == 'All':
        year = '2023'

    yr_li=['2018','2019','2020','2021','2022','2023']    
    if year in yr_li and quarter=='All':
         year = year
         quarter = 4  

    cursor.execute(f"""SELECT State,sum(Transaction_count) FROM phonepe_data.aggregated_transaction where Year={year} and quarter={quarter} group by State;""")
    agg_count = cursor.fetchall() 
    agg_count_df = pd.DataFrame(agg_count,columns=['State','Count'])
    agg_count_df = agg_count_df.astype({'Count':'int64'})

    cursor.execute(f"""SELECT State,sum(Transaction_amount) FROM phonepe_data.aggregated_transaction where Year={year} and quarter={quarter} group by State;""")
    agg_amount = cursor.fetchall() 
    agg_amount_df = pd.DataFrame(agg_amount,columns=['State','Amount'])

    cursor.execute(f"""SELECT Districts,sum(Transaction_count) FROM phonepe_data.map_transaction where Year={year} and quarter={quarter} group by Districts;""")
    map_year = cursor.fetchall() 
    map_count_df = pd.DataFrame(map_year,columns=['Districts','Transaction Count']) 
    map_count_df = map_count_df.astype({'Transaction Count':'int64'})

    cursor.execute(f"""SELECT Districts,sum(Transaction_amount) FROM phonepe_data.map_transaction where Year={year} and quarter={quarter} group by Districts;""")
    map_amount = cursor.fetchall() 
    map_amount_df = pd.DataFrame(map_amount,columns=['Districts','Transaction Amount']) 
    map_amount_df = map_amount_df.astype({'Transaction Amount':'int64'})
    
    cursor.execute(f"""SELECT Pincodes,sum(Transaction_amount) FROM phonepe_data.top_transaction where Year={year} and quarter={quarter} group by Pincodes;""")
    top_amount = cursor.fetchall() 
    top_amount_df = pd.DataFrame(top_amount,columns=['Pincodes','Transaction Amount']) 
    top_amount_df = top_amount_df.astype({'Transaction Amount':'int64'})
    
    cursor.execute(f"""SELECT Pincodes,sum(Transaction_count) FROM phonepe_data.top_transaction where Year={year} and quarter={quarter} group by Pincodes;""")
    top_count = cursor.fetchall() 
    top_count_df = pd.DataFrame(top_count,columns=['Pincodes','Transaction Count']) 
    top_count_df = top_count_df.astype({'Transaction Count':'int64'})

    col1,col2 = st.columns(2)
    total_trans = sum(agg_count_df['Count'])
    col1.metric(
    label = f'Total Phonepe Transaction count till {year} (UPI+Cards+Wallets)',
    value = '{:.2f} Cr'.format(total_trans/100000000),
    delta = 'Upward Trend'
)
    total_amount = sum(agg_amount_df['Amount'])
    col2.metric(
    label = f'Total Phonepe Transaction Amount till {year} (UPI+Cards+Wallets)',
    value = '{:.2f} Cr'.format(total_amount/100000000),
    delta = 'Upward Trend'
)


    if sel == 'Transaction Count':                 
        fig = px.choropleth(
        agg_count_df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='State',
        color='Count',
        color_continuous_scale='speed'
        )
        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig,use_container_width=True,use_container=True)
        tabs1,tabs2,tabs3 = st.tabs(['States','Districts','Postal Codes'])
        with tabs1:
            st.write('### :orange[Top 10 States]') 
            st.dataframe(agg_count_df.nlargest(10,'Count'),hide_index=True)
        with tabs2:
            st.write('### :orange[Top 10 Districts]')
            st.dataframe(map_count_df.nlargest(10,'Transaction Count'),hide_index=True)
        with tabs3:
            st.write('### :orange[Top 10 Postal Codes]')
            st.dataframe(top_count_df.nlargest(10,'Transaction Count'),hide_index=True)
            

    elif sel == 'Transaction Amount':                 
        fig = px.choropleth(
        agg_amount_df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='State',
        color='Amount',
        color_continuous_scale='speed'
        )
        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig,use_container_width=True,use_container=True)   

        tabs1,tabs2,tabs3 = st.tabs(['States','Districts','Postal Codes'])
        with tabs1:
            st.write('### :orange[Top 10 States]')
            st.dataframe(agg_amount_df.nlargest(10,'Amount'),hide_index=True) 
        with tabs2:
            st.write('### :orange[Top 10 Districts]')
            st.dataframe(map_amount_df.nlargest(10,'Transaction Amount'),hide_index=True)
        with tabs3:
            st.write('### :orange[Top 10 Postal Codes]') 
            st.dataframe(top_amount_df.nlargest(10,'Transaction Amount'),hide_index=True)
    
 

#-----------------------------------------------User Block------------------------------------------------------------
with tab2:
    col1,col2 = st.columns(2)
    with col1:
        year2 = st.selectbox('**Year**',('All','2018','2019','2020','2021','2022'),key='year2') 
    with col2:
        quarter2 = st.selectbox('**Quarter**',('All','1','2','3','4'),key='quarter2')
    if year2 == 'All':
        year2 = '2022'

    yr_li=['2018','2019','2020','2021','2022']    
    if year2 in yr_li and quarter2=='All':
         year2 = year2
         quarter2 = 1   
                     
    cursor.execute(f"""SELECT State,sum(Transaction_count) as Users FROM phonepe_data.aggregated_user where Year={year2} and Quarter={quarter2} group by State;""")
    us_year = cursor.fetchall()
    us_year_df = pd.DataFrame(us_year,columns=['State','Transaction_count'])
    us_year_df = us_year_df.astype({'Transaction_count':'int64'})

    cursor.execute(f"""SELECT Districts,sum(RegisteredUsers) FROM phonepe_data.map_user where Year={year2} and Quarter={quarter2} group by Districts;""")
    map_user = cursor.fetchall() 
    map_user_df = pd.DataFrame(map_user,columns=['Districts','RegisteredUsers']) 
    map_user_df = map_user_df.astype({'RegisteredUsers':'int64'})

    cursor.execute(f"""SELECT Pincodes,sum(RegisteredUsers) FROM phonepe_data.top_user where Year={year2} and Quarter={quarter2} group by Pincodes;""")
    top_user = cursor.fetchall() 
    top_user_df = pd.DataFrame(top_user,columns=['Pincodes','RegisteredUsers']) 
    top_user_df = top_user_df.astype({'RegisteredUsers':'int64'})

    col1,col2 = st.columns(2)
    total_users = sum(us_year_df['Transaction_count'])
    col1.metric(
    label = f'Total Phonepe Users count till {year2} (UPI+Cards+Wallets)',
    value = '{:.2f} Cr'.format(total_users/100000000),
    delta = 'Upward Trend'
)

    fig = px.choropleth(
    us_year_df,
    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
    featureidkey='properties.ST_NM',
    locations='State',
    color='Transaction_count',
    color_continuous_scale='speed'
    )
    fig.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig,use_container_width=True,use_container=True)
 
    tabs1,tabs2,tabs3 = st.tabs(['States','Districts','Postal Codes'])
    with tabs1:
        st.write('### :orange[Top 10 States]')
        st.dataframe(us_year_df.nlargest(10,'Transaction_count'),hide_index=True) 
    with tabs2:
        st.write('### :orange[Top 10 Districts]')
        st.dataframe(map_user_df.nlargest(10,'RegisteredUsers'),hide_index=True)
    with tabs3:
        st.write('### :orange[Top 10 Postal Codes]') 
        st.dataframe(top_user_df.nlargest(10,'RegisteredUsers'),hide_index=True) 

show_pages([
    Page('test1.py','Dashboard','📁'),
    Page('sample.py','Insights','💼')
])
