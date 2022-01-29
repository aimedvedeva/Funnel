import psycopg2
import pandas.io.sql as sqlio
import datetime

conn = psycopg2.connect("dbname='postgres' user='' host='' password=")

### parameters
start_date = '2021-03-01'
end_date = '2021-03-31'

### extract ids registered during the time period
reg_query = """
SELECT id,
       __update_date as id_update_date,
       country_code
FROM view_user_manager_user
WHERE __update_date >= DATE(%s)
AND __update_date <= DATE(%s)
AND status = 'ACTIVE'
ORDER BY id;
""" 
id_data = sqlio.read_sql_query(reg_query, conn, params=(start_date, end_date))  

    
def batch_select(conn, batch_size, query):
    last_nonce = 0
    
    while True:
        try:
            batch_data = sqlio.read_sql_query(query, conn, params=(last_nonce, batch_size))
        except:
            conn = None
            while conn is None:
                try:
                    conn = psycopg2.connect("dbname='postgres' user='amedvedeva' host='135.181.61.116' password='JhnbgLrt@345nbvYukfbg^739cdsg'")
                except:
                    pass
            batch_data = sqlio.read_sql_query(query, conn, params=(last_nonce, batch_size))
        
        if (last_nonce == 0):
            data = batch_data
        else:
            data = data.append(batch_data)
        
        if(len(batch_data) < batch_size):
            break;

        last_nonce = batch_data['nonce'].iloc[-1].item()
        print('next')
    return data


trade_query = """
SELECT taker_trader as taker_id,
       maker_trader as maker_id,
       __update_datetime AS trade_date,
       nonce
FROM view_market_aggregator_trade
WHERE EXISTS (
    SELECT id
    FROM view_user_manager_user
    WHERE __update_datetime >= '2021-03-01'
    AND __update_datetime <= '2021-03-31'
    AND status = 'ACTIVE'
    ORDER BY id
    )
AND nonce > %s
AND  __update_datetime >= '2021-03-01'
AND  __update_datetime <= '2021-03-31'
AND status = 'EXECUTED'
AND taker_trader != maker_trader
ORDER BY trade_date
LIMIT %s;
"""

trade_data = batch_select(conn, 200000, trade_query)

takers = trade_data[['taker_id','trade_date']]
takers = takers.rename(columns={'taker_id':'id'})
takers.drop_duplicates(subset='id', keep='first', inplace=True)
takers.set_index('id', inplace=True)
takers = takers.squeeze()


makers = trade_data[['maker_id','trade_date']]
makers = makers.rename(columns={'maker_id':'id'})
makers.drop_duplicates(subset='id', keep='first', inplace=True)
makers.set_index('id', inplace=True)
makers = makers.squeeze()

traders = makers.append(takers)
traders = traders[~traders.index.duplicated(keep='first')]

id_data.set_index('id', inplace=True)
id_data = id_data.join(traders, on='id', how='left')

# replace countries' codes by its' names
from country_codes import ISO3166 
id_data['country'] = id_data['country_code'].apply(lambda x: 'undefinied_country' if (ISO3166.get(x) == None) else ISO3166.get(x))
id_data.drop(columns=['country_code'], inplace=True)

deposit_query = """
SELECT user_id as id,
       value as deposit_value,
       updated_at as deposit_confirmation_date
FROM view_transaction_deposit
WHERE EXISTS (
    SELECT id
    FROM view_user_manager_user
    WHERE __update_datetime >= '2021-03-01'
    AND __update_datetime <= '2021-03-31'
    AND status = 'ACTIVE'
    ORDER BY id
    )
AND updated_at >= '2021-03-01'
AND updated_at <= '2021-03-31'
AND status = 'CONFIRMED'
ORDER BY id;
"""  
depositors = sqlio.read_sql_query(deposit_query, conn)
depositors.drop_duplicates(subset='id', keep='first', inplace=True)
depositors.set_index('id', inplace=True)

id_data = id_data.join(depositors, on='id', how='left')

withdrawal_query = """
SELECT user_id as id,
       value as withdrawal_value,
       updated_at as withdrawal_confirmation_date
FROM view_transaction_withdrawal
WHERE EXISTS (
    SELECT id
    FROM view_user_manager_user
    WHERE __update_datetime >= '2021-03-01'
    AND __update_datetime <= '2021-03-31'
    AND status = 'ACTIVE'
    ORDER BY id
    )
AND updated_at >= '2021-03-01'
AND updated_at <= '2021-03-31'
AND status = 'CONFIRMED'
ORDER BY id;
"""  

withdrawals = sqlio.read_sql_query(withdrawal_query, conn)
withdrawals.drop_duplicates(subset='id', keep='first', inplace=True)
withdrawals.set_index('id', inplace=True)

id_data = id_data.join(withdrawals, on='id', how='left')

transfers_query = """
SELECT COUNT(id) AS transfers,
       SUM(transferring_amount) AS transfers_sum,
       SUM(usd_value) AS usd_sum,
       from_account_id as id
FROM view_account_manager_transfer
WHERE EXISTS (
    SELECT id
    FROM view_user_manager_user
    WHERE __update_datetime >= '2021-03-01'
    AND __update_datetime <= '2021-03-31'
    AND status = 'ACTIVE'
    ORDER BY id
    )
AND  __update_datetime >= '2021-03-01'
AND  __update_datetime <= '2021-03-31'
AND status = 'COMPLETED'
GROUP BY id
ORDER BY id;
"""

transfers = sqlio.read_sql_query(transfers_query, conn)
transfers.drop_duplicates(subset='id', keep='first', inplace=True)
transfers.set_index('id', inplace=True)

id_data = id_data.join(transfers, on='id', how='left')


### upload data into Google Sheet
import pygsheets

# set connection
sheet_name = 'funnel'
gc = pygsheets.authorize(service_file='funneldata-3e2cf01dc135.json')
sheet = gc.open(sheet_name)

#select the first sheet
worksheet = sheet[0]
worksheet.clear()
id_data.reset_index(level=0, inplace=True)

import pandas as pd

id_data['Trading Flag'] = id_data['trade_date'].apply(lambda x: False if pd.isnull(x) else True)
id_data['Depositing Flag'] = id_data['deposit_confirmation_date'].apply(lambda x: False if pd.isnull(x) else True)
id_data['Withdrawal Flag'] = id_data['withdrawal_confirmation_date'].apply(lambda x: False if pd.isnull(x) else True)

worksheet.set_dataframe(id_data, (1,1), fit=True)
print('finish, babe') 


# set connection
sheett_name = 'Countries'
gc = pygsheets.authorize(service_file='funneldata-3e2cf01dc135.json')
sheet = gc.open(sheett_name)

#select the first sheet
worksheet = sheet[0]
worksheet.clear()

id_data.reset_index(level=0, inplace=True)
country_data = id_data[['id','country']]
worksheet.set_dataframe(country_data, (1,1), fit=True)
print('finish, babe') 



