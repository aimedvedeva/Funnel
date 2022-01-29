import psycopg2
import pandas.io.sql as sqlio
from country_codes import ISO3166 
import exchange
import datetime
from datetime import timedelta  
from datetime import timezone

conn = psycopg2.connect("dbname='postgres' user='amedvedeva' host='135.181.61.116' password='JhnbgLrt@345nbvYukfbg^739cdsg'")

start_date = datetime.datetime(2021, 4, 26)
from datetime import datetime
start_date = datetime(start_date.year, start_date.month, start_date.day, 0, 0).replace(tzinfo=timezone.utc)
days = 14
end_date = start_date + timedelta(days=days)

def extract_day_data(start_date, end_date, query, conn, data):
    while start_date < end_date:
        
        next_date = start_date + timedelta(days=1) 
      
        day_data = sqlio.read_sql_query(query, conn, params=(start_date, next_date))
        day_data['date'] = start_date
        
        try:
            data = data.append(day_data)
        except:
            data = day_data
            
        start_date = start_date + timedelta(days=1)
        print('next day')
    return data

organic_query = """
select  currency.tag as currency_tag,
        quote.tag as quote_tag,
        taker_info.country_code as taker_country,
        maker_info.country_code as maker_country,
        SUM(trade.cost) as volume,
        trade.__create_date as date
from view_market_aggregator_trade trade
join view_user_manager_user taker_info
    on trade.taker_trader = taker_info.id
join view_user_manager_user maker_info
    on trade.maker_trader = maker_info.id
join view_asset_manager_currency as currency
    on trade.currency = currency.id
join view_asset_manager_currency as quote
    on trade.quote = quote.id
where trade.taker_trader != trade.maker_trader
and trade.__create_datetime >=  DATE(%s)
and trade.__create_datetime <  DATE(%s)
group by currency_tag, quote_tag, taker_country, maker_country, date
order by volume desc;
"""
organic_data = None   
organic_data = extract_day_data(start_date, end_date, organic_query, conn, organic_data)

organic_data['taker_country'] = organic_data['taker_country'].apply(lambda x: 'undefinied_country' if (ISO3166.get(x) == None) else ISO3166.get(x))
organic_data['maker_country'] = organic_data['maker_country'].apply(lambda x: 'undefinied_country' if (ISO3166.get(x) == None) else ISO3166.get(x))
organic_data['Pair'] = organic_data['currency_tag'] + organic_data['quote_tag']

organic_data = exchange.convert_to_USDT(organic_data, columns=['volume'])
organic_data['type'] = 'Organic'

mm_query = """
select  currency.tag as currency_tag,
        quote.tag as quote_tag,
        taker_info.country_code as taker_country,
        maker_info.country_code as maker_country,
        SUM(trade.cost) as volume,
        trade.__update_date as date
from view_market_aggregator_trade trade
join view_user_manager_user taker_info
    on trade.taker_trader = taker_info.id
join view_user_manager_user maker_info
    on trade.maker_trader = maker_info.id
join view_asset_manager_currency as currency
    on trade.currency = currency.id
join view_asset_manager_currency as quote
    on trade.quote = quote.id
where trade.taker_trader = trade.maker_trader
and taker_fee != 0
and trade.__update_datetime >=  DATE(%s)
and trade.__update_datetime <  DATE(%s)
group by currency_tag, quote_tag, taker_country, maker_country, date
order by volume desc;
"""
mm_data = None   
mm_data = extract_day_data(start_date, end_date, mm_query, conn, mm_data)

mm_data['taker_country'] = mm_data['taker_country'].apply(lambda x: 'undefinied_country' if (ISO3166.get(x) == None) else ISO3166.get(x))
mm_data['maker_country'] = mm_data['taker_country'].apply(lambda x: 'undefinied_country' if (ISO3166.get(x) == None) else ISO3166.get(x))
mm_data['Pair'] = mm_data['currency_tag'] + mm_data['quote_tag']

mm_data = exchange.convert_to_USDT(mm_data, columns=['volume'])
mm_data['type'] = 'Market makers'

zero_acc_mm_query = """
select  currency.tag as currency_tag,
        quote.tag as quote_tag,
        taker_info.country_code as taker_country,
        maker_info.country_code as maker_country,
        SUM(trade.cost) as volume,
        trade.__update_date as date
from view_market_aggregator_trade trade
join view_user_manager_user taker_info
    on trade.taker_trader = taker_info.id
join view_user_manager_user maker_info
    on trade.maker_trader = maker_info.id
join view_asset_manager_currency as currency
    on trade.currency = currency.id
join view_asset_manager_currency as quote
    on trade.quote = quote.id
where trade.taker_trader = trade.maker_trader
and taker_fee = 0
and trade.__update_datetime >=  DATE(%s)
and trade.__update_datetime <  DATE(%s)
group by currency_tag, quote_tag, taker_country, maker_country, date
order by volume desc;
"""
zero_acc_mm_data = None   
zero_acc_mm_data = extract_day_data(start_date, end_date, zero_acc_mm_query, conn, zero_acc_mm_data)

zero_acc_mm_data['taker_country'] = zero_acc_mm_data['taker_country'].apply(lambda x: 'undefinied_country' if (ISO3166.get(x) == None) else ISO3166.get(x))
zero_acc_mm_data['maker_country'] = zero_acc_mm_data['taker_country'].apply(lambda x: 'undefinied_country' if (ISO3166.get(x) == None) else ISO3166.get(x))
zero_acc_mm_data['Pair'] = zero_acc_mm_data['currency_tag'] + zero_acc_mm_data['quote_tag']

zero_acc_mm_data = exchange.convert_to_USDT(zero_acc_mm_data, columns=['volume'])
zero_acc_mm_data['type'] = 'Zero account market makers'

general_data = organic_data.append(mm_data).append(zero_acc_mm_data)
#general_data['date'] = general_data['date'].apply(lambda x: x.date())
import pandas as pd
general_data['date'] = general_data['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
general_data.sort_values(by='date', inplace=True)
                                                  
import pygsheets
sheet_name = 'Volumes widget'
gc = pygsheets.authorize(service_file='funneldata-3e2cf01dc135.json')
sheet = gc.open(sheet_name)

worksheet = sheet[0]
worksheet.clear()
general_data.reset_index(level=0, inplace=True)
worksheet.set_dataframe(general_data, (1,1), fit=True)
            
print('Stopbb!')

data_copy = general_data.copy()
data_copy.drop(columns=['maker_country'], inplace=True)
data_copy.rename(columns={'taker_country' : 'country'}, inplace=True)

general_data.drop(columns=['taker_country'], inplace=True)
general_data.rename(columns={'maker_country' : 'country'}, inplace=True)

general_data = general_data.append(data_copy)
general_data = general_data[['country', 'volume_USDT', 'date', 'Pair', 'type']]

#general_data['date'].unique()

sheet_name = 'Volumes by country widget'
gc = pygsheets.authorize(service_file='funneldata-3e2cf01dc135.json')
sheet = gc.open(sheet_name)

worksheet = sheet[0]
worksheet.clear()
general_data.reset_index(level=0, inplace=True)
worksheet.set_dataframe(general_data, (1,1), fit=True)