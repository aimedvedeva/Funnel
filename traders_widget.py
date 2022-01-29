import psycopg2
import pandas.io.sql as sqlio
from country_codes import ISO3166 
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
      
        day_data = sqlio.read_sql_query(query, conn, \
                                        params=(start_date, next_date, start_date, next_date))
        day_data['date'] = start_date
        
        try:
            data = data.append(day_data)
        except:
            data = day_data
            
        start_date = start_date + timedelta(days=1)
        print('next day')
    return data

# =============================================================================
# traders_query ="""SELECT COUNT(distinct traders.trader) as traders,
#        traders.currency_tag,
#        traders.quote_tag,
#        traders.country
#     from
#     (SELECT trade.taker_trader as trader,
#             currency.tag as currency_tag,
#             quote.tag as quote_tag,
#             taker_info.country_code as country
#     FROM view_market_aggregator_trade trade
#     left join view_user_manager_user taker_info
#         on trade.taker_trader = taker_info.id
#     left join view_asset_manager_currency as currency
#         on trade.currency = currency.id
#     left join view_asset_manager_currency as quote
#         on trade.quote = quote.id
#     WHERE trade.taker_trader != trade.maker_trader
#     and trade.__update_datetime >= DATE(%s)
#     and trade.__update_datetime < DATE(%s)
#     UNION
#     SELECT trade.maker_trader as trader,
#            vamc_currency.tag as currency_tag,
#            vamc_quote.tag as quote_tag,
#            maker_info.country_code as country
#     FROM view_market_aggregator_trade trade
#     left join view_user_manager_user maker_info
#         on trade.taker_trader = maker_info.id
#     left join view_asset_manager_currency as vamc_currency
#         on trade.currency = vamc_currency.id
#     left join view_asset_manager_currency as vamc_quote
#         on trade.quote = vamc_quote.id
#     WHERE trade.taker_trader != trade.maker_trader
#     and trade.__update_datetime >= DATE(%s)
#     and trade.__update_datetime < DATE(%s)) AS traders
#     group by currency_tag, quote_tag, country
#     order by traders desc;"""
# =============================================================================

traders_query = """
select count(unique_traders_in_pairs.trader) as traders,
        unique_traders_in_pairs.currency_tag,
        unique_traders_in_pairs.quote_tag,
        user_info.country_code as country
from
    (select distinct traders.trader,
            traders.currency_tag,
            traders.quote_tag
        from
        (
            (select trade.taker_trader as trader,
                    currency.tag as currency_tag,
                    quote.tag as quote_tag
            from view_market_aggregator_trade trade
            join view_asset_manager_currency as currency
                on trade.currency = currency.id
            join view_asset_manager_currency as quote
                on trade.quote = quote.id
            where trade.taker_trader != trade.maker_trader
            and trade.__create_datetime >= DATE(%s)
            and trade.__create_datetime < DATE(%s)
            ) -- takers
        union
            (select trade.maker_trader as trader,
                    vamc_currency.tag as currency_tag,
                    vamc_quote.tag as quote_tag
            from view_market_aggregator_trade trade
            join view_asset_manager_currency as vamc_currency
                on trade.currency = vamc_currency.id
            join view_asset_manager_currency as vamc_quote
                on trade.quote = vamc_quote.id
            where trade.taker_trader != trade.maker_trader
            and trade.__create_datetime >= DATE(%s)
            and trade.__create_datetime < DATE(%s)
            ) -- makers
        ) as traders
    ) as unique_traders_in_pairs
join view_user_manager_user user_info
    on user_info.id = unique_traders_in_pairs.trader
group by currency_tag, quote_tag, country;
"""

traders_data = None   
traders_data = extract_day_data(start_date, end_date, traders_query, conn, traders_data)

traders_data['country'] = traders_data['country'].apply(lambda x: 'undefinied_country' if (ISO3166.get(x) == None) else ISO3166.get(x))
traders_data['Pair'] = traders_data['currency_tag'] + traders_data['quote_tag']

#traders_data['date_date'] = traders_data['date'].apply(lambda x: x.strftime('%d-%m-%Y'))
#traders_data['date'] = traders_data['date'].apply(lambda x: x.date())
import pandas as pd
#traders_data['date'] = traders_data['date'].apply(lambda x: pd.to_datetime(x, format='%d.%m.%Y'))

traders_data['date'] = traders_data['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

import pygsheets
sheet_name = 'Traders widget'
gc = pygsheets.authorize(service_file='funneldata-3e2cf01dc135.json')
sheet = gc.open(sheet_name)

worksheet = sheet[0]
worksheet.clear()
traders_data.reset_index(level=0, inplace=True)
worksheet.set_dataframe(traders_data, (1,1), fit=True)
            
print('Stopbb!')