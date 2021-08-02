# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 17:20:11 2020

@author: Micha
"""

import pandas as pd
import datetime
import calendar
import time
import config
import tesla
import oauth

email=config.email
pw=config.pw

filename='solar_data.zip'
creds_file='creds.zip'

bill_starts=[20201114,20201217,20210120,20210219,20210320,20210420,20210519,20210618,20210719,20210819]


#Open credentials
try:
    print('loading credentials')
    creds=pd.read_pickle(creds_file)
    print('credentials loaded')
except:
    print('no credentials found. starting new file')
    print('e-mail:')
    email=input()
    print('password:')
    pw=input()
    cred=oauth.authenticate(email,pw)
    cred['user']=email
    cred['expiry']=cred['created_at']+cred['expires_in']
    creds=pd.DataFrame.from_dict([cred])
    creds.set_index('user',inplace=True)

#Refresh any tokens
refresh_days=7
limit=time.time()+refresh_days*24*60*60
to_refresh=creds[creds['expiry']<limit]
new_creds=pd.DataFrame()
for index, row in to_refresh.iterrows():
    print(f'refreshing credential {index}')
    print(row['refresh_token'])
    new_cred=oauth.refresh(row['refresh_token'])
    new_cred['user']=row.name
    new_cred['expiry']=new_cred['created_at']+new_cred['expires_in']
    new_creds=new_creds.append(pd.DataFrame.from_dict([new_cred]))

#Update creds with new creds
if len(new_creds)>0:
    new_creds.set_index('user',inplace=True)
    creds.update(new_creds)
    creds.to_pickle(creds_file)

access_token=creds['access_token'][0]


try:
    data=pd.read_pickle(filename)
    start=(data.datetime.max()+datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    print(f'old data loaded. starting new data load from {start}')
except:
    print('no data found. starting new dataset')
    data=pd.DataFrame()
    start='2020-12-01'

def populate_solar(start,email):
    end=(datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    sites=[]
    products=tesla.GetProducts(access_token)
    if products[0:5]=='Error':
        print(products)
    for product in products:
        if 'energy_site_id' in product:
            sites.append(product['energy_site_id'])
    startdate=datetime.datetime.strptime(start, '%Y-%m-%d')
    enddate=datetime.datetime.strptime(end, '%Y-%m-%d')
    numdays=(enddate-startdate).days+1
    date_list = [startdate + datetime.timedelta(days=x) for x in range(numdays)]
    df=pd.DataFrame()
    for site in sites:
        for date in date_list:
            print(f"fetching data for site {site} and date {date}")
            tmp=tesla.GetSolarHistory(access_token,site,date)
            df_day=(pd.DataFrame.from_dict(tmp['time_series']))
            df_day['datetime']=pd.to_datetime(df_day['timestamp'],errors='coerce',utc=True)
            df_day['site']=site
            df_day.drop(columns='timestamp',inplace=True)
            df=df.append(df_day)
    if len(df)>0:        
        df['datetime']=df['datetime'].dt.tz_convert('US/Pacific')
        df['year']=df['datetime'].dt.year
        df['month']=df['datetime'].dt.month
        df['day']=df['datetime'].dt.day
        df['dow']=df['datetime'].dt.dayofweek
        df['hour']=df['datetime'].dt.hour
        df['minute']=df['datetime'].dt.minute
        df['solar_battery_power']=df['solar_power']+df['battery_power']
        df['home_power']=df['solar_battery_power']+df['grid_power']
        df['weekend']=df['dow']>4
        df['datetime'] = df['datetime'].dt.tz_localize(None)
        df['email']=email
    return df

data=data.append(populate_solar(start,email))
data.to_pickle(filename)


from sqlalchemy import create_engine
engine = create_engine('sqlite://', echo=False)

seasons=pd.DataFrame(columns=['rate', 'season_start', 'season_end','season'])
seasons.loc[1]=['TOD',1,5,'winter']
seasons.loc[2]=['TOD',6,9,'summer']
seasons.loc[3]=['TOD',10,12,'winter']
seasons.to_sql('seasons',con=engine)

times=pd.DataFrame(columns=['rate','season','weekend','holiday','time_start','time_end','billing_determinant'])
times.loc[1]=['TOD','winter',False,False,0,1700,'winter off-peak']
times.loc[2]=['TOD','winter',False,False,1700,2000,'winter peak']
times.loc[3]=['TOD','winter',False,False,2000,2400,'winter off-peak']
times.loc[4]=['TOD','winter',True,False,0000,2400,'winter off-peak']
times.loc[5]=['TOD','winter',False,True,0000,2400,'winter off-peak']
times.loc[6]=['TOD','winter',True,True,0000,2400,'winter off-peak']
times.loc[7]=['TOD','summer',False,False,0,1200,'summer off-peak']
times.loc[8]=['TOD','summer',False,False,1200,1700,'summer mid-peak']
times.loc[9]=['TOD','summer',False,False,1700,2000,'summer peak']
times.loc[10]=['TOD','summer',False,False,2000,2400,'summer mid-peak']
times.loc[11]=['TOD','summer',True,False,0000,2400,'summer off-peak']
times.loc[12]=['TOD','summer',False,True,0000,2400,'summer off-peak']
times.loc[13]=['TOD','summer',True,True,0000,2400,'summer off-peak']

times.to_sql('times',con=engine)

rates=pd.DataFrame(columns=['rate','start','end','billing_determinant','price'])
rates.loc[1]=['TOD','2020-01-01','2021-01-01','winter off-peak',.1035]
rates.loc[2]=['TOD','2020-01-01','2021-01-01','winter peak',.1430]
rates.loc[3]=['TOD','2020-01-01','2021-01-01','summer off-peak',.1209]
rates.loc[4]=['TOD','2020-01-01','2021-01-01','summer mid-peak',.1671]
rates.loc[5]=['TOD','2020-01-01','2021-01-01','summer peak',.2941]
rates.loc[6]=['TOD','2021-01-01','9999-01-01','winter off-peak',.1061]
rates.loc[7]=['TOD','2021-01-01','9999-01-01','winter peak',.1465]
rates.loc[8]=['TOD','2021-01-01','9999-01-01','summer off-peak',.1277]
rates.loc[9]=['TOD','2021-01-01','9999-01-01','summer mid-peak',.1765]
rates.loc[10]=['TOD','2021-01-01','9999-01-01','summer peak',.3105]
rates.to_sql('rates',con=engine)

fees_credits=pd.DataFrame(columns=['eff_from', 'eff_to', 'credit', 'time_start', 'time_end','price'])
fees_credits.loc[1]=['2020-01-01','9999-01-01','EV', 0000, 600, -.015]


#range=(data.datetime.min(),data.datetime.max())


def get_nth_DOW_for_YY_MM(nth, dow, yy, mm) -> datetime.date:
    #dow: 0 = Monday
    #nth can use -1 for last
    i = -1 if nth == -1 or nth == 5 else nth -1
    return list(filter(lambda x: x.month == mm,list(map(lambda x: x[0],calendar.Calendar(dow).monthdatescalendar(yy, mm)))))[i]

def mov_holidays(year):
    moving_holidays=[
          get_nth_DOW_for_YY_MM(3, 0, year, 1),
          get_nth_DOW_for_YY_MM(3, 0, year, 2),
          get_nth_DOW_for_YY_MM(-1, 0, year, 5),
          get_nth_DOW_for_YY_MM(1, 0, year, 9),
          get_nth_DOW_for_YY_MM(2, 0, year, 10),
          get_nth_DOW_for_YY_MM(4, 3, year, 11)
    ]
    return moving_holidays

def holiday_list(year):
    holidays=[]
    for i in mov_holidays(year):
        holidays.append(i)
    fixed_holidays=[(1,1),(2,12),(7,4),(11,11),(12,25)]
    for i in fixed_holidays:
        holidays.append(datetime.date(year,i[0],i[1]))
    return holidays

holidays=[]
for year in data.year.unique():
    for i in holiday_list(year):
        holidays.append(i)

data['holiday']=data.datetime.dt.date.isin(holidays)
data['weekend']=data.dow>4
data.to_sql('data',con=engine)


engine.execute('''CREATE VIEW season_data AS
               SELECT data.*,seasons.season
               FROM data
               LEFT JOIN seasons
               ON data.month between seasons.season_start AND seasons.season_end''')
               
engine.execute('''
               CREATE VIEW cost_benefit AS
               SELECT  season_data.year, season_data.month, season_data.day,
                       season_data.year*10000+season_data.month*100+season_data.day AS ymd,
                       season_data.hour*100+season_data.minute AS time,
                       times.billing_determinant, rates.price,
                       season_data.home_power/12000 AS home_kWh,
                       season_data.home_power/12000*rates.price AS home_value,
                       season_data.solar_power/12000 AS solar_kWh,
                       season_data.battery_power/12000 AS battery_kWh,
                       season_data.grid_power/12000 AS grid_kWh,
                       season_data.solar_power/12000*rates.price AS solar_value,
                       season_data.battery_power/12000*rates.price AS battery_value,
                       season_data.grid_power/12000*rates.price AS grid_value,
                       season_data.grid_power<0 AS generation
               FROM season_data
               INNER JOIN times
               ON  season_data.weekend=times.weekend
                   AND season_data.holiday=times.holiday
                   AND season_data.season=times.season
                   AND season_data.hour*100+season_data.minute>=times.time_start
                   AND season_data.hour*100+season_data.minute<times.time_end
               LEFT JOIN rates
               ON times.billing_determinant=rates.billing_determinant
                   AND rates.start<=season_data.datetime
                   AND rates.end>season_data.datetime
               ''')


for i in range(0,len(bill_starts)-1):
    start=(bill_starts[i])
    end=(bill_starts[i+1]-1)
    print(f"Bill period from {datetime.datetime.strptime(str(start), '%Y%m%d').strftime('%b %d, %Y')} to {datetime.datetime.strptime(str(end), '%Y%m%d').strftime('%b %d, %Y')}------------------------------------")
    length=(datetime.datetime.strptime(str(end), '%Y%m%d')-datetime.datetime.strptime(str(start), '%Y%m%d')).days
    print(f'{length} days in cycle.')
    df=pd.read_sql_query(f'''
                   SELECT  sum(home_kWh) as "Total kWh Used This Period",
                           sum(grid_kWh) AS "kWh from SMUD less kWh to SMUD"
                   FROM cost_benefit
                   WHERE ymd>={start} AND ymd<={end}
                   ''',engine)
    print(df.round(decimals=2))
    print('')
    df=pd.read_sql_query(f'''
                   SELECT  billing_determinant, price, generation,
                           sum(grid_kWh) as grid_kWh,
                           sum(grid_value) AS grid_value
                   FROM cost_benefit
                   WHERE ymd>={start} AND ymd<={end}
                   GROUP BY billing_determinant, price, generation
                   ORDER BY billing_determinant, generation, price
                   ''',engine)
    print(df.round(decimals=4))
    print('')

    df=pd.read_sql_query(f'''
               SELECT  sum(grid_value) AS 'Billed Amount', sum(solar_value) AS solar_value, sum(battery_value) AS battery_value, sum(grid_value)+sum(solar_value)+sum(battery_value) AS 'bill would have been'
               FROM cost_benefit
               WHERE ymd>={start} AND ymd<={end}
               ''',engine)
    print(df.round(decimals=2))
    print('')
        
#Validity checks for cartesians or failed joins

pd.read_sql_query('''
               SELECT  year, month, SUM(solar_value) AS solar_value, sum(battery_value) AS battery_value
               FROM cost_benefit
               GROUP BY year, month
               ''',engine)
              
tmp=pd.read_sql_query('''
               SELECT  ymd, SUM(solar_value)+SUM(battery_value) AS system_value, sum(solar_kwh) AS solar_kwh, sum(solar_value) as solar_value, sum(battery_value) as battery_value, sum(home_kwh) as home_kwh, sum(home_value) as home_value
               FROM cost_benefit
               GROUP BY ymd
               ''',engine)
tmp['date']=pd.to_datetime(tmp.ymd,format="%Y%m%d")

tmp.set_index('date').system_value.plot()
tmp.set_index('date').home_value.plot()

tmp.set_index('date')[['solar_kwh','home_kwh']].cumsum().plot()
tmp.set_index('date')[['system_value','solar_value','battery_value','home_value']].cumsum().plot()

tmp.set_index('date')[['battery_value']].plot()

               
# plots of value and prduction over time
#data.to_excel('data.xlsx')

"""

EV 0-6 -.015


"""
