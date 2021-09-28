import pandas as pd
from sql_utilities import create_conn, insert_to

def create_traf_data():
    traffic = pd.read_csv('data/weekday.csv')
    day_week = range(1,7)

    days = []
    for i in range(int(len(traffic)/6)):
        for k in day_week:
            days.append(k)

    traffic['weekday'] = days

    traffic.to_csv('data/traffic.csv')

def fill_traf():
    script =  """INSERT INTO `stations` (`station_id`, `weekday_id`, `hours`, `min`, `sec`, `tot_flow`) VALUES (%s, %s, %s, %s)"""
    myConnection = create_conn( 
                                host='localhost', 
                                user='warehouse', 
                                password='password', 
                                database='Warehouse'
                                )
    traffic = pd.read_csv('data/traffic.csv')
    for row,k in traffic.iterrows():
        vals = (k['ID'], k['weekday'], k['hours'], k['min'], k['sec'], k['totalflow'])
        insert_to(connection=myConnection, vals=vals, script=script)