import pandas as pd
from sql_utilities import create_conn, insert_to

script =  """INSERT INTO `stations` (`id`, `name`, `lat`, `long`) VALUES (%s, %s, %s, %s)"""
myConnection = create_conn( 
                            host='localhost', 
                            user='warehouse', 
                            password='password', 
                            database='Warehouse'
                            )

stations = pd.read_csv('data/I80_stations.csv')
for row,k in stations.iterrows():
    vals = (k['ID'], k['Name'],k['Latitude'], k['Longitude'])
    insert_to(connection=myConnection, vals=vals, script=script)