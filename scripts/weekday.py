from sql_utilities import create_conn, insert_to

def fill_weekdays():
    script =  """INSERT INTO `weekdays` (`weekday_id`, `day`) VALUES (%s, %s)"""
    myConnection = create_conn( 
                                host='localhost', 
                                user='warehouse', 
                                password='password', 
                                database='Warehouse'
                                )

    weekdays=['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday']

    for k in enumerate(weekdays):
        insert_to(connection=myConnection, vals=k, script=script)