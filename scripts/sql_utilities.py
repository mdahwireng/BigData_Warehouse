from mysql.connector import connect, Error

def create_conn(host,user,password,database=None):
    """creates a connection to mysql server taking host, user, 
    password, database. database is None by default"""
    try:
        print('Creating connection...\n')
        if database:
            connection = connect( 
                host=host, 
                user=user, 
                passwd=password,
                database=database)
            print('Connection created and returned!\n')
            return connection

        else:
            connection = connect( 
                host=host, 
                user=user, 
                passwd=password)
            print('Connection created and returned!\n')
            return connection
    except Error as e:
        print(e)

def run_script(connection, script):
    """executes a given script through a parsed connection"""
    try:
        print('Creating cursor...\n')
        mycursor = connection.cursor()
        print('Creating cursor... completed\n')

        print('Executing script...\n')
        mycursor.execute(script)
        print('Executing script... completed\n')
        mycursor.close()
        
    except Error as e:
        print("Error encounted\n")
        print(e)

