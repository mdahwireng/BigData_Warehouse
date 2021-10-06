from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from mysql.connector import connection, connect, Error
import pandas as pd

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

def insert_to(connection,script,vals):
    """Insert data into tables given a connection to database,script and values"""
    try:
        print('Creating cursor...\n')
        mycursor = connection.cursor()
        print('Creating cursor... completed\n')

        print('Inserting values...\n')
        mycursor.execute(script, vals)
        print('Inserting values... completed\n')
        connection.commit()
        mycursor.close()

    except Error as e:
        print("Error encounted\n")
        print(e)

script = """CREATE SCHEMA IF NOT EXISTS `Warehouse` ;
USE `Warehouse` ;


CREATE TABLE IF NOT EXISTS `Warehouse`.`stations` (
  `id` INT NOT NULL,
  `name` VARCHAR(225) NOT NULL,
  `lat` DECIMAL(10,8) NULL,
  `long` DECIMAL(11,8) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `Warehouse`.`weekdays` (
  `weekday_id` INT NOT NULL,
  `day` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`weekday_id`))
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `Warehouse`.`traffic` (
  `station_id` INT NOT NULL,
  `weekday_id` INT NOT NULL,
  `hour` INT NOT NULL,
  `min` INT NOT NULL,
  `sec` INT NOT NULL,
  `tot_flow` INT NOT NULL,
  INDEX `fk_weekdays_idx` (`weekday_id` ASC) VISIBLE,
  INDEX `fk_station_idx` (`station_id` ASC) VISIBLE,
  CONSTRAINT `fk_station`
    FOREIGN KEY (`station_id`)
    REFERENCES `Warehouse`.`stations` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_weekdays`
    FOREIGN KEY (`weekday_id`)
    REFERENCES `Warehouse`.`weekdays` (`weekday_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;"""

def create_schema():
  myConnection = create_conn( host='localhost', user='warehouse', password='password')

  run_script(connection=myConnection, script=script)


def fill_stations():
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
    script =  """INSERT INTO `traffic` (`station_id`, `weekday_id`, `hour`, `min`, `sec`, `tot_flow`) VALUES (%s, %s, %s, %s, %s, %s)"""
    myConnection = create_conn( 
                                host='localhost', 
                                user='warehouse', 
                                password='password', 
                                database='Warehouse'
                                )
    traffic = pd.read_csv('data/I80_median.csv')
    for row,k in traffic.iterrows():
        vals = (int(k['ID']), int(k['weekday']), int(k['hour']), int(k['minute']), int(k['second']), int(k['totalflow']))
        insert_to(connection=myConnection, vals=vals, script=script)

def populate_traff():
    fill_traf()
    

DAG_CONFIG = {
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email': ['kaaymyke@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'owner' : 'admin',
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '0 0/1 0 ? * * *',
}

with DAG("my_wh_dag", # Dag id
    default_args=DAG_CONFIG,
    catchup=False,
    schedule_interval='@once'
) as dag:
    # Tasks are implemented under the dag object
    create_database = PythonOperator(
        task_id="Create_database",
        python_callable= create_schema
    )
    fill_stations_tbl = PythonOperator(
    task_id="populate_station_table",
    python_callable= fill_stations
    )
    fill_weekdays_tbl= PythonOperator(
    task_id="populate_weekdays_table",
    python_callable= fill_weekdays
    )
    fill_traffic_tbl= PythonOperator(
    task_id="populate_traffic_table",
    python_callable= populate_traff
    )
    create_database >> [fill_stations_tbl, fill_weekdays_tbl] >> fill_traffic_tbl