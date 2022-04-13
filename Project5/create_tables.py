import configparser
import psycopg2
from psycopg2 import OperationalError
from initial_sql_queries import drop_sql, create_table_queries

def drop_table(connection):
 
    table_list = [
        'staging_events', 
        'staging_songs', 
        'songplays', 
        'songs', 
        'artists', 
        'users',
        'time'
    ]
    
    connection.autocommit= True
    cursor= connection.cursor()
    
    for table in table_list:
        try:
            cursor.execute(drop_sql.format(table))
            print(f'Drop {table} successfully!')
        except OperationalError as e:
            print(f'Drop error at table {table}, error:')
            print(e)

def create_table(connection):
    
    connection.autocommit= True
    cursor= connection.cursor()
    
    for q in create_table_queries:
        try:
            cursor.execute(q)
        except OperationalError as e:
            print(f'Create error, error at query {q}:')
            print(e)
    print('Table created successfully')
                  
def main():
    config= configparser.ConfigParser()
    config.read('config.cfg')
    
    DB_NAME= config['RS_CLUSTER']['DB_NAME']
    DB_USER= config['RS_CLUSTER']['DB_USER']
    DB_PWSD= config['RS_CLUSTER']['DB_PWSD']
    DB_HOST= config['RS_CLUSTER']['DB_HOST']
    DB_PORT= config['RS_CLUSTER']['DB_PORT']
    
    try:
        connection= psycopg2.connect(
            database= DB_NAME,
            user= DB_USER,
            password= DB_PWSD,
            host= DB_HOST,
            port= DB_PORT
        )
        
        print('Connection to Redshift database successfully')
        
        drop_table(connection)
        create_table(connection)
    
        connection.close()
  
    except OperationalError as e:
        print('Connection to Redshift can not be established, error:')
        print(e)
                  
    
                  
if __name__ == "__main__":
    main()