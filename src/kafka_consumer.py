from configparser import ConfigParser
from json import loads
from kafka import KafkaConsumer
import psycopg2

def MyKafkaConsumer():
    print("Starting application..")
    consumer = KafkaConsumer(
        'data_test',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    def config(filename='database.ini', section='postgresql'):
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(filename)

        # get section, default to postgresql
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))

        return db

    def create_connection(db_user, db_pass, db_host, db_port, db_name=None):
            connection = None
            try:
                if db_name is None:
                    connection = psycopg2.connect("host=" + db_host + " port=" + db_port + " user=" + db_user + " password=" + db_pass)
                else:
                    connection = psycopg2.connect("host=" + db_host + " port=" + db_port + " user=" + db_user + " password=" + db_pass + " dbname=" + db_name)
                
                return connection
            except Exception as e:
                print(e)
                exit(1)

            return connection

    def create_database(connection, database_name):
        try:
            cursor = connection.cursor()
            query_check_database = "SELECT datname FROM pg_catalog.pg_database WHERE datname = "+ database_name
            cursor.execute(query_check_database)
            
            exists = cursor.fetchone()
            if not exists:
                # Close the transaction from select
                connection.commit()
                # Set autocommit to create database
                connection.autocommit = True
                query_create_database = "CREATE DATABASE "+ database_name +";"
                cursor.execute(query_create_database)

        except Exception as e:
            print(e)
            exit(1)

    def create_table(connection, query_create):
        try:
            cursor = connection.cursor()
            cursor.execute(query_create)
        except Exception as e:
            print(e)

    # Setup params
    params = config()
    db_user = "'" + params['db_user'] + "'"
    db_pass = "'" + params['db_pass'] + "'"
    db_host = "'" + params['db_host'] + "'"
    db_port = "'" + params['db_port'] + "'"
    db_name = "'" + params['db_name'] + "'"
    
    # First connect to create db if not exists
    conn = create_connection(db_user, db_pass, db_host, db_port)
    if conn is not None:
        create_database(conn, db_name)
    conn.close()

    # Second connect after db exists
    conn = create_connection(db_user, db_pass, db_host, db_port, db_name)
    if conn is not None:
        print("Application is running")
        c = conn.cursor()
        for message in consumer:
            data = message.value
            print("Queue in: " + data)
            for row in data['activities']:
                if row['operation'].upper() == 'INSERT':
                    # TODO 1: if the table is not yet available in the db,
                    # it will be created accordingly with the relevant fields.
                    table_name = row['table']
                    
                    # query_check_table_exists = "SELECT count(name) FROM sqlite_master WHERE type='table' AND name='" + table_name + "'"
                    query_check_table_exists = "SELECT EXISTS (SELECT FROM pg_tables WHERE  tablename  = '"+ table_name +"');"
                    c.execute(query_check_table_exists)
                    # if the count is 1, then table exists
                    if c.fetchone()[0] == 0:
                        # Query preparation
                        column_name_types = ""
                        for i in range(0, len(row['col_names'])):
                            column_name_types += row['col_names'][i] + " " + row['col_types'][i] + ","

                        # remove trailing comma
                        column_name_types = column_name_types[:-1]
                        query_create_table = ""
                        query_create_table = "CREATE TABLE " + table_name + " ( " + column_name_types + " );"
                        c.execute(query_create_table)

                    else:
                        # TODO 2: if the table is already available in the db,
                        # but the relevant field does not exists,
                        # it will alter table to add relevant field

                        # SELECT column_name FROM information_schema.columns WHERE table_name='your_table' and column_name='your_column';
                        query_check_column_exists = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='"+ table_name +"';"
                        c.execute(query_check_column_exists)

                        table_columns = c.fetchall()
                        db_column_names = []
                        db_column_names_types = {}
                        for table_column in table_columns:
                            db_column_names.append(table_column[0])
                            db_column_names_types[table_column[0]] = table_column[1]

                        dict_column_names_types = dict(zip(row['col_names'], row['col_types']))

                        for columnname in row['col_names']:
                            if columnname not in db_column_names:
                                query_alter_table = "ALTER TABLE " + table_name + " ADD COLUMN " + columnname + " " + \
                                                    dict_column_names_types[columnname] + ";"
                                c.execute(query_alter_table)

                            # TODO 3: if the table is already available in the db,
                            # but the relevant field mismatch the one in the table,
                            # it will fail the entire transaction and output error
                            else:
                                if dict_column_names_types[columnname].upper() != db_column_names_types[columnname].upper():
                                    print("Column type mismatch. Rollback transaction.")
                                    conn.rollback()
                                    break
                        else:
                            continue
                        break

                    # TODO 4: insert record
                    column_inserts = ""
                    question_marks = ""
                    for columnname in row['col_names']:
                        column_inserts += columnname + ","
                        question_marks += "%s,"
                    # remove trailing comma
                    column_inserts = column_inserts[:-1]
                    question_marks = question_marks[:-1]

                    query_insert = "INSERT INTO " + table_name + "(" + column_inserts + ") VALUES (" + question_marks + ");"
                    c.execute(query_insert, row['col_values'])

                elif row['operation'].upper() == 'DELETE':
                    # TODO 5: if the table is not available,
                    # it will fail the entire transaction and output error
                    table_name = row['table']

                    query_check_table_exists = "SELECT EXISTS (SELECT FROM pg_tables WHERE  tablename  = '"+ table_name +"');"
                    c.execute(query_check_table_exists)
                    # if the count is 1, then table exists
                    if c.fetchone()[0] == 0:
                        print('Table does not exist. Rollback transaction')
                        conn.rollback()
                        break

                    # TODO 6: if the table is available
                    # but the record is not found,
                    # it will fail the entire transaction and output error
                    else:
                        query_check_row_exists = "SELECT count(1) FROM " + table_name
                        where_keys = " WHERE 1=1"

                        for i in range(0, len(row['old_value']['col_names'])):
                            where_keys += " AND " + row['old_value']['col_names'][i] + "=%s"

                        query_check_row_exists_where = query_check_row_exists + where_keys
                        c.execute(query_check_row_exists_where, row['old_value']['col_values'])
                        # if the count is 1, then row exists
                        if c.fetchone()[0] == 0:
                            print('A data row does not exist. Rollback transaction')
                            conn.rollback()
                            break
                        
                        # TODO 7: delete record
                        else:
                            query_delete = "DELETE FROM " + table_name + where_keys
                            c.execute(query_delete, row['old_value']['col_values'])

                else:
                    print("Activities not available")

            conn.commit()
            print("Done!")
        else:
            print("Error! cannot create the database connection.")

        if conn:
            print("Closing connection")
            conn.close()

if __name__ == '__main__':
    MyKafkaConsumer()