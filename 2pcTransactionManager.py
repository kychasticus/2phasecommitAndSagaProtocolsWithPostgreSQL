import psycopg2


def kick_connection(db_name):

    try:
        connection = psycopg2.connect(
            dbname=db_name,
            user='ivankychatyi',
            password=None,
            host='localhost',
            port=5432
        )

        return connection

    except psycopg2.Error as connection_error:
        print('The error occured during the connection:', connection_error)


def execute_query_post(connection, query):

    with connection.cursor() as cursor:
        try:
            cursor.execute(query)

        except psycopg2.Error as cursor_error:
            print('The error occured while executing the query:', cursor_error)
            connection.rollback()

def execute_query_pull(connection, query):

    with connection.cursor() as cursor:
        try:
            cursor.execute(query)
            request_output = cursor.fetchall()
            request_columns = [desc[0] for desc in cursor.description]

        except psycopg2.Error as cursor_error:
            print('The error occured while executing the query:', cursor_error)
            connection.rollback()

        return request_columns, request_output


CreateFlyBookTableQuery = """
CREATE TABLE public.fly_booking (
Booking_ID integer PRIMARY KEY,
Client_Name VARCHAR(50),
Fly_Number VARCHAR(10),
From_air VARCHAR(10),
To_air VARCHAR(10),
Date DATE);
"""

CreateHtlBookTableQuery = """
CREATE TABLE htl_booking (
Booking_ID integer,
Client_Name VARCHAR(50),
Hotel_Name VARCHAR(10),
Arrival DATE,
Departure DATE);
"""

CreateAccountTableQuery = """
CREATE TABLE account (
Account_ID integer,
Client_Name VARCHAR(50),
Amount numeric(20,2) CHECK(Amount >= 0)
);
"""

def init_table(db_name, table_name, table_query):
    conn = kick_connection(db_name)

    try:
        execute_query_post(conn, f'DROP TABLE IF EXISTS public.{table_name}')
        execute_query_post(conn, table_query)

    except psycopg2.Error as init_error:
        print(init_error)
        conn.close()

    if conn:
        conn.close()


init_table('FlyBooking', 'fly_booking', CreateFlyBookTableQuery)
init_table('HotelBooking', 'htl_booking', CreateHtlBookTableQuery)
init_table('Account', 'account', CreateAccountTableQuery)


test_connection = kick_connection('FlyBooking')
test_query = "select * from public.fly_booking"
col, out = execute_query_pull(test_connection, test_query)
test_connection.close()
