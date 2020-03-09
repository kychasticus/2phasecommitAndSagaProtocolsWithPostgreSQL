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


def execute_query_post_NoAutocommit(connection, query):

    with connection.cursor() as cursor:
        try:
            cursor.execute(query)
            response = cursor.rowcount

            return response

        except psycopg2.Error as cursor_error:
            print('The error occured while executing the query:', cursor_error)

            return cursor_error

def execute_query_post_autocommitrollback(connection, query):

    with connection.cursor() as cursor:
        try:
            cursor.execute(query)
            connection.commit()
            response = cursor.rowcount

            return response

        except psycopg2.Error as cursor_error:
            print('The error occured while executing the query:', cursor_error)
            connection.rollback()

            return cursor_error


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
booking_id integer PRIMARY KEY,
client_name VARCHAR(50),
fly_number VARCHAR(10),
from_air VARCHAR(10),
to_air VARCHAR(10),
date DATE);
"""

CreateHtlBookTableQuery = """
CREATE TABLE public.htl_booking (
booking_id integer PRIMARY KEY,
client_name VARCHAR(50),
hotel_name VARCHAR(10),
arrival DATE,
departure DATE);
"""

CreateAccountTableQuery = """
CREATE TABLE public.account (
account_id integer PRIMARY KEY,
client_name VARCHAR(50),
amount numeric(20,2) CHECK(Amount >= 0)
);
"""

Record1 = {'flight': ['345235', 'peter', 'UA3456', 'KBP', 'TGL', '2020-02-20']}


def init_table(db_name, table_name, table_query):
    conn = kick_connection(db_name)

    try:
        a_1 = execute_query_post_autocommitrollback(conn, f'DROP TABLE IF EXISTS public.{table_name}')
        a_2 = execute_query_post_autocommitrollback(conn, table_query)
        print(a_1, a_2)

    except psycopg2.Error as init_error:
        print(init_error)
        conn.close()

    finally:
        conn.close()

def init_account_balance():

    db_name = 'Account'
    conn = kick_connection(db_name)
    query = """
    INSERT INTO public.account
    (account_id, client_name, amount)
    VALUES('456754', 'peter', 500.00)
    """

    a_1 = execute_query_post_autocommitrollback(conn, query)
    print(a_1)

    conn.close()


def prepare_flight_transaction():

    db_name = 'FlyBooking'

    conn = kick_connection(db_name)

    query = f"""
    INSERT INTO public.fly_booking
    (booking_id, client_name, fly_number, from_air, to_air, date) 
    VALUES('345235', 'peter', 'UA3456', 'KBP', 'TGL', '2020-02-20');
    """

    begin_response = execute_query_post_NoAutocommit(conn, 'BEGIN')
    transaction_response = execute_query_post_NoAutocommit(conn, query)
    prepare_response = execute_query_post_NoAutocommit(conn, "PREPARE TRANSACTION 'peter_flight'")

    conn.close()

    return begin_response, transaction_response, prepare_response

def prepare_hotel_transaction():

    db_name = 'HotelBooking'

    conn = kick_connection(db_name)

    query = f"""
    INSERT INTO public.htl_booking
    (booking_id, client_name, hotel_name, arrival, departure) 
    VALUES('457634', 'peter', 'Ritz', '2020-02-20', '2020-03-20');
    """

    begin_response = execute_query_post_NoAutocommit(conn, 'BEGIN')
    transaction_response = execute_query_post_NoAutocommit(conn, query)
    prepare_response = execute_query_post_NoAutocommit(conn, "PREPARE TRANSACTION 'peter_hotel'")

    conn.close()

    return begin_response, transaction_response, prepare_response

def prepare_account_transaction():

    db_name = 'Account'

    conn = kick_connection(db_name)

    query = f"""
    UPDATE public.account
    SET amount = amount - 100 
    WHERE client_name = 'peter';
    """

    begin_response = execute_query_post_NoAutocommit(conn, 'BEGIN')
    transaction_response = execute_query_post_NoAutocommit(conn, query)
    prepare_response = execute_query_post_NoAutocommit(conn, "PREPARE TRANSACTION 'peter_account'")

    conn.close()

    return begin_response, transaction_response, prepare_response


# Initialize tables at the distinct DB's
init_table('FlyBooking', 'fly_booking', CreateFlyBookTableQuery)
init_table('HotelBooking', 'htl_booking', CreateHtlBookTableQuery)
init_table('Account', 'account', CreateAccountTableQuery)
init_account_balance()

# Prepare transactions
f1, f2, f3 = prepare_flight_transaction()
h1, h2, h3 = prepare_hotel_transaction()
a1, a2, a3 = prepare_account_transaction()


# Test data
test_connection = kick_connection('FlyBooking')
test_query = "select * from public.fly_booking"
col, out = execute_query_pull(test_connection, test_query)
test_connection.close()
