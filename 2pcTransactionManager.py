import psycopg2


# PREPARATION FUNCTIONS
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


# INITIALIZE TABLES AND BALANCES FUNCTIONS
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
    query_peter = """
    INSERT INTO public.account
    (account_id, client_name, amount)
    VALUES('456754', 'peter', 500.00)
    """

    query_julee = """
        INSERT INTO public.account
        (account_id, client_name, amount)
        VALUES('546765', 'julee', 100.00)
        """

    a_1 = execute_query_post_autocommitrollback(conn, query_peter)
    print(a_1)

    a_2 = execute_query_post_autocommitrollback(conn, query_julee)
    print(a_2)

    conn.close()


# PREPARE TRANSACTION BLOCK
def prepare_flight_transaction(transaction_name, transaction_details):

    db_name = 'FlyBooking'
    conn = kick_connection(db_name)

    query = """
    INSERT INTO public.fly_booking
    (booking_id, client_name, fly_number, from_air, to_air, date) 
    VALUES(%s, %s, %s, %s, %s, %s);
    """

    query_data = (f'{transaction_details[0]}',
                  f'{transaction_details[1]}',
                  f'{transaction_details[2]}',
                  f'{transaction_details[3]}',
                  f'{transaction_details[4]}',
                  f'{transaction_details[5]}')

    cursor = conn.cursor()
    responses = []

    try:
        cursor.execute('BEGIN')
        responses.append(cursor.rowcount)

        cursor.execute(query, query_data)
        responses.append(cursor.rowcount)

        cursor.execute("PREPARE TRANSACTION %s", (transaction_name,))
        responses.append(cursor.rowcount)
        cursor.execute("END")

        return responses

    except psycopg2.Error as cursor_error:
        print('The error occured while executing the query:', cursor_error)
        conn.rollback()
        responses.append(cursor_error)

        return responses

    finally:
        cursor.close()
        cursor2 = conn.cursor()
        cursor2.execute("END")
        cursor2.close()
        conn.close()



def prepare_hotel_transaction(transaction_name, transaction_details):

    db_name = 'HotelBooking'
    conn = kick_connection(db_name)

    query = f"""
    INSERT INTO public.htl_booking
    (booking_id, client_name, hotel_name, arrival, departure) 
    VALUES(%s, %s, %s, %s, %s);
    """

    query_data = (f'{transaction_details[0]}',
                  f'{transaction_details[1]}',
                  f'{transaction_details[2]}',
                  f'{transaction_details[3]}',
                  f'{transaction_details[4]}')

    cursor = conn.cursor()
    responses = []

    try:
        cursor.execute('BEGIN')
        responses.append(cursor.rowcount)

        cursor.execute(query, query_data)
        responses.append(cursor.rowcount)

        cursor.execute("PREPARE TRANSACTION %s", (transaction_name,))
        responses.append(cursor.rowcount)
        cursor.execute("END")

        return responses

    except psycopg2.Error as cursor_error:
        print('The error occured while executing the query:', cursor_error)
        conn.rollback()
        responses.append(cursor_error)

        return responses

    finally:
        cursor.close()
        cursor2 = conn.cursor()
        cursor2.execute("END")
        cursor2.close()
        conn.close()


def prepare_account_transaction(transaction_name, transaction_details):

    db_name = 'Account'
    conn = kick_connection(db_name)

    query = f"""
    UPDATE public.account
    SET amount = amount - %s 
    WHERE account_id = %s;
    """
    query_data = (f'{transaction_details[2]}',
                  f'{transaction_details[0]}')

    cursor = conn.cursor()
    responses = []
    try:
        cursor.execute('BEGIN')
        responses.append(cursor.rowcount)

        cursor.execute(query, query_data)
        responses.append(cursor.rowcount)

        cursor.execute("PREPARE TRANSACTION %s", (transaction_name,))
        responses.append(cursor.rowcount)
        cursor.execute("END")

        return responses

    except psycopg2.Error as cursor_error:
        print('The error occured while executing the query:', cursor_error)
        conn.rollback()
        responses.append(cursor_error)

        return responses

    finally:
        cursor.close()
        cursor2 = conn.cursor()
        cursor2.execute("END")
        cursor2.close()
        conn.close()



# COMMIT OR ROLLBACK PREPARED BLOCK
def finish_prepared_flight(flight_transaction, action):

    db_fly = 'FlyBooking'
    query = "COMMIT PREPARED %s" if action == 'commit' else "ROLLBACK PREPARED %s"
    responses = []
    conn = kick_connection(db_fly)

    with conn.cursor() as cursor:
        try:
            cursor.execute("END")
            cursor.execute(query, (flight_transaction,))
            responses.append(cursor.rowcount)

            return responses

        except psycopg2.Error as cursor_error:
            print('The error occured while executing COMMIT PREPARED:', cursor_error)
            conn.rollback()

            return cursor_error

        finally:
            conn.close()


def finish_prepared_hotel(hotel_transaction, action):

    db_hotel = 'HotelBooking'
    query = "COMMIT PREPARED %s" if action == 'commit' else "ROLLBACK PREPARED %s"
    responses = []
    conn = kick_connection(db_hotel)

    with conn.cursor() as cursor:
        try:
            cursor.execute(query, (hotel_transaction,))
            responses.append(cursor.rowcount)

            return responses

        except psycopg2.Error as cursor_error:
            print('The error occured while executing the query:', cursor_error)
            conn.rollback()

            return cursor_error

        finally:
            conn.close()


def finish_prepared_account(account_transaction, action):

    db_account = 'Account'
    query = "COMMIT PREPARED %s" if action == 'commit' else "ROLLBACK PREPARED %s"
    responses = []
    conn = kick_connection(db_account)

    with conn.cursor() as cursor:
        try:
            cursor.execute(query, (account_transaction,))
            responses.append(cursor.rowcount)

            return responses

        except psycopg2.Error as cursor_error:
            print('The error occured while executing the query:', cursor_error)
            conn.rollback()

            return cursor_error

        finally:
            conn.close()


def revert_flight_saga(transaction_details):
    db_name = 'FlyBooking'
    conn = kick_connection(db_name)

    query = f"""
        DELETE FROM public.fly_booking 
        WHERE client_name = %s;
        """
    query_data = (f'{transaction_details[1]}',)

    cursor = conn.cursor()
    responses = []

    try:
        cursor.execute(query, query_data)
        responses.append(cursor.rowcount)
        conn.commit()

        return responses

    except psycopg2.Error as cursor_error:
        print('The error occured while executing the query:', cursor_error)
        conn.rollback()
        responses.append(cursor_error)

        return responses

    finally:
        cursor.close()
        conn.close()


def revert_hotel_saga(transaction_details):

    db_name = 'HotelBooking'
    conn = kick_connection(db_name)

    query = f"""
        DELETE FROM public.htl_booking 
        WHERE client_name = %s;
        """
    query_data = (f'{transaction_details[1]}',)

    cursor = conn.cursor()
    responses = []

    try:
        cursor.execute(query, query_data)
        responses.append(cursor.rowcount)
        conn.commit()

        return responses

    except psycopg2.Error as cursor_error:
        print('The error occured while executing the query:', cursor_error)
        conn.rollback()
        responses.append(cursor_error)

        return responses

    finally:
        cursor.close()
        conn.close()


# INITIALIZE TABLES QUERIES
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

# TRANSACTION MANAGER CODE ITSELF
def book_trip(name, data):

    flight = name+'_flight'
    hotel = name+'_hotel'
    account = name+'_account'

    prepare_flight_response = prepare_flight_transaction(flight, data['flight'])
    prepare_hotel_response = prepare_hotel_transaction(hotel, data['hotel'])
    prepare_account_response = prepare_account_transaction(account, data['account'])
    print('Transactions sent to DB for check')


    # Commit transactions
    if ((prepare_flight_response[1] == 1) &
        (prepare_hotel_response[1] == 1) &
        (prepare_account_response[1] == 1)):

        commit_flight_response = finish_prepared_flight(flight, 'commit')
        commit_hotel_response = finish_prepared_hotel(hotel, 'commit')
        commit_account_response = finish_prepared_account(account, 'commit')
        print('Transaction is successfull')

    # Rollback transactions
    else:
        rollback_flight_response = finish_prepared_flight(flight, 'rollback')
        rollback_hotel_response = finish_prepared_hotel(hotel, 'rollback')
        rollback_account_response = finish_prepared_account(account, 'rollback')
        print('Transaction is unsuccessfull, not enough funds')

def book_trip_saga(name, data):

    flight = name + '_flight'
    hotel = name + '_hotel'
    account = name + '_account'

    prepare_flight_response = prepare_flight_transaction(flight, data['flight'])

    if prepare_flight_response[1] == 1:
        commit_flight_response = finish_prepared_flight(flight, 'commit')
        print('Flight is booked')
        prepare_hotel_response = prepare_hotel_transaction(hotel, data['hotel'])

        if prepare_hotel_response[1] == 1:
            commit_hotel_response = finish_prepared_hotel(hotel, 'commit')
            print('Hotel is booked')
            prepare_account_response = prepare_account_transaction(account, data['account'])

            if prepare_account_response[1] == 1:
                commit_account_response = finish_prepared_account(account, 'commit')
                print('Money for the trip is charged. Trip is successfully booked!')

            else:
                rollback_account_response = finish_prepared_account(account, 'rollback')
                print('There is a problem with a bank account, All bookings would be reverted')
                revert_flight_saga(data['flight'])
                print('Flight is cancelled')
                revert_hotel_saga(data['hotel'])
                print('Hotel is cancelled')

        else:
            rollback_hotel_response = finish_prepared_hotel(hotel, 'rollback')
            print('The hotel was unable to be booked. The flight is going to be cancelled as well')
            revert_flight_saga(data['flight'])
            print('Flight is cancelled')

    else:
        rollback_flight_response = finish_prepared_flight(flight, 'rollback')
        print('Flight booking was unsuccessfull. Trip is cancelled')


# Initialize tables at the distinct DB's
init_table('FlyBooking', 'fly_booking', CreateFlyBookTableQuery)
init_table('HotelBooking', 'htl_booking', CreateHtlBookTableQuery)
init_table('Account', 'account', CreateAccountTableQuery)
init_account_balance()

# TRANSACTIONS TO PERFORM

# The one that should be committed
Peter = {'flight': ['345235', 'peter', 'UA3456', 'KBP', 'TGL', '2020-02-20'],
           'hotel': ['457634', 'peter', 'Ritz', '2020-02-20', '2020-03-20'],
           'account': ['456754', 'peter', '150']}

# The one that should be rollbacked
Julee = {'flight': ['345633', 'julee', 'UA7556', 'HGL', 'NYJ', '2020-05-20'],
           'hotel': ['768465', 'julee', 'Ritz', '2020-05-20', '2020-05-23'],
           'account': ['546765', 'julee', '500']}


# ACTUALLY THE 2PC TRANSACTION RUNS

book_trip('peter', Peter)
book_trip('julee', Julee)


# ACTUALLY THE SAGA TRANSACTION RUNS

book_trip_saga('peter', Peter)
book_trip_saga('julee', Julee)