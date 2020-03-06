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


def execute_query(connection, query):

    with connection.cursor() as cursor:
        try:
            cursor.execute(query)
            request_output = cursor.fetchall()
            request_columns = [desc[0] for desc in cursor.description]

            return request_columns, request_output

        except psycopg2.Error as cursor_error:
            print('The error occured while executing the query:', cursor_error)
            connection.rollback()



test_connection = kick_connection('ivankychatyi')

test_query = "select * from ivankychatyi"

execute_query(test_connection,test_query)

test_connection.close()