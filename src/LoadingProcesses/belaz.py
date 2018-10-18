import ast
import getopt
import sys
import json
import pg8000
from bsonstream import KeyValueBSONInput
from collections import defaultdict


def usage():
    print('Usage: belaz.py')
    print('       Loads data from bson files to target db\n')
    print('Arguments: --config_file        - The file that contains the configuration')

    sys.exit()


def get_connection_params(config_file):
    with open(config_file) as configuration:
        json_source = json.load(configuration)

    conn_host = json_source['host']
    conn_db = json_source['database']
    conn_schema = json_source['schema']
    conn_user = json_source['user']
    conn_pass = json_source['password']
    conn_port = json_source['port']

    connection_params = {
        'conn_host': conn_host,
        'conn_db': conn_db,
        'conn_schema': conn_schema,
        'conn_user': conn_user,
        'conn_pass': conn_pass,
        'conn_port': conn_port
    }
    return connection_params


def get_files_location(config_file):
    with open(config_file) as configuration:
        json_source = json.load(configuration)

    table_files_array = json_source['tables']
    table_files = defaultdict(list)

    for table in table_files_array:
        if table['dump_flag'] == '1':
            table_files[table['name']] = [table['file'], table['columns']]
    return table_files


def get_insert_statement(data_stream, schema, table):
    insert_values = ''
    for data_row in data_stream:
        row = '('
        for column in data_row:
            row += '\'' + str(column)[:200].replace('\'', '\'\'') + '\','
        row = row[:-1] + '),\n'
        insert_values += row
    insert_values = insert_values[:-2]
    insert_statement = """
            INSERT INTO {schema}.{table}
            VALUES
                {insert_values}
            ;
            """.format(schema=schema, table=table, insert_values=insert_values)
    return insert_statement


def get_truncate_statement(schema, table):
    truncate_statement = 'TRUNCATE TABLE {schema}.{table};'.format(schema=schema, table=table)
    return truncate_statement


def get_db_connection(host, db, user, password, port):
    print('Connecting to the PostgreSQL database...')
    conn = pg8000.connect(host=host, database=db, user=user,
                          password=password,
                          port=int(port))
    conn.autocommit = True

    return conn


def execute_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    print('The amount of records affected are: {row_count}'.format(row_count=cursor.rowcount))
    try:
        results = cursor.fetchall()
    except pg8000.ProgrammingError as e:
        if "no result set" in str(e):
            return None
        else:
            raise e

    return results


def close_db_connection(conn):
    conn.close()
    print('Database connection has been closed')


def load_data(config):
    table_files = get_files_location(config)
    connection_params = get_connection_params(config)

    for table_name, table_content in table_files.items():
        rows = []
        columns_list = ast.literal_eval(table_content[1])
        try:
            print("""
*********************************************************
Loading table: {table}
            """.format(table=table_name))
            with open(table_content[0], 'rb') as f:

                # Data sampling
                try:
                    stream = KeyValueBSONInput(fh=f)
                    for dict_data in stream:
                        row_tuple = []
                        for column in columns_list:
                            try:
                                row_tuple.append(dict_data[column])
                            except KeyError:
                                row_tuple.append('')
                                pass
                        rows.append(row_tuple)
                except ValueError:
                    pass
        except StopIteration:
            pass
        print('output: \n')

        # Insert statement execution
        schema = connection_params['conn_schema']
        insert_statement = get_insert_statement(rows, schema, table_name)
        truncate_statement = get_truncate_statement(schema, table_name)

        # Connection to PostgreSQL db & Insert
        conn_host = connection_params['conn_host']
        conn_db = connection_params['conn_db']
        conn_user = connection_params['conn_user']
        conn_pass = connection_params['conn_pass']
        conn_port = connection_params['conn_port']
        conn = None

        try:
            conn = get_db_connection(conn_host, conn_db, conn_user, conn_pass, conn_port)

            # Truncate staging table
            execute_query(conn, truncate_statement)

            # Insert into staging table
            execute_query(conn, insert_statement)

            conn.commit()

        except (Exception, pg8000.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                close_db_connection(conn)


def main(argv):
    supported_args = """config_file="""
    optlist = []

    # Extract parameters
    try:
        optlist, remaining = getopt.getopt(argv[1:], "", supported_args.split())
    except getopt.GetoptError as err:
        print(str(err))
        usage()

    # Parse parameters
    kwargs = {}
    optlist_flag = 0

    for arg, value in optlist:
        optlist_flag = 1
        if arg == "--config_file":
            if value == '':
                usage()
            else:
                kwargs['config_file'] = value
        else:
            usage()
        load_data(kwargs['config_file'])

    if optlist_flag == 0:
        usage()


if __name__ == '__main__':
    main(sys.argv)
