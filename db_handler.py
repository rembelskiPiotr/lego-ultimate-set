import psycopg2 as ps
import pandas as pd


def connect_to_db(host_name, dbname, username, password, port):
    try:
        conn = ps.connect(host=host_name, database=dbname,
                          user=username, password=password, port=port)
    except ps.OperationalError as err:
        raise err
    else:
        return conn


def create_table_statement(dataframe, table_name):
    replacements = {
        'timedelta64[ns]': 'varchar',
        'object': 'varchar',
        'float64': 'float',
        'int64': 'int',
        'datetime64': 'timestamp'
    }
    column_string = ", ".join("{} {}".format(column, dtype) for (
        column, dtype) in zip(dataframe.columns, dataframe.dtypes.replace(replacements)))

    table_statement = 'create table if not exists {} ({})'.format(
        table_name, column_string)

    return table_statement


def create_table(curr, table_statment):
    create_table_command = table_statment

    curr.execute(create_table_command)


def check_if_row_exists(curr, record, column, table_name):
    query = ("""SELECT {} FROM {} WHERE {} = %s""").format(
        column, table_name, column)
    curr.execute(query, (record,))

    return curr.fetchone() is not None


def update_row_sets(curr, name, year, theme_id, num_parts, set_img_url, set_url, last_modified_dt):
    query = ("""UPDATE set_num
            SET name = %s,
                year = %s,
                theme_id = %s,
                num_parts = %s,
                set_img_url = %s,
            WHERE set_num = %s""")

    vars_to_update = (name, year, theme_id, num_parts,
                      set_img_url, set_url, last_modified_dt)
    curr.execute(query, vars_to_update)


def update_db_sets(curr, dataframe):
    temp_df = pd.DataFrame(columns=dataframe.columns)

    for i, row in dataframe.iterrows():
        if check_if_row_exists(curr, row['set_num'], 'set_num', 'sets'):
            update_row_sets(curr, row['name'], row['year'],
                       row['theme_id'], row['num_parts'], row['set_img_url'])
        else:
            temp_df = temp_df.append(row)
    return temp_df


def insert_into_table_sets(curr, set_num, name, year, theme_id, num_parts, set_img_url, set_url, last_modified_dt):
    insert_into_sets = (
        """INSERT INTO sets (set_num, name, year, theme_id, num_parts, set_img_url) VALUES(%s, %s, %s, %s, %s, %s)""")

    rows_to_insert = (set_num, name, year, theme_id, num_parts,
                      set_img_url, set_url, last_modified_dt)
    curr.execute(insert_into_sets, rows_to_insert)


def append_from_df_to_db_sets(curr, df):
    for i, row in df.iterrows():
        insert_into_table_sets(curr, row['set_num'], row['name'], row['year'],
                          row['theme_id'], row['num_parts'], row['set_img_url'])


def insert_into_table_themes(curr,dataframe):
    insert_statement = ("""INSERT INTO themes (id, parent_id, name) VALUES(%s, %s, %s)""")
    for i, row in dataframe.iterrows():
        rows_to_insert = (row['id'], row['parent_id'], row['name'])
        curr.execute(insert_statement, rows_to_insert)
