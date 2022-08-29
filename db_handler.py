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


def create_table(curr):
    create_table_command = ("""CREATE TABLE IF NOT EXISTS sets (
                    set_num VARCHAR(255) PRIMARY KEY,
                    name TEXT,
                    year INTEGER,
                    theme_id INTEGER,
                    num_parts INTEGER,
                    set_img_url TEXT,
    )""")

    curr.execute(create_table_command)


def check_if_row_exists(curr, set_num):
    query = ("""SELECT set_num FROM sets WHERE set_num = %s""")
    curr.execute(query, (set_num,))

    return curr.fetchone() is not None


def update_row(curr, name, year, theme_id, num_parts, set_img_url, set_url, last_modified_dt):
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


def update_db(curr, df):
    temp_df = pd.DataFrame(
        columns=['set_num', 'name', 'year', 'theme_id', 'num_parts', 'set_img_url'])

    for i, row in df.iterrows():
        if check_if_row_exists(curr, row['set_num']):
            update_row(curr, row['name'], row['year'],
                       row['theme_id'], row['num_parts'], row['set_img_url'])
        else:
            temp_df = temp_df.append(row)
    return temp_df


def insert_into_table(curr, set_num, name, year, theme_id, num_parts, set_img_url, set_url, last_modified_dt):
    insert_into_sets = (
        """INSERT INTO sets (set_num, name, year, theme_id, num_parts, set_img_url) VALUES(%s, %s, %s, %s, %s, %s)""")

    rows_to_insert = (set_num, name, year, theme_id, num_parts,
                      set_img_url, set_url, last_modified_dt)
    curr.execute(insert_into_sets, rows_to_insert)


def append_from_df_to_db(curr, df):
    for i, row in df.iterrows():
        insert_into_table(curr, row['set_num'], row['name'], row['year'],
                          row['theme_id'], row['num_parts'], row['set_img_url'])
