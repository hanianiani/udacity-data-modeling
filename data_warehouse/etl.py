import psycopg2
from create_database import conn_string
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print("Problem with query: {}".format(query[:60]))
            print(e)
            break


def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print("Problem with query: {}".format(query[:60]))
            print(e)
            break


def main():
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()