import psycopg2
from create_database import conn_string
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()