import duckdb
from table_definitions import (
    CREATE_PLAYERS_CURRENT_SUMMARY,
    CREATE_PLAYERS_FIXTURES,
    CREATE_PLAYERS_HISTORY,
    CREATE_PLAYERS_HISTORY_PAST,
    CREATE_TEAMS,
)


def create_connection(db_file):
    conn = None
    try:
        conn = duckdb.connect(db_file)
    except duckdb.Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """create a table from the create_table_sql statement"""
    try:
        conn.execute(create_table_sql)
    except duckdb.Error as e:
        print(e)


def main():
    database = "/Users/milosztaracha/projects/istar/db/rhun.db"

    conn = create_connection(database)
    if conn:
        create_table(conn, CREATE_PLAYERS_CURRENT_SUMMARY)
        create_table(conn, CREATE_PLAYERS_FIXTURES)
        create_table(conn, CREATE_PLAYERS_HISTORY)
        create_table(conn, CREATE_PLAYERS_HISTORY_PAST)
        create_table(conn, CREATE_TEAMS)


if __name__ == "__main__":
    main()
