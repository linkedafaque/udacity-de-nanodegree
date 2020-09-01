import configparser
import psycopg2
import logging
from sql_queries import create_table_queries, drop_table_queries

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)


def drop_tables(cur, conn):
    """
    This method runs drop table queries listed in drop_table_queries
    :param cur: Cursor to redshift
    :param conn: Connection to Redshift
    :return:
    """
    for table_name, query in drop_table_queries.items():
        cur.execute(query)
        conn.commit()
        log.info(f"dropped table: {table_name}")


def create_tables(cur, conn):
    """
    This method runs create table queries listed in create_table_queries
    :param cur: Cursor to redshift
    :param conn: Connection to Redshift
    :return:
    """
    for table_name, query in create_table_queries.items():
        cur.execute(query)
        conn.commit()
        log.info(f"created table: {table_name}")


def main():
    """
    Main function to drop and create tables for the ETL pipeline
    :return:
    """
    config = configparser.ConfigParser()
    config.read("dwh.cfg")
    log.info(', '.join(config["CLUSTER"].values()))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config["CLUSTER"].values()))
    cur = conn.cursor()
    log.info("connected to redshift cluster")

    drop_tables(cur, conn)
    log.info("drop tables completed")
    create_tables(cur, conn)
    log.info("create tables completed")

    conn.close()


if __name__ == "__main__":
    main()
