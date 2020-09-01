import configparser
import psycopg2
import logging
from sql_queries import copy_table_queries, insert_table_queries

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)


def load_staging_tables(cur, conn):
    """
    This method loads data from S3 into staging tables in redshift
    :param cur: Cursor to redshift
    :param conn: Connection to Redshift
    :return:
    """
    for table_name, query in copy_table_queries.items():
        cur.execute(query)
        conn.commit()
        log.info(f"loaded data into staging table: {table_name}")


def insert_tables(cur, conn):
    """
    This method inserts data from staging tables into fact
    and dim tables in redshift
    :param cur: Cursor to redshift
    :param conn: Connection to Redshift
    :return:
    """
    for table_name, query in insert_table_queries.items():
        cur.execute(query)
        conn.commit()
        log.info(f"inserted data into table: {table_name}")


def main():
    """
    Main function to load data into staging tables from S3 and insert
    data into fact and dim tables in redshift
    :return:
    """
    config = configparser.ConfigParser()
    config.read("dwh.cfg")
    log.info(', '.join(config["CLUSTER"].values()))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config["CLUSTER"].values()))
    cur = conn.cursor()
    log.info("connected to redshift cluster")

    load_staging_tables(cur, conn)
    log.info("loading into staging tables completed")
    insert_tables(cur, conn)
    log.info("inserting into fact and dim tables completed")

    conn.close()


if __name__ == "__main__":
    main()
