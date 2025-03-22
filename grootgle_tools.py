from datetime import datetime
import time
import random
import logging
from argparse import ArgumentParser, RawTextHelpFormatter
import psycopg2
from psycopg2.errors import SerializationFailure
from alive_progress import alive_bar
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
from datetime import datetime

class grootgle:

    #generic run transaction class on a database
    def run_transaction(conn, op, max_retries=3):
        """
        Execute the operation *op(conn)* retrying serialization failure.

        If the database returns an error asking to retry the transaction, retry it
        *max_retries* times before giving up (and propagate it).
        """
        # leaving this block the transaction will commit or rollback
        # (if leaving with an exception)
        with conn:
            for retry in range(1, max_retries + 1):
                try:
                    op(conn)
                    # If we reach this point, we were able to commit, so we break
                    # from the retry loop.
                    return

                except SerializationFailure as e:
                    # This is a retry error, so we roll back the current
                    # transaction and sleep for a bit before retrying. The
                    # sleep time increases for each failed transaction.
                    logging.debug("got error: %s", e)
                    conn.rollback()
                    logging.debug("EXECUTE SERIALIZATION_FAILURE BRANCH")
                    sleep_ms = (2 ** retry) * 0.1 * (random.random() + 0.5)
                    logging.debug("Sleeping %s seconds", sleep_ms)
                    time.sleep(sleep_ms)

                except psycopg2.Error as e:
                    logging.debug("got error: %s", e)
                    logging.debug("EXECUTE NON-SERIALIZATION_FAILURE BRANCH")
                    raise e

            raise ValueError(f"Transaction did not succeed after {max_retries} retries")

    #generic class to get the last data added to a table
    def last_sync_datas(conn,table,optionnal=""):
        with conn.cursor() as cur:
            query="SELECT MAX(sync_date) as sync_date FROM {} {}".format(table,optionnal)
            logging.debug("last_sync_datas query :{}".format(query))
            cur.execute(query)
            logging.debug("last_sync_datas() for table %s : status message: %s", table, cur.statusmessage)
            conn.commit()
            last_synced_date = cur.fetchone()[0]
            if last_synced_date is None:
                last_synced_date = datetime.strptime("01/01/2020", "%d/%m/%Y")
            logging.debug("last_sync_datas() for table %s : last_synced_date: %s", table, last_synced_date)
        return last_synced_date
    
    def batch_insert_fuel_price_history_data(conn, rows):
        sync_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with conn.cursor() as cur:
            # Création d'une table temporaire pour staging
            cur.execute("""
                CREATE TEMP TABLE tmp_fuel_price_history_data (
                    id INTEGER,
                    latitude FLOAT,
                    longitude FLOAT,
                    zip TEXT,
                    pop TEXT,
                    address TEXT,
                    city TEXT,
                    fuel_name TEXT,
                    price FLOAT,
                    rate_date TIMESTAMP
                ) ON COMMIT DROP;
            """)

            # Insertion rapide dans la table temporaire
            execute_values(cur,
                "INSERT INTO tmp_fuel_price_history_data VALUES %s",
                rows
            )

            # UPSERT vers la vraie table avec comparaison
            query = f"""
            INSERT INTO fuel_price_history_data (
                sync_date, id, latitude, longitude, zip, pop, address, city, fuel_name, price, rate_date
            )
            SELECT 
                '{sync_date}'::timestamp,
                id, latitude, longitude, zip, pop, address, city, fuel_name, price, rate_date
            FROM tmp_fuel_price_history_data AS tmp
            ON CONFLICT (id, fuel_name, rate_date) DO UPDATE
            SET 
                sync_date = EXCLUDED.sync_date,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                zip = EXCLUDED.zip,
                pop = EXCLUDED.pop,
                address = EXCLUDED.address,
                city = EXCLUDED.city,
                price = EXCLUDED.price
            WHERE 
                fuel_price_history_data.latitude IS DISTINCT FROM EXCLUDED.latitude OR
                fuel_price_history_data.longitude IS DISTINCT FROM EXCLUDED.longitude OR
                fuel_price_history_data.zip IS DISTINCT FROM EXCLUDED.zip OR
                fuel_price_history_data.pop IS DISTINCT FROM EXCLUDED.pop OR
                fuel_price_history_data.address IS DISTINCT FROM EXCLUDED.address OR
                fuel_price_history_data.city IS DISTINCT FROM EXCLUDED.city OR
                fuel_price_history_data.price IS DISTINCT FROM EXCLUDED.price;
            """
            cur.execute(query)
            conn.commit()
            logging.info(f"[BATCH UPSERT] {len(rows)} lignes traitées.")    
        
    # insert_station_service data
    def insert_station_service(conn,commune,id,marque,nom,departement,regionCode,zipCode,adresse,coordLatitude,coordLongitude,sync_date=datetime.now()):
        return_variable = None
        with conn.cursor() as cur:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM stations_services WHERE id={}".format(id))
            if cur.rowcount <= 0:
                query = "INSERT INTO stations_services(sync_date,commune,id,marque,nom,departement,regioncode,coordlatitude,coordlongitude,zipcode,address) " \
                        "VALUES ('{}','{}',{},'{}','{}','{}','{}','{}',{},'{}','{}')".format(
                    sync_date.strftime("%Y-%m-%d %H:%M:%S"),
                    commune,
                    id,
                    marque,
                    nom,
                    departement,
                    regionCode,
                    coordLatitude,
                    coordLongitude,
                    zipCode,
                    adresse,
                    )
                logging.debug("stations_services query {}".format(query))
                cur.execute(query)
                return_variable = {"insert": 1}
            else:
                cur.execute("SELECT 1 FROM stations_services WHERE id={} and commune='{}' "
                            "and marque='{}' and nom='{}' "
                            "and departement='{}' and regioncode='{}' "
                            "and coordlatitude='{}' and coordlongitude='{}' "
                            "and zipcode='{}' and address='{}' "
                            .format(id,commune,marque,nom,departement,regionCode,coordLatitude,coordLongitude,zipCode,adresse))
                if cur.rowcount <= 0:
                    query = "UPDATE stations_services SET sync_date='{}', commune='{}', marque='{}', " \
                            "nom='{}', departement='{}', regioncode='{}', coordlatitude='{}', coordlongitude='{}', " \
                            "zipcode='{}', address='{}'" \
                            "WHERE id={}".format(
                        sync_date.strftime("%Y-%m-%d %H:%M:%S"),
                        commune,
                        marque,
                        nom,
                        departement,
                        regionCode,
                        coordLatitude,
                        coordLongitude,
                        zipCode,
                        adresse,
                        id)
                    logging.debug("stations_services *UPDATED* query {}".format(query))
                    cur.execute(query)
                    return_variable = {"update": 1}
                else:
                    logging.debug("stations_services data already catched")
                    return_variable = {"nothing": 1}
            cur.close()
            conn.commit()
        return return_variable

    # insert_fuel_price_history_data data
    def insert_fuel_price_history_data(conn,id,latitude,longitude,zip,pop,address,city,fuel_name,price,rate_date,sync_date=datetime.now()):
        return_variable=None
        with conn.cursor() as cur:
            cur = conn.cursor()
            query="SELECT 1 FROM fuel_price_history_data WHERE " \
                  "id={} and fuel_name='{}' and rate_date='{}'".format(id,
                                                                       fuel_name,
                                                                       rate_date.strftime("%Y-%m-%d %H:%M:%S"))
            cur.execute(query)
            if cur.rowcount <= 0:
                query = "INSERT INTO fuel_price_history_data(sync_date,id,latitude,longitude," \
                        "zip,pop,address,city,fuel_name,price,rate_date) " \
                        "VALUES ('{}',{},{},{}," \
                        "'{}','{}','{}','{}','{}',{},'{}')".format(
                    sync_date.strftime("%Y-%m-%d %H:%M:%S"),
                    id,
                    latitude,
                    longitude,
                    zip,
                    pop,
                    address,
                    city,
                    fuel_name,
                    price,
                    rate_date.strftime("%Y-%m-%d %H:%M:%S"))
                logging.debug("fuel_price_history_data query {}".format(query))
                cur.execute(query)
                return_variable = {"insert": 1}
            else:
                cur.execute("SELECT 1 FROM fuel_price_history_data WHERE id={} and latitude={} "
                            "and longitude={} and zip='{}' and pop='{}' and address='{}' and city='{}' "
                            "and fuel_name='{}' and price={} and rate_date='{}'".format(id,latitude,longitude,
                                                                                        zip,pop,address,city,fuel_name,
                                                                                        price,
                                                                                        rate_date.strftime("%Y-%m-%d %H:%M:%S")))
                if cur.rowcount <= 0:
                    query = "UPDATE fuel_price_history_data SET sync_date='{}', latitude={}, longitude={}, " \
                            "zip='{}', pop='{}', address='{}', city='{}',price={} " \
                            "WHERE id={} and fuel_name='{}' and rate_date='{}'".format(
                        sync_date.strftime("%Y-%m-%d %H:%M:%S"),latitude,longitude,zip,pop,address,
                        city,price,id,fuel_name,rate_date.strftime("%Y-%m-%d %H:%M:%S"))
                    logging.debug("fuel_price_history_data *UPDATED* query {}".format(query))
                    cur.execute(query)
                    return_variable = {"update": 1}
                else:
                    logging.debug("fuel_price_history_data data already catched for id {}, "
                                  "fuel_name {} and rate_date {}".format(id,
                                                                         fuel_name,
                                                                         rate_date.strftime("%Y-%m-%d %H:%M:%S")))
                    return_variable = {"nothing": 1}
            cur.close()
            conn.commit()
        return return_variable

    # insert fuel history data data
    def insert_fuel_price_history(conn,station_name,station_brand,cp,city,update_date,price_sp95,price_e10,
                                  price_e85, price_gazole, price_sp98, price_gplc, latitude, longitude):
        with conn.cursor() as cur:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM fuel_price_history WHERE latitude={} "
                        "and longitude={} "
                        "and update_date='{}'".format(latitude,longitude,update_date.strftime("%Y-%m-%d %H:%M:%S")))
            if cur.rowcount <= 0:
                query = "INSERT INTO fuel_price_history (sync_date,station_name,station_brand,cp,city," \
                        "update_date,price_sp95,price_e10,price_e85,price_gazole,price_sp98,price_gplc," \
                        "latitude,longitude) " \
                        "VALUES ('{}','{}','{}','{}','{}','{}',{},{},{},{},{},{},{},{})".format(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    station_name,
                    station_brand,
                    cp,
                    city,
                    update_date.strftime("%Y-%m-%d %H:%M:%S"),
                    price_sp95,
                    price_e10,
                    price_e85,
                    price_gazole,
                    price_sp98,
                    price_gplc,
                    latitude,
                    longitude)
                logging.debug("insert_fuel_price_history query {}".format(query))
                cur.execute(query)
            else:
                logging.debug("Fuel data already catched")
            cur.close()
            conn.commit()

    # insert_fake_station_service data
    def insert_fake_station_service(conn):
        with conn.cursor() as cur:
            cur.execute(
                "insert into stations_services (sync_date,id,commune,marque, nom) "
                "select now(), a.id,'empty','unknown','unknown' from fuel_price_history_data as a "
                "left join stations_services as b on a.id = b.id where b.id is null group by a.id")
            cur.close()
            conn.commit()

    # refresh_last_rate_view
    def refresh_last_rate_view(conn):
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute("refresh materialized view last_rate;")
            cur.close()
            #conn.commit()

    # refresh_fuel_rate_summary_view
    def refresh_fuel_rate_summary_view(conn):
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute("refresh materialized view fuel_rate_summary;")
            cur.close()
            #conn.commit()
            
    # refresh_last_rate_view
    def refresh_fuel_rate_ratio_brent_view(conn):
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute("refresh materialized view fuel_rate_ratio_brent;")
            cur.close()
            #conn.commit()

    # insert brent history price data
    def insert_brent_spot_price(conn,rate_date,rate,sync_date=datetime.now()):
        return_variable = None
        with conn.cursor() as cur:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM brent_spot_price WHERE rate_date='{}'".format(rate_date.strftime("%Y-%m-%d %H:%M:%S")))
            if cur.rowcount <= 0:
                query = "INSERT INTO brent_spot_price (sync_date,rate_date,rate) VALUES ('{}','{}',{})".format(
                    sync_date.strftime("%Y-%m-%d %H:%M:%S"),
                    rate_date.strftime("%Y-%m-%d %H:%M:%S"),
                    rate)
                logging.debug("insert_brent_spot_price *INSERT* query {}".format(query))
                cur.execute(query)
                return_variable = {"insert": 1}
            else:
                cur.execute("SELECT 1 FROM brent_spot_price WHERE rate_date='{}' "
                "and rate={}".format(rate_date.strftime("%Y-%m-%d %H:%M:%S"),rate))
                if cur.rowcount <= 0:
                    query = "UPDATE brent_spot_price SET sync_date='{}', rate={} WHERE rate_date='{}'".format(
                        sync_date.strftime("%Y-%m-%d %H:%M:%S"),
                        rate,
                        rate_date.strftime("%Y-%m-%d %H:%M:%S"))
                    logging.debug("insert_brent_spot_price *UPDATE* query {}".format(query))
                    cur.execute(query)
                    return_variable = {"update": 1}
                else:
                    logging.debug("insert_brent_spot_price data already catched")
                    return_variable = {"nothing": 1}
            cur.close()
            conn.commit()
        return return_variable

    # insert USD rate history data
    def insert_usd_rate(conn,rate_date,rate,sync_date=datetime.now()):
        return_variable = None
        with conn.cursor() as cur:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM usd_rate WHERE rate_date='{}'".format(rate_date.strftime("%Y-%m-%d %H:%M:%S")))
            if cur.rowcount <= 0:
                query = "INSERT INTO usd_rate (sync_date,rate_date,rate) VALUES ('{}','{}',{})".format(
                    sync_date.strftime("%Y-%m-%d %H:%M:%S"),
                    rate_date.strftime("%Y-%m-%d %H:%M:%S"),
                    rate)
                logging.debug("insert_usd_rate *INSERT* query {}".format(query))
                cur.execute(query)
                return_variable = {"insert": 1}
            else:
                cur.execute("SELECT 1 FROM usd_rate WHERE rate_date='{}' "
                "and rate={}".format(rate_date.strftime("%Y-%m-%d %H:%M:%S"),rate))
                if cur.rowcount <= 0:
                    query = "UPDATE usd_rate SET sync_date='{}', rate={} WHERE rate_date='{}'".format(
                        sync_date.strftime("%Y-%m-%d %H:%M:%S"),
                        rate,
                        rate_date.strftime("%Y-%m-%d %H:%M:%S"))
                    logging.debug("insert_usd_rate *UPDATE* query {}".format(query))
                    cur.execute(query)
                    return_variable = {"update": 1}
                else:
                    logging.debug("insert_usd_rate data already catched")
                    return_variable = {"nothing": 1}
            cur.close()
            conn.commit()
        return return_variable

    # synchronise local and distant fuel price history
    def sync_fuel_price_history(conn, localconn, last_synced_date):
        with localconn.cursor() as local_cur, conn.cursor() as cur:
            query="SELECT sync_date,station_name,station_brand,update_date,price_sp95,price_e10,price_e85," \
                  "price_gazole,price_sp98,price_gplc,latitude,longitude,cp,city FROM fuel_price_history " \
                  "WHERE sync_date>'{}'".format(last_synced_date)
            local_cur.execute(query)
            rows = local_cur.fetchall()
            localconn.commit()
            for row in rows:
                logging.debug("sync_fuel_price_history() for row %s", row)

                if row[4] is None:
                    price_sp95="null"
                else:
                    price_sp95 = row[4]
                if row[5] is None:
                    price_e10="null"
                else:
                    price_e10 = row[5]
                if row[6] is None:
                    price_e85="null"
                else:
                    price_e85 = row[6]
                if row[7] is None:
                    price_gazole="null"
                else:
                    price_gazole = row[7]
                if row[8] is None:
                    price_sp98="null"
                else:
                    price_sp98 = row[8]
                if row[9] is None:
                    price_gplc="null"
                else:
                    price_gplc = row[9]

                cur.execute(
                    "INSERT INTO fuel_price_history(sync_date,station_name,station_brand,update_date,price_sp95,"
                    "price_e10,price_e85,price_gazole,price_sp98,price_gplc,latitude,longitude,cp,city) "
                    "VALUES ('{}','{}','{}','{}',{},{},{},{},{},{},{},{},'{}','{}')".format(row[0], row[1], row[2], row[3],
                                                                                  price_sp95, price_e10,price_e85,
                                                                                  price_gazole,price_sp98,price_gplc,
                                                                                  row[10],row[11],row[12],row[13]))

    # synchronise local and distant brent_spot_price
    def sync_station_service(conn, localconn, last_synced_date):
        with localconn.cursor() as local_cur, conn.cursor() as cur:
            query="SELECT sync_date,commune,id,marque,nom,departement FROM stations_services " \
                  "WHERE sync_date>'{}' ORDER BY sync_date ASC".format(last_synced_date)
            local_cur.execute(query)
            rows = local_cur.fetchall()
            localconn.commit()
            logging.info("{} : {} sync elements in stations_services".format(
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"), len(rows)))
            stats = []
            with alive_bar(len(rows)) as bar:
                for row in rows:
                    logging.debug("sync_station_service() for row %s", row)
                    stats.append(grootgle.insert_station_service(conn, row[1], row[2],row[3],row[4],row[5],row[0]))
                    bar()
            insert = 0
            update = 0
            nothing = 0
            for x in stats:
                if 'insert' in x:
                    insert += x.get("insert")
                if 'update' in x:
                    update += x.get("update")
                if 'nothing' in x:
                    nothing += x.get("nothing")
            logging.info(
                "end of sync elements in stations_services : insert {}, update {}, nothing {}".format(insert, update, nothing))

    # synchronise local and distant brent_spot_price
    def sync_brent_spot_price(conn, localconn, last_synced_date):
        with localconn.cursor() as local_cur, conn.cursor() as cur:
            query="SELECT sync_date,rate_date,rate FROM brent_spot_price " \
                  "WHERE sync_date>'{}' ORDER BY sync_date ASC".format(last_synced_date)
            local_cur.execute(query)
            rows = local_cur.fetchall()
            localconn.commit()
            logging.info("{} : {} sync elements in brent_spot_price".format(
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"), len(rows)))
            stats = []
            with alive_bar(len(rows)) as bar:
                for row in rows:
                    logging.debug("sync_brent_spot_price() for row %s", row)
                    stats.append(grootgle.insert_brent_spot_price(conn, row[1], row[2], row[0]))
                    bar()
            insert = 0
            update = 0
            nothing = 0
            for x in stats:
                if 'insert' in x:
                    insert += x.get("insert")
                if 'update' in x:
                    update += x.get("update")
                if 'nothing' in x:
                    nothing += x.get("nothing")
            logging.info(
                "end of sync elements in brent_spot_price : insert {}, update {}, nothing {}".format(insert, update, nothing))

    # synchronise local and distant brent_spot_price
    def sync_fuel_price_history_data(conn, localconn, last_synced_date):
        with localconn.cursor() as local_cur, conn.cursor() as cur:
            #last_synced_date="2000-01-01"
            query="SELECT count(*) FROM fuel_price_history_data " \
                  "WHERE sync_date>'{}'".format(last_synced_date)
            local_cur.execute(query)
            row = local_cur.fetchone()
            localconn.commit()
            nb_result_remaning_to_sync=row[0]

            offset=0
            packet=500000
            continue_loop=True

            while continue_loop:
                print(nb_result_remaning_to_sync)
                if (nb_result_remaning_to_sync <= packet):
                    continue_loop = False
                query="SELECT sync_date,id,latitude,longitude,zip,pop,address,city,fuel_name,price,rate_date " \
                      "FROM fuel_price_history_data " \
                      "WHERE sync_date>'{}' ORDER BY sync_date ASC,rate_uid asc limit {} offset {}".format(last_synced_date,
                                                                                              packet,
                                                                                              offset)

                local_cur.execute(query)
                rows = local_cur.fetchall()
                localconn.commit()
                logging.info("{} : {} sync elements in fuel_price_history_data".format(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"), len(rows)))
                stats = []
                with alive_bar(len(rows)) as bar:
                    for row in rows:
                        logging.debug("sync_fuel_price_history_data() for row %s", row)
                        stats.append(
                            grootgle.insert_fuel_price_history_data(conn, row[1], row[2], row[3], row[4], row[5],
                                                                    row[6],
                                                                    row[7], row[8], row[9], row[10], row[0]))
                        bar()
                insert = 0
                update = 0
                nothing = 0
                for x in stats:
                    if 'insert' in x:
                        insert += x.get("insert")
                    if 'update' in x:
                        update += x.get("update")
                    if 'nothing' in x:
                        nothing += x.get("nothing")
                logging.info(
                    "end of sync elements in fuel_price_history_data : insert {}, update {}, "
                    "nothing {}".format(insert, update, nothing))
                nb_result_remaning_to_sync-=packet
                offset+=packet


    # get the last sensor data
    def get_unknow_station_services(conn):
        with conn.cursor() as cur:
            cur.execute("select a.id, a.commune, b.city, a.marque, a.nom, b.latitude, b.longitude, "
                        "max(rate_date), b.address from stations_services as a "
                        "inner join fuel_price_history_data as b "
                        "on a.id=b.id "
                        "where a.marque='unknown' "
                        "group by a.id, a.commune, b.city, a.marque, a.nom, b.latitude, b.longitude, b.address "
                        "order by max(rate_date) ;")
            rows=cur.fetchall()
            conn.commit()
        return rows

    # get the last sensor data
    def corrected_city_in_station_services(conn):
        with conn.cursor() as cur:
            cur.execute(" select stations.id, stations.commune,datas.city,stations.marque,stations.nom "
                        " from stations_services as stations "
                        " inner join (select id, latitude, longitude, city, zip "
                        "             from fuel_price_history_data "
                        "             group by id, latitude, longitude, city, zip) as datas"
                        "  on stations.id = datas.id"
                        " where stations.commune <>  datas.city")
            rows=cur.fetchall()
            conn.commit()
        return rows


    #generic class to parse a command line
    def parse_cmdline():
        parser = ArgumentParser(description=__doc__,formatter_class=RawTextHelpFormatter)
        parser.add_argument(
            "dsn",
            help="""\
    database connection string
    For cockroach demo, use
    'postgresql://<username>:<password>@<hostname>:<port>/bank?sslmode=require',
    with the username and password created in the demo cluster, and the hostname
    and port listed in the (sql/tcp) connection parameters of the demo cluster
    welcome message.

    For CockroachCloud Free, use
    'postgres://<username>:<password>@free-tier.gcp-us-central1.cockroachlabs.cloud:26257/<cluster-name>.bank?sslmode=verify-full&sslrootcert=<your_certs_directory>/cc-ca.crt'.

    If you are using the connection string copied from the Console, your username,
    password, and cluster name will be pre-populated. Replace
    <your_certs_directory> with the path to the 'cc-ca.crt' downloaded from the
    Console.

    """
        )

        parser.add_argument("-v", "--verbose",action="store_true", help="print debug info")
        parser.add_argument("-u", "--url", help="URL for grabbing fuel datas")
        opt = parser.parse_args()
        return opt