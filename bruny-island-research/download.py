import argparse
import logging
import datetime as dt
import requests
import psycopg2 as sql
from psycopg2.extras import Json


def download():
    # Setup logging configuration
    logging.basicConfig(level=logging.DEBUG)
    logging.info('running forecasts.py')
    # Setup timestamp for file organisation
    utc_now = dt.datetime.now(tz=dt.timezone.utc)
    logging.debug('set program timestamp: {}'.format(utc_now))
    # Setup argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('key', help='api key for solcast account')
    parser.add_argument('database', help='database name')
    parser.add_argument('--quantity', default='forecasts',
                        help='request / table name (forecasts or estimated_actuals)')
    parser.add_argument('--create', action='store_true',
                        help='create database table')
    parser.add_argument('--take', type=int, default=12 * 24,
                        help='maximum number of time points to take')
    parser.add_argument('--flatten', action='store_true',
                        help='lift json time steps to table level')
    args = parser.parse_args()

    conn = sql.connect(dbname=args.database)
    if args.create:
        create_table(conn, args.quantity)

    # Retrieve all sites
    sites_url = (
        'https://api.solcast.com.au/utility_scale_sites/'
        + 'search?tags=bruny-island-research&format=json&api_key={}'.format(args.key)
        #'https://api.solcast.com.au/weather_sites/'
        #+ 'search?tags=bruny-island-research&format=json&api_key={}'.format(args.key)
    )
    logging.debug('retrieving all sites from url: {}'.format(sites_url))
    sites_response = requests.get(sites_url)
    # Handle case where request to solcast fails
    if not sites_response.ok:
        logging.error('sites request failed | response status code: {}'.format(sites_response.status_code))
        logging.debug('sites response text: {}'.format(sites_response.text))
        logging.debug('terminating')
        return
    # Else, handle successful response from solcast and extract sites from response
    logging.debug('sites request successful')
    sites_json = sites_response.json()
    sites = [site for site in sites_json['sites']]
    logging.debug('number of sites: {}'.format(len(sites)))

    # Retrieve "quantity" for each site and store the response in database
    count = 0
    for site in sites:
        site_id = site['resource_id']
        logging.debug('retrieving data for site with id: {}'.format(site_id))
        quantity_url = (
            'https://api.solcast.com.au/utility_scale_sites/'
            + '{}/weather/{}?period=PT5M&format=json&api_key={}'.format(site_id, args.quantity, args.key)
            #'https://api.solcast.com.au/weather_sites/'
            #+ '{}/{}?period=PT5M&format=json&api_key={}'.format(site_id, args.quantity, args.key)
        )

        data_response = requests.get(quantity_url, stream=True)
        if data_response.ok:
            logging.debug('request successful for site with id: {}'.format(site_id))
            data = data_response.json()[args.quantity][:args.take]
            try:
                if args.flatten:
                    count += flat_insert(conn, args.quantity, site_id, data)
                else:
                    insert(conn, args.quantity, utc_now, site_id, data)
                    count += 1
            except sql.Error as err:
                logging.error('Error on inserting resource {}: {}'.format(site_id, err))
        else:
            logging.error('request failed for site with id: {} | response status code: {}'
                          .format(site_id, data_response.status_code))
            logging.debug('response text: {}'.format(data_response.text))
    logging.info('Inserted {} entries'.format(count))


def create_table(conn, quantity):
    c = conn.cursor()
    try:
        c.execute("CREATE TABLE {} (".format(quantity)
                  + "time TIMESTAMP (0) WITH TIME ZONE NOT NULL,"
                  + "resource BIGINT NOT NULL,"
                  + "data JSONB,"
                  + "PRIMARY KEY (time, resource)"
                  + ")"
                  )
        conn.commit()
        logging.debug('Table {} created'.format(quantity))
    except sql.OperationalError as err:
        logging.debug('Error creating table: {}'.format(err))


def insert(conn, table, time, resource, value):
    rid = resource_to_bigint(resource)
    with conn:
        with conn.cursor() as curs:
            curs = conn.cursor()
            curs.execute("INSERT INTO forecasts VALUES (%s, %s, %s)",
                         (time, rid, Json(value)))


def flat_insert(conn, table, resource, value):
    rid = resource_to_bigint(resource)
    count = 0
    with conn:
        with conn.cursor() as curs:
            curs = conn.cursor()
            for row in value:
                time = parse_time(row['period_end'])
                curs.execute("INSERT INTO forecasts VALUES (%s, %s, %s)"
                             + " ON CONFLICT DO NOTHING",
                             (time, rid, Json(row)))
                count += 1
    return count


def parse_time(time_str):
    """ Parse the returned time format

    Doesn't conform to iso8601 standard.
    The "microseconds" have 7 digits so doesn't work with %f.
    Don't care about sub-seconds.
    This will break if they ever change this.
    """
    time, rhs = time_str.split('.')
    if rhs[-1] != 'Z':
        raise ValueError('Time not in UTC: {}'.format(time_str))
    return dt.datetime.strptime(time + '+0000', '%Y-%m-%dT%H:%M:%S%z')


def resource_to_bigint(resource):
    """ Convert resource_id type to postgres bigint.

    bigint is signed, so need subtract by offset.
    Another option would be to pad it as UUID.
    """
    return int(resource.replace('-', ''), 16) - (16**16 // 2)


if __name__ == '__main__':
    download()

# python3 download.py <KEY> solcast --quantity forecasts
# python3 download.py <KEY> solcast --quantity estimated_actuals --flatten
