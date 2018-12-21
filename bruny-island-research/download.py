import argparse
import logging
from datetime import datetime
import requests
import psycopg2 as sql
from psycopg2.extras import Json


def download():
    # Setup logging configuration
    logging.basicConfig(level=logging.DEBUG)
    logging.info('running forecasts.py')
    # Setup timestamp for file organisation
    utc_now = datetime.strftime(datetime.utcnow(), '%y-%m-%dT%H-%M-%S')
    logging.debug('set program timestamp: {}'.format(utc_now))
    # Setup argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('key', help='api key for solcast account')
    parser.add_argument('database', help='database name')
    parser.add_argument('--quantity', default='forecasts',
                        help='request / table name')
    parser.add_argument('--create', action='store_true',
                        help='create database table')
    parser.add_argument('--take', type=int, default=12 * 24,
                        help='maximum number of time points to take')
    args = parser.parse_args()

    conn = sql.connect(args.database, timeout=10)
    if args.create:
        create_table(conn, args.quantity)

    # Retrieve all sites
    sites_url = \
        'https://api.solcast.com.au/utility_scale_sites/search?tags=bruny-island-research&format=json&api_key={}'\
        .format(args.key)
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

    # Retrieve "quantity" for each site and store the response in csv
    for site in sites:
        site_id = site['resource_id']
        logging.debug('retrieving forecast for site with id: {}'.format(site_id))
        forecast_url = \
            'https://api.solcast.com.au/utility_scale_sites/{}/weather/{}?period=PT5M&format=json&api_key={}'\
            .format(site_id, args.quantity, args.key)

        forecast_response = requests.get(forecast_url, stream=True)
        if forecast_response.ok:
            logging.debug('forecast request successful for site with id: {}'.format(site_id))
            forecast = forecast_response.json()[args.quantity][:args.take]
            try:
                insert(conn, args.quantity, utc_now, site_id, forecast)
            except sql.Error as err:
                logging.error('Error on inserting resource {}: {}'.format(site_id, err))
        else:
            logging.error('forecast request failed for site with id: {} | response status code: {}'
                          .format(site_id, forecast_response.status_code))
            logging.debug('forecast response text: {}'.format(forecast_response.text))


def create_table(conn, quantity):
    c = conn.cursor()
    try:
        c.execute("CREATE TABLE %s ("
                  + "time INTEGER NOT NULL,"
                  + "resource BIGINT NOT NULL,"
                  + "data JSONB,"
                  + "PRIMARY KEY (time, resource)"
                  + ")",
                  (quantity)
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
                         (time.timestamp(), rid, Json(value)))


def resource_to_bigint(resource):
    int(resource.replace('-', ''), 16)


if __name__ == '__main__':
    download()
