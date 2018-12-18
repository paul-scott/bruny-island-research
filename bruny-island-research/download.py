import argparse
import logging
import os
from datetime import datetime

import requests


def download():
    # Setup logging configuration
    logging.basicConfig(level=logging.DEBUG)
    logging.info('running forecasts.py')
    # Setup timestamp for file organisation
    utc_now = datetime.strftime(datetime.utcnow(), '%y-%m-%dT%H-%M-%S')
    logging.debug('set program timestamp: {}'.format(utc_now))
    # Setup argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--key', dest='key', help='api key for solcast account')
    parser.add_argument('--dest', dest='dest',
                        help='absolute path to existing directory to which forecasts will be stored')
    args = parser.parse_args()

    # Check program arguments
    # If argument aren't valid, exit program
    if not args.key:
        raise ValueError('argument provided for --key is not valid | key: {}'.format(args.key))

    if not args.dest:
        raise ValueError('argument provided for --dest is not valid | dest: {}'.format(args.key))

    if not os.path.exists(args.dest):
        raise OSError('directory does not exist | dest: {}'.format(args.dest))

    logging.debug('using solcast api key: {}'.format(args.key))
    logging.debug('storing solcast forecast response in directory: {}'.format(args.dest))

    # Retrieve all sites that need forecasts
    sites_url = 'https://api.solcast.com.au/weather_sites/search?tags=bruny-island-research&format=json&api_key={}'\
        .format(args.key)
    logging.debug('retrieving all sites from url: {}'.format(sites_url))
    sites_response = requests.get(sites_url)
    # Handle case where request to solcast fails
    if not sites_response.ok:
        logging.error('sites request failed | response status code: {}'.format(sites_response.status_code))
        logging.debug('sites response text: {}'.format(sites_response.text))
        logging.debug('terminating forecasts.py')
        return
    # Else, handle successful response from solcast and extract sites from response
    logging.debug('sites request successful')
    sites_json = sites_response.json()
    sites = [site for site in sites_json['sites']]
    logging.debug('number of sites: {}'.format(len(sites)))

    # Retrieve forecasts for each site and store the response in csv
    for site in sites:
        site_id = site['resource_id']
        logging.debug('retrieving forecast for site with id: {}'.format(site_id))
        forecast_url = 'https://api.solcast.com.au/weather_sites/{}/forecasts?format=csv&api_key={}'\
            .format(site_id, args.key)
        logging.debug('forecast url: {}'.format(forecast_url))
        forecast_response = requests.get(forecast_url, stream=True)
        # Handle case where request to solcast fails
        if not forecast_response:
            logging.error('forecast request failed for site with id: {} | response status code: {}'
                          .format(site_id, forecast_response.status_code))
            logging.debug('forecast response text: {}'.format(forecast_response.text))
            logging.debug('skipping site with id: {}'.format(site_id))
            # Skip site and move to next
            next(sites, None)
            continue
        # Else, handle forecast response and save to file destination
        else:
            logging.debug('forecast request successful for site with id: {}'.format(site_id))
            # Generate unique file name with timestamp and site id
            file_name = '{}_{}.csv'.format(site_id, utc_now)
            combined = os.path.join(args.dest, file_name)
            # Save response content to file location
            with open(combined, 'wb') as file:
                for chunk in forecast_response.iter_content(chunk_size=1024):
                    file.write(chunk)
            logging.debug('saved forecast to file to dest: {}'.format(combined))

    logging.info('completing forecasts.py')
