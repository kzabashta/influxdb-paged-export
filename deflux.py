#!/usr/bin/env python

import click
from influxdb import InfluxDBClient

@click.command()
@click.option('--host', prompt='InfluxDB hostname',
              help='InfluxDB hostname.', default="localhost")
@click.option('--port', prompt='InfluxDB port',
              help='InfluxDB port.', default=8086)
@click.option('--user', prompt='Connect as this user',
              help='Connect to InfluxDB as this user.')
@click.option('--password', prompt=True, hide_input=True,
              confirmation_prompt=True, 
              help='Password of the user to connect to InfluxDB.')
@click.option('--dbname', prompt='Database to extract',
              help='Database to extract.')
@click.option('--measurement', prompt='Measurement to extract',
              help='Measurement to extract.')
@click.option('--pagesize', default=10000, 
              help='Number of points to extract at a time')

def extract_data(host, port, user, password, dbname, measurement, pagesize):
    """
    Exracts points from InfluxDB in a paginated manner to prevent
    the service from consuming too much memory from caching the results.
    Especially useful when running InfluxDB on low-powered devices, such as
    Raspberry Pi.
    """

    QUERY_TEMPLATE = 'select * from %s limit %i offset %i'
    client = InfluxDBClient(host, port, user, password, dbname)
    offset = 0

    while True:
        result = client.query(QUERY_TEMPLATE % (measurement, pagesize, offset)).get_points()
        offset += pagesize
        print(list(result))


if __name__ == '__main__':
    extract_data()