#!/usr/bin/env python

# built-in stuff
import os
import shutil
import json

# third party stuff
import click
from influxdb import InfluxDBClient

def init(outdir):
    # remove the directory with all its contents
    if os.path.exists(outdir):
        shutil.rmtree(outdir)
    os.makedirs(outdir)

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
@click.option('--outdir', default='./out', 
              help='Output directory')
def extract_data(host, port, user, password, dbname, measurement, pagesize, outdir):
    """
    Exracts points from InfluxDB in a paginated manner to prevent
    the service from consuming too much memory from caching the results.
    Especially useful when running InfluxDB on low-powered devices, such as
    Raspberry Pi.
    """

    QUERY_TEMPLATE = 'select * from %s limit %i offset %i'
    client = InfluxDBClient(host, port, user, password, dbname)
    offset = 0

    # initialize the CLI
    init(outdir)

    while True:
        query = QUERY_TEMPLATE % (measurement, pagesize, offset)
        result = list(client.query(query).get_points())
        save_page(result, offset, outdir)
        if len(result) == 0:
            break
        offset += pagesize

def save_page(points, offset, outdir):
    click.echo('saving at offset %i' % offset)
    with open(os.path.join(outdir, '%i.json' % offset), 'w') as f:
        json.dump(points, f)

if __name__ == '__main__':
    extract_data()