import argparse

import resource
import aiohttp
import aiodns
import asyncio

from collections import Counter
import datetime

import time
import socket
import pickle
import struct

import urllib.request
import gzip
import json

import psycopg2

async def fetch(session, url, headers=None):
    # print(f'trying {url}')
    async with session.get(url, ssl=False, headers=headers) as response:
        return await response.json(content_type=None)

def ensure_rlimit_file():
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < 100000 or hard < 100000:
        print(f'collection with aiohttp will not work reliably using soft {soft} and hard {hard} open file limit, increase limit for user')

async def homeserver_for_domain(session, resolver, domain):
    # for spec see https://matrix.org/docs/spec/server_server/r0.1.3#resolving-server-names
    # boils down to .well-known, _matrix._tcp. SRV, domain:8448
    headers = None
    try:
        res = await fetch(session, f'https://{domain}/.well-known/matrix/server')
        homeserver_initial = res['m.server']
        if ':' not in homeserver_initial:
            homeserver = f'{homeserver_initial}:8448'
        else:
            homeserver = homeserver_initial
        return homeserver, headers, 'well-known'
    except Exception as e:
        # well-known failed, try SRV
        try:
            res = await resolver.query(f'_matrix._tcp.{domain}', 'SRV')
            homeserver = f'{res[0].host}:{res[0].port}'
            # if srv is found, request needs to include Host header for domain, not srv host. matrix.org is a good example for this
            headers = {'Host': domain}
            return homeserver, headers, 'SRV'
        except Exception as e:
            # well-known failed, SRV failed, use domain:8448 as fallback
            homeserver = f'{domain}:8448'
            return homeserver, headers, 'fallback'
 
async def version_for_homeserver(session, resolver, domain, homeserver, headers, method, debug=False):
    try:
        res = await fetch(session, f'https://{homeserver}/_matrix/federation/v1/version', headers)
        versions = res['server']
        # version number may look like 1.6.1 (abcd,branch,...), remove everything after first space with split()[0]
        version_string = '{0}/{1}'.format(versions['name'], versions['version'].split()[0])
        if debug:
            print(f'{domain} has {homeserver} via {method} with {version_string}')
        return version_string
    except Exception as e:
        if debug:
            print(f'{domain} failed with {e} using {method}')

async def version_for_domain(session, resolver, domain, debug):
    homeserver, headers, method = await homeserver_for_domain(session, resolver, domain)
    return await version_for_homeserver(session, resolver, domain, homeserver, headers, method, debug)


def file_destinations(file_destinations_file='test_destinations.txt'):
    with open(file_destinations_file) as f:
        destinations = []
        for line in f.readlines():
            destinations.append(line.rstrip('\n'))
        return destinations

def test_destinations():
    # matrix: no well-known, srv with port, host header matrix.org necessary
    # librem.one: no well-known, srv with port, host header not necessary
    # synod.im: well-known with port 443
    # digitale-gesellschaft.ch: well-known without port (thus implicit 8448)
    # matrix.tum.de: no well-known, no srv, thus fallback
    return ['matrix.org', 'synod.im', 'matrix.tum.de', 'digitale-gesellschaft.ch', 'librem.one']

def postgres_destinations():
    conn = psycopg2.connect('dbname=synapse host=localhost user=matrix_stats password=password')
    cur = conn.cursor()
    cur.execute('SELECT destination FROM destinations;')
    destinations = []
    for destination in cur:
        destination = destination[0]
        destinations.append(destination)
    return destinations

def serverstats_destinations():
    with urllib.request.urlopen('https://serverstats.nordgedanken.dev/servers?include_members=true') as f:
        servers = json.loads(gzip.decompress(f.read()))['servers']
        return servers

def format_report(versions):
    report = ""
    total = 0
    for _, count in versions.items():
        total = total + count

    report += '{0}\n'.format(datetime.datetime.now().isoformat())
    report += f'{total} homeservers online\n\n'
    for version, count in versions.items():
        report += f'{count:<4} {version}\n'
    return report

def graphite(versions, host='localhost', port=2004):
    now = int(time.time())
    tuples = ([])
    for version, count in list(versions.items())[:15]:
        version = version.lower().replace('.', '-').replace('/', '.')
        tuples.append((version, (now, count)))

    package = pickle.dumps(tuples, 2)
    size = struct.pack('!L', len(package))
 
    try:
        sock = socket.socket()
        sock.connect( (host, port) )
        sock.sendall(size)
        sock.sendall(package)
    except socket.error:
        print(f'Couldnt connect to {host} on port {port}, is carbon-cache.py running?')

async def main(args):
    debug = args.debug
    ensure_rlimit_file()

    if args.enable_postgres:
        destination_func = postgres_destinations
    elif args.enable_file_destinations:
        destination_func = file_destinations
    elif args.enable_serverstats:
        destination_func = serverstats_destinations
    else:
        destination_func = test_destinations

    timeout = aiohttp.ClientTimeout(sock_connect=5, sock_read=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        resolver = aiodns.DNSResolver()
        tasks = []
        for domain in destination_func():
            tasks.append(version_for_domain(session, resolver, domain, debug))
        versions_initial = Counter(await asyncio.gather(*tasks)).most_common()

    # remove "None" version count for unreachable servers
    versions = {}
    for version, count in versions_initial:
        if version is not None:
            versions[version] = count

    if debug:
        print('\n' + format_report(versions), end='')
    if args.enable_report:
        reportfile = 'reports/report-{0}.txt'.format(datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds'))
        with open(reportfile, 'w') as f:
            f.write(format_report(versions))
        wwwfile = '/var/www/html/mxversions.txt'
        with open(wwwfile, 'w') as f:
            f.write(format_report(versions))
    if args.enable_graphite:
        graphite(versions)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--enable-postgres", help="use postgres destinations, if not specified, uses internal test destinations", action="store_true")
    parser.add_argument("--enable-file-destinations", help="destinations from file test_destinations.txt in current folder, do not use with --enable-postgres", action="store_true")
    parser.add_argument("--enable-serverstats", help="fetch servers from serverstats.nordgedanken.dev/servers, do not use with --enable-postgres or --enable-file-destinations", action="store_true")
    parser.add_argument("--enable-report", help="write out a report to file", action="store_true")
    parser.add_argument("--enable-graphite", help="send result to graphite on localhost:2004 via pickle", action="store_true")
    parser.add_argument("--debug", help="print out report for debugging to stdout", action="store_true")
    args = parser.parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
