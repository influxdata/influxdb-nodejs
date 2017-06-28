#!/usr/bin/env python3

import docker
import sys
import argparse
import subprocess
import time
from influxdb import InfluxDBClient

client = docker.from_env()

parser = argparse.ArgumentParser()
parser.add_argument("protocol", help="http or https should be specified")
parser.add_argument("--admin", help="admin user name")
parser.add_argument("--password", "--pw", help="admin user password")
parser.add_argument("--nopull", help="do not pull new image")
args = parser.parse_args()

def success_start_msg(container):
    print("Container {0} is now running.".format(container))
    print("To follow the logs use 'docker logs -f {0}'".format(container.name))

def pull_image():
    if args.nopull:
        print("Not pulling new image")
        return
    print("Pulling latest influxdb image")

    client.images.pull('influxdb', 'latest')
    image = client.images.get('influxdb')
    print("Pulled: " + image.id)

def start_https():
    container = start_http()
    print(container.name + " " + container.status)
    p = subprocess.Popen(['docker','cp', '../etc/influxdb-https.conf',
            container.name + ':/etc/influxdb/influxdb.conf'])
    p = subprocess.Popen(['docker','cp', '../etc/ssl/influxdb-selfsigned.crt',
            container.name + ':/etc/ssl/influxdb-selfsigned.crt'])
    p = subprocess.Popen(['docker','cp', '../etc/ssl/influxdb-selfsigned.key',
            container.name + ':/etc/ssl/influxdb-selfsigned.key'])
    p.wait()
    container.restart()
    return container

def start_http():
    container = client.containers.run('influxdb', detach=True, ports={8086:8086,8083:8083})
    return container

def set_admin_account(name, password, container):
    print("Setting up admin account")
    print("Waiting 1 min to ensure server start")
    time.sleep(60)
    if args.protocol == 'http':
        print("setting admin in http")
        client = InfluxDBClient('localhost','8086')
        client.create_user(name, password, admin=True)
        p = subprocess.Popen(['docker','cp', '../etc/influxdb-http-auth.conf',
                 container.name + ':/etc/influxdb/influxdb.conf'])
        p.wait()
        container.restart()
    elif args.protocol == 'https':
        print("setting admin in https")
        client = InfluxDBClient('localhost', '8086', ssl=True, verify_ssl=False)
        client.create_user(name, password, admin=True)
        p = subprocess.Popen(['docker','cp', '../etc/influxdb-https-auth.conf',
                 container.name + ':/etc/influxdb/influxdb.conf'])
        p.wait()
        container.restart()

pull_image()

if args.protocol == 'https':
    ctr = start_https()
elif args.protocol == 'http':
    ctr = start_http()

success_start_msg(ctr)

if not args.admin is None and not args.password is None:
    set_admin_account(args.admin, args.password, ctr)
