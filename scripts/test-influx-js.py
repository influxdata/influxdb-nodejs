#!/usr/bin/env python3

import subprocess
import docker
import time


client = docker.from_env()

print("Pulling latest influxdb image")

client.images.pull('influxdb', 'latest')

container = client.containers.run('influxdb', detach=True, ports={8086:8086,8083:8083})

print("Started container ", container.image, " ", container.name, " ", container)

print("Waiting 1 minute for server to start")
time.sleep(60)

p = subprocess.Popen(['npm','install'], cwd='/home/karl/bonitoo/prjs/influxdb-nodejs')
p.wait()
p = subprocess.Popen(['npm','test'], cwd='/home/karl/bonitoo/prjs/influxdb-nodejs')
p.wait()

print("Killing container", container.name, " ", container)
container.kill()
