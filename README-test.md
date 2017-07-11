## Running unit tests 

When running the unit tests on a freshly installed host the following 
need to be preinstalled:

   * Docker
   * Nodejs and npm - Nodejs 6 and higher is recommended.
   * Python3
    
If these dependencies have been satisfied the general steps for running 
these tests is as follows:

   * Install Python3 dependencies
   * Use the `test-server.py` script to pull and then run an InfluxDB server
   * Install npm dependencies
   * Run the tests
       
### Install Python libraries for server scripts

```bash
cd ./scripts
sudo apt install python3-pip
pip3 install docker
pip3 install --upgrade pip3
pip install --upgrade pip

pip3 install --upgrade pip

pip3 install docker
pip3 install influx-python

# 2139  pip3 search influx
sudo pip install influxdb
sudo chown -R ivan /usr/local/lib/python3.5/
pip install influxdb
```
### Start the server

The python test server script pulls a docker image and starts it running 
on the localhost.  Influxdb Docker releases can be specified with the 
`--version` argument (see https://hub.docker.com/_/influxdb/).   

```bash
./test-server.py http
 #2155  docker logs -f sharp_spence
```
### Install node dependencies

```bash
cd ..
npm install 
# 2162  npm install babel-register --save-dev
```

### Run the tests

```bash
npm test
 ```
### Kill the docker container
```bash
# 2171  docker kill sharp_spence
 ```
 
 ### Start a new Influxdb docker and specify its version
 
 ```bash
cd -
./test-server.py http --version 1.3
#docker logs -f compassionate_yalow
cd ..
npm test
```
