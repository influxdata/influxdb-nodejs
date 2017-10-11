// common helpers to work with connections
const InfluxDB = require('../../src/InfluxDB');

const getAnonymousConnection = (db) => {
  const connection = new InfluxDB.Connection({
    database: db,
  });
  return connection;
};

const getConnection = db => getAnonymousConnection(db);

const getCloudConnection = (db) => {
  const connection = new InfluxDB.Connection({
    hostUrl: 'https://futureboy-bf9e2f8a.influxcloud.net:8086',
    username: 'admin',
    password: 'changeit@123',
    database: db,
  });
  return connection;
};

const getSecureConnection = () => {
  const connection = new InfluxDB.Connection({
    database: 'for_otto',
    username: 'otto',
    password: 'noname',
    autoCreateDatabase: false,
  });
  return connection;
};

module.exports = { getConnection, getSecureConnection, getCloudConnection, getAnonymousConnection };
