import exec from 'child_process';
import testconf from '~/../etc/testconf.json';

// const exec = require('child_process').exec;
//const testconf = require('../etc/testconf.json');

let dockerProcess;

console.log(testconf);

function getFieldType(fields, fieldname) {
  const match = fields.find(f => f.fieldKey === fieldname);
  if (match) {
    return match.fieldType;
  }
  return false;
}
/*
function getFieldTypeOrig(fields, fieldname){

  for(let f of fields){

    if(f.fieldKey == fieldname){
      return f.fieldType;
    }
  }
  return false;
}
*/

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function dropMeasurement(connection, measurement) {
  connection.connect().then(() => {
    connection.executeQuery(`DROP MEASUREMENT ${measurement}`).then(() => {


    }).catch(e => e);
  }).catch(e => e);
}

function leftpad(num, size) {
  let s = `${num}`;
  while (s.length < size) s = `0${s}`;
  return s;
}

function buildValue(type, base, index, padd) {
  switch (type) {
    case 'string':
    case 'str':
    case 'STRING':
      return base + leftpad(index, padd); // intpad + index.toString()
    case 'integer':
    case 'int':
    case 'INT':
    case 'INTEGER':
      return base + index;
    case 'float':
    case 'FLOAT':
      return Number.parseFloat(base) + (Math.random() * (index + 1));
    case 'BOOL':
    case 'boolean':
    case 'bool':
    case 'BOOLEAN':
      if (Math.random() < 0.5) {
        return false;
      }
      return true;
    default:
      return false;
  }
}

function buildTags(tags, index, padd) {
  const data = [];
  tags.forEach((tag) => {
    data.push({ key: tag.name, value: buildValue(tag.type, tag.base, index, padd) });
  });
  return data;
}
/**
 *
 * @param measurementName - name of the measurement
 * @param tags - array of structures of name and type
 *      e.g. [{ name: 'widget', base: 'foo', type: 'string'}]
 * @param fields - array of structures of name and type
 *      e.g. [{ name: 'widget', base: '1', type: 'integer'}]
 * @param count - number of elements to generate
 */
function buildDatapoints(measurementName, tags, fields, count) {
  const dps = [];

  for (let i = 0; i < count; i += 1) {
    dps.push({ measurement: measurementName,
      timestamp: new Date().getTime(),
      tags: buildTags(tags, i, Math.ceil(Math.log10(count))),
      fields: buildTags(fields, i),
    });
  }

  return dps;
}

function pad(n, width, z) {
  const zz = z || '0';
  const nn = `${n}`;
  return nn.length >= width ? n : new Array(width - (nn.length + 1)).join(zz) + n;
}

/**
 *
 * @param args - array of arguments to the python script
 *    valid arguments:
 *       * http || https
 *       * --version VERSION version of influxdb to pull - default 'latest'
 *       * --admin ADMIN admin user name
 *       * --password PASSWORD admin password
 *       * --nopull NOPULL do not pull new image
 *       * --name NAME of the container - default 'influxdb'
 */
function startDockerInfluxdb(args) {
  const pyArgs = [`${__dirname}/../scripts/test-server.py`, args];

  console.log(`(re)starting influxbd docker container ${pyArgs}`);

    // dockerProcess = exec('python3', pyArgs)
//    dockerProcess = exec(`${__dirname}/../scripts/test-server.py ${args}`)
  dockerProcess = exec(`${__dirname}/../scripts/test-server.py https`);

  let stdout = '';
  dockerProcess.stdout.on('data', (data) => {
    stdout += data.toString();
  });

  dockerProcess.on('close', () => {
    console.log('docker process closed ', stdout);
  });

    // wait a 30 secs for server to start
  console.log('Waiting 30 seconds for influxdb server to start');
  sleep(30000).then(() => {
        // console.log('STDOUT ' + stdout)
    console.log(`started influxdb server with args ${args}`);
  });
}

module.exports = { getFieldType,
  dropMeasurement,
  buildDatapoints,
  sleep,
  pad,
  startDockerInfluxdb,
  testconf };
