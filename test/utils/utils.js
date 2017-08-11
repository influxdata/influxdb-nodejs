// import exec from 'child_process';
import testconf from '~/../etc/testconf.json';

console.log(testconf);

function getFieldType(fields, fieldname) {
  const match = fields.find(f => f.fieldKey === fieldname);
  if (match) {
    return match.fieldType;
  }
  return false;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function dropMeasurement(connection, measurement) {
  connection.connect().then(() => {
    connection.executeQuery(`DROP MEASUREMENT ${measurement}`).then(() => {}).catch(e => e);
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
      return Math.random() >= 0.5;
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
  const time = new Date().getTime();
  for (let i = 0; i < count; i += 1) {
    dps.push({
      measurement: measurementName,
      timestamp: time + i,
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

module.exports = {
  getFieldType,
  dropMeasurement,
  buildDatapoints,
  sleep,
  pad,
  testconf,
};
