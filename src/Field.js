/**
 * Field is a key-value pair in InfluxDB’s data structure that records metadata and the actual data value.
 * See: https://docs.influxdata.com/influxdb/latest/concepts/glossary/#field
 *
 * @public
 * @typedef {Object} Field
 * @property {String} key The key part of the key-value pair that makes up a field.
 * @property {(String|Number|Boolean)} value The value part of the key-value pair that makes up a field.
 *
 * @example
 * let field={
 *   key: 'temperature',
 *   value: 23.7
 * }
 *
 */


/**
 * FieldType is an enumeration of InfluxDB field data types.
 * @typedef {Number} FieldType
 * @example
 * const schema = {
 *   measurement: 'my_measurement',
 *   fields: {
 *     my_int: FieldType.INTEGER,
 *     my_float: FieldType.FLOAT,
 *     my_string: FieldType.STRING,
 *     my_boolean: FieldType.BOOLEAN
 *   }
 * }
 */
let FieldType = {

    INTEGER:0,FLOAT:1,BOOLEAN:2,STRING:3

};

module.exports={FieldType};


/**
 * Schema describes tags and fields that can be used with a measurement.
 *
 * It's recommended, but not required to use a schema; it is used to:
 *
 *    * Coerce your data (properly converting JavasSript Number to either floats or integers that are available
 *      in InfluxDB)
 *    * Provide immediate error feedback if data supplied to the {@Connection#write} method are not compliant
 *      with the schema. An error is signalled when there is a field/tag which is not present in your schema,
 *      or the data type of a field does not match the schema.
 *
 * See {@link FieldType} for available field types.
 *
 * @typedef {Object} Schema
 * @property {String} measurement Name of the measurement
 * @property {Object[]} [fields] Field names and their corresponding types for the given measurement.If not defined
 *     no field validation will be executed.
 * @property {String[]} [tags] List of allowed tag names for the measurement. If not defined no tag validation
 *     will be executed.
 * @example
 * const schema = {
 *   measurement: 'my_measurement',
 *   tags: ['someTag', 'someOtherTag']
 *   fields: {
 *     my_int: InfluxDB.FieldType.INTEGER,
 *     my_float: InfluxDB.FieldType.FLOAT,
 *     my_string: InfluxDB.FieldType.STRING,
 *     my_boolean: InfluxDB.FieldType.BOOLEAN
 *   }
 * }
 */
