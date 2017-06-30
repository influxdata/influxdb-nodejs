/**
 * Field is a key-value pair in InfluxDB’s data structure that records metadata and the actual data value.
 * See: https://docs.influxdata.com/influxdb/v1.2/concepts/glossary/#field
 *
 * @public
 * @typedef {Object} Field
 * @property {String} key The key part of the key-value pair that makes up a field.
 * @property {(String|Number|Boolean)} value The value part of the key-value pair that makes up a field.
 *
 * @example
 *
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
 *
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

module.exports={FieldType}


/**
 * Schema describes tags and fields that can be used with a measurement.
 *
 * It is used to decide which type to use (Integer or Float) when a javascript Number is stored as as a field value.
 *
 * It's recommended, but not required, that you make use of schema; internally we use them to be smarter about coercing your data,
 * and providing immediate error feedback if you try to write data which doesn't fit in your schema: either if you include tags of
 * fields which are not present in your schema, or you enter the wrong data type for one of your schema fields.
 *
 * See {@link FieldType} for available field types.
 *
 * @typedef {Object} Schema
 * @property {String} measurement Name of the measurement
 * @property {Object[]} fields Field names and their corresponding types for the given measurement. If you won't define this
 *    property fields will not be validated.
 * @property {String[]} tags List of allowed tag names for the measurement. If you won't define this any tag value will
 *    be defined.
 * @example
 *
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
