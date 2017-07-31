const streamBuffers = require('stream-buffers');

const InfluxDBError = require('../InfluxDBError').InfluxDBError;
const FieldType = require('../Field').FieldType;

/**
 * Represents a single data point batch buffer written into into the database. It provides a write method to add
 * new points and a getContent() method to retrieve the content of the buffer.
 *
 * @ignore
 */
class WriteBuffer {

    constructor(schemas, autoGenerateTimestamps) {
        this.schemas=schemas;
        this.autoGenerateTimestamps=autoGenerateTimestamps;
        this.stream = new streamBuffers.WritableStreamBuffer();
        this.firstWriteTimestamp = null;
        this.batchSize = 0;
        // stores promises for writes that are buffered so that these are resolved/rejected when the
        // buffer is flushed. The values are arrays of resolve/reject function of the promise:
        // [resolve function, reject function]
        // The RESOLVE and REJECT variables can be used as index to get either the resolve or reject function
        this.writePromises = [];
    }

    addWritePromiseToResolve(promise) {
        this.writePromises.push(promise);
    }

    resolveWritePromises() {
        for (let promise of this.writePromises) {
            promise.resolve();
        }
    }

    rejectWritePromises(error) {
        for (let promise of this.writePromises) {
            promise.reject(error);
        }
    }

    /*
     * Escape tag keys, tag values, and field keys
     * see https://docs.influxdata.com/influxdb/latest/write_protocols/line_protocol_reference/#special-characters
     */
    static escape(s) {
        return s.replace(/([,= ])/g, '\\$1');
    }

    static escapeMeasurementName(s) {
        return s.replace(/([ ,])/g, '\\$1');
    }

    static escapeStringFieldValue(s) {
        return s.replace(/(["])/g, '\\$1');
    }

    serializeFieldValue(value, fieldName, dataPoint) {
        let requiredType = null;
        const schema = this.schemas[dataPoint.measurement];
        if (schema && schema.fields) {
            requiredType = schema.fields[fieldName];
            if (requiredType === null) throw new InfluxDBError(`Field ${fieldName} is not declared in the schema` +
                ` for measurement ${dataPoint.measurement}`);
        }

        if (requiredType === null) {
            switch (typeof value) {
                case 'string':
                    return '\"' + WriteBuffer.escapeStringFieldValue(value) + '\"';
                case 'boolean':
                    return value ? '1' : '0';
                case 'number':
                    return '' + value;
                default:
                    throw new InfluxDBError('Unsupported value type:' + (typeof value));
            }
        }

        const validateType=(expectedType) => {
            if ((typeof value) !== expectedType)
                throw new InfluxDBError(`Invalid type supplied for field ${fieldName} of ` +
                    `measurement ${dataPoint.measurement}. ` +
                    `Supplied ${typeof value} but '${expectedType}' is required`);
        };

        switch (requiredType) {
            case FieldType.STRING:
                validateType('string');
                return '\"' + WriteBuffer.escapeStringFieldValue(value) + '\"';
            case FieldType.BOOLEAN:
                validateType('boolean');
                return value ? 'T' : 'F';
            case FieldType.FLOAT:
                validateType('number');
                return '' + value;
            case FieldType.INTEGER:
                validateType('number');
                if (value !== Math.floor(value)) {
                    throw new InfluxDBError(`Invalid value supplied for field ${fieldName} of ` +
                        `measurement ${dataPoint.measurement}.` +
                        'Should have been an integer but supplied number has a fraction part.');
                }
                return value + 'i';
            default:
                throw new InfluxDBError(`Unsupported value type: ${typeof value}`);
        }
    }

    serializeTagValue(value, key, dataPoint) {
        if ((typeof value) !== 'string') throw new InfluxDBError('Invalid tag value type, must be a string');
        const schema = this.schemas[dataPoint.measurement];
        if (schema && schema.tagsDictionary && !schema.tagsDictionary[key]) {
            throw new InfluxDBError(`Tag value '${value}' is not allowed for measurement ` +
                `${dataPoint.measurement} based on schema.`);
        }
        return WriteBuffer.escape(value);
    }

    // convert number/string in unix ms format into nanoseconds as required by InfluxDB
    static convertMsToNs(ms) {
        return ms+'000000';
    }

    serializeTimestamp(timestamp) {
        switch (typeof timestamp) {
            case 'string':
                return timestamp;
            case 'object':
                if ((typeof timestamp.getTime) !== 'function')
                    throw new InfluxDBError('Timestamp must be an instance of Date');
                return WriteBuffer.convertMsToNs(timestamp.getTime());
            case 'number':
                return WriteBuffer.convertMsToNs(timestamp);
            case 'undefined':
                return this.autoGenerateTimestamps ? WriteBuffer.convertMsToNs(new Date().getTime()) : '';
            default:
                throw new InfluxDBError(`Unsupported timestamp type: ${typeof timestamp}`);
        }
    }

    write(dataPoints) {
        let outputStream=this.stream;
        for (let dataPoint of dataPoints) {
            outputStream.write(WriteBuffer.escapeMeasurementName(dataPoint.measurement));
            if (dataPoint.tags) {
                outputStream.write(',');
                // tags
                if (Array.isArray(dataPoint.tags)) {
                    for (let tag of dataPoint.tags) {
                        if ((typeof tag) !== 'object') {
                            throw new InfluxDBError('When defining tags as an array, all array members must be objects' +
                                ` with key and value properties: Measurement: ${dataPoint.measurement}`);
                        }
                        if (!tag.key) {
                            throw new InfluxDBError("When defining tags as objects, key property " +
                                ` must be supplied. Measurement: ${dataPoint.measurement}`);
                        }
                        outputStream.write(WriteBuffer.escape(tag.key));
                        outputStream.write('=');
                        outputStream.write(this.serializeTagValue(tag.value, tag.key, dataPoint));
                    }
                } else if ((typeof dataPoint.tags) === 'object') {
                    for (let tagKey in dataPoint.tags) {
                        outputStream.write(WriteBuffer.escape(tagKey));
                        outputStream.write('=');
                        outputStream.write(this.serializeTagValue(dataPoint.tags[tagKey], tagKey, dataPoint));
                    }
                }
            }
            outputStream.write(' ');

            //fields
            function invalidFieldsDefinition() {
                throw new InfluxDBError(`Supplied data point is missing fields ` +
                    `for measurement '${dataPoint.measurement}'`);
            }

            if (!dataPoint.fields) invalidFieldsDefinition();
            let fieldsDefined = false;
            if (Array.isArray(dataPoint.fields)) {
                if (dataPoint.fields.length === 0) invalidFieldsDefinition();
                for (let field of dataPoint.fields) {
                    // do not serialize fields with null & undefined values
                    if (field.value != null) {
                        if ((typeof field.key) !== 'string')
                            throw new InfluxDBError(`Field key must be a string, measurement: '${dataPoint.measurement}'`);
                        outputStream.write(WriteBuffer.escape(field.key));
                        outputStream.write('=');
                        outputStream.write(this.serializeFieldValue(field.value, field.key, dataPoint));
                        fieldsDefined = true;
                    }
                }
            } else if ((typeof dataPoint.fields) === 'object') {
                for (let fieldKey in dataPoint.fields) {
                    const value = dataPoint.fields[fieldKey];
                    // do not serialize fields with null & undefined values
                    if (value != null) {
                        outputStream.write(WriteBuffer.escape(fieldKey));
                        outputStream.write('=');
                        outputStream.write(this.serializeFieldValue(value, fieldKey, dataPoint));
                        fieldsDefined = true;
                    }
                }
            }
            if (!fieldsDefined) invalidFieldsDefinition();
            // timestamp & new line
            outputStream.write(' ');
            outputStream.write(this.serializeTimestamp(dataPoint.timestamp));
            outputStream.write('\n');
        }
    }

}

module.exports = WriteBuffer;