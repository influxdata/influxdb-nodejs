import * as streamBuffers from 'stream-buffers';

import InfluxDBError from '~/InfluxDBError';
import {FieldType} from '~/Field';
import * as _ from 'lodash';

/**
 * Represents a single data point batch buffer written into into the database. It provides a write method to add
 * new points and a getContent() method to retrieve the content of the buffer.
 *
 * @ignore
 */
class WriteBuffer {

    constructor(schemas, autoGenerateTimestamps) {
        this.schemas = schemas;
        this.autoGenerateTimestamps = autoGenerateTimestamps;
        this.stream = new streamBuffers.WritableStreamBuffer();
        this.firstWriteTimestamp = null;
        this.batchSize = 0;
        // stores promises for writes that are buffered so that these are resolved/rejected when the
        // buffer is flushed. The values are arrays of resolve/reject function of the promise:
        // [resolve function, reject function]
        // The RESOLVE and REJECT variables can be used as index to get either the resolve or reject function
        this.writePromises = [];
    }

    buildPromiseToResolveOnFlush() {
        return new Promise((resolve, reject)=> {
            this.writePromises.push({ resolve: resolve, reject: reject});
        });
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

    write(dataPoints) {
        for (let dataPoint of dataPoints) {
            this.validateDataPoint(dataPoint);
        }
        for (let dataPoint of dataPoints) {
            this.serializeDataPoint(dataPoint);
            this.batchSize++;
        }
    }

    validateDataPoint(dataPoint) {
        this.validateTags(dataPoint);
        this.validateFields(dataPoint);
        WriteBuffer.validateTimestamp(dataPoint);
    }

    validateTags(dataPoint) {
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
                this.validateTagValue(tag.value, tag.key, dataPoint);
            }
        } else {
            if ((typeof dataPoint.tags) === 'object') {
                _.forOwn(dataPoint.tags, (tagValue, tagKey) => {
                    this.validateTagValue(tagValue, tagKey, dataPoint);
                });
            }
            else {
                if(dataPoint.tags) throw new InfluxDBError('Datapoint tags must be supplied as an array or object');
            }
        }
    }

    validateTagValue(value, tagName, dataPoint) {
        const schema = this.schemas[dataPoint.measurement];
        if ((typeof value) !== 'string') throw new InfluxDBError('Invalid tag value type, must be a string');
        if (schema && schema.tagsDictionary && !schema.tagsDictionary[tagName]) {
            throw new InfluxDBError(`Tag value '${value}' is not allowed for measurement ` +
                `${dataPoint.measurement} based on schema.`);
        }
    }

    validateFields(dataPoint) {
        let fieldsDefined = false;
        if (!dataPoint.fields) WriteBuffer.reportMissingFields(dataPoint);
        if (Array.isArray(dataPoint.fields)) {
            for (let field of dataPoint.fields) {
                if ((typeof field.key) !== 'string')
                    throw new InfluxDBError(`Field key must be a string, measurement: '${dataPoint.measurement}'`);
                if (field.value != null) {
                    this.validateFieldValue(field.value, field.key, dataPoint);
                    fieldsDefined = true;
                }
            }
        }
        else {
            if ((typeof dataPoint.fields) === 'object') {
                _.forOwn(dataPoint.fields, (fieldValue, fieldKey) => {
                    if (fieldValue != null) {
                        this.validateFieldValue(fieldValue, fieldKey, dataPoint);
                        fieldsDefined = true;
                    }
                });
            }
            else {
                throw new InfluxDBError('Data point fields property must be an array or an object');
            }
        }
        if (!fieldsDefined) WriteBuffer.reportMissingFields(dataPoint);
    }

    static reportMissingFields(dataPoint) {
        throw new InfluxDBError(`Data point has no fields in measurement '${dataPoint.measurement}'`);
    }

    validateFieldValue(value, fieldName, dataPoint) {
        const schema = this.schemas[dataPoint.measurement];
        let userSpecifiedType = WriteBuffer.getUserSpecifiedType(schema, fieldName);
        if (schema && schema.fields) {
            if (userSpecifiedType === null) throw new InfluxDBError(`Field ${fieldName} is not declared in the schema` +
                ` for measurement ${dataPoint.measurement}`);
        }
        if (userSpecifiedType) {
            switch (userSpecifiedType) {
                case FieldType.STRING:
                    WriteBuffer.validateType('string', value, fieldName, dataPoint);
                    break;
                case FieldType.BOOLEAN:
                    WriteBuffer.validateType('boolean', value, fieldName, dataPoint);
                    break;
                case FieldType.FLOAT:
                    WriteBuffer.validateType('number', value, fieldName, dataPoint);
                    break;
                case FieldType.INTEGER:
                    WriteBuffer.validateType('number', value, fieldName, dataPoint);
                    WriteBuffer.validateInteger(value, fieldName, dataPoint);
                    break;
                default:
                    throw new InfluxDBError(`Unsupported value type: ${typeof value}`);
            }
        }
        else {
            switch (typeof value) {
                case 'string':
                case 'boolean':
                case 'number':
                    break;
                default:
                    throw new InfluxDBError('Unsupported value type:' + (typeof value));
            }
        }
    }

    static getUserSpecifiedType(schema, fieldKey) {
        if (schema && schema.fields) return schema.fields[fieldKey];
    }

    static validateType(expectedType, value, fieldName, dataPoint) {
        if ((typeof value) !== expectedType)
            throw new InfluxDBError(`Invalid type supplied for field '${fieldName}' of ` +
                `measurement '${dataPoint.measurement}.' ` +
                `Supplied '${typeof value}' but '${expectedType}' is required`);
    }

    static validateInteger(value, fieldName, dataPoint) {
        if (value !== Math.floor(value)) {
            throw new InfluxDBError(`Invalid value supplied for field '${fieldName}' of ` +
                `measurement '${dataPoint.measurement}'. ` +
                'Should have been an integer but supplied number has a fraction part.');
        }
    }

    static validateTimestamp(dataPoint) {
        const timestamp = dataPoint.timestamp;
        switch (typeof timestamp) {
            case 'string':
            case 'number':
            case 'undefined':
                break;
            case 'object':
                if ((typeof timestamp.getTime) !== 'function')
                    throw new InfluxDBError('Timestamp must be an instance of Date');
                break;
            default:
                throw new InfluxDBError(`Unsupported timestamp type: ${typeof timestamp}`);
        }
    }

    serializeDataPoint(dataPoint) {
        const outputStream = this.stream;
        outputStream.write(WriteBuffer.escapeMeasurementName(dataPoint.measurement));
        if (dataPoint.tags) {
            outputStream.write(',');
            this.serializeTags(dataPoint);
        }
        outputStream.write(' ');
        this.serializeFields(dataPoint);
        outputStream.write(' ');
        outputStream.write(WriteBuffer.serializeTimestamp(dataPoint.timestamp));
        outputStream.write('\n');
    }

    serializeTags(dataPoint) {
        let outputStream = this.stream;
        if (Array.isArray(dataPoint.tags)) {
            for (let tag of dataPoint.tags) {
                outputStream.write(WriteBuffer.escape(tag.key));
                outputStream.write('=');
                outputStream.write(WriteBuffer.escape(tag.value));
            }
        } else if ((typeof dataPoint.tags) === 'object') {
            _.forOwn(dataPoint.tags, (tagValue, tagKey) => {
                outputStream.write(WriteBuffer.escape(tagKey));
                outputStream.write('=');
                outputStream.write(WriteBuffer.escape(tagValue));
            });
        }
    }

    serializeFields(dataPoint) {
        const schema = this.schemas[dataPoint.measurement];
        let outputStream = this.stream;
        if (Array.isArray(dataPoint.fields)) {
            for (let field of dataPoint.fields) {
                // do not serialize fields with null & undefined values
                if (field.value != null) {
                    outputStream.write(WriteBuffer.escape(field.key));
                    outputStream.write('=');
                    let userSpecifiedType = WriteBuffer.getUserSpecifiedType(schema, field.key);
                    outputStream.write(WriteBuffer.serializeFieldValue(field.value, userSpecifiedType));
                }
            }
        } else if ((typeof dataPoint.fields) === 'object') {
            _.forOwn(dataPoint.fields, (fieldValue, fieldKey) => {
                // do not serialize fields with null & undefined values
                if (fieldValue != null) {
                    outputStream.write(WriteBuffer.escape(fieldKey));
                    outputStream.write('=');
                    let userSpecifiedType = WriteBuffer.getUserSpecifiedType(schema, fieldKey);
                    outputStream.write(WriteBuffer.serializeFieldValue(fieldValue, userSpecifiedType));
                }
            });
        }
    }

    static serializeFieldValue(value, userSpecifiedType) {
        if (userSpecifiedType) {
            switch (userSpecifiedType) {
                case FieldType.STRING:
                    return '\"' + WriteBuffer.escapeStringFieldValue(value) + '\"';
                case FieldType.BOOLEAN:
                    return value ? 'T' : 'F';
                case FieldType.FLOAT:
                    return '' + value;
                case FieldType.INTEGER:
                    return value + 'i';
            }
        }
        else {
            switch (typeof value) {
                case 'string':
                    return '\"' + WriteBuffer.escapeStringFieldValue(value) + '\"';
                case 'boolean':
                    return value ? 'T' : 'F';
                case 'number':
                    return '' + value;
            }
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

    static serializeTimestamp(timestamp) {
        switch (typeof timestamp) {
            case 'string':
                return timestamp;
            case 'object':
                return WriteBuffer.convertMsToNs(timestamp.getTime());
            case 'number':
                return WriteBuffer.convertMsToNs(timestamp);
            case 'undefined':
                return this.autoGenerateTimestamps ? WriteBuffer.convertMsToNs(new Date().getTime()) : '';
        }
    }

    // convert number/string in unix ms format into nanoseconds as required by InfluxDB
    static convertMsToNs(ms) {
        return ms + '000000';
    }
}

export default WriteBuffer;