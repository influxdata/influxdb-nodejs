//import * as request from "request";
const request = require('request');
const streamBuffers = require('stream-buffers');
const FieldType = require('../Field').FieldType;
const InfluxDBError = require('../InfluxDBError').InfluxDBError;

/* Indexes to get resolve/reject function from values of ConnectionImpl.cachedPromises property.
   See below for more information. */
const RESOLVE=0;
const REJECT=1;

/**
 * @ignore
 */
class ConnectionImpl {

    // Copy the supplied schema so that it won't get affected by further modifications from
    // the user. Also convert tags to a map for faster access during serialization
    processSchemas() {
        this.schemas={};
        let schemas=this.options.schema;
        this.options.schema=[];
        if(schemas!==undefined)
        {
            for(let originalSchema of schemas) {
                let connectionSchema={};
                if(originalSchema.measurement===undefined)
                    throw new InfluxDBError('Each data point schema must have "measurement" property defined');
                connectionSchema.measurement=originalSchema.measurement;
                if(originalSchema.tags!==undefined) {
                    connectionSchema.tagsDictionary={};
                    connectionSchema.tags=[];
                    for(let tag of originalSchema.tags) {
                        connectionSchema.tagsDictionary[tag]=true;
                        connectionSchema.tags.push(tag);
                    }
                }
                if(originalSchema.fields!==undefined) {
                    connectionSchema.fields={};
                    for(let fieldKey in originalSchema.fields) {
                        connectionSchema.fields[fieldKey]=originalSchema.fields[fieldKey];
                    }
                }
                this.schemas[originalSchema.measurement]=connectionSchema;
            }
        }
    }

    onProcessExit() {
        if(this.cachedBatchSize!==undefined) {
            console.error('Warning: there are still cached data points to be written into InfluxDB, '+
                'but the process is about to exit. Forgot to call Connection.flush() ?');
        }
        for(let promise of this.cachedPromises) {
            let e=new InfluxDBError('Can\'t write data points to InfluxDB, process is exiting');
            promise[REJECT](e);
        }
    }

    registerShutdownHook() {
        if(ConnectionImpl.activeConnections==undefined) {
            ConnectionImpl.connectionIdGenerator=0;
            ConnectionImpl.activeConnections={};
            process.on('exit', () => {
                for(let connectionId in ConnectionImpl.activeConnections) {
                    ConnectionImpl.activeConnections[connectionId].onProcessExit();
                }
            });
        }
        this.id=ConnectionImpl.connectionIdGenerator++;
    }

    constructor(options) {
        if(options.database===undefined) throw new InfluxDBError('database option must be specified');
        this.options = Object.assign({
            username:'root',
            password:'root',
            hostUrl: 'http://localhost:8086',
            autoCreateDatabase: true,
            autoResolvePromisedWritesToCache:true,
            maximumWriteDelay:1000,
            batchSize:1000,
            batchWriteErrorHandler(e, dataPoints) {
                console.error('[DefaultBatchWriteErrorHandler] Error writing data points into InfluxDB:'+
                    dataPoints,e);
            }
        }, options);


        this.processSchemas();

        this.hostUrl=this.options.hostUrl;
        if(this.hostUrl.endsWith('/')) this.hostUrl=this.hostUrl.substring(0,this.hostUrl.length-1);

        // stores promises for writes that are cached so that these are resolved/rejected when the
        // cache is flushed. The values are arrays of resolve/reject function of the promise:
        // [resolve function, reject function]
        // The RESOLVE and REJECT variables can be used as index to get either the resolve or reject function
        this.cachedPromises=[];
        this.connected = !this.options.autoCreateDatabase;

        this.registerShutdownHook();
    }

    /*
     For tag keys, tag values, and field keys always use a backslash character \ to escape:
     commas ,
     weather,location=us\,midwest temperature=82 1465839830100400200
     equal signs =
     weather,location=us-midwest temp\=rature=82 1465839830100400200
     spaces
     weather,location\ place=us-midwest temperature=82 1465839830100400200
     */
    static escape(s) {
        return s.replace(/([,= ])/g, '\\$1');
    }

    /**
     For measurements always use a backslash character \ to escape:
     commas ,
     wea\,ther,location=us-midwest temperature=82 1465839830100400200
     spaces
     wea\ ther,location=us-midwest temperature=82 1465839830100400200
     */
    static escapeMeasurement(s) {
        return s.replace(/([ ,])/g, '\\$1');
    }

    /*
     For string field values use a backslash character \ to escape:
     double quotes "
     */
    static escapeString(s) {
        return s.replace(/(["])/g, '\\$1');
    }

    serializeFieldValue(value, fieldName, dataPoint) {
        let requiredType=undefined;
        let schema=this.schemas[dataPoint.measurement];
        if(schema!==undefined && schema.fields!==undefined) {
            requiredType=schema.fields[fieldName];
            if(requiredType===undefined)
                throw new InfluxDBError(`Field ${fieldName} is not declared in the schema`+
                    ` for measurement ${dataPoint.measurement}`);
        }

        if(requiredType===undefined) {
            switch(typeof value) {
                case 'string':
                    return '\"'+ConnectionImpl.escapeString(value)+'\"';
                case 'boolean':
                    return value ? '1' : '0';
                case 'number':
                    return ''+value;
                default:
                    throw new InfluxDBError('Unsupported value type:'+(typeof value));
            }
        }
        else {

            function validateType(expectedType) {
                if((typeof value)!==expectedType)
                throw new InfluxDBError(`Invalid type supplied for field ${fieldName} of `+
                    `measurement ${dataPoint.measurement}. `+
                    `Supplied ${typeof value} but '${expectedType}' is required`);
            }

            switch(requiredType) {
                case FieldType.STRING:
                    validateType('string');
                    return '\"'+ConnectionImpl.escapeString(value)+'\"';
                case FieldType.BOOLEAN:
                    validateType('boolean');
                    return value ? 'T' : 'F';
                case FieldType.FLOAT:
                    validateType('number');
                    return ''+value;
                case FieldType.INTEGER:
                    validateType('number');
                    if(value!==Math.floor(value)) {
                        throw new InfluxDBError(`Invalid value supplied for field ${fieldName} of `+
                            `measurement ${dataPoint.measurement}.`+
                            'Should have been an integer but supplied number has a fraction part.' +
                            ' Use Math.round/ceil/floor for conversion.');
                    }
                    return value+'i';
                default:
                    throw new InfluxDBError('Unsupported value type:'+(typeof value));
            }
        }
    }

    serializeTagValue(v,key,dataPoint) {
        if((typeof v)!=='string') throw new InfluxDBError('Invalid tag value type, must be a string');
        let schema=this.schemas[dataPoint.measurement];
        if(schema!==undefined && schema.tagsDictionary!==undefined
            && schema.tagsDictionary[key]===undefined) {
            throw new InfluxDBError(`Tag value '${v}' is not allowed for measurement `+
                `${dataPoint.measurement} based on schema.`);
        }
        return ConnectionImpl.escape(v);
    }

    static serializeTimestamp(t) {
        switch(typeof t) {
            case 'string':
                return t;
            case 'object':
                if((typeof t.getTime)!=='function')
                    throw new InfluxDBError('Timestamp is an object but it has to declare getTime() method as well'+
                      'Perhaps you intended to supply a Date object, but it has not been received.');
                return t.getTime()+'000000';
            case 'number':
                return t+'000000';
            default:
                throw new InfluxDBError('Unsupported timestamp type:'+(typeof t));
        }
    }

    convertDataPointsToText(stream, dataPoints) {
        for(let dataPoint of dataPoints) {
            stream.write(ConnectionImpl.escapeMeasurement(dataPoint.measurement));
            if(dataPoint.tags!==undefined)
            {
                stream.write(',');
                // tags
                if(Array.isArray(dataPoint.tags)) {
                    for(let tag of dataPoint.tags) {
                        if((typeof tag) !=='object') {
                            throw new InfluxDBError('When defining tags as an array, all array members must be objects'+
                                ` with key and value properties: Measurement: ${dataPoint.measurement}`);
                        }
                        if(tag.key==undefined) {
                            throw new InfluxDBError("When defining tags as objects, key property "+
                                ` must be supplied. Measurement: ${dataPoint.measurement}` );
                        }
                        stream.write(ConnectionImpl.escape(tag.key));
                        stream.write('=');
                        stream.write(this.serializeTagValue(tag.value,tag.key,dataPoint));
                    }
                } else if((typeof dataPoint.tags)==='object') {
                    for(let tagKey in dataPoint.tags) {
                        stream.write(ConnectionImpl.escape(tagKey));
                        stream.write('=');
                        stream.write(this.serializeTagValue(dataPoint.tags[tagKey],tagKey,dataPoint));
                    }
                }
            }
            stream.write(' ');

            //fields
            function invalidFieldsDefinition() {
                throw new InfluxDBError(`Supplied data point is missing fields `+
                    `for measurement '${dataPoint.measurement}'`);
            }

            if(dataPoint.fields==undefined) invalidFieldsDefinition();
            let fieldsDefined=false;
            if(Array.isArray(dataPoint.fields)) {
                if(dataPoint.length===0) invalidFieldsDefinition();
                for(let field of dataPoint.fields) {
                    if(field.value!=undefined) {
                        if((typeof field.key)!=='string')
                            throw new InfluxDBError(`Field key must be a string, measurement: '${dataPoint.measurement}'`);
                        stream.write(ConnectionImpl.escape(field.key));
                        stream.write('=');
                        stream.write(this.serializeFieldValue(field.value, field.key, dataPoint));
                        fieldsDefined=true;
                    }
                }
            } else if((typeof dataPoint.fields)==='object') {
                for(let fieldKey in dataPoint.fields) {
                    let value=dataPoint.fields[fieldKey];
                    if(value!=undefined) {
                        stream.write(ConnectionImpl.escape(fieldKey));
                        stream.write('=');
                        stream.write(this.serializeFieldValue(value, fieldKey, dataPoint));
                        fieldsDefined=true;
                    }
                }
            }
            if(!fieldsDefined) invalidFieldsDefinition();
            // timestamp & new line
            stream.write(' ');
            stream.write(ConnectionImpl.serializeTimestamp(dataPoint.timestamp));
            stream.write('\n');
        }
    }

    writeWhenConnectedAndInputValidated(dataPoints,forceFlush) {
        let batchSizeLimitNotReached=this.options.batchSize>0 &&
            (this.cachedBatchSize===undefined
            || this.cachedBatchSize+dataPoints.length<this.options.batchSize);
        let timeoutLimitNotReached=this.options.maximumWriteDelay>0 &&
            (this.cacheAge===undefined || new Date().getTime()-this.cacheAge<this.options.maximumWriteDelay);

        if(batchSizeLimitNotReached && timeoutLimitNotReached && forceFlush!==true) {
            return new Promise((resolve,reject) => {
                if(this.cache===undefined) {
                    this.cache=new streamBuffers.WritableStreamBuffer();
                }
                this.convertDataPointsToText(this.cache,dataPoints);
                // make the shutdown hook aware of data in the cache
                ConnectionImpl.activeConnections[this.id]=this;

                if(this.cacheAge===undefined) {
                    this.cacheAge=new Date().getTime();
                    this.cacheExpirationHandle=setTimeout(()=>{
                        this.flush().then().catch((e)=>{
                            if(this.options.autoResolvePromisedWritesToCache) {
                                this.options.batchWriteErrorHandler(e,e.data);
                            }
                        });
                    },this.options.maximumWriteDelay);
                }

                if(this.cachedBatchSize===undefined)
                    this.cachedBatchSize=dataPoints.length; else this.cachedBatchSize+=dataPoints.length;

                if(this.options.autoResolvePromisedWritesToCache) {
                    resolve();
                }
                else {
                    this.cachedPromises.push([resolve,reject]);
                }
            });
        }
        else {
            return this.flushOnInternalRequest(dataPoints);
        }
    }

    write(dataPoints,forceFlush) {

        let onBadArguments=() => {
            if(forceFlush)
                return this.flushOnInternalRequest(dataPoints);
            else
                return new Promise();
        };

        if(dataPoints==null) return onBadArguments();
        if(!Array.isArray(dataPoints)) {
            if(typeof dataPoints==='object')
                return write([dataPoints],forceFlush);
            else
                throw new InfluxDBError('Invalid arguments supplied');
        }
        if(dataPoints.length===0) return onBadArguments();


        if(!this.connected) {
            return new Promise((resolve, reject) => {
                this.connect().then(() => {
                    this.write(dataPoints,forceFlush).then(() => {
                        resolve();
                    }).catch((e) => {
                        reject(e)
                    });
                }).catch((e) => {
                    reject(e)
                });
            });
        }
        else
        {
            return this.writeWhenConnectedAndInputValidated(dataPoints,forceFlush);
        }
    }

    flushOnInternalRequest(dataPoints) {
        // prevent repeated flush call if flush invoked before expiration timeout
        if(this.cacheExpirationHandle!==undefined) {
            clearTimeout(this.cacheExpirationHandle);
            this.cacheExpirationHandle=undefined;
        }

        let cachedDataStream=this.cache;
        let noDataCached=false;
        if(cachedDataStream===undefined) {
            cachedDataStream=new streamBuffers.WritableStreamBuffer();
            noDataCached=true;
        }

        if(dataPoints!==undefined) {
            this.convertDataPointsToText(cachedDataStream,dataPoints);
        } else {
            if(noDataCached) return new Promise((resolve, reject) => {
                resolve()
            });
        }

        this.cache=undefined;
        this.cachedBatchSize=undefined;
        this.cacheAge=undefined;
        let promises=this.cachedPromises;
        this.cachedPromises=[];
        // shutdown hook doesn't need to track this connection any more
        delete ConnectionImpl.activeConnections[this.id];

        let db=this.options.database;
        if(db===undefined) throw new InfluxDBError('Assertion failed: database not specified');
        let url=this.hostUrl+'/write?db='+db;

        return new Promise((resolve, reject) => {
            let bodyBuffer=cachedDataStream.getContents();
            request.post({
                    url: url,
                    method: 'POST',
                    headers: { "Content-Type": "application/text" },
                    body: bodyBuffer,
                    auth: {
                        user: this.options.username,
                        pass: this.options.password
                    }
                },
                (error, result) => {
                    if(error) {
                        reject(error);
                        for(let promise of promises) {
                            promise[REJECT](error);
                        }
                    }
                    else {
                        if(result.statusCode>=200 && result.statusCode<400) {
                            resolve();
                            for(let promise of promises) {
                                promise[RESOLVE]();
                            }
                        }
                        else {
                            let message=result.statusCode+' Influx db sync failed';
                            try {
                                message+='; '+JSON.parse(result.body).error;
                            }
                            catch(e) {}
                            let error=new InfluxDBError(message, bodyBuffer.toString());
                            reject(error);
                            for(let promise of promises) {
                                promise[REJECT](error);
                            }
                        }
                    }
                }
            );
        });
    }

    flush() {
        return this.flushOnInternalRequest();
    }

    executeRawQuery(query,database) {
        return new Promise((resolve, reject) => {
            let url=this.hostUrl;
            url+='/query?';
            let db;
            if(database===undefined) {
                db=this.options.database;
            }
            else
            {
                db=database;
            }
            url+='db='+encodeURIComponent(db)+'&';
            request.post({
                    url: url+'q='+encodeURIComponent(query),
                    auth: {
                        user: this.options.username,
                        pass: this.options.password
                    }
                },
                (error, result) => {
                    if(error) {
                        reject(error);
                    }
                    else {
                        if(result.statusCode>=200 && result.statusCode<400) {
                            let contentType=result.headers['content-type'];
                            if(contentType==='application/json') {
                                let data=JSON.parse(result.body);
                                if(data.results[0].error) reject(new InfluxDBError(data.results[0].error));
                                resolve(data);
                            }
                            else {
                                reject(new InfluxDBError('Unexpected result content-type:'+contentType));
                            }
                        }
                        else {
                            let e=new InfluxDBError(result.statusCode+' communication error');
                            reject(e);
                        }
                    }
                }
            );
        });
    }

    postProcessQueryResults(results) {
        let outcome=[];
        for(let result of results.results)
        {
            if(result.series)
            for(let s of result.series) {
                for(let v of s.values) {
                    let result={ };
                    let i=0;
                    for(let c of s.columns) {
                        if(c==='time') {
                            try {
                                result[c]=new Date(v[i]);
                            }
                            catch(e) {
                                result[c]=v[i];
                            }
                        }
                        else {
                            result[c]=v[i];
                        }
                        i++;
                    }
                    if(s.tags!==undefined) {
                        for(let t in s.tags) {
                            result[t]=s.tags[t];
                        }
                    }
                    outcome.push(result);
                }
            }
        }
        return outcome;
    }

    executeQuery(query,database) {
        return new Promise((resolve, reject) => {
            this.executeRawQuery(query, database).then((data) => {
                resolve(this.postProcessQueryResults(data));
            }).catch((e) => {
                reject(e);
            });
        });
    }

    connect() {
        return new Promise((resolve, reject) => {
            if (this.options.autoCreateDatabase) {
                let p = this.executeRawQuery('SHOW DATABASES');
                p.then((result) => {
                    let values = result.results[0].series[0].values;
                    for (let value of values) {
                        if (value[0] === this.options.database) {
                            this.connected = true;
                            resolve();
                            return;
                        }
                    }

                    let createPromise = this.executeQuery(`CREATE DATABASE ${this.options.database};`);
                    createPromise.then(() => {
                        this.connected = true;
                        resolve();
                    }).catch((e) => {
                        reject(e);
                    })
                }).catch((e) => {
                    reject(e)
                });
            }
            else {
                resolve();
            }
        });
    }
}

//export default InfluxDbConnectionImpl;
module.exports=ConnectionImpl;
