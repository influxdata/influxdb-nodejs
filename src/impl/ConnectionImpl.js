//import * as request from "request";
const request = require('request');
const streamBuffers = require('stream-buffers');
const FieldType = require('../Field').FieldType;

const RESOLVE=0;
const REJECT=1;

/**
 * @ignore
 */
class ConnectionImpl {


    constructor(options) {
        if(options.database===undefined) throw new Error('database option must be specified');
        this.options = Object.assign({
            username:'root',
            password:'root',
            hostUrl: 'http://localhost:8086',
            autoCreateDatabase: true,
            autoResolvePromisedWritesToCache:true,
            maximumWriteDelay:1000,
            batchSize:1000,
            batchWriteErrorHandler(e, dataPoints) {
                console.log('[DefaultBatchWriteErrorHandler] Error writing data points into InfluxDB:'+
                    dataPoints,e);
            }
        }, options);
        this.schemas={};
        this.hostUrl=this.options.hostUrl;
        if(this.hostUrl.endsWith('/')) this.hostUrl=this.hostUrl.substring(0,this.hostUrl.length-1);
        if(this.options.schema!==undefined)
        {
            for(let s of this.options.schema) {
                if(s.measurement===undefined)
                    throw new Error('Each data point schema must have "measurement" property defined');
                this.schemas[s.measurement]=s;
                if(s.measurement.tags!==undefined) {
                    s.measurement.tagsDictionary={};
                    for(let tag of s.measurement.tags) s.measurement.tagsDictionary[tag]=true;
                }
            }
        }
        this.cachedPromises=[];
        this.connected = false;

        process.on('exit', () => {
            if(this.cachedBatchSize!==undefined) {
                console.log('Warning: there are still cached data points to be written into InfluxDB, '+
                    'but the process is about to exit. Forgot to call Connection.flush() ?');
            }
            for(let promise of this.cachedPromises) {
                let e=new Error('Can\'t write data points to InfluxDB, process is exiting');
                promise[REJECT](e);
            }
        });
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

    serializeFieldValue(v, fieldName, dataPoint) {

        let requiredType=undefined;
        let schema=this.schemas[dataPoint.measurement];
        if(schema!==undefined && schema.fields!==undefined) {
            let requiredType=schema.fields[fieldName];
            if(requiredType===undefined)
                throw new Error(`Field ${fieldName} is not declared in the schema`+
                    ` for measurement ${dataPoint.measurement}`);
        }

        if(requiredType===undefined) {
            switch(typeof v) {
                case 'string':
                    return '\"'+ConnectionImpl.escapeString(v)+'\"';
                case 'boolean':
                    return v ? '1' : '0';
                case 'number':
                    return ''+v;
                default:
                    throw new Error('Unsupported value type:'+(typeof v));
            }
        }
        else {

            function validateType(required) {
                if((typeof v)!==required)
                throw new Error(`Invalid type supplied for field ${fieldName} of `+
                    `measurement ${dataPoint.measurement}. `+
                    `Supplied ${typeof v} but '${required}' is required`);
            }

            switch(requiredType) {
                case FieldType.STRING:
                    validateType('string');
                    return '\"'+ConnectionImpl.escapeString(v)+'\"';
                case FieldType.BOOLEAN:
                    validateType('boolean');
                    return v ? '1' : '0';
                case FieldType.FLOAT:
                    validateType('number');
                    return ''+v;
                case FieldType.INTEGER:
                    validateType('number');
                    if(v!==Math.round(v)) {
                        throw new Error(`Invalid value supplied for field ${fieldName} of `+
                            `measurement ${dataPoint.measurement}.`+
                            'Should have been an integer but supplied number has a fraction part.' +
                            ' Use Math.round/ceil/floor for conversion.');
                    }
                    return ''+v;
                default:
                    throw new Error('Unsupported value type:'+(typeof v));
            }
        }
    }

    serializeTagValue(v,key,dataPoint) {
        if((typeof v)!=='string') throw new Error('Invalid tag value type, must be a string');
        let schema=this.schemas[dataPoint.measurement];
        if(schema!==undefined && schema.tagsDictionary!==undefined
            && schema.tagsDictionary[key]===undefined) {
            throw new Error(`Tag value ${v} is not allowed for measurement `+
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
                    throw new Error('Timestamp is an object but it has to declare getTime() method as well'+
                      'Perhaps you intended to supply a Date object, but it has not been received.');
                return t.getTime()+'000000';
            case 'number':
                return t+'000000';
            default:
                throw new Error('Unsupported timestamp type:'+(typeof t));
        }
    }

    convertDataPointsToText(stream, dataPoints) {

        for(let dataPoint of dataPoints) {
            stream.write(ConnectionImpl.escapeMeasurement(dataPoint.measurement)  );
            stream.write(',');
            // tags
            if(Array.isArray(dataPoint.tags)) {
                for(let tag of dataPoint.tags) {
                    stream.write(ConnectionImpl.escape(tag.key));
                    stream.write('=');
                    stream.write(this.serializeTagValue(tag.value,tag.key,dataPoint));
                }
            } else if((typeof dataPoint.tags)==='object') {
                for(let k in dataPoint.tags) {
                    stream.write(ConnectionImpl.escape(k));
                    stream.write('=');
                    stream.write(this.serializeTagValue(dataPoint.tags[k],k,dataPoint));
                }
            }
            stream.write(' ');
            //fields
            if(Array.isArray(dataPoint.fields)) {
                for(let field of dataPoint.fields) {
                    stream.write(ConnectionImpl.escape(field.key));
                    stream.write('=');
                    stream.write(this.serializeFieldValue(field.value, field.key, dataPoint));
                }
            } else if((typeof dataPoint.fields)==='object') {
                for(let k in dataPoint.fields) {
                    stream.write(ConnectionImpl.escape(k));
                    stream.write('=');
                    stream.write(this.serializeFieldValue(dataPoint.fields[k], k, dataPoint));
                }
            }
            stream.write(' ');
            stream.write(ConnectionImpl.serializeTimestamp(dataPoint.timestamp));
            stream.write('\n');
        }
    }

    write(dataPoints) {

        if(!this.connected) {
            return new Promise((resolve, reject) => {
                this.connect().then(() => {
                    this.write(dataPoints).then(() => {
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
            let batchSizeNotLimited=this.options.batchSize>0 &&
                (this.cachedBatchSize===undefined
                || this.cachedBatchSize+dataPoints.length<this.options.batchSize);
            let timeoutNotLimited=this.options.maximumWriteDelay>0 &&
                (this.cacheAge===undefined || new Date().getTime()-this.cacheAge<this.options.maximumWriteDelay);

            if(batchSizeNotLimited && timeoutNotLimited) {

                return new Promise((resolve,reject) => {
                    if(this.cache===undefined) {
                        this.cache=new streamBuffers.WritableStreamBuffer();
                    }
                    this.convertDataPointsToText(this.cache,dataPoints);

                    if(this.cacheAge===undefined) {
                        this.cacheAge=new Date().getTime();
                        setTimeout(()=>{
                            this.flush().then().catch((e)=>{
                                if(this.options.autoResolvePromisedWritesToCache)
                                    this.options.batchWriteErrorHandler(e,this.cache.getContents());
                            });
                        },this.options.maximumWriteDelay);
                    }

                    if(this.cachedBatchSize===undefined)
                        this.cachedBatchSize=dataPoints.length; else this.cachedBatchSize+=dataPoints.length;

                    if(this.autoResolvePromisedWritesToCache) {
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
    }


    flushOnInternalRequest(dataPoints) {
        let dataStream=this.cache;
        let noCachedData=false;
        if(dataStream===undefined) {
            dataStream=new streamBuffers.WritableStreamBuffer();
            noCachedData=true;
        }
        if(dataPoints!==undefined) {
            this.convertDataPointsToText(dataStream,dataPoints);
        } else {
            if(noCachedData) return new Promise((resolve, reject) => {
                resolve()
            });
        }
        this.cache=undefined;
        this.cachedBatchSize=undefined;
        this.cacheAge=undefined;
        let promises=this.cachedPromises;
        this.cachedPromises=[];

        let db=this.options.database;
        if(db===undefined) throw new Error('Assertion failed: database not specified');
        let url=this.hostUrl+'/write?db='+db;

        return new Promise((resolve, reject) => {
            let bodyBuffer=dataStream.getContents();
            request.post({
                    url: url,
                    method: 'POST',
                    headers: { "Content-Type": "application/text" },
                    body: bodyBuffer
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
                            let e=new Error(result.statusCode+' Influx db sync failed');
                            reject(e);
                            for(let promise of promises) {
                                promise[REJECT](e);
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
                                if(data.results[0].error) reject(new Error(data.results[0].error));
                                resolve(data);
                            }
                            else {
                                reject(new Error('unexpected result content-type:'+contentType));
                            }
                        }
                        else {
                            let e=new Error(result.statusCode+' communication error');
                            reject(e);
                        }
                    }
                }
            );
        });
    }

    postprocessQueryResults(results) {
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
                resolve(this.postprocessQueryResults(data));
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
        });
    }
}

//export default InfluxDbConnectionImpl;
module.exports=ConnectionImpl;
