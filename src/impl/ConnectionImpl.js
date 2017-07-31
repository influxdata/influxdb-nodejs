//import * as request from "request";
const request = require('request');


const InfluxDBError = require('../InfluxDBError').InfluxDBError;
const WriteBuffer = require('./WriteBuffer');

/**
 * @ignore
 */
class ConnectionImpl {

    // Copy the supplied schema so that it won't get affected by further modifications from
    // the user. Also convert tags to a map for faster access during serialization
    processSchemas(schemas) {
        const result = {};
        if (schemas) {
            for (let originalSchema of schemas) {
                const connectionSchema = {};
                if (!originalSchema.measurement)
                    throw new InfluxDBError('Each data point schema must have "measurement" property defined');
                connectionSchema.measurement = originalSchema.measurement;
                if (originalSchema.tags) {
                    connectionSchema.tagsDictionary = {};
                    connectionSchema.tags = [];
                    for (let tag of originalSchema.tags) {
                        connectionSchema.tagsDictionary[tag] = true;
                        connectionSchema.tags.push(tag);
                    }
                }
                if (originalSchema.fields) {
                    connectionSchema.fields = {};
                    for (let fieldKey in originalSchema.fields) {
                        connectionSchema.fields[fieldKey] = originalSchema.fields[fieldKey];
                    }
                }
                result[originalSchema.measurement] = connectionSchema;
            }
        }
        return result;
    }

    onProcessExit() {
        if (this.writeBuffer.batchSize>0) {
            console.error('Warning: there are still buffered data points to be written into InfluxDB, ' +
                'but the process is about to exit. Forgot to call Connection.flush() ?');
        }
        for (let promise of this.bufferedWritesPromises) {
            const error = new InfluxDBError('Can\'t write data points to InfluxDB, process is exiting');
            promise.reject(error);
        }
    }

    registerShutdownHook() {
        if (!ConnectionImpl.activeConnections) {
            ConnectionImpl.connectionIdGenerator = 0;
            ConnectionImpl.activeConnections = {};
            process.on('exit', () => {
                for (let connectionId in ConnectionImpl.activeConnections) {
                    ConnectionImpl.activeConnections[connectionId].onProcessExit();
                }
            });
        }
        this.id = ConnectionImpl.connectionIdGenerator++;
    }

    stripTrailingSlashIfNeeded(url) {
        if (url.endsWith('/')) return url.substring(0, url.length - 1); else return url;
    }

    calculateOptions(options) {
        if (!options.database) throw new InfluxDBError("'database' option must be specified");

        const results={};
        const defaults={
            username: 'root',
            password: 'root',
            hostUrl: 'http://localhost:8086',
            autoCreateDatabase: true,
            autoResolveBufferedWritePromises: true,
            maximumWriteDelay: 1000,
            batchSize: 1000,
            batchWriteErrorHandler(e, dataPoints) {
                console.error(`Error writing data points into InfluxDB:\n${dataPoints}`, e);
            }
        };
        Object.assign(results,defaults);
        Object.assign(results,options);
        return results;
    }

    constructor(options) {
        this.schemas={};
        this.schemas=this.processSchemas(options.schema);
        this.options = this.calculateOptions(options);
        this.hostUrl = this.stripTrailingSlashIfNeeded(this.options.hostUrl);
        this.writeBuffer=new WriteBuffer(this.schemas, this.options.autoGenerateTimestamps);
        this.bufferFlushTimerHandle = null;
        this.registerShutdownHook();
    }


    scheduleFlush(onFlush, delay) {
        if (this.bufferFlushTimerHandle === null) {
            this.bufferFlushTimerHandle = setTimeout(onFlush, delay);
        }
    }

    cancelFlushSchedule() {
        if (this.bufferFlushTimerHandle !== null) {
            clearTimeout(this.bufferFlushTimerHandle);
            this.bufferFlushTimerHandle = null;
        }
    }

    writeWhenConnectedAndInputValidated(dataPoints, forceFlush) {
        if (this.writeBuffer.firstWriteTimestamp===null) this.writeBuffer.firstWriteTimestamp = new Date().getTime();
        const batchSizeLimitNotReached = this.options.batchSize > 0 &&
            (this.writeBuffer.batchSize + dataPoints.length < this.options.batchSize);
        const timeoutLimitNotReached = this.options.maximumWriteDelay > 0 &&
            (new Date().getTime() - this.writeBuffer.firstWriteTimestamp < this.options.maximumWriteDelay);
        if (batchSizeLimitNotReached && timeoutLimitNotReached && !forceFlush) {
            let promise=new Promise((resolve, reject) => {
                this.writeBuffer.write(dataPoints);
                // make the shutdown hook aware of buffered writes
                ConnectionImpl.activeConnections[this.id] = this;

                this.scheduleFlush(() => {
                    this.flush().then().catch((e) => {
                        if (this.options.autoResolveBufferedWritePromises) {
                            this.options.batchWriteErrorHandler(e, e.data);
                        }
                    });
                }, this.options.maximumWriteDelay);

                this.writeBuffer.batchSize += dataPoints.length;

                if (this.options.autoResolveBufferedWritePromises) {
                    resolve();
                }
                else {
                    this.writeBuffer.addWritePromiseToResolve(promise);
                }
            });
            return promise;
        }
        else {
            return this.flushOnInternalRequest(dataPoints);
        }
    }

    writeEmptySetOfPoints(forceFlush) {
        if (forceFlush)
            return this.flushOnInternalRequest();
        else
            return Promise.resolve();
    }

    write(dataPoints, forceFlush) {
        if (!dataPoints) return this.writeEmptySetOfPoints(forceFlush);
        if (!Array.isArray(dataPoints)) {
            if (typeof dataPoints === 'object')
                return this.write([dataPoints], forceFlush);
            else
                throw new InfluxDBError('Invalid arguments supplied');
        }
        if (dataPoints.length === 0) return this.writeEmptySetOfPoints(forceFlush);

        if (!this.connected) {
            return new Promise((resolve, reject) => {
                let connectToDatabase = this.connect();
                let writeToDatabase = this.write(dataPoints, forceFlush);
                connectToDatabase.then(writeToDatabase).catch((e) => {
                    reject(e)
                });
            });
        }
        else {
            return this.writeWhenConnectedAndInputValidated(dataPoints, forceFlush);
        }
    }

    flushOnInternalRequest(dataPoints) {
        if (dataPoints) {
            this.writeBuffer.write(dataPoints);
        } else {
            if (this.writeBuffer.batchSize===0) return new Promise((resolve, reject) => {
                resolve()
            });
        }

        const flushedWriteBuffer=this.writeBuffer;
        // prevent repeated flush call if flush invoked before expiration timeout
        this.cancelFlushSchedule();
        this.writeBuffer=new WriteBuffer(this.schemas, this.options.autoGenerateTimestamps);

        // shutdown hook doesn't need to track this connection any more
        delete ConnectionImpl.activeConnections[this.id];
        const url = `${this.hostUrl}/write?db=${this.options.database}`;

        return new Promise((resolve, reject) => {
            let bodyBuffer = flushedWriteBuffer.stream.getContents();
            request.post({
                    url: url,
                    method: 'POST',
                    headers: {"Content-Type": "application/text"},
                    body: bodyBuffer,
                    auth: {
                        user: this.options.username,
                        pass: this.options.password
                    }
                },
                (error, result) => {
                    if (error) {
                        reject(error);
                        flushedWriteBuffer.resolveWritePromises(error);
                    }
                    else {
                        if (result.statusCode >= 200 && result.statusCode < 400) {
                            flushedWriteBuffer.resolveWritePromises();
                            resolve();
                        }
                        else {
                            let message = `Influx db write failed ${result.statusCode}`;
                            // add information returned by the server if possible
                            try {
                                message += ': ' + JSON.parse(result.body).error;
                            }
                            catch (e) {
                            }
                            let error = new InfluxDBError(message, bodyBuffer.toString());
                            flushedWriteBuffer.rejectWritePromises(error);
                            reject(error);
                        }
                    }
                }
            );
        });
    }

    flush() {
        return this.flushOnInternalRequest();
    }

    executeRawQuery(query, database) {
        console.log('In raw query');
        return new Promise((resolve, reject) => {
            const db = !database ? this.options.database : database;
            const url = `${this.hostUrl}/query?db=${encodeURIComponent(db)}&q=${encodeURIComponent(query)}`;
            request.post({
                    url: url,
                    auth: {
                        user: this.options.username,
                        pass: this.options.password
                    }
                },
                (error, result) => {
                    if (error) {
                        reject(error);
                    }
                    else {
                        if (result.statusCode >= 200 && result.statusCode < 400) {
                            let contentType = result.headers['content-type'];
                            if (contentType === 'application/json') {
                                const data = JSON.parse(result.body);
                                if (data.results[0].error) reject(new InfluxDBError(data.results[0].error));
                                resolve(data);
                            }
                            else {
                                reject(new InfluxDBError('Unexpected result content-type:' + contentType));
                            }
                        }
                        else {
                            const error = new InfluxDBError(result.statusCode + ' communication error');
                            reject(error);
                        }
                    }
                }
            );
        });
    }

    postProcessQueryResults(results) {
        const outcome = [];
        for (let result of results.results) {
            if (result.series)
                for (let series of result.series) {
                    for (let values of series.values) {
                        let result = {};
                        let i = 0;
                        for (let columnName of series.columns) {
                            if (columnName === 'time') {
                                try {
                                    result[columnName] = new Date(values[i]);
                                }
                                catch (e) {
                                    result[columnName] = values[i];
                                }
                            }
                            else {
                                result[columnName] = values[i];
                            }
                            i++;
                        }
                        if (series.tags) {
                            for (let tagName in series.tags) {
                                result[tagName] = series.tags[tagName];
                            }
                        }
                        outcome.push(result);
                    }
                }
        }
        return Promise.resolve(outcome);
    }

    executeQuery(query, database) {
        return new Promise((resolve, reject) => {
            this.executeRawQuery(query, database).then((data) => {
                resolve(this.postProcessQueryResults(data));
            }).catch((e) => {
                reject(e);
            });
        });
        //return this.executeRawQuery(query, database).then(this.postProcessQueryResults);
    }

    doesDatabaseExists(showDatabasesResult) {
        const values = showDatabasesResult.results[0].series[0].values;
        for (let value of values) {
            if (value[0] === this.options.database) {
                return true;
            }
        }
        return false;
    }

    connect() {
/*
        if (this.options.autoCreateDatabase) {
            return this.executeRawQuery(`CREATE DATABASE ${this.options.database}`).then(() => {
                this.connected = true;
            });
        }
        else {
            const showDatabases = this.executeRawQuery('SHOW DATABASES').then();

        }*/

        return new Promise((resolve, reject) => {

            const showDatabases = this.executeRawQuery('SHOW DATABASES');
            showDatabases.then((result) => {
                if(this.doesDatabaseExists(result)) {
                    this.connected = true;
                    resolve();
                }
                else {
                    if (this.options.autoCreateDatabase) {
                        // If the database doesn't already exist, create it
                        const createDatabase = this.executeRawQuery(`CREATE DATABASE ${this.options.database}`);
                        createDatabase.then(() => {
                            this.connected = true;
                            resolve();
                        }).catch((e) => {
                            reject(e);
                        })
                    }
                    else {
                        resolve(new InfluxDBError('Database ' + this.options.database + ' does not exist'));
                    }
                }
            }).catch((e) => {
                reject(e)
            });
        });
    }
}

//export default ConnectionImpl;
module.exports = ConnectionImpl;
