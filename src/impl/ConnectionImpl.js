import * as request from 'request';
import * as _ from 'lodash';

import InfluxDBError from '~/InfluxDBError';
import WriteBuffer from '~/impl/WriteBuffer';
import ConnectionTracker from '~/impl/ConnectionTracker';
import DefaultConnectionOptions from '~/impl/DefaultConnectionOptions';

/**
 * @ignore
 */
class ConnectionImpl {

    constructor(options) {
        this.schemas = {};
        this.options = ConnectionImpl.calculateOptions(options);
        this.schemas = ConnectionImpl.prepareSchemas(options.schema);
        this.hostUrl = ConnectionImpl.stripTrailingSlashIfNeeded(this.options.hostUrl);
        this.writeBuffer = new WriteBuffer(this.schemas, this.options.autoGenerateTimestamps);
        this.bufferFlushTimerHandle = null;
        this.connectionId = ConnectionTracker.generateConnectionID();
        this.connected = false;
        this.disconnected = false;
    }

    static stripTrailingSlashIfNeeded(url) {
        if (url.endsWith('/')) return url.substring(0, url.length - 1); else return url;
    }

    // Copy the supplied schema so that it won't get affected by further modifications from
    // the user. Also convert tags to a map for faster access during serialization
    static prepareSchemas(schemas) {
        if (schemas) {
            const copy = _.cloneDeep(schemas);
            for (let schema of copy) {
                if (!schema.measurement)
                    throw new InfluxDBError('Each data point schema must have "measurement" property defined');
                if (schema.tags) {
                    schema.tagsDictionary = {};
                    for (let tag of schema.tags) schema.tagsDictionary[tag] = true;
                }
            }
            return _.keyBy(copy, 'measurement');
        }
        else {
            return {};
        }
    }

    static calculateOptions(options) {
        if (!options.database) throw new InfluxDBError("'database' option must be specified");
        const results = {};
        Object.assign(results, DefaultConnectionOptions);
        Object.assign(results, options);
        return results;
    }

    // called by the connection tracker when the node process is exiting
    onProcessExit() {
        if (this.writeBuffer.batchSize > 0) {
            console.error('Warning: there are still buffered data points to be written into InfluxDB, ' +
                'but the process is about to exit. Forgot to call Connection.flush() ?');
        }
        this.writeBuffer.rejectWritePromises(new InfluxDBError('Can\'t write data points to InfluxDB, process is exiting'));
    }

    write(dataPoints, forceFlush) {
        try {
            if (!dataPoints) return this.writeEmptySetOfPoints(forceFlush);
            if (!Array.isArray(dataPoints)) {
                if (typeof dataPoints === 'object')
                    return this.write([dataPoints], forceFlush);
                else
                    return Promise.reject(new InfluxDBError('Invalid arguments supplied'));
            }
            if (dataPoints.length === 0) return this.writeEmptySetOfPoints(forceFlush);

            return this.whenConnected(() => {
                return this.writeWhenConnectedAndInputValidated(dataPoints, forceFlush);
            });
        }
        catch (e) {
            return Promise.reject(e);
        }
    }

    writeEmptySetOfPoints(forceFlush) {
        if (forceFlush)
            return this.flush();
        else
            return Promise.resolve();
    }

    writeWhenConnectedAndInputValidated(dataPoints, forceFlush) {
        if (this.writeBuffer.firstWriteTimestamp === null) this.writeBuffer.firstWriteTimestamp = new Date().getTime();
        const batchSizeLimitNotReached = this.options.batchSize > 0 &&
            (this.writeBuffer.batchSize + dataPoints.length < this.options.batchSize);
        const timeoutLimitNotReached = this.options.maximumWriteDelay > 0 &&
            (new Date().getTime() - this.writeBuffer.firstWriteTimestamp < this.options.maximumWriteDelay);
        if (batchSizeLimitNotReached && timeoutLimitNotReached && !forceFlush) {
            return this.promiseBufferedWrite(dataPoints);
        }
        else {
            this.writeBuffer.write(dataPoints);
            return this.flush();
        }
    }

    promiseBufferedWrite(dataPoints) {
        if (this.options.autoResolveBufferedWritePromises) {
            this.directBufferedWrite(dataPoints);
            return Promise.resolve();
        }
        else {
            let promise = new Promise(() => {
                this.directBufferedWrite(dataPoints)
            });
            this.writeBuffer.addWritePromiseToResolve(promise);
            return promise;
        }
    }

    directBufferedWrite(dataPoints) {
        this.writeBuffer.write(dataPoints);
        // make the shutdown hook aware of buffered writes
        ConnectionTracker.startTracking(this);

        this.scheduleFlush(() => {
            this.flush().then().catch((e) => {
                if (this.options.autoResolveBufferedWritePromises) {
                    this.options.batchWriteErrorHandler(e, e.data);
                }
            });
        }, this.options.maximumWriteDelay);
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

    flush() {
        // prevent sending empty requests to the db
        if (this.writeBuffer.batchSize === 0) return Promise.resolve();
        // from now on all writes will be redirected to a new buffer
        const flushedWriteBuffer = this.writeBuffer;
        this.writeBuffer = new WriteBuffer(this.schemas, this.options.autoGenerateTimestamps);
        // prevent repeated flush call if flush invoked before expiration timeout
        this.cancelFlushSchedule();
        // shutdown hook doesn't need to track this connection any more
        ConnectionTracker.stopTracking(this);

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
                        flushedWriteBuffer.rejectWritePromises(error);
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

    executeQuery(query, database) {
        return this.executeRawQuery(query, database).then(ConnectionImpl.postProcessQueryResults);
    }

    executeRawQuery(query, database) {
        return this.whenConnected(() => {
            return this.executeInternalQuery(query, database);
        });
    }

    executeInternalQuery(query, database) {
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
                                reject(new InfluxDBError(`Unexpected result content-type: ${contentType}`));
                            }
                        }
                        else {
                            const error = new InfluxDBError(`HTTP ${result.statusCode} communication error`);
                            reject(error);
                        }
                    }
                }
            );
        });
    }

    static postProcessQueryResults(results) {
        const outcome = [];
        _.forEach(results.results, (result) => {
            _.forEach(result.series, (series) => {
                // use for loops form now on to get better performance
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
                    if (series.tags) Object.assign(result, series.tags);
                    outcome.push(result);
                }
            });
        });
        return Promise.resolve(outcome);
    }

    connect() {
        if (this.options.autoCreateDatabase) {
            // this works because:
            //  1) create database operation is idempotent
            //  2) the create database operation doesn't require any privileges
            return this.executeInternalQuery(`CREATE DATABASE ${this.options.database}`).then(() => {
                this.connected = true;
                this.disconnected = false;
            });
        }
        else {
            this.executeInternalQuery('SHOW DATABASES').then((databases) => {
                this.connected = this.doesDatabaseExists(databases);
                this.disconnected = !this.connected;
                if (!this.connected) new InfluxDBError(`Database '${this.options.database}' does not exist`);
            });
        }
    }

    disconnect() {
        let result = this.flush();
        this.connected = false;
        this.disconnected = true;
        return result;
    }

    doesDatabaseExists(showDatabasesResult) {
        // there is always _internal database available/visible by any user
        const values = showDatabasesResult.results[0].series[0].values;
        return _.findIndex(values, this.options.database) >= 0;
    }

    whenConnected(action) {
        try {
            if (this.disconnected)
                return Promise.reject(new InfluxDBError('Attempt to use a disconnected connection detected'));
            if (!this.connected)
                return this.connect().then(action); else return action();
        }
        catch (e) {
            return Promise.reject(e);
        }
    }
}


export default ConnectionImpl;
