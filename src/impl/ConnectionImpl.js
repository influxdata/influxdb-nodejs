import * as RequestPromise from 'request-promise';
import * as _ from 'lodash';
/** @namespace JSON */

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
    ConnectionImpl.validateSchemas(options.schema);
    this.schemas = ConnectionImpl.prepareSchemas(options.schema);
    // for convenience of the user ignore a slash at the end of the URL
    this.hostUrl = ConnectionImpl.stripTrailingSlashIfNeeded(this.options.hostUrl);
    // buffer for data points, new instance is created for every batch
    this.writeBuffer = new WriteBuffer(this.schemas, this.options.autoGenerateTimestamps);
    // timer handle used to flush the buffer when it becomes too old
    this.bufferFlushTimerHandle = null;
    // unique ID of this connection
    this.connectionId = ConnectionTracker.generateConnectionID();
    // becomes true when connection to InfluxDB gets verified either automatically or by
    // calling connect()
    this.connected = false;
    // becomes false after the user explicitly calls disconnect(); the connection will
    // not be usable without reconnecting
    this.disconnected = false;
  }

  static stripTrailingSlashIfNeeded(url) {
    if (url.endsWith('/')) return url.substring(0, url.length - 1);
    return url;
  }

  static validateSchemas(schemas) {
    _.forEach(schemas, (schema) => {
      if (!schema.measurement) {
        throw new InfluxDBError('Each data point schema must have "measurement" property defined');
      }
    });
  }

  // Copy the supplied schema so that it won't get affected by further modifications from
  // the user. Also convert tags to a map for faster access during serialization
  static prepareSchemas(schemas) {
    if (schemas) {
      const data = _.map(schemas, schema =>
        ({
          measurement: schema.measurement,
          schema: _.cloneDeep(schema),
          tagsDictionary: _.groupBy(schema.tags),
        }));
      return _.keyBy(data, 'measurement');
    }
    return {};
  }

  static calculateOptions(options) {
    const results = {};
    if (!options.database) throw new InfluxDBError("'database' option must be specified");
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
        if (typeof dataPoints === 'object') {
          return this.write([dataPoints], forceFlush);
        }
        return Promise.reject(new InfluxDBError('Invalid arguments supplied'));
      }
      if (dataPoints.length === 0) return this.writeEmptySetOfPoints(forceFlush);
      return this.whenConnected(() =>
        this.writeWhenConnectedAndInputValidated(dataPoints, forceFlush));
    } catch (e) {
      return Promise.reject(e);
    }
  }

  writeEmptySetOfPoints(forceFlush) {
    if (forceFlush) {
      return this.flush();
    }
    return Promise.resolve();
  }

  writeWhenConnectedAndInputValidated(dataPoints, forceFlush) {
    const batchSizeLimitNotReached = this.options.batchSize > 0 &&
        (this.writeBuffer.batchSize + dataPoints.length < this.options.batchSize);
    const timeoutLimitNotReached = this.writeBuffer.firstWriteTimestamp === null ||
        (this.options.maximumWriteDelay > 0 &&
        (new Date().getTime() - this.writeBuffer.firstWriteTimestamp <
        this.options.maximumWriteDelay));

    this.writeBuffer.write(dataPoints);

    if (batchSizeLimitNotReached && timeoutLimitNotReached && !forceFlush) {
      // just write into the buffer
      return this.promiseBufferedWrite();
    }
    // write to InfluxDB now, but serialize submitted data points first
    return this.flush();
  }

  promiseBufferedWrite() {
    if (this.writeBuffer.firstWriteTimestamp === null) {
      this.writeBuffer.firstWriteTimestamp = new Date().getTime();
      ConnectionTracker.startTracking(this);
      this.scheduleFlush(() => {
        this.flush();
      }, this.options.maximumWriteDelay);
    }
    if (this.options.autoResolveBufferedWritePromises) {
      return Promise.resolve();
    }
    return this.writeBuffer.createPromiseToResolveOnFlush();
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
    const url = `${this.hostUrl}/write?db=${this.options.database}`;
    const bodyBuffer = this.writeBuffer.stream.getContents();
    const flushedWriteBuffer = this.writeBuffer;
    // prevent sending empty requests to the db
    if (flushedWriteBuffer.batchSize === 0) return Promise.resolve();
    // from now on all writes will be redirected to a new buffer
    this.writeBuffer = new WriteBuffer(this.schemas, this.options.autoGenerateTimestamps);
    // prevent repeated flush call if flush invoked before expiration timeout
    this.cancelFlushSchedule();
    // shutdown hook doesn't need to track this connection any more
    ConnectionTracker.stopTracking(this);

    return new RequestPromise.Request({
      resolveWithFullResponse: true,
      url,
      method: 'POST',
      headers: { 'Content-Type': 'application/text' },
      body: bodyBuffer,
      auth: {
        user: this.options.username,
        pass: this.options.password,
      },
    }).then((result) => {
      if (result.statusCode >= 200 && result.statusCode < 400) {
        flushedWriteBuffer.resolveWritePromises();
        return Promise.resolve();
      }
      let message = `Influx db write failed ${result.statusCode}`;
      // add information returned by the server if possible
      try {
        message += `: ${JSON.parse(result.body).error}`;
      } catch (e) {
        // we append the message only if it can be parsed form the response
      }
      return this.onFlushError(flushedWriteBuffer, message, bodyBuffer.toString());
    }).catch((e) => {
      const message = `Cannot write data to InfluxDB, reason: ${e.message}`;
      return this.onFlushError(flushedWriteBuffer, message, bodyBuffer.toString());
    });
  }

  onFlushError(flushedWriteBuffer, message, data) {
    const error = new InfluxDBError(message);
    if (this.options.autoResolveBufferedWritePromises) {
      this.options.batchWriteErrorHandler(error, data);
    } else {
      flushedWriteBuffer.rejectWritePromises(error);
    }
    return Promise.reject(error);
  }

  executeQuery(query, database) {
    return this.executeRawQuery(query, database).then(ConnectionImpl.postProcessQueryResults);
  }

  executeRawQuery(query, database) {
    return this.whenConnected(() => this.executeInternalQuery(query, database));
  }

  executeInternalQuery(query, database) {
    const db = !database ? this.options.database : database;
    const url = `${this.hostUrl}/query?db=${encodeURIComponent(db)}&q=${encodeURIComponent(query)}`;
    return new RequestPromise.Request({
      resolveWithFullResponse: true,
      url,
      auth: {
        user: this.options.username,
        pass: this.options.password,
      },
    }).then((result) => {
      if (result.statusCode >= 200 && result.statusCode < 400) {
        const contentType = result.headers['content-type'];
        if (contentType === 'application/json') {
          const data = JSON.parse(result.body);
          if (data.results[0].error) {
            return Promise.reject(new InfluxDBError(data.results[0].error));
          }
          return Promise.resolve(data);
        }
        return Promise.reject(new InfluxDBError(`Unexpected result content-type: ${contentType}`));
      }
      const error = new InfluxDBError(`HTTP ${result.statusCode} communication error`);
      return Promise.reject(error);
    }).catch(
      e => Promise.reject(new InfluxDBError(`Cannot read data from InfluxDB, reason: ${e.message}`)));
  }

  static postProcessQueryResults(results) {
    const outcome = [];
    _.forEach(results.results, (queryResult) => {
      _.forEach(queryResult.series, (series) => {
        // use for loops form now on to get better performance
        for (const values of series.values) {
          const result = {};
          let i = 0;
          for (const columnName of series.columns) {
            if (columnName === 'time') {
              try {
                result[columnName] = new Date(values[i]);
              } catch (e) {
                result[columnName] = values[i];
              }
            } else {
              result[columnName] = values[i];
            }
            i += 1;
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
      // This works because create database operation is idempotent; unfortunately,
      // SHOW DATABASES requires the same admin permissions as CREATE DATABASE. Therefore
      // there is no point in trying to list the databases first and checking for the one
      // the user is trying to use.
      return this.executeInternalQuery(`CREATE DATABASE ${this.options.database}`).then(() => {
        this.connected = true;
        this.disconnected = false;
      });
    }
    const result = this.executeInternalQuery('SHOW DATABASES').then((databases) => {
      this.connected = this.doesDatabaseExists(databases);
      this.disconnected = !this.connected;
      if (!this.connected) {
        return result.reject(new InfluxDBError(`Database '${this.options.database}' does not exist`));
      }
      return Promise.resolve();
    }).catch(() => {
      // When user authentication is in use, SHOW DATABASES will fail due to insufficient user
      // privileges Therefore we try if the server is alive at least...
      const url = `${this.hostUrl}/ping`;
      return new RequestPromise.Request({ uri: url }).then(() => {
        this.connected = true;
        this.disconnected = false;
      }).catch(e => Promise.reject(
        new InfluxDBError(`Unable to contact InfluxDB, ping operation on '${url}' failed, reason: ${e.message}`)),
      );
    });
    return result;
  }

  disconnect() {
    const result = this.flush();
    this.connected = false;
    this.disconnected = true;
    return result;
  }

  doesDatabaseExists(showDatabasesResult) {
    // there is always _internal database available/visible
    // noinspection JSUnresolvedVariable - we don't want to document InfluxDB result format here
    const values = showDatabasesResult.results[0].series[0].values;
    // this is a lodash problem; it doesn't declare string parameter but it is documented
    // noinspection JSCheckFunctionSignatures
    return _.findIndex(values, this.options.database) >= 0;
  }

  whenConnected(action) {
    try {
      if (this.disconnected) {
        return Promise.reject(new InfluxDBError('Attempt to use a disconnected connection detected'));
      }
      if (!this.connected) {
        return this.connect().then(action);
      }
      return action();
    } catch (e) {
      return Promise.reject(e);
    }
  }
}


export default ConnectionImpl;
