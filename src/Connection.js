//import InfluxDBConnectionImpl from './impl/InfluxDbConnectionImpl';
let ConnectionImpl=require('./impl/ConnectionImpl');


/**
 * Main class used to communicate with InfluxDB
 *
 * @public
 */
class Connection {

    /**
     * Create new Connection object. To verify that everything is set correctly, call the {@link Connection#connect} method
     *
     * @param {ConnectionConfiguration} options - all settings needed to connect to Influx DB and configure the communication protocol
     * @returns new {@link Connection} object
     */
    constructor(options) {
        this.stub=new ConnectionImpl(options);
    }

    /**
     * Verify the connection to InfluxDB is available. If you won't call this method on your own, it will be called before first write into
     * InfluxDB automatically.
     *
     * @returns {Promise}
     */
    connect() {
        return this.stub.connect();
    }

    /**
     * Write measurement data points into InfluxDB
     *
     * @param {DataPoint[]} dataPoints - an array of measurement points to write into InfluxDB
     * @returns {Promise} - a promise that is evaluated when data are either written into InfluxDB or
     * a i/o error occurs. (You have to call Promise.then() method of course)
     * @example
     * {
     *   measurement : 'temperature',
     *   timestamp: '1465839830100400200'
     *   tags: []
     *   fields: []
     * }
     *
     */
    write(dataPoints) {
        return this.stub.write(dataPoints);
    }

    /**
     * Flush buffered measurement data into InfluxDB server(s)
     * @returns {Promise}
     */
    flush() {
        return this.stub.flush();
    }

    /**
     * Execute query on InfluxDB
     * @param {String} query text definition of the query, 'select * from outdoorTemperature' for example
     * @returns {Array} post-processed data so that these are easy to work with. The result is an array of objects where
     * fields and tags are stored as regular properties. There is also a time property holding the
     * timestamp of the measurement as javascript Date object.
     *
     * @example
     * [
     *   {
     *     "time": "1970-01-18T07:59:02.227Z",
     *     "temperature": 23.7,
     *     "location": "greenhouse"
     *   },
     *   {
     *     "time": "1970-01-18T07:59:02.265Z",
     *     "temperature": 23.7,
     *     "location": "greenhouse"
     *   }
     * ]
     */
    executeQuery(query) {
        return this.stub.executeQuery(query);
    }

    /**
     * Execute query on InfluxDB and get unmodified result JSON data
     * @param {String} query text definition of the query, 'select * from outdoorTemperature' for example
     * @returns {Array} unmodified result JSON data as responded by InfluxDb
     */
    executeRawQuery(query) {
        return this.stub.executeRawQuery(query);
    }

}

//export default InfluxDBConnection;

module.exports=Connection;