/*
 * Default options for {@link Connection.connect}
 * @ignore
 */
const DefaultConnectionOptions={
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


export default DefaultConnectionOptions;