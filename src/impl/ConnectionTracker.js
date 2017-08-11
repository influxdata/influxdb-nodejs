import * as _ from 'lodash';
/**
 * Watch for situations when the node process is about to exit but the connection buffers are still
 * non-empty for some connections.
 *
 * @ignore
 */
class ConnectionTracker {
  constructor() {
    this.activeConnections = {};
    this.connectionIdGenerator = 0;
    this.registerShutdownHook();
  }

  registerShutdownHook() {
    process.on('exit', () => {
      _.forOwn(this.activeConnections, (connection) => {
        connection.onProcessExit();
      });
    });
  }

  generateConnectionID() {
    const id = this.connectionIdGenerator;
    this.connectionIdGenerator += 1;
    return id;
  }

  startTracking(connection) {
    this.activeConnections[connection.connectionId] = connection;
  }

  stopTracking(connection) {
    delete this.activeConnections[connection.connectionId];
  }
}

export default new ConnectionTracker();
