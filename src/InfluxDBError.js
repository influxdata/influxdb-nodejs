/**
 * When using the {@link Connection}, errors will be signaled by this class.
 *
 * @property
 */
class InfluxDBError extends Error {
  /**
   * @param {String} message Information about the error
   * @param {String} [data] Holds data points formatted using InfluxDB line protocol that were not
   *  written into the database due to errors in the communication with InfluxDB
   */
  constructor(message, data) {
    super(message);
    this.data = data;
  }
}

export default InfluxDBError;
