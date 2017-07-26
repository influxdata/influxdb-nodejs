const exec = require("child_process").exec
const testconf = require('../etc/testconf.json')

let docker_process;

function getFieldType(fields, fieldname){

    for(let f of fields){

        if(f.fieldKey == fieldname){
            return f.fieldType;
        }
    }
    return false;
}

function sleep(ms){
    return new Promise( resolve => setTimeout(resolve, ms));
}

function dropMeasurement(connection, measurement){

    connection.connect().then(() => {


        connection.executeQuery('DROP MEASUREMENT ' + measurement).then(() => {


        }).catch((e) => {
            return e;
        })


    }).catch((e) => {
        return e;
    });

}

function leftpad(num, size) {
    var s = num+"";
    while (s.length < size) s = "0" + s;
    return s;
}

function buildValue(type, base, index, pad){

    switch(type){
        case 'string':
        case 'str':
        case 'STRING':
            return base + leftpad(index, pad); //intpad + index.toString()
            break;
        case 'integer':
        case 'int':
        case 'INT':
        case 'INTEGER':
            return base + index;
        case 'float':
        case 'FLOAT':
            return Number.parseFloat(base) + Math.random() * (index + 1);
        case 'BOOL':
        case 'boolean':
        case 'bool':
        case 'BOOLEAN':
            if( Math.random() < 0.5){
                return false;
            }else{
                return true;
            }

    }

}

function buildTags(tags, index, pad){
    let data = []
    for( let tag of tags){
        data.push({ key: tag.name,  value: buildValue(tag.type, tag.base, index, pad)})
    }
    return data;
}
/**
 *
 * @param measurementName - name of the measurement
 * @param tags - array of structures of name and type e.g. [{ name: 'widget', base: 'foo', type: 'string'}]
 * @param fields - array of structures of name and type e.g. [{ name: 'widget', base: '1', type: 'integer'}]
 * @param count - number of elements to generate
 */
function buildDatapoints(measurementName, tags, fields, count){
    let dps = []

    for(let i = 0; i < count; i++){
        dps.push({  measurement: measurementName,
                    timestamp: new Date().getTime(),
                    tags: buildTags(tags, i, Math.ceil(Math.log10(count))),
                    fields: buildTags(fields, i)
        })
    }

    return dps;
}

function pad(n, width, z) {
    z = z || '0';
    n = n + '';
    return n.length >= width ? n : new Array(width - n.length + 1).join(z) + n;
}

/**
 *
 * @param args - array of arguments to the python script
 *    valid arguments:
 *       * http || https
 *       * --version VERSION version of influxdb to pull - default 'latest'
 *       * --admin ADMIN admin user name
 *       * --password PASSWORD admin password
 *       * --nopull NOPULL do not pull new image
 *       * --name NAME of the container - default 'influxdb'
 */
function start_docker_influxdb(args){

    let py_args = [__dirname + '/../scripts/test-server.py', args]

    console.log("(re)starting influxbd docker container " + py_args)

    //docker_process = exec('python3', py_args)
//    docker_process = exec(`${__dirname}/../scripts/test-server.py ${args}`)
    docker_process = exec(__dirname + '/../scripts/test-server.py https');

    let stdout = ''
    docker_process.stdout.on('data', function(data){
        stdout += data.toString()
    })

    docker_process.on('close', () => {
        console.log("docker process closed ", stdout)
    })

    //wait a 30 secs for server to start
    console.log("Waiting 30 seconds for influxdb server to start")
    sleep(30000).then(() => {

        // console.log('STDOUT ' + stdout)
        console.log(`started influxdb server with args ${args}`)

    })

}

module.exports={ getFieldType, dropMeasurement, buildDatapoints, sleep, pad, start_docker_influxdb, testconf };