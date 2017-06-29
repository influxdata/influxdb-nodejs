function getFieldType(fields, fieldname){

    for(let f of fields){

        if(f.fieldKey == fieldname){
            return f.fieldType;
        }
    }
    return false;
}

function dropMeasurement(connection, measurement){

    connection.connect().then(() => {

        connection.executeQuery('drop measurement ' + measurement).catch((e) => {
            return e;
        })
    }).catch((e) => {
        return e;
    });

}

module.exports={ getFieldType, dropMeasurement };