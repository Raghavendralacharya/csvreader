const oracledb = require('oracledb');

oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;

const mypw = "oracle"  // set mypw to the hr schema password

async function getConnection() {
    const connection = await oracledb.getConnection ({
        user          : "hr",
        password      : mypw,
        connectString : "localhost/FREEPDB1"
    });

    // const result = await connection.execute(
    //     `insert into location(location_id,location_name, street_number, street_name, address2, city, state, zipcode, latitude, longitude, created_at, created_by, modified_at, modified_by) 
    //     values('12345','shrini','abc','achamane','ullur', 'kunda', 'karna','576219', '12345','12345',TO_DATE('2023/05/03', 'yyyy/mm/dd'),'raghav',TO_DATE('2023/05/03', 'yyyy/mm/dd'), 'raghav')`
    // );

    // console.log(result.rows);
    return connection;
}


module.exports = getConnection();