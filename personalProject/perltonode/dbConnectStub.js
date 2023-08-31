const oracledb = require('oracledb');

async function dbConnect(inst) {
    try {
        const dbConfig = {
            user: process.env.DB_USER || inst.DB_USER || "hr", // Update with your database user
            password: process.env.DB_PASSWORD || inst.DB_USER || "oracle", // Update with your password
            connectString: "localhost/FREEPDB1",
            externalAuth: false
        };

        const connection = await oracledb.getConnection(dbConfig);
        connection.autoCommit = false; // Don't commit automatically
        await connection.execute("alter session set NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'");

        return connection;
    } catch (error) {
        console.error(`Error connecting to ${inst}: ${error.message}`);
        process.exit(1);
    }
}

async function executeStatement(connection, query, bindData) {
    try {
        let result = {};
        if(query.startsWith('\n      SELECT crawler_rowid,\n        parcel_number,\n        source_id,\n        address_type,\n        address,\n        city,\n        state,\n        zip\n')){
            result.rows = [{
                CRAWLER_ROWID: "1234",
                PARCEL_NUMBER: "1234",
                SOURCE_ID: "1234",
                ADDRESS_TYPE: "office",
                ADDRESS: "jp nagar",
                CITY: "bangalore",
                STATE: "karnataka",
                ZIP: "560000"
            }]
        } else if(query.startsWith('\n      SELECT TO_CHAR(sdo_geometry.get_wkt(sdo_aggr_centroid(mdsys.sdoaggrtype(sdo_cs.transform(c.coordinates, 8307), 0.005))))\n      FROM dwilson.crawler_parcels c\n      WHERE rowid = :rowid\n    ')) {
            // TO DO check if this query return string coordinate directly or string coordinate in an object with key name as coordinate.
            // result.rows =[{
            //     coordinates : "123456"
            // }];

            result = "123456"
        } else if(query.startsWith('\n        SELECT next_globalid.NEXTVAL FROM dual\n    ')) {
            result.rows =[[Math.random().toString()]];
        } else if(query.startsWith('\n      SELECT ipid\n      FROM sde.address\n      WHERE threegisid = :threegisid\n    ')) {
            result.rows =[{ipid: 'ff09023834-r2378shr'}];
        }

        return result;
    } catch (error) {
        console.error('Error executing statement:', error);
    }
}

module.exports = {
    dbConnect : dbConnect,
    executeStatement: executeStatement
}
// module.exports.main = {};