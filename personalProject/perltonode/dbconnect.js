const oracledb = require('oracledb');

async function db_connect(inst) {
    try {
        const dbConfig = {
            user: process.env.DB_USER || "hr", // Update with your database user
            password: process.env.DB_PASSWORD || "oracle", // Update with your password
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

module.exports = {
    db_connect : db_connect
}
// module.exports.main = {};