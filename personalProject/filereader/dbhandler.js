let dbconn = require('./myscript')
async function execute(query){
    // const result = await db.execute(
    //     `SELECT manager_id, department_id, department_name
    //      FROM departments
    //      WHERE manager_id = :id`,
    //     [103],  // bind value for :id
    // );
    const result = await db.execute(
        `SELECT *
         FROM persons`
    );

    console.log(result.rows);
}

async function insertlocation(input){
    // const result = await db.execute(
    //     `insert into location(location_id,location_name, street_number, street_name, address2, city, state, zipcode, latitude, longitude, created_at, created_by, modified_at, modified_by) 
    //     values('12345','shrini','abc','achamane','ullur', 'kunda', 'karna','576219', '12345','12345',TO_DATE('2023/05/03', 'yyyy/mm/dd'),'raghav',TO_DATE('2023/05/03', 'yyyy/mm/dd'), 'raghav')`
    // );
    let table = "location";
    let created_at = new Date();
    let modified_at =  new Date();
    const result = await db.execute(
        "INSERT INTO "+table+"(location_id,location_name, street_number, street_name, address2, city, state, zipcode, latitude, longitude, created_at, created_by, modified_at, modified_by) VALUES "+
        "(:0, :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)",
        [input.location_id, input.location_name, input.street_number, input.street_name, input.address2, input.city, input.state, input.zipcode, input.latitude, input.longitude, created_at, input.created_by, modified_at, input.modified_by],
        { autoCommit: true });

    console.log(result.rows);
}

module.exports = {
    execute: execute,
    insertlocation: insertlocation
}