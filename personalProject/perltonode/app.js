const streetTypeAbbreviations = [
    'ALLEY', 'ALY', 'AV', 'AVE', 'AVENUE', 'BEND', 'BLV', 'BLVD', 'BND', 'CIRCLE', 'CIR', 'CR',
    'CRES', 'CRESCENT', 'CRK', 'CT', 'CRT', 'COURT', 'CTR', 'CV', 'DR', 'DRIVE', 'EXT',
    'FLS', 'FORD', 'FRD', 'GRN', 'HILL', 'HILLS', 'HL', 'HLS', 'HS', 'HTS', 'HVN',
    'HWY', 'LN', 'LNDG', 'LANDING', 'MDWS', 'MHP', 'MNR', 'PARK', 'PASS', 'PIKE',
    'PKWY', 'PL', 'PLZ', 'RD', 'RDG', 'ROAD', 'ROW', 'RUN', 'SPGS',
    'SQ', 'ST', 'STA', 'STREET', 'TER', 'TR', 'TRCE', 'TRL', 'WAY',
    'XING', 'CROSSING', 'SQUARE', 'OVERLOOK', 'LOOP', 'RIDGE', 'PARKWAY', 'PTE',
    'PKY', 'POINT', 'PT', 'WY', 'BL', 'COVE', 'TRAIL', 'LANE', 'DRV'
];

const streetTypeObject = {};
streetTypeAbbreviations.forEach(abbreviation => {
    streetTypeObject[abbreviation] = 1;
});

const replaceStreetTypeMapping = {
    'AV': 'AVE',
    'AVENUE': 'AVE',
    'BL': 'BLVD',
    'BLV': 'BLVD',
    'COURT': 'CT',
    'COVE': 'CV',
    'CR': 'CIR',
    'CRT': 'CT',
    'CROSSING': 'XING',
    'DRIVE': 'DR',
    'DRV': 'DR',
    'HILL': 'HL',
    'LANDING': 'LNDG',
    'LANE': 'LN',
    'PARKWAY': 'PKWY',
    'PKY': 'PKWY',
    'POINT': 'PT',
    'RIDGE': 'RDG',
    'ROAD': 'RD',
    'SQUARE': 'SQ',
    'STREET': 'ST',
    'TR': 'TRL',
    'TRAIL': 'TRL',
    'WY': 'WAY'
};


const { program } = require('commander');
const  dbConnect = require('./dbconnect').db_connect; // Replace with actual module path

program
  .option('-d, --db <db>', 'Database name')
  .option('--debug', 'Enable debug mode')
  .option('-p, --poly <poly>', 'Polygon option')
  .option('-g, --gis <gis>', 'GIS option')
  .parse(process.argv);

const options = program.opts();

let db = options.db || 'dwh1';
const debug = options.debug || false;
const poly = options.poly || "1234";
let gis = options.gis || 'gis1';

if (!poly) {
  usage();
}

// function usage() {
//   console.log('Usage: node script.js [options]');
//   process.exit(1);
// }
async function process(){
    // Connect to Databases
    const dbh =  await dbConnect(db);
    // const gdb = await dbConnect(gis);
    const args = {
        dbh: dbh,
        poly: '1234'
    };
    main(args);
}
process();

// Run main function
// main({ dbh, gdb });

// Disconnect databases and close logs, then exit
// dbh.end();
// gdb.end();

console.log('Closing LOGFILE');
// process.exit(0);

async function main(args) {
    // Log script name and polygon id
    console.log(`Starting ${process.argv[1]}, polygon id passed in (${args.poly})`);
  
    // Assign args: $dbh => DWH dbh passed in, $gdb => GIS dbh passed in
    const { dbh, gdb } = args;
  
    // Prepare STHs
    const addrq = dbh.prepare(`
      SELECT crawler_rowid,
        parcel_number,
        source_id,
        address_type,
        address,
        city,
        state,
        zip
      FROM (
        SELECT cp.crawler_rowid,
          cp.parcel_number,
          cp.source_id,
          CASE
            WHEN (cp.property_type = 'RESIDENTIAL') THEN 'SFU'
            WHEN (cp.property_type = 'COMMERCIAL') THEN 'SBU'
            WHEN (cp.property_type = 'GOVERNMENT') THEN 'GOVT'
            WHEN (cp.property_type = 'APARTMENTS') THEN 'MTU'
            WHEN (cp.property_type = 'VACANT') THEN 'VAC'
            ELSE 'OTH'
          END address_type,
          cp.address,
          cp.city,
          cp.state,
          NVL(cp.zip, '00000') zip
        FROM (
          SELECT /*+ NO_MERGE */
            crawler_rowid,
            parcel_number,
            polygon_id,
            polygon_name,
            MAX(id) source_id,
            address,
            city,
            state,
            zip,
            property_type
          FROM (
            SELECT x.ROWID crawler_rowid,
              x.parcel_number,
              gp.id polygon_id,
              gp.polygon_name,
              x.id,
              REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
                REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
                RTRIM(RTRIM(UPPER(TRIM(x.address)), ','), '.'),
                ' STREET ST$', ' ST'), ' DRIVE DR$', ' DR'), ' ROAD RD$', ' RD'),
                ' STREET ST ', ' ST '), ' DRIVE DR ', ' DR '), ' ROAD RD ', ' RD ')
              address,
              UPPER(x.city) city,
              UPPER(x.state) state,
              x.zip,
              x.property_type
            FROM dwilson.gis_polygon gp,
              dwilson.crawler_parcels x
            WHERE (address IS NOT NULL OR property_type = 'VACANT')
              AND SDO_RELATE(x.coordinates, gp.coordinates, 'mask=anyinteract querytype=window') = 'TRUE'
          )
          GROUP BY crawler_rowid, parcel_number, polygon_id, polygon_name, address, city, state, zip, property_type
        ) cp
        WHERE cp.polygon_id = :poly
      )
    `);
    console.log(addrq)
    const getCoords = dbh.prepare(`
      SELECT TO_CHAR(sdo_geometry.get_wkt(sdo_aggr_centroid(mdsys.sdoaggrtype(sdo_cs.transform(c.coordinates, 8307), 0.005))))
      FROM dwilson.crawler_parcels c
      WHERE rowid = :rowid
    `);
  
    const insertAddress = gdb.prepare(`
      INSERT INTO sde.address
        (objectid, creationuser, datecreated, addresstext, addresstype, city, state, zipcode,
          ipid, id, threegisid, globalid, shape, address_notes,
          house_number, street_direction, streetaddress, street_suffix, post_dir, unitnumber)
      VALUES
        (sde.gdb_util.next_rowid('SDE', 'Address'), sde.sde_util.sde_user, SYSDATE, UPPER(:new_address), :address_type, UPPER(:city), UPPER(:state), :zip,
          :ipid, :id, :threegisid, :globalid, sdo_cs.transform(sdo_geometry(:geom, 8307), 3857), '(${args.poly})[' || :parcel_number || ']',
          UPPER(:house_nbr), UPPER(:pre_dirx), UPPER(:street_nm), UPPER(:street_type), UPPER(:post_dirx), UPPER(:unit_nbr))
    `);
  
    const insertAudit = gdb.prepare(`
      INSERT INTO sde.audittracking
        (objectid, edittype, layername, username, edit_date, threegisid, globalid)
      VALUES
        (sde.gdb_util.next_rowid('SDE', 'AUDITTRACKING'), 'Create', 'Address', sde.sde_util.sde_user, sysdate, :threegisid, :globalid)
    `);
  
    const checkForAddr = gdb.prepare(`
      SELECT ipid
      FROM sde.address
      WHERE threegisid = :threegisid
    `);
  
    // Execute GET address from DWH query
    await addrq.execute();
  
    // For each address in the address query, get necessary info (coordinates, GUIDs, etc.), parse the address, and insert it into 3GIS (with audit tracking and cleanup)
    while (addrq.fetch()) {
      const rowid = addrq.column("crawler_rowid");
      const parcel_number = addrq.column("parcel_number");
      const source_id = addrq.column("source_id");
      const address_type = addrq.column("address_type");
      const address = addrq.column("address");
      const city = addrq.column("city");
      const state = addrq.column("state");
      const zip = addrq.column("zip");
  
      // Get transformed coordinates for this location
      let geom;
      try {
        getCoords.execute(rowid);
        geom = getCoords.fetch();
        getCoords.finish();
      } catch (error) {
        console.log(`Error for crawler_parcels rowid ${rowid}`);
        continue;
      }
  
      // Get GUIDs from sde.gdb_util
      const ipid = getGlobal();
      const id = getGlobal();
      const threegisid = getGlobal();
      const globalid = getGlobal();
  
      // Trim the brackets away for ipid
      const trimmedIpid = ipid.replace(/[{}]/g, "");
  
      // Process the address: Parse the address and insert it into 3GIS (with audit tracking), deleting any address with the same threegisid that already exists
      console.log(`main: processing address -> ${parcel_number}`);
  
      // Check if this threegisid already exists
      // If so, set ipidEx to its ipid
      let ipidEx;
      try {
        checkForAddr.execute(threegisid);
        ipidEx = checkForAddr.fetch();
        checkForAddr.finish();
      } catch (error) {
        console.log(`Error checking for address with threegisid ${threegisid}`);
      }
  
      // Parse address into variables
      let newAddress = "";
      let houseNbr = "";
      let preDirx = "";
      let streetNm = "";
      let streetType = "";
      let postDirx = "";
      let unitNbr = "";
      parseAddr({
        address,
        return_address: newAddress,
        return_house_nbr: houseNbr,
        return_pre_dirx: preDirx,
        return_street_nm: streetNm,
        return_street_type: streetType,
        return_post_dirx: postDirx,
        return_unit_nbr: unitNbr,
      });
  
      // If the address already existed, delete it and insert the new info (with audit tracking)
      if (ipidEx) {
        deleteAddr({ IPID: ipidEx });
      }
  
        try {
            insertAddress.execute({
            new_address: newAddress,
            address_type: address_type,
            city: city,
            state: state,
            zip: zip,
            ipid: trimmedIpid,
            id: id,
            threegisid: threegisid,
            globalid: globalid,
            geom: geom,
            parcel_number: parcel_number,
            house_nbr: houseNbr,
            pre_dirx: preDirx,
            street_nm: streetNm,
            street_type: streetType,
            post_dirx: postDirx,
            unit_nbr: unitNbr
            });
        } catch (error) {
            console.log("main: Error inserting ${parcel_number}");
            continue;
        }
    }
          // Finish
          addrq.finish();
}
// Call the main function with the necessary arguments







//---------------------//


function usage() {
    const scriptName = process.argv[1];
    console.log(`
    Usage: ${scriptName} [parameters]
              Parameters:
      -debug (optional)
      -gis <3gis database> (gis1/gis2) default gis1
      -poly <polygon id> (required)
    `);
    process.exit(1);
}



