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

const hashOfStreetTypes = {};
streetTypeAbbreviations.forEach(abbreviation => {
  hashOfStreetTypes[abbreviation] = 1;
});

const hashOfReplaceStreetTypes = {
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

const  { dbConnect, executeStatement }  = require('./dbConnectStub'); // Replace with actual module path

//TODO to be taken as input from cmd
let options = {
  db:"dwh1",
  debug: false,
  poly: "1234",
  gis: "gis1"
}

let db = options.db || 'dwh1';
const debug = options.debug || false;
const poly = options.poly || "1234";
let gis = options.gis || 'gis1';

if (!poly) {
  usage();
}


(async () => {
    // Connect to Databases
    const dbh =  await dbConnect(db);
    const gdb = await dbConnect(gis);
    const args = {
        dbh: dbh,
        gdb : gdb,
        poly: '1234'
    };
    main(args);
})();

console.log('Closing LOGFILE');

async function main(args) {
    // Log script name and polygon id
    console.log(`polygon id passed in (${args.poly})`);
  
    // Assign args: $dbh => DWH dbh passed in, $gdb => GIS dbh passed in
    const { dbh, gdb } = args;
  
    // Prepare STHs
    const addrq =`
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
    `;

    const getCoords = `
      SELECT TO_CHAR(sdo_geometry.get_wkt(sdo_aggr_centroid(mdsys.sdoaggrtype(sdo_cs.transform(c.coordinates, 8307), 0.005))))
      FROM dwilson.crawler_parcels c
      WHERE rowid = :rowid
    `;
    /*Insert address in GIS:
        #objectid as next rowID from gdb_util for SDE ADDRESS, creationuser as sde_user, datecreated as sysdate, addresstext as parameter forced to caps,
        #addresstype as parameter, city parameter forced to caps, state parameter forced to caps, zipcode parameter,  
        # ipid paramtert, id, parameter, threegisid parameter, globalid, paramteter, parameter transformed into SDE.GEOMETRY, address_notes $poly argument and another parameter put in square brackets
        #house_number as parameter forced to caps, street_direction as parameter forced to caps, streetaddress as parameter forced to caps, street_suffix as parameter forced to caps, post_dir as parameter forced to caps, unit_number as parameter forced to caps
      */
    const insertAddress = `
      INSERT INTO sde.address
        (objectid, creationuser, datecreated, addresstext, addresstype, city, state, zipcode,
          ipid, id, threegisid, globalid, shape, address_notes,
          house_number, street_direction, streetaddress, street_suffix, post_dir, unitnumber)
      VALUES
        (sde.gdb_util.next_rowid('SDE', 'Address'), sde.sde_util.sde_user, SYSDATE, UPPER(:new_address), :address_type, UPPER(:city), UPPER(:state), :zip,
          :ipid, :id, :threegisid, :globalid, sdo_cs.transform(sdo_geometry(:geom, 8307), 3857), '(${args.poly})[' || :parcel_number || ']',
          UPPER(:house_nbr), UPPER(:pre_dirx), UPPER(:street_nm), UPPER(:street_type), UPPER(:post_dirx), UPPER(:unit_nbr))
    `;

    /*Insert address info into audit tracking
    object_id as next rowID from gdb_util for SDE ADDRESS, 'Create' as edittype, 'Address' layername, sde_user as username, sysdate as edit_date, paramter as threegisid, paramter as globalid
    */
    const insertAudit = `
      INSERT INTO sde.audittracking
        (objectid, edittype, layername, username, edit_date, threegisid, globalid)
      VALUES
        (sde.gdb_util.next_rowid('SDE', 'AUDITTRACKING'), 'Create', 'Address', sde.sde_util.sde_user, sysdate, :threegisid, :globalid)
    `;
  
    const checkForAddr = `
      SELECT ipid
      FROM sde.address
      WHERE threegisid = :threegisid
    `;
    // let dummysql = `SELECT ROWID as crawler_rowid, parcel_number, id as source_id, address, city, zip, state, property_type as address_type  FROM crawler_parcels1`
  
    // Execute GET address from DWH query
    let addrRes = await executeStatement(dbh, addrq, { poly: args.poly});

    // For each address in the address query, get necessary info (coordinates, GUIDs, etc.), parse the address, and insert it into 3GIS (with audit tracking and cleanup)
    for (const row of addrRes.rows) {
      const rowid = row["CRAWLER_ROWID"];
      const parcel_number = row["PARCEL_NUMBER"];
      const source_id = row["SOURCE_ID"];
      const address_type = row["ADDRESS_TYPE"];
      const address = row["ADDRESS"];
      const city = row["CITY"];
      const state = row["STATE"];
      const zip = row["ZIP"];
  
      // Get transformed coordinates for this location
      let geom;
      try {
        geom = await executeStatement(dbh, getCoords, { rowid: rowid});
      } catch (error) {
        console.log(`Error for crawler_parcels rowid ${rowid}`);
        continue;
      }
  
      // Get GUIDs from sde.gdb_util
      const ipid = await getGlobal(dbh);
      const id = await getGlobal(dbh);
      const threegisid = await getGlobal(dbh);
      const globalid = await getGlobal(dbh);
  
      // Trim the brackets away for ipid
      const trimmedIpid = ipid.replace(/[{}]/g, "");
  
      // Process the address: Parse the address and insert it into 3GIS (with audit tracking), deleting any address with the same threegisid that already exists
      console.log(`main: processing address -> ${parcel_number}`);
  
      // Check if this threegisid already exists
      // If so, set ipidEx to its ipid
      let ipidEx;
      try {
        ipidEx = await executeStatement(gdb, checkForAddr, { threegisid: threegisid});
        ipidEx = ipidEx.rows[0].ipid;
      } catch (error) {
        console.log(`Error checking for address with threegisid ${threegisid}`);
      }
  
      // Parse address into variables
      let addressInfo = {
        address,
        returnAddress: "",
        returnHouseNbr: "",
        returnPreDirx: "",
        returnStreetNm: "",
        returnStreetType: "",
        returnPostDirx: "",
        returnUnitNbr: "",
      }
      parseAddr(addressInfo);
  
      // If the address already existed, delete it and insert the new info (with audit tracking)
      if (ipidEx) {
        deleteAddr(gdb, { IPID: ipidEx });
      }
  
      try {
          await  executeStatement( gdb, insertAddress,{
              new_address: addressInfo.returnAddress,
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
              house_nbr: addressInfo.returnHouseNbr,
              pre_dirx: addressInfo.returnPreDirx,
              street_nm: addressInfo.returnStreetNm,
              street_type: addressInfo.returnStreetType,
              post_dirx: addressInfo.returnPostDirx,
              unit_nbr: addressInfo.returnUnitNbr
          });

          await  executeStatement( gdb, insertAudit,[threegisid, globalid]);
          console.log("address inserted");
          process.exit(1)
      } catch (error) {
          console.log("main: Error inserting ${parcel_number}");
          continue;
      }
    }
}
// Call the main function with the necessary arguments

// TODO CHECK
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


async function getGlobal(connection) {
  
    const sql = `
        SELECT next_globalid.NEXTVAL FROM dual
    `;
  
    const result =  await executeStatement(connection, sql, {}); // Execute the query
  
    const gid = result.rows[0][0]; // Get the value from the first row and first column

    return gid;
}

async function deleteAddr(connection, ipid) {
    console.log(`delete_addr: executing for ${ipid}`);
  
    const deletePoleSQL = `
      DELETE FROM sde.address WHERE ipid = :ipid
    `;
  
    const deleteAuditSQL = `
      INSERT INTO sde.audittracking
        (objectid, edittype, layername, username, edit_date, threegisid, globalid)
      SELECT sde.gdb_util.next_rowid('SDE', 'AUDITTRACKING'), 'Delete', 'Address', sde.sde_util.sde_user, sysdate, threegisid, globalid
      FROM sde.address
      WHERE ipid = :ipid
    `;
  
    const deletePoleBind = [ipid];
    const deleteAuditBind = [ipid];
    try {
        const deletePoleResult = await  executeStatement(connection, deletePoleSQL, deletePoleBind);
        const deleteAuditResult = await executeStatement(connection, deleteAuditSQL, deleteAuditBind);
    } catch (error) {
        throw error;
    }
}


function trimPeriod(s) {
    return s.replace(/^\.+|\.+$/g, '');
}
  
function parseAddr(addrInfo) {
let { address, returnAddress, returnHouseNbr, returnPreDirx, returnStreetNm, returnStreetType, returnPostDirx, returnUnitNbr } = addrInfo;

// Initialize local variables
let splitAddress = [];
let newAddress = '';
let houseNbr = '';
let preDirx = '';
let streetNm = '';
let streetType = '';
let postDirx = '';
let altUnitNbr = '';
let unitNbr = '';
let startUnitNbrIndex = 0;
let possibleEndIndex = -1;


// Trim spaces from address
  address = address.replace(/^\s+|\s+$/g, '');
  address = address.replace(/\s+/g, ' ');

// Extract House Number into house_nbr with rest of string in address
  const houseNumberPattern = /^(([0-9]{1,}[0-9A-Z]{0,})( {0,}(&|-|,|\/) {0,}([0-9]{1,}[0-9A-Z]{0,})){0,}( 1\/2){0,1})/;
  const houseNumberMatch = address.match(houseNumberPattern);

  if (houseNumberMatch) {
    const houseNumber = houseNumberMatch[1];
    address = address.substring(houseNumber.length + 1);
  }
// Split Address Into Pieces on every space, comma, or tab
  address = address.trim(); // parse out leading and trailing spaces
  address = address.replace(/\s+/g, ' '); // remove duplicate spaces
  splitAddress = address.split(/[ ,\t]/);

// Check For Alternate Unit Number, ensuring it isn't ('N', 'S', 'W', 'E') and store it in altUnitNbr
// Accepted formats: (<> means optional; each A-Z and '#' are only matched once)
// <#>(<0-9><A-Z><0-9>)<(-|/ <#>(<0-9><A-Z><0-9>))
// Examples:
// #123A123
// #123123
// 123A123
// A123
// 123-123
// 123/123
// 123-#123
// 123/#123

  const altUnitNbrRegex = /^((#)?[0-9]*[A-Z]?[0-9]*(?:[-\/](#)?[0-9]*[A-Z]?[0-9]*)*)$/;
  if (altUnitNbrRegex.test(splitAddress[0])) {
    // Make Sure Not Possible To Be Directional
    if (
      !splitAddress[0].match(/^(N|S|E|W)$/) ||
      (splitAddress[1] && splitAddress[1].match(/^(N|S|E|W|NORTH|SOUTH|EAST|WEST|NORT|SOUT|NE|NW|SE|SW)(\.){0,}$/))
    ){
      altUnitNbr = splitAddress.shift();
    }
  }

// Add pre-directional (with or without period or abbreviation) if it exists to preDirx
  if (splitAddress[0].match(/^(N|S|E|W|NORTH|SOUTH|EAST|WEST|NORT|SOUT|NE|NW|SE|SW)(\.){0,}$/)) {
    preDirx = splitAddress.shift();
  }

// Cut out unit number and after from splitAddress
// Loop through split address to find index of everything before unit number
  for (const index in splitAddress) {
    // If this piece is a unit number, end this loop
    if (splitAddress[index].match(/^(APT|LOT|UNIT|STE|SUITE|UNITS|SUITES)$/) || splitAddress[index].match(/^#/)) {
      break;
    }

    // If this piece (ignoring leading/trailing periods) is in the street type hash,
    // mark possibleEndIndex as this index
    if (hashOfStreetTypes.hasOwnProperty(trimPeriod(splitAddress[index]))) {
      possibleEndIndex = Number(index);
    }

    // If this piece is a post-directional (N,S,E,W or some abbreviation thereof),
    // mark possibleEndIndex as this index
    if (/^(N|S|E|W|NORTH|SOUTH|EAST|WEST|NORT|SOUT|NE|NW|SE|SW)(\.){0,}$/.test(splitAddress[index])) {
      possibleEndIndex = Number(index);
    }

    // Set startUnitNbrIndex value = this index + 1
    startUnitNbrIndex = Number(index) + 1;
  }

  // If startUnitNbrIndex is at the end of the array of pieces and possibleEndIndex was changed,
  // reset startUnitNbrIndex to possibleEndIndex + 1
  if (splitAddress.length === startUnitNbrIndex && possibleEndIndex >= 0) {
    startUnitNbrIndex = possibleEndIndex + 1;
  }
  // Set unitNbr to everything in splitAddress at or after startUnitNbrIndex,
  // joined by spaces and parsing out any leading/trailing '.', '-', ' 's
  let tempAddress = splitAddress.slice(startUnitNbrIndex);
  unitNbr = tempAddress.join(' ');

  // Clean Preceding Garbage Of Unit Number
  unitNbr = unitNbr.replace(/^(\.|-| )+|(\.|-| )+$/g, '');
  unitNbr = unitNbr.replace(/\s+/g, ' ');


  // Cut unit number piece from splitAddress
  if ((splitAddress.length - startUnitNbrIndex) > 0) {
    splitAddress.splice(-(splitAddress.length - startUnitNbrIndex));
    // splitAddress.splice(startUnitNbrIndex, splitAddress.length - startUnitNbrIndex);
  }



// Reverse iterate through what's left in splitAddress and remove any '-' or '.' that exist at the end
  let size = splitAddress.length - 1;
  for (let revindex = 0; revindex <= size; revindex++) {
    // Convert reverse index to index
    let index = size - revindex;

    if (/^(-|\.)+$/.test(splitAddress[index])) {
      splitAddress.pop();
    } else {
      break;
    }
  }

  // If the last value in splitAddress is post-directional (N,S,E,W or an abbreviation), set postDirx to it and pop it off
  if (splitAddress.length > 0) {
    let lastValue = splitAddress[splitAddress.length - 1];
    if (/^(N|S|E|W|NORTH|SOUTH|EAST|WEST|NORT|SOUT|NE|NW|SE|SW)(\.)*$/.test(lastValue)) {
      postDirx = splitAddress.pop();
    }
  }

  // If the last item in splitAddress (with periods trimmed) is in hashOfStreetTypes,
  // replace it with hashOfReplaceStreetTypes if it exists and set streetType to the value
  if (splitAddress.length > 0) {
    let lastItem = trimPeriod(splitAddress[splitAddress.length - 1].trim()) // Trim trailing period
    if (hashOfStreetTypes.hasOwnProperty(lastItem)) {
      streetType = lastItem;

      // Check for Replacement Street Type
      if (hashOfReplaceStreetTypes.hasOwnProperty(streetType)) {
        streetType = hashOfReplaceStreetTypes[streetType];
      }
    }
  }

  // Join the remaining array elements and parse out all leading/trailing spaces (and replacing double spaces with single spaces)
  // and set streetNm to the result
  streetNm = splitAddress.join(' ');
  streetNm = streetNm.replace(/^\s+|\s+$/g, ''); // Trim leading and trailing spaces
  streetNm = streetNm.replace(/\s+/g, ' '); // Replace multiple spaces with a single space

  // If we don't have a street number, use either pre- or post-directional for streetNm
  if (!streetNm) {
    // Check Pre Directional
    if (preDirx.match(/^(NORTH|SOUTH|EAST|WEST)$/)) {
      // Use Full Pre Directional Value As Street Name
      streetNm = preDirx;
      preDirx = '';
    } else if (postDirx.match(/^(NORTH|SOUTH|EAST|WEST)$/) && !streetType) {
      // Use Full Post Directional Value As Street Name
      streetNm = postDirx;
      postDirx = '';
    }
  }

  // If we don't have a pre-dirx but we do have altUnitNbr, use it for preDirx
  // (and delete the value from altUnitNbr) parsing it to only the first character
  if (!preDirx && altUnitNbr.match(/^(N|S|E|W)$/)) {
    // Set Pre Directional Value As Alternate Unit Nbr
    preDirx = altUnitNbr;
    altUnitNbr = '';
  }

  // Only Take First Character For North, South, East, West For Pre Directional
  if (preDirx.match(/^(N|S|E|W|NORTH|SOUTH|EAST|WEST|NORT|SOUT)(\.){0,}$/)) {
    preDirx = preDirx.substring(0, 1);
  }

  // Only Take First Character For North, South, East, West For Post Directional
  if (postDirx.match(/^(N|S|E|W|NORTH|SOUTH|EAST|WEST|NORT|SOUT)(\.){0,}$/)) {
    postDirx = postDirx.substring(0, 1);
  }

/*  If Alternate Unit Number and Unit Number Exist, 
      # if pre-directional exits, add $alt_unit_nbr to $house_nbr,
      # else add it to $street_nm
    Else if it's just one letter and $house_number doesn't have '&', '-', ',', '/', ' ', or a letter in it already,
      #add $alt_unit_nbr to $house_nbr
      #else add it to unit_nbr
    and reset $alt_unit_nbr
*/
  if (altUnitNbr) {
    if (unitNbr) {
      if (preDirx) {
        // Pre Directional Exists So Add To House Number
        houseNbr = houseNbr + ' ' + altUnitNbr;
      } else {
        // Pre Directional Does Not Exist So Add To Street Name
        streetNm = streetNm + ' ' + altUnitNbr;
      }
    } else {
      if (altUnitNbr.match(/^[A-Z]{1}$/) && !houseNbr.match(/(&|-|,|\/| |[A-Z])/)) {
        // Add To House Number
        houseNbr = houseNbr + altUnitNbr;
      } else {
        // Set Unit Number As Alternate Unit Number
        unitNbr = altUnitNbr;
      }
    }
    altUnitNbr = '';
  }
  
  // Build New Address as $house_nbr + $pre_dirx + $street_nm + $street_type + $post_dirx + $unit_nbr (joined with spaces)
  let newAddressArray = [houseNbr, preDirx, streetNm, streetType, postDirx, unitNbr];
  newAddress = newAddressArray.join(' ');

  // Trim leading/trailing/extra spaces
  newAddress = newAddress.replace(/^\s+|\s+$/g, ''); // Trim leading/trailing spaces
  newAddress = newAddress.replace(/\s+/g, ' '); // Replace multiple spaces with single space


  // Assign values to the objects  
    addrInfo.returnAddress = newAddress
    addrInfo.returnHouseNbr = houseNbr,
    addrInfo.returnPreDirx = preDirx;
    addrInfo.returnStreetNm = streetNm;
    addrInfo.returnStreetType = streetType;
    addrInfo.returnPostDirx = postDirx;
    addrInfo.returnUnitNbr = unitNbr;
}


