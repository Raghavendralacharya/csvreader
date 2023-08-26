#!/usr/bin/perl
# Add use statements (DB and Dumper and CLI options)
  use strict;
  use Getopt::Long;
  use lib "/local/db/lib";
  use lib "/mis/lib";
  use db_std;
  use Data::Dumper;
#

#Initialize log with name 'gis_address_load.logfile.txt'
  my $myname = $0;
  $myname =~ s#.*/##;
  my $logfile = $myname . ".logfile.txt";
  open (LOGFILE, ">$logfile");
#

#Initialize Street type mapping cache 
  my %hashofstreettypes = map { $_ => 1 } ('ALLEY', 'ALY', 'AV', 'AVE', 'AVENUE', 'BEND', 'BLV', 'BLVD', 'BND', 'CIRCLE', 'CIR', 'CR',
                                          'CRES', 'CRESCENT', 'CRK', 'CT', 'CRT', 'COURT', 'CTR', 'CV', 'DR', 'DRIVE','EXT',
                                          'FLS', 'FORD', 'FRD', 'GRN', 'HILL', 'HILLS', 'HL', 'HLS', 'HS', 'HTS', 'HVN',
                                          'HWY', 'LN', 'LNDG', 'LANDING', 'MDWS', 'MHP', 'MNR', 'PARK', 'PASS', 'PIKE',
                                          'PKWY', 'PL', 'PLZ', 'RD', 'RDG', 'ROAD', 'ROW', 'RUN', 'SPGS',
                                          'SQ', 'ST', 'STA', 'STREET', 'TER', 'TR', 'TRCE', 'TRL', 'WAY',
                                          'XING', 'CROSSING', 'SQUARE', 'OVERLOOK', 'LOOP', 'RIDGE', 'PARKWAY', 'PTE',
                                          'PKY', 'POINT', 'PT', 'WY', 'BL', 'COVE', 'TRAIL', 'LANE', 'DRV');

  my %hashofreplacestreettypes = ('AV' => 'AVE',
                                  'AVENUE' => 'AVE',
                                  'BL' => 'BLVD',
                                  'BLV' => 'BLVD',
                                  'COURT' => 'CT',
                                  'COVE' => 'CV',
                                  'CR' => 'CIR',
                                  'CRT' => 'CT',
                                  'CROSSING' => 'XING',
                                  'DRIVE' => 'DR',
                                  'DRV' => 'DR',
                                  'HILL' => 'HL',
                                  'LANDING' => 'LNDG',
                                  'LANE' => 'LN',
                                  'PARKWAY' => 'PKWY',
                                  'PKY' => 'PKWY',
                                  'POINT' => 'PT',
                                  'RIDGE' => 'RDG',
                                  'ROAD' => 'RD',
                                  'SQUARE' => 'SQ',
                                  'STREET' => 'ST',
                                  'TR' => 'TRL',
                                  'TRAIL' => 'TRL',
                                  'WY' => 'WAY');
#

#Get Parameters from CLI
  #db -> DWH instance (defaults to 'dwh1')
  #debug -> debug flag (defaults to false)
  #poly -> polygon ID (required) -- used to fetch addresses from dwilson<objs> TODO: understand
  #gis -> GIS instance (defaults to 'gis1')
#
  my ($db, $debug, $poly, $gis, $job);
  GetOptions(
    'db=s' => \$db,
    'debug' =>\$debug,
    'poly=s'=>\$poly,
    'gis=s'=>\$gis,
  ) or usage();
  $db = 'dwh1' unless($db);
  $gis = 'gis1' unless($gis);
  usage() unless($poly);
#

#Connect To Databases
  my $dbh = db_connect($db);
  my $gdb = db_connect($gis);
#

#Run main function
  main('dbh'=>$dbh, 'gdb'=>$gdb);
#

#Disconnect databases and close logs, then exit
  $dbh->disconnect();
  $gdb->disconnect();

  close LOGFILE;
  exit;
#

#-----------------------------------------
#main definition
#-----------------------------------------
  #Parameters:
    # 'dbh' -> DHW DBH instance
    # 'gdb' -> GIS DBH instance
  #Function:
    #Parses all addresses for the $poly global provided to the script via CLI and inserts them into 3GIS
  #Returns: none
#-----------------------------------------
  sub main
  {
    #Log script name and polygon id
      plog("Starting $0, polygon id passed in($poly)");
    #

    #assign args: 
      # $dbh => DWH dbh passed in
      # $gdb => GIS dbh passed in
    #
      my %args = @_;
      my $dbh = $args{'dbh'};
      my $gdb = $args{'gdb'};
    #

    #prepare STHs
      #Get crawler_rowid, parcel_number, source_id, address_type, address, address, city, state, zip
      #from subquery: (
          #Get crawler_rowID, parcel_number, source_id, address_type (based on property_type), address, city, state, zip (with default '00000')
          #from subquery: (
              #Get crawler_rowID, parcel_number, polygon_id, polygon_name, max(id) as source_id, address, city, state, zip, property_type
              #from subquery: (
                  #Get crawler_rowID, parcel_number, id (unique), formatted address, city, state, zip, and property type from crawler_parcels
                  # along with polygon_id (not unique), polygon_name from gis_polygon
                  # joined on coordinates (MDSYS.SDO_GEOMETRY)
              #)
              # grouped by crawler_rowid, parcel_number, polygon_id, polygon_name, address, city, state, zip, property_type
          #)
          # where polygon_id = variable passed into query
      #)
        my $addrq = $dbh->prepare("
          SELECT  crawler_rowid,
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
                      SELECT  x.ROWID crawler_rowid,
                              x.parcel_number,
                              gp.id polygon_id,
                              gp.polygon_name,
                              x.id,
                              REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
                                  REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
                                  RTRIM(RTRIM(UPPER(TRIM(x.address)), ','), '.'),
                                  ' STREET ST\$', ' ST'), ' DRIVE DR\$', ' DR'), ' ROAD RD\$', ' RD'),
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
              WHERE cp.polygon_id = '$poly'
          )
        ");
      #

      #Get a specific row's coordinates transformed from SDO_GEOMETRY to WGS84 to SDOAGGRTYPE to centroid to WKT to character
        my $get_coords = $dbh->prepare("
          SELECT TO_CHAR(sdo_geometry.get_wkt(sdo_aggr_centroid(mdsys.sdoaggrtype(sdo_cs.transform(c.coordinates, 8307), 0.005))))
          FROM dwilson.crawler_parcels c
          WHERE rowid = ?
        ");
      #

      #Insert address in GIS:
        #objectid as next rowID from gdb_util for SDE ADDRESS, creationuser as sde_user, datecreated as sysdate, addresstext as parameter forced to caps,
        #addresstype as parameter, city parameter forced to caps, state parameter forced to caps, zipcode parameter,  
        # ipid paramtert, id, parameter, threegisid parameter, globalid, paramteter, parameter transformed into SDE.GEOMETRY, address_notes $poly argument and another parameter put in square brackets
        #house_number as parameter forced to caps, street_direction as parameter forced to caps, streetaddress as parameter forced to caps, street_suffix as parameter forced to caps, post_dir as parameter forced to caps, unit_number as parameter forced to caps
      #
        my $insert_address = $gdb->prepare("
          INSERT INTO sde.address
            (objectid, creationuser, datecreated, addresstext, addresstype, city, state, zipcode,
              ipid, id, threegisid, globalid, shape, address_notes,
              house_number, street_direction, streetaddress, street_suffix, post_dir, unitnumber)
          VALUES
            (sde.gdb_util.next_rowid('SDE', 'Address'), sde.sde_util.sde_user, SYSDATE, UPPER(?), ?, UPPER(?), UPPER(?), ?,
              ?, ?, ?, ?, sdo_cs.transform(sdo_geometry(?, 8307), 3857), '($poly)[' || ? || ']',
              UPPER(?), UPPER(?), UPPER(?), UPPER(?), UPPER(?), UPPER(?))
        ");
      #

      #Insert address info into audit tracking
        #object_id as next rowID from gdb_util for SDE ADDRESS, 'Create' as edittype, 'Address' layername, sde_user as username, sysdate as edit_date, paramter as threegisid, paramter as globalid
      #
        my $insert_audit = $gdb->prepare("
          INSERT INTO sde.audittracking
          (objectid, edittype, layername, username, edit_date, threegisid, globalid)
          VALUES
          (sde.gdb_util.next_rowid('SDE', 'AUDITTRACKING'), 'Create', 'Address', sde.sde_util.sde_user, sysdate, ?, ?)
        ");
      #

      #get ipid for a threegisid passed in as paramter
        my $check_for_addr = $gdb->prepare("
          SELECT ipid
          FROM sde.address
          WHERE threegisid = ?
        ");
      #

    #

    #Execute GET address from DWH query
    $addrq->execute();

    #Foreach address in the address query,
    #get necessary info (coordinates, GUIDs, etc.), parse the address, and insert it into 3GIS (with audit tracking and cleanup)
    #
      while(my($rowid, $parcel_number, $source_id, $address_type, $address, $city, $state, $zip) = $addrq->fetchrow())
      {
        # get transformed coordinates for this location
          my $geom;
          eval
          {
            $get_coords->execute($rowid);
            $geom = $get_coords->fetchrow();
            $get_coords->finish();
          };
          if($@)
          {
            plog("error for crawler_parcels rowid $rowid");
            next;
          }
        #

        # Get GUIDS from sde.gdb_util
          my ($ipid, $id, $threegisid, $globalid);
          $ipid = get_global();
          $id = get_global();
          $threegisid = get_global();
          $globalid = get_global();
        #

        #trim the brackets away for ipid
          $ipid =~ tr/{}//d;
        #

        #Process the address: 
          #Parse an address 
          # and insert it into 3GIS (with audit tracking), 
          # deleting any address with the same threegisid that exists already
        #
          plog("main: processing address->$parcel_number");

          #check if this threegisid already exists
          #if so, set $ipid_ex to its ipid
            $check_for_addr->execute($threegisid);
            my ($ipid_ex) = $check_for_addr->fetchrow();
            $check_for_addr->finish();
          #

          #Parse address into $new_address, $house_nbr, $pre_dirx, $street_nm, $street_type, $post_dirx, $unit_nbr
            my $new_address = "";
            my $house_nbr = "";
            my $pre_dirx = "";
            my $street_nm = "";
            my $street_type = "";
            my $post_dirx = "";
            my $unit_nbr = "";
            parse_addr(
              'address' => $address,
              'return_address' => \$new_address,
              'return_house_nbr' => \$house_nbr,
              'return_pre_dirx' => \$pre_dirx,
              'return_street_nm' => \$street_nm,
              'return_street_type' => \$street_type,
              'return_post_dirx' => \$post_dirx,
              'return_unit_nbr' => \$unit_nbr
            );
          #

          #If the address already existed, delete it and insert the new info (with audit tracking)
            delete_addr('IPID'=>$ipid_ex) if ($ipid_ex);
            eval
            {
              $insert_address->execute($new_address, $address_type, $city, $state, $zip, $ipid, $id, $threegisid, $globalid, $geom, $parcel_number, $house_nbr, $pre_dirx, $street_nm, $street_type, $post_dirx, $unit_nbr);
              $insert_address->finish();

              $insert_audit->execute($threegisid, $globalid);
              $insert_audit->finish();

              plog("insert: original_address->$address, new_address->$new_address, house_nbr->$house_nbr, pre_dirx->$pre_dirx, street_nm->$street_nm, street_type->$street_type, post_dirx->$post_dirx, unit_nbr->$unit_nbr");
            };
            if ($@)
            {
              plog("main: Error inserting $parcel_number");
              next;
            }
          #
        #

      }
    #

    #finish sth and loop
      $addrq->finish();

      return;
    #
  }
#

#-----------------------------------------
#delete_addr definition
#-----------------------------------------
  #Parameters:
    # IPID -> ipid of address to delete
  #Function:
    # Deletes address from SDE.ADDRESS in GIS based on input IPID. Also logs in LOGFILE and in SDE.AUDITTRACKING
  #Returns: none
#-----------------------------------------
  sub delete_addr
  {
    my %args = @_;
    my $ipid = $args{'IPID'};

    plog("delete_addr: executing for $ipid");

    my $delete_pole = $gdb->prepare("
  DELETE FROM sde.address where ipid = ?
  ");

    my $delete_audit = $gdb->prepare("
  INSERT INTO sde.audittracking
    (objectid, edittype, layername, username, edit_date, threegisid, globalid)
  SELECT sde.gdb_util.next_rowid('SDE', 'AUDITTRACKING'), 'Delete', 'Address', sde.sde_util.sde_user, sysdate, threegisid, globalid
    FROM sde.address
    WHERE ipid = ?
  ");

    $delete_pole->execute($ipid);
    $delete_pole->finish();

    $delete_audit->execute($ipid);
    $delete_audit->finish();

  }
#

#-----------------------------------------
#get_global definition
#-----------------------------------------
  #Parameters:
    # none
  #Function:
    #uses GIS DB connection to get next globalid from sde.gdb_util
  #Returns: 
    #$gid -> GUID from GIS DB
#-----------------------------------------
  sub get_global
  {
    my $sql = $gdb->prepare("
      select sde.gdb_util.next_globalid from dual
    ");

    $sql->execute();
    my ($gid) = $sql->fetchrow();
    $sql->finish();

    return $gid;
  }
#

#-----------------------------------------
#parse_addr definition
#-----------------------------------------
  #Parameters:
    # address -> address string to parse
    # return_address -> return variable (in and out)
    # return_house_nbr -> return variable (in and out)
    # return_pre_dirx -> return variable (in and out)
    # return_street_nm -> return variable (in and out)
    # return_street_type -> return variable (in and out)
    # return_post_dirx -> return variable (in and out)
    # return_unit_nbr -> return variable (in and out)
  #Function:
    # parses out $address into its pieces and assigns them to input reference variables
  #Returns: 
    # Nothing (technically) but sets reference variables:
      # $$return_address_ref = $house_nbr + $pre_dirx + $street_nm + $street_type + $post_dirx + $unit_nbr (joined with spaces); #all found from input address
      # $$return_house_nbr_ref = $house_nbr;
      # $$return_pre_dirx_ref = $pre_dirx;
      # $$return_street_nm_ref = $street_nm;
      # $$return_street_type_ref = $street_type;
      # $$return_post_dirx_ref = $post_dirx;
      # $$return_unit_nbr_ref = $unit_nbr;
#-----------------------------------------
  sub parse_addr
  {
    #-----------------------------------------
    #trim_period definition
    #-----------------------------------------
      #Parameters:
        # $_ -> unnamed string to trim
      #Function:
        # deletes any number of periods at the beginning or end of a string
      #Returns: 
        # $s -> trimmed string
    #-----------------------------------------
      sub trim_period
      {
        my $s = $_[0];
        $s =~ s/^\.+|\.+$//g;
        return $s;
      }
    #

    #Initialize args into vars
      my %arg = @_;
      my $address = $arg{'address'};
      my $return_address_ref = $arg{'return_address'};
      my $return_house_nbr_ref = $arg{'return_house_nbr'};
      my $return_pre_dirx_ref = $arg{'return_pre_dirx'};
      my $return_street_nm_ref = $arg{'return_street_nm'};
      my $return_street_type_ref = $arg{'return_street_type'};
      my $return_post_dirx_ref = $arg{'return_post_dirx'};
      my $return_unit_nbr_ref = $arg{'return_unit_nbr'};
    #

    #Initialize local Variables
      #@split_address -> array
      #$new_address -> empty string
      #$house_nbr -> empty string
      #$pre_dirx -> empty string
      #$street_nm -> empty string
      #$street_type -> empty string
      #$post_dirx -> empty string
      #$alt_unit_nbr -> empty string
      #$unit_nbr -> empty string
      #$start_unit_nbr_index -> 0
      #$possible_end_index -> -1
    #
      my @split_address;
      my $new_address = "";
      my $house_nbr = "";
      my $pre_dirx = "";
      my $street_nm = "";
      my $street_type = "";
      my $post_dirx = "";
      my $alt_unit_nbr = "";
      my $unit_nbr = "";
      my $start_unit_nbr_index = 0;
      my $possible_end_index = -1;
    #


    #Trim Spaces From Address
      $address =~ s/^\s+//g;
      $address =~ s/\s+$//g;
      $address =~ s/\s+/ /g;
    #

    #Extract House Number into $house_nbr with rest of string in $address
      #accepted formats: (maybe others, not sure)
        # 1234 test st
        # 12/5 test st
        # 12&3 test st
        # 12,3 test st
        # 1 1/2 test st
    #
      if ($address =~ /^(([0-9]{1,}[0-9A-Z]{0,})( {0,}(&|-|,|\/) {0,}([0-9]{1,}[0-9A-Z]{0,})){0,}( 1\/2){0,1}) /) {
        #Extract House Number Pattern
        $house_nbr = $1;
        $address = substr($address, length($house_nbr) + 1);
      }
    #

    #Split Address Into Pieces on every space, comma, or tab
      $address =~ s/^\s+//g; #parse out leading spaces
      $address =~ s/\s+$//g; #parse out trailing spaces
      $address =~ s/\s+/ /g; #remove duplicate spaces
      @split_address = split(/ |,|\t/, $address);
    #
    

    #Check For Alternate Unit Number, ensuring it isn't ('N', 'S', 'W', 'E') and store it in $alt_unit_nbr
      #Accepted formats: (<> means optional; each A-Z and '#' are only matched once)
        # <#>(<0-9><A-Z><0-9>)<(-|/ <#>(<0-9><A-Z><0-9>))
      #Examples
        # #123A123
        # #123123
        # 123A123
        # A123
        # 123-123
        # 123/123
        # 123-#123
        # 123/#123
      #
    #
      if (@split_address[0] =~ /^((#){0,1}[0-9]{0,}[A-Z]{0,1}[0-9]{0,}((-|\/)(#){0,1}[0-9]{0,}[A-Z]{0,1}[0-9]{0,}){0,})$/) {
        #Make Sure Not Possible To Be Directional
        if ((@split_address[0] !~ /^(N|S|E|W)$/) || (@split_address[1] =~ /^(N|S|E|W|NORTH|SOUTH|EAST|WEST|NORT|SOUT|NE|NW|SE|SW)(\.){0,}$/)) {
          $alt_unit_nbr = shift(@split_address);
        }
      }
    #

    #Add pre-directional (with or without period or abbreviation) if it exists to $pre_dirx
      if (@split_address[0] =~ /^(N|S|E|W|NORTH|SOUTH|EAST|WEST|NORT|SOUT|NE|NW|SE|SW)(\.){0,}$/) {
        $pre_dirx = shift(@split_address);
      }
    #

    #Cut out unit number and after from @split_address
      #Loop Through split address to find index of everything before unit number
      for my $index (0..(scalar(@split_address) - 1)) {
        #If this piece is unit number, end this loop
          if ((@split_address[$index] =~ /^(APT|LOT|UNIT|STE|SUITE|UNITS|SUITES)$/) || (@split_address[$index] =~ /^#/)) {
            last;
          }
        #

        #If this piece (ignoring leading/trailing periods) is in the street type hash, mark $possible_end_index as this index
          if (exists($hashofstreettypes{trim_period(@split_address[$index])})) {
            $possible_end_index = $index;
          }
        #

        #if this piece is a post-directional (N,S,E,W or some abbreviation thereof), mark $possible_end_index as this index
          if (@split_address[$index] =~ /^(N|S|E|W|NORTH|SOUTH|EAST|WEST|NORT|SOUT|NE|NW|SE|SW)(\.){0,}$/) {
            $possible_end_index = $index;
          }
        #

        #Set Start Unit Index Value = this index + 1
          $start_unit_nbr_index = $index + 1;
        #
      }

      #if $start_unit_nbr_index is at the end of the array of pieces and $possible_end_index was changed, 
      #reset $start_unit_nbr_index to $possible_end_index + 1
        if ((scalar(@split_address) == $start_unit_nbr_index) && ($possible_end_index >= 0)) {
          $start_unit_nbr_index = $possible_end_index + 1;
        }
      #

      #Set $unit_nbr to everything in @split_address at or after $start_unit_nbr_index,
      #joined by spaces and parsing out any leading/trailing '.', '-', ' 's
        my @tempaddress = @split_address;
        splice(@tempaddress, 0, $start_unit_nbr_index);
        $unit_nbr = join(' ', @tempaddress);
        #Clean Preceding Garbage Of Unit Number
        $unit_nbr =~ s/^(\.|-| )+|(\.|-| )+$//g;
        $unit_nbr =~ s/\s+/ /g;
      #
    #

      #Cut unit number piece from @split_address
        if ((scalar(@split_address) - $start_unit_nbr_index) > 0) {
          splice(@split_address, -(scalar(@split_address) - $start_unit_nbr_index));
        }
      #

    #Reverse iterate through what's left in @split_address and remove any of '-', '.' that exist at the end
      my $size = scalar(@split_address) - 1;
      for my $revindex (0..$size) {
        #Convert Reverse Index To Index
        my $index = $size - $revindex;

        if (@split_address[$index] =~ /^(-|\.){1,}$/) {
          pop(@split_address);
        } else {
          last;
        }
      }
    #

    #If the last value in @split_address is post-directional (N,S,E,W or an abbreviation), set $post_dirx to it and pop it off
      if (scalar(@split_address) > 0) {
        if (@split_address[(scalar(@split_address) - 1)] =~ /^(N|S|E|W|NORTH|SOUTH|EAST|WEST|NORT|SOUT|NE|NW|SE|SW)(\.){0,}$/) {
          $post_dirx = pop(@split_address);
        }
      }
    #

    #If the last item in @split_address (with periods trimmed) is in $hashofstreettypes, 
    #replace it with $hashofreplacestreettypes if it exists and set $street_type to the value
      if (scalar(@split_address) > 0) {
        if (exists($hashofstreettypes{trim_period(@split_address[(scalar(@split_address) - 1)])})) {
          $street_type = trim_period(pop(@split_address));

          #Check For Replacement Street Type
          if (exists($hashofreplacestreettypes{$street_type})) {
            $street_type = $hashofreplacestreettypes{$street_type};
          }
        }
      }
    #

    #Join Remaining Array and parse out all leading/trailing spaces (and replacing double spaces with single spaces)
    # and set $street_nm to the result
      $street_nm = join(' ', @split_address);
      $street_nm =~ s/^\s+//g;
      $street_nm =~ s/\s+$//g;
      $street_nm =~ s/\s+/ /g;
    #

    #If we don't have street number, use either pre- or post-directional for $street_nm
      if (!($street_nm)) {
        #Check Pre Directional
        if ($pre_dirx =~ /^(NORTH|SOUTH|EAST|WEST)$/) {
          #Use Full Pre Directional Value As Street Name
          $street_nm = $pre_dirx;
          $pre_dirx = "";
        } elsif (($post_dirx =~ /^(NORTH|SOUTH|EAST|WEST)$/) && (!($street_type))) {
          #Use Full Post Directional Value As Street Name
          $street_nm = $post_dirx;
          $post_dirx = "";
        }
    #

    #If we don't have a pre-dirx we do have $alt_unit_nbr, use it for pre_dirx (and delete the value it it)
    # parsing it to only the first character
      if ((!($pre_dirx)) && ($alt_unit_nbr =~ /^(N|S|E|W)$/)) {
          #Set Pre Directional Value As Alternate Unit Nbr
          $pre_dirx = $alt_unit_nbr;
          $alt_unit_nbr = "";
        }
      }
    

      #Only Take First Character For North, South, East, West For Pre Directional
      if ($pre_dirx =~ /^(N|S|E|W|NORTH|SOUTH|EAST|WEST|NORT|SOUT)(\.){0,}$/) {
        $pre_dirx = substr($pre_dirx, 0, 1);
      }
    #

    #Only Take First Character For North, South, East, West For Post Directional
      if ($post_dirx =~ /^(N|S|E|W|NORTH|SOUTH|EAST|WEST|NORT|SOUT)(\.){0,}$/) {
        $post_dirx = substr($post_dirx, 0, 1);
      }
    #

    #If Alternate Unit Number and Unit Number Exist, 
      # if pre-directional exits, add $alt_unit_nbr to $house_nbr,
      # else add it to $street_nm
    # Else if it's just one letter and $house_number doesn't have '&', '-', ',', '/', ' ', or a letter in it already,
      #add $alt_unit_nbr to $house_nbr
      #else add it to unit_nbr
    # and reset $alt_unit_nbr
      if ($alt_unit_nbr) {
        #Check If Unit Number Also Exists
        if ($unit_nbr) {
          #Unit Number Exists As Well So Append Unit Number To Another Piece Of Address
          #Check For Pre Directional
          if ($pre_dirx) {
            #Pre Directional Exists So Add To House Number
            $house_nbr = $house_nbr . ' ' . $alt_unit_nbr;
          } else {
            #Pre Directional Does Not Exists So Add To Street Name
            $street_nm = $street_nm . ' ' . $alt_unit_nbr;
          }
        } else {
          #Unit Number Does Not Exist So Determine Best Place To Append It
          if (($alt_unit_nbr =~ /^[A-Z]{1,1}$/) && ($house_nbr !~ /(&|-|,|\/| |[A-Z])/)) {
            #Add To House Number
            $house_nbr = $house_nbr . $alt_unit_nbr;

          } else {
            #Set Unit Number As Alternate Unit Number
            $unit_nbr = $alt_unit_nbr;
          }
        }
        $alt_unit_nbr = '';
      }
    #

    #Build New Address as $house_nbr + $pre_dirx + $street_nm + $street_type + $post_dirx + $unit_nbr (joined with spaces)
    #and trim leading/trailing/extra spaces
      $new_address = join(' ', ($house_nbr, $pre_dirx, $street_nm, $street_type, $post_dirx, $unit_nbr));
      $new_address =~ s/^\s+//g;
      $new_address =~ s/\s+$//g;
      $new_address =~ s/\s+/ /g;
    #

    #Return Results as ref to variables
      $$return_address_ref = $new_address;
      $$return_house_nbr_ref = $house_nbr;
      $$return_pre_dirx_ref = $pre_dirx;
      $$return_street_nm_ref = $street_nm;
      $$return_street_type_ref = $street_type;
      $$return_post_dirx_ref = $post_dirx;
      $$return_unit_nbr_ref = $unit_nbr;
    #
  }
#

#-----------------------------------------
#plog definition (Perl-specific connection)
#-----------------------------------------
  #Parameters:
    # $_ (unnamed parameter) -> String to print to the log
  #Function:
    #Prints passed in string to the LOGFILE created at the beginning of the script; if $debug set, also prints to CLI
  #Returns: none
#-----------------------------------------
  sub plog
  {
    my $message = $_[0];
    print STDOUT "$message\n" if ($debug);
    print LOGFILE "$message\n";
  }
#

#-----------------------------------------
#usage definition (Perl-specific function)
#-----------------------------------------
  #Parameters:none
  #Function:
    #Prints to CLI the correct parameters if improper CLI parameters were given and errors out
  #Returns: nothing
#-----------------------------------------
  sub usage {
    print <<EOM;

    Usage: $0 [parameters]
              Parameters:
      -debug (optional)
      -gis <3gis database> (gis1/gis2) default gis1
      -poly <polygon id> (required)


EOM
    exit;
  }
#

#-----------------------------------------
#db_connect definition (Perl-specific function)
#-----------------------------------------
  #Parameters:
    #inst => database instance to connect to (string as it appears in TNS file)
  #Function:
    #connects given instance as a new db_std object with autocommit off and that fails whenever there's an error
  #Returns:
    #$dbh -> database handle for the given db object
#-----------------------------------------
  sub db_connect
  {
    my $inst = shift;
    my ($db_cfg, $dbh);

    $db_cfg = new db_std($inst);
    $dbh = $db_cfg->connect($inst) || die "Can't connect to $inst";
    $dbh->{RaiseError} = 1; #| croak whenever there's a DBI error
    $dbh->{AutoCommit} = 1; #| don't commit automatically
    $dbh->do("alter session set NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'");

    return $dbh;
  }
#
