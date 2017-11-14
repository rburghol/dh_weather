<?php

# lib_noaa.php
function dh_weather_get_noaa_gridded_precip ($config = array(), $overwrite = FALSE, $single = FALSE, $debug=FALSE) {
  global $databases;
  error_log("config" . print_r($config,1));
  $config += array(
   'baseurl' => "http://water.weather.gov/precip/downloads",
   'date' => date('Y-m-d'),
   'entity_type' => 'dh_feature',
   'ext' => '_conus.tif',
   'scratchdir' => '/tmp',
   'multiplicity' => 'tstime_singular', // other options = 'wateryear_singular'
   'dataset' => "nws_precip_1day_",
   'varkey' => "noaa_precip_raster",
   'dbname' => $databases['default']['default']['database'],
   'username' => $databases['default']['default']['username'],
   //'entity_id' => entity_load_single(
  );
  list($yr, $mo, $da) = explode('-', $config['date']);
  $dataset= $config['dataset'];
  $ext=$config['ext'];
  $fil="$dataset$yr$mo$da$ext";
  $varkey = $config['varkey'];
  $file_path = $config['scratchdir'] . '/' . $fil;
  $sql_path = $config['scratchdir'] . '/' . 'tmp_precipgrid.sql';
  $url="http://water.weather.gov/precip/downloads/$yr/$mo/$da/$fil";
  // download the file
  /*
  # Download a test file
  yr='2017'
  mo='07'
  da='23'
  ext='_conus.tif'
  dataset="nws_precip_1day_"
  #dataset="nws_precip_last365days_"
  fil="$dataset$yr$mo$da$ext"
  url="http://water.weather.gov/precip/downloads/$yr/$mo/$da/$fil"
  echo $url

  wget $url 

  # Convert to projection 4326
  gdalwarp $fil -t_srs EPSG:4326 "$fil.conus-4326.tif"
  # If you like, you can just download this new file with sftp and test it out on QGIS to see if the transformation was good
  # export to postgis format
  raster2pgsql "$fil.conus-4326.tif" tmp_precipgrid > tmp_precipgrid.sql
  # add to database
  echo "drop table tmp_precipgrid;" | psql -h dbase2 drupal.beta
  cat tmp_precipgrid.sql | psql -h dbase2 drupal.beta
  */
  error_log("copy($url, $config[scratchdir])");
  if (!file_exists($file_path)) {
    if (!copy($url, $file_path)) {
       echo "failed to copy $fil...\n";
    }
  }
  if (!file_exists($file_path)) {
    echo "Could not download $url ...\n";
    return FALSE;
  }
  error_log("gdalwarp $file_path -t_srs EPSG:4326 \"$file_path.conus-4326.tif\"");
  shell_exec("gdalwarp $file_path -t_srs EPSG:4326 \"$file_path.conus-4326.tif\"");
  error_log("raster2pgsql \"$file_path.conus-4326.tif\" tmp_precipgrid > $sql_path ");
  shell_exec("raster2pgsql \"$file_path.conus-4326.tif\" tmp_precipgrid > $sql_path ");
  $q = "drop table tmp_precipgrid";
  error_log("echo \"drop table tmp_precipgrid\" | psql -h dbase2 $config[dbname] -U $config[username] ");
  shell_exec("echo \"drop table tmp_precipgrid\" | psql -h dbase2 $config[dbname] -U $config[username] ");
  error_log("cat $sql_path | psql -h dbase2 $config[dbname] -U $config[username] ");
  shell_exec("cat $sql_path | psql -h dbase2 $config[dbname] -U $config[username] ");
  error_log("echo \"grant all on table tmp_precipgrid to drupal\" | psql -h dbase2 $config[dbname] -U $config[username] ");
  shell_exec("echo \"grant all on table tmp_precipgrid to drupal\" | psql -h dbase2 $config[dbname] -U $config[username] ");
  
  // process the file and import into a temp table
  $ts = dh_handletimestamp($config['date']);
  
  $q = " delete from dh_timeseries_weather ";
  $q .=" where varid in (select hydroid from dh_variabledefinition where varkey = '$varkey' ) ";
  $q .="   and entity_type = 'dh_feature' ";
  $q .="   and featureid in ( ";
  $q .="     select hydroid ";
  $q .="     from dh_feature ";
  $q .="     where bundle = 'weather_sensor' ";
  $q .="       and ftype = 'nws_precip' ";
  $q .="    )";
  switch ($config['multiplicity']) {
    case 'tstime_singular':
      // just overwrite the date if requested
      $q .="   and tstime = $ts ";
    break;
    case 'wy2date_singular':
      // just overwrite the date if requested
      list($yr, $mo, $da) = explode('-', $config['date']);
      $wy_begin = ($mo < 10) ? $yr - 1: $yr;
      $wy_ts = dh_handletimestamp("$wy_begin-10-01");
      $tsend = $ts;
      $ts = $wy_ts;
      $q .="   and tstime = $ts ";
      $q .="   and tsendtime = $tsend ";
    break;   
  }
  $q .="  ";
  error_log($q);
  
  // insert a clipped copy of the raster into the dh_timeseries_weather table
  $q = "  insert into dh_timeseries_weather (entity_type, featureid, varid, ";
  $q .="   tstime, ";
  if (isset($tsend)) {
    $q .="  tsendtime, ";
  }
  $q .="   rast) ";
  $q .=" select 'dh_feature', a.hydroid, c.hydroid as varid, $ts, ";
  if (isset($tsend)) {
    $q .="  $tsend, ";
  }
  $q .="   st_clip(noaa.rast, st_envelope(st_setsrid(b.dh_geofield_geom,4326))) ";
  $q .=" from dh_feature as a ";
  $q .=" left outer join field_data_dh_geofield as b ";
  $q .=" on ( ";
  $q .="   a.hydroid = b.entity_id ";
  $q .="   and b.entity_type = 'dh_feature' ";
  $q .=" ) ";
  $q .=" left outer join dh_variabledefinition as c ";
  $q .=" on ( ";
  $q .="   c.varkey = '$varkey' ";
  $q .=" ) ";
  $q .=" left outer join tmp_precipgrid as noaa ";
  $q .=" on (1 = 1) ";
  $q .=" left outer join dh_timeseries_weather as xst ";
  $q .=" on (";
  $q .="   xst.entity_type = 'dh_feature' ";
  $q .="   and xst.tstime = $ts ";
  if (isset($tsend)) {
    $q .="   and xst.tsendtime = $tsend ";
  }
  $q .="   and xst.featureid = a.hydroid ";
  $q .=" ) ";
  $q .=" where b.entity_id is not null ";
  $q .=" and c.hydroid is not null ";
  $q .=" and xst.tid is null ";
  $q .=" and a.hydrocode = 'vahydro_nws_precip_grid' ";
  $q .=" and a.bundle = 'landunit' ";
  $q .=" and a.ftype = 'nws_precip_grid' ";
  error_log($q);
  db_query($q);
  return TRUE;
/*
   if ($debug) { print("Downloading $fileURL.<br>"); }
   $imgdata = imagecreatefromgif ( $fileURL );
   
   if ($debug) { print("Saving to $filename.<br>"); }
   imagegif ( $imgdata, $config['scratchdir'] . '/' . $filename );
*/
}

function dh_weather_grid_summary($config, $overwrite, $single, $debug) {
  // @todo: do not delete previous until we verify that the desired date exists in the temp table
  $ts = dh_handletimestamp($config['date']);
  
  $q = " delete from dh_timeseries_weather ";
  $q .=" where varid in (select hydroid from dh_variabledefinition where varkey = 'precip_pct_wy2date' ) ";
  $q .="   and entity_type = 'dh_feature' ";
  $q .="   and featureid in ( ";
  $q .="     select hydroid ";
  $q .="     from dh_feature ";
  $q .="     where bundle = 'weather_sensor' ";
  $q .="       and ftype = 'nws_precip' ";
  $q .="    )";
  list($yr, $mo, $da) = explode('-', $config['date']);
  $wy_begin = ($mo < 10) ? $yr - 1: $yr;
  $wy_ts = dh_handletimestamp("$wy_begin-10-01");
  $tsend = $ts;
  $ts = $wy_ts;
  $q .="   and tstime = $ts ";
  $q .="   and tsendtime = $tsend ";
  $q .="  ";
  error_log($q);
  db_query($q);
  
  $q = " insert into dh_timeseries_weather (entity_type, featureid, varid, tstime, tsendtime, rain) ";
  $q .=" select 'dh_feature', foo.hydroid, var.hydroid as varid, $ts, $tsend, ";
  $q .="   CASE ";
  $q .="     WHEN SUM(nml) > 0 THEN 100.0 * SUM(obs) / sum(nml) ";
  $q .="     ELSE 0";
  $q .="   END as pct_normal ";
  $q .=" from dh_variabledefinition as var left outer join (";
  $q .="   select a.hydrocode, to_timestamp(tstime), a.hydroid, st_astext(b.dh_geofield_geom),";
  $q .="      CASE";
  $q .="        WHEN ST_Value(rast, 1, b.dh_geofield_geom) is null THEN 0";
  $q .="        ELSE ST_Value(rast, 1, b.dh_geofield_geom)";
  $q .="      END as obs,";
  $q .="      CASE";
  $q .="        WHEN ST_Value(rast, 2, b.dh_geofield_geom) is null THEN 0";
  $q .="        ELSE ST_Value(rast, 2, b.dh_geofield_geom)";
  $q .="      END as nml, ";
  $q .="      CASE";
  $q .="        WHEN ST_Value(rast, 4, b.dh_geofield_geom) is null THEN 0 ";
  $q .="        ELSE ST_Value(rast, 4, b.dh_geofield_geom) ";
  $q .="      END as pct";
  $q .="   from dh_feature as a ";
  $q .="   left outer join field_data_dh_geofield as b ";
  $q .="   on (";
  $q .=" entity_id = a.hydroid ";
  $q .="     and b.entity_type = 'dh_feature' ";
  $q .="   )";
  $q .="   left outer join dh_timeseries_weather as c ";
  $q .="   on (";
  $q .="     c.varid in (select hydroid from dh_variabledefinition where varkey = 'precip_obs_wy2date' ) ";
  $q .="   )";
  $q .="   where a.bundle = 'weather_sensor' ";
  $q .="     and c.featureid in (";
  $q .="       select hydroid ";
  $q .="       from dh_feature ";
  $q .="       where hydrocode = 'vahydro_nws_precip_grid' ";
  $q .="         and bundle = 'landunit' ";
  $q .="         and ftype = 'nws_precip_grid'";
  $q .="     )";
  $q .="     and c.entity_type = 'dh_feature'";
  $q .="     and a.ftype = 'nws_precip' ";
  $q .="     and varid in (select hydroid from dh_variabledefinition where varkey = 'precip_obs_wy2date' ) ";
  $q .="     and c.tstime = $ts and c.tsendtime = $tsend ";
  $q .=" ) as foo ";
  $q .=" on (1 = 1) ";
  $q .=" where var.varkey = 'precip_pct_wy2date' ";
  $q .=" group by var.hydroid, foo.hydroid ";
  error_log("Rolling Current Water-Year Summary for $wy_begin-10-01 ($ts) to $config[date] ($tsend)");
  error_log($q);
  db_query($q);
  
  # Now do the rolling water year
  # we always just decrement the beginning water year by 1 since it's rolling.
  $wy1_begin = $wy_begin - 1;
  $wy1_end = $wy1_begin + 1;
  $wy1_ts = dh_handletimestamp("$wy1_begin-10-01");
  $wy1_ets = dh_handletimestamp("$wy1_end-09-30");
  
  dh_weather_clear_vahydro_summary($wy1_ts, $tsend, 'precip_pct_2wy2date');
  
  $q = " insert into dh_timeseries_weather (entity_type, featureid, varid, tstime, tsendtime, rain) ";
  $q .=" select 'dh_feature', foo.hydroid, var.hydroid as varid, $wy1_ts, $tsend, ";
  $q .="   CASE ";
  $q .="     WHEN SUM(nml) > 0 THEN 100.0 * SUM(obs) / sum(nml) ";
  $q .="     ELSE 0";
  $q .="   END as pct_normal ";
  $q .=" from dh_variabledefinition as var left outer join (";
  $q .="   select a.hydrocode, to_timestamp(tstime), a.hydroid, st_astext(b.dh_geofield_geom),";
  $q .="      CASE";
  $q .="        WHEN ST_Value(rast, 1, b.dh_geofield_geom) is null THEN 0";
  $q .="        ELSE ST_Value(rast, 1, b.dh_geofield_geom)";
  $q .="      END as obs,";
  $q .="      CASE";
  $q .="        WHEN ST_Value(rast, 2, b.dh_geofield_geom) is null THEN 0";
  $q .="        ELSE ST_Value(rast, 2, b.dh_geofield_geom)";
  $q .="      END as nml, ";
  $q .="      CASE";
  $q .="        WHEN ST_Value(rast, 4, b.dh_geofield_geom) is null THEN 0 ";
  $q .="        ELSE ST_Value(rast, 4, b.dh_geofield_geom) ";
  $q .="      END as pct";
  $q .="   from dh_feature as a ";
  $q .="   left outer join field_data_dh_geofield as b ";
  $q .="   on (";
  $q .=" entity_id = a.hydroid ";
  $q .="     and b.entity_type = 'dh_feature' ";
  $q .="   )";
  $q .="   left outer join dh_timeseries_weather as c ";
  $q .="   on (";
  $q .="     c.varid in (select hydroid from dh_variabledefinition where varkey = 'precip_obs_wy2date' ) ";
  $q .="   )";
  $q .="   where a.bundle = 'weather_sensor' ";
  $q .="     and c.featureid in (";
  $q .="       select hydroid ";
  $q .="       from dh_feature ";
  $q .="       where hydrocode = 'vahydro_nws_precip_grid' ";
  $q .="         and bundle = 'landunit' ";
  $q .="         and ftype = 'nws_precip_grid'";
  $q .="     )";
  $q .="     and c.entity_type = 'dh_feature'";
  $q .="     and a.ftype = 'nws_precip' ";
  $q .="     and varid in (select hydroid from dh_variabledefinition where varkey = 'precip_obs_wy2date' ) ";
  $q .="     and ( ";
  $q .="        (c.tstime = $wy1_ts and c.tsendtime = $wy1_ets) ";
  $q .="        OR ";
  $q .="        (c.tstime =  $ts and c.tsendtime = $tsend) ";
  $q .="      ) ";
  $q .=" ) as foo ";
  $q .=" on (1 = 1) ";
  $q .=" where var.varkey = 'precip_pct_2wy2date' ";
  $q .=" and foo.hydroid is not null ";
  $q .=" group by var.hydroid, foo.hydroid ";
  error_log("Rolling 2-Water-Year Summary for $wy1_begin-10-01 to $wy1_end-10-01 and $wy_begin-10-01 to $config[date]");
  error_log($q);
  db_query($q);
  
}

function dh_weather_clear_vahydro_summary($ts, $tsend, $varkey) {
  $q = " delete from dh_timeseries_weather ";
  $q .=" where varid in (select hydroid from dh_variabledefinition where varkey = '$varkey' ) ";
  $q .="   and entity_type = 'dh_feature' ";
  $q .="   and featureid in ( ";
  $q .="     select hydroid ";
  $q .="     from dh_feature ";
  $q .="     where bundle = 'weather_sensor' ";
  $q .="       and ftype = 'nws_precip' ";
  $q .="    )";
  $q .="   and tstime = $ts ";
  $q .="   and tsendtime = $tsend ";
  $q .="  ";
  error_log($q);
  db_query($q);
}
  
function dh_timeseries_weather_most_recent($config, $debug) {
  // @todo: do not delete previous until we verify that the desired date exists in the temp table
  $config += array(
    'varkey' => 'weather_obs',
    'entity_type' => 'dh_feature',
    'bundle' => 'weather_sensor',
    'ftype' => 'nws_precip',
    'tstime' => mktime(),
    'tsendtime' => mktime(),
    'ftype' => 'nws_precip',
  );
  $vars = dh_varkey2varid($config['varkey']);
  error_log("Vars for $config[varkey]:" . print_r($vars,1));
  $config['varid'] = array_shift(dh_varkey2varid($config['varkey']));
  $params = array(
    ':varid' => $config['varid'],
    ':featureid' => $config['featureid'],
    ':entity_type' => $config['entity_type'],
  );
  $q = " select max(tstime) as last_time from dh_timeseries_weather ";
  $q .=" where varid = :varid ";
  $q .="   and entity_type = :entity_type ";
  $q .="   and featureid = :featureid";
  foreach (array('tstime_op' => 'tstime', 'tsendtime_op' => 'tsendtime') as $top => $tcol) {
    if (isset($config[$top])) {
      $ts = dh_handletimestamp($config[$tcol]);
      switch ($config[$top]) {
        case 'lte':
        case '<=':
          $op = '<=';
        break;
        case 'lt':
        case '<':
          $op = '<';
        break;
        case 'gt':
        case '>':
          $op = '>';
        break;
        case 'gte':
        case '>=':
        default:
          $op = '>=';
        break;
        $config[$top] = $op;
      }
      $q .="   and $tcol $op " . $config[$tcol];
    }
  }
  error_log("Query:" . $q . " params: " . print_r($params,1));
  $last_time = db_query($q,$params)->fetchField();
  return $last_time;
}

function dh_weather_get_noaa_vector_precip ($config = array(), $overwrite = FALSE, $debug = FALSE) {
   $shp2pg_cmd = 'shp2pgsql'; // windows, use shp2pgsql.exe
   $srid = 4326; // this is a psuedo value that only is needed to keep the import from breaking.
                 // we don't actually use the spatial component of this 
   if (empty($config)) {
     return FALSE;
   }
   $config += array(
     'baseurl' => "http://www.srh.noaa.gov/rfcshare/p_download_new",
     'date' => date('Y-m-d')
   );
   $fileURL = $config['baseurl'] . '/' . $filename;
   
   $result = array();
   $results['fileURL'] = $fileURL;
   $results['numrecs'] = 0;
   
   $getfile = 0;
   if (fopen($config['scratchdir'] . '/' . $filename, "r")) {
      if ($overwrite) {
         $getfile = 1;
         print("File $filename exists locally, but refresh from network requested.<br>");
         print("Attempting retrieval from network.<br>");
      } else { 
         print("File $filename exists locally, no refresh requested.<br>");
      }
      fclose($config['scratchdir'] . '/' . $filename);
   } else {
      print("File $filename does not exist locally, attempting retrieval from network.<br>");
      $getfile = 1;
   }
   
   if ($getfile) {
      if ($debug) { print("Initializing server info.<br>"); }
      if ($debug) { print("Trying to retrieve $fileURL.<br>"); }
      if (!copy($fileURL, $config['scratchdir'] . '/' . $filename)) {
          $results['error'] .= "failed to retrieve $fileURL...\n";
          $ftpfile = 'ftp://63.77.98.88/pub/rfcshare/precip_new/' . $filename;
          $results['error'] .= "trying ftp file $fileURL...\n";
          if (!copy($ftpfile, $config['scratchdir'] . '/' . $filename)) {
             $results['error'] .= "failed to retrieve $ftpfile...\n";
          }
      }
   }

   # unzipping the archive
   if ($debug) { print("Unpacking file $filename.<br>"); }
   #gunzip($tarfile, $filename);
   $tar = new Archive_Tar($config['scratchdir'] . '/' . $filename);
   @$tar->extract($config['scratchdir']);

   $files = $tar->listContent();       // array of file information
   if ( !count($files) ) {
      $results['error'] .= "Could not extract files!";
      return $results;
   }

   $shapename = '';
   #print_r($files);
   foreach ($files as $f) {
      #print_r($f);
       $fn = $f['filename'];
       if ($debug) {
          print("Examining archive member $fn <br>");
       }
       $ext = substr($fn,-4,4);
       print("Extension = $ext <br>");
       if (substr($fn,-4,4) == '.shp') {
          # we found the shape file base, extract the name
          $shapename = substr($fn,0,strlen($fn)-4);
       }
   }
   
   if ($shapename == '') {
      $results['error'] .= "Could not locate shapefile in archive!";
      return $results;
   }
   
   $results['shapename'] = $shapename;
   
   $shapefilename = "$config[scratchdir]/" . $shapename;

   print("Creating PostGIS loadable data from file.<br>");
   print("Using command: $shp2pg_cmd -s $srid $shapefilename tmp_precipvector > $shapefilename.sql <br>");
   exec("$shp2pg_cmd -s $srid $shapefilename tmp_precipvector > $shapefilename.sql", $cout);


   # assumes 8k line lenght maximum. This should be OK for these data records
   # since they are only point data, but would be much larger if it were shape data

   if ($listobject->tableExists('tmp_precipvector')) {
      $listobject->querystring = "drop table tmp_precipvector ";
      if ($debug) { print("$listobject->querystring ; <br>"); }
      $listobject->performQuery();
   }
   print("Reading the contents of $shapefilename.sql into PG database.<br>");
   $shphandle = fopen("$shapefilename.sql","r");
   while ($thisline = fgets($shphandle) ) {
      $listobject->querystring = $thisline;
      while (substr(rtrim($thisline), -1, 1) <> ';') {
        # keep looking for more, this is a multi-line query
        $thisline = fgets($shphandle);
        $listobject->querystring .= $thisline;
      }
      # Can't uncomment this one, or will end up with a billion records printed out
      #if ($debug) { print("$listobject->querystring ; <br>"); }
      $listobject->performQuery();
      $i++;
      if (($i / 500.0) == intval($i/500.0)) {
        if ($debug) { print("$i records processed.<br>"); }
        #break;
      }
   }
   if ($debug) { print("<b>Total Records</b> = $i.<br>"); }
   $results['error'] .= "<b>Total Lines Parsed</b> = $i.<br>";
   $results['numrecs'] = $i;

   $listobject->querystring = "select count(*) as recs from tmp_precipvector ";
   $listobject->performQuery();
   if ($debug) {
      print("$listobject->querystring ; <br>");
      $listobject->showList();
   }
   $imps = $listobject->getRecordValue(1,'recs');
   $results['error'] .= "<b>Total Records Imported</b> = $imps.<br>";

   return $results;
}

function dh_weather_get_noaa_gridded_precip_to_date($config = array(), $overwrite = FALSE, $single = FALSE, $debug=FALSE) {
   if (empty($config)) {
     return FALSE;
   }
  $today = date('Y-m-d');
  $config += array(
    'enddate' => $today,
    'vars' => array(
      'observation' => array('varkey' => 'precip_obs_daily', 'band' => 1),
      'departure_from_normal' => array('varkey' => 'precip_obs_daily', 'band' => ''),
      'percent_of_normal' => array('varkey' => 'precip_obs_daily', 'band' => ''),
    ),
  );
  $varkeys = array();
  foreach ($bases as $base) {
    $varkeys[] = $base['varkey'];
  }
  $datear = array();
  $thisdate = new DateTime(date($enddate));
  $tds = $thisdate->format('Y-m-d');
  $month = $thisdate->format('m');
  $year = $thisdate->format('Y');
  $endts = $thisdate->format('u');
  if ($single) {
    $md = $enddate;
  } else {
    # Or, get all data from most recent entry in database forward till today
    // makes sure that we get all variables, and re-import all vars for a given time period if any failed
    $q = "  select to_timestamp(min(tstime)) as maxdate ";
    $q .= " from ( ";
    $q .= "   select CASE ";
    $q .= "     WHEN tstime < tsendtime THEN tsendtime ";
    $q .= "     ELSE tstime ";
    $q .= "     END as tstime ";
    $q .= "   from ( ";
    $q .= "     select varid, max(tstime) as tstime, max(tsendtime) as tsendtime ";
    $q .= "     from dh_timeseries_weather ";
    $q .= "     where tstime <= extract (epoch from '$tds'::timestamptz) ";
    $q .= "     and varid in (";
    $q .= "       select hydroid from dh_variabledefinition ";
    $q .= "       where varkey in ( '" . implode("','", $varkeys) . "')";
    $q .= "     )";
    $q .= "     group by varid ";
    $q .= "   ) as foo ";
    $q .= " ) as bar ";

    //if ($debug) { 
      print("$q ; <br>"); 
    //}
    $rez = db_query($q);
    $rez_obj = $rez->fetchObject();
    $md = $rez_obj->maxdate;

    if (strlen($md) == 0) {
      $md = "$year-$month-01";
    }
  }
  $nextdate = new DateTime($md);
  $nd = $nextdate->format('Y-m-d');

  print ("Next Date: $nd - end date: $enddate - overwrite = $overwrite \n<br>");

  if ( (!($nextdate->format('u') > $endts)) or ($overwrite)) {
    # nothing to do if we have the data current
    
    if ($nextdate->format('Y-m-d') <> $tds) {
      $nds = '';
      for ($i = 1; $tds <> $nds; $i++) {
        $nextdate->modify("+1 day");
        $day = $nextdate->format('d');
        $month = $nextdate->format('m');
        $year = $nextdate->format('Y');
        array_push($datear, array($year,$month,$day) );
        $nds = $nextdate->format('Y-m-d');
      }
    } else {
      $day = $thisdate->format('d');
      $month = $thisdate->format('m');
      $year = $thisdate->format('Y');
      array_push($datear, array($year,$month,$day) );
      if ($debug) { print("Adding $year-$month-$day <br>"); }
    }

    # add in manual date
    #array_push($datear, array('2007','06','22'));

    foreach ($datear as $thisdate) {
      $thisyear = $thisdate[0];
      $thismo = $thisdate[1];
      $thisday = $thisdate[2];
      #$debug = 1;
      dh_weather_import_noaa_gridded_precip($config, $overwrite);
      print(implode("-", $thisdate) . ": $message <br>");
    }
  }
  // Now, finish up by summarizing the water year in question for each point in our weather sensor array
  // extract the year from end date submitted
  // then delete the old water year summary 
  // add a new water year summary.  
  // We do a query that stretches from oct 1 to sep 30th, since there should only be 1 WY summary
  // and the end of the period will be whenever the last date of the record
}

function dh_weather_get_noaa_vector_precip_to_date($config, $overwrite = TRUE, $single = FALSE, $debug) {
   if (empty($config)) {
     return FALSE;
   }
  $today = date('Y-m-d');
  if (empty($config['enddate'])) {
    $config['enddate'] = $today;
  }
  $bases = array(
    'nws_precip_1day_observed_shape_' => array(
      'filebase' => 'nws_precip_1day_observed_shape_',
      'varkey' => 'precip_obs_daily',
    ), 
    'nws_precip_wateryear2date_departure_shape_' => array(
      'filebase' => 'nws_precip_wateryear2date_departure_shape_',
      'varkey' => 'precip_depart_wy2date',
    ), 
    'nws_precip_wateryear2date_observed_shape_' => array(
      'filebase' => 'nws_precip_wateryear2date_observed_shape_',
      'varkey' => 'precip_obs_wy2date',
    ), 
    'nws_precip_wateryear2date_percent_shape_' => array(
      'filebase' => 'nws_precip_wateryear2date_percent_shape_',
      'varkey' => 'precip_pct_wy2date',
    ), 
    'nws_precip_wateryear2date_normal_shape_' => array(
      'filebase' => 'nws_precip_wateryear2date_normal_shape_',
      'varkey' => 'precip_nml_wy2date',
    ), 
  );
  $varkeys = array();
  foreach ($bases as $config) {
    $varkeys[] = $config['varkey'];
  }
  $datear = array();
  $thisdate = new DateTime(date($enddate));
  $tds = $thisdate->format('Y-m-d');
  $month = $thisdate->format('m');
  $year = $thisdate->format('Y');
  $endts = $thisdate->format('u');
  if ($single) {
    $md = $enddate;
  } else {
    # Or, get all data from most recent entry in database forward till today
    // makes sure that we get all variables, and re-import all vars for a given time period if any failed
    $listobject->querystring = "  select to_timestamp(min(tstime)) as maxdate ";
    $listobject->querystring .= " from ( ";
    $listobject->querystring .= "   select CASE ";
    $listobject->querystring .= "     WHEN tstime < tsendtime THEN tsendtime ";
    $listobject->querystring .= "     ELSE tstime ";
    $listobject->querystring .= "     END as tstime ";
    $listobject->querystring .= "   from ( ";
    $listobject->querystring .= "     select varid, max(tstime) as tstime, max(tsendtime) as tsendtime ";
    $listobject->querystring .= "     from dh_timeseries_weather ";
    $listobject->querystring .= "     where tstime <= extract (epoch from '$tds'::timestamptz) ";
    $listobject->querystring .= "     and varid in (";
    $listobject->querystring .= "       select hydroid from dh_variabledefinition ";
    $listobject->querystring .= "       where varkey in ( '" . implode("','", $varkeys) . "')";
    $listobject->querystring .= "     )";
    $listobject->querystring .= "     group by varid ";
    $listobject->querystring .= "   ) as foo ";
    $listobject->querystring .= " ) as bar ";

    //if ($debug) { 
      print("$listobject->querystring ; <br>"); 
    //}
    $listobject->performQuery();
    $md = $listobject->getRecordValue(1,'maxdate');

    if (strlen($md) == 0) {
      $md = "$year-$month-01";
    }
  }
  $nextdate = new DateTime($md);
  $nd = $nextdate->format('Y-m-d');

  print ("Next Date: $nd - end date: $enddate - overwrite = $overwrite \n<br>");

  if ( (!($nextdate->format('u') > $endts)) or ($overwrite)) {
    # nothing to do if we have the data current

    if ($nextdate->format('Y-m-d') <> $tds) {
      $nds = '';
      for ($i = 1; $tds <> $nds; $i++) {
        $nextdate->modify("+1 day");
        $day = $nextdate->format('d');
        $month = $nextdate->format('m');
        $year = $nextdate->format('Y');
        array_push($datear, array($year,$month,$day) );
        $nds = $nextdate->format('Y-m-d');
      }
    } else {
      $day = $thisdate->format('d');
      $month = $thisdate->format('m');
      $year = $thisdate->format('Y');
      array_push($datear, array($year,$month,$day) );
      if ($debug) { print("Adding $year-$month-$day <br>"); }
    }

    # add in manual date
    #array_push($datear, array('2007','06','22'));

    foreach ($datear as $thisdate) {
      $thisyear = $thisdate[0];
      $thismo = $thisdate[1];
      $thisday = $thisdate[2];
      #$debug = 1;
      foreach ($bases as $config) {
        dh_weather_import_noaa_vector_precip($listobject, $thisyear, $thismo, $thisday, $config['scratchdir'], $basedataurl, $debug, $overwrite, $config);
      }
      print("$message <br>");
    }

    $padmo = str_pad($thismo, 2, '0', STR_PAD_LEFT);
    $startdate = "$thisyear-$padmo-01";
    $dateobj = new DateTime("$startdate");
    $maxday = $dateobj->format('t');
    $enddate = "$thisyear-$padmo-$maxday";
  }
}

function dh_weather_import_noaa_gridded_precip ($config = array(), $overwrite = 1) {
  list($year, $month, $day) = explode('-', $config['date']);
  $month = str_pad($month, 2, '0', STR_PAD_LEFT);
  $day = str_pad($day, 2, '0', STR_PAD_LEFT);
  $wateryear = $thisyear;
  if ($thismo < 10) {
   $wateryear = $wateryear - 1;
  }

  if (empty($config)) {
    return FALSE;
  }

  print("Downloading Data for $year-$month-$day<br>");
  $filebase = $config['filebase'];
  $varkey = $config['varkey'];

  // New file name structure: nws_precip_1day_observed_shape_20110304.tar.gz
  $thisbase = $basedataurl . '/' . $year . '/' . $month . '/' . $day;
  $filename = $filebase;
  $filename .= $year;
  $filename .= $month;
  $filename .= $day;
  $filename .= '.tar.gz';

  // creates and populates tmp_precipgrid
  $results = dh_weather_get_noaa_gridded_precip ($config, $debug);
  return;
  
  if ($results['numrecs'] == 0) {
    return $results['error'];
  }

  # process the new data
  $thisdate = "$year-$month-$day";
  $tstime = $thisdate;
  $tsendtime = FALSE;
  if (!(strpos($varkey, 'wy2') === FALSE)) {
    $tsendtime = $tstime;
    $tstime = "$wateryear-10-01";
  }
  
  print("Clearing Old Data from Gridded Database<br>");
  $listobject->querystring = "  delete from dh_timeseries_weather ";
  //$listobject->querystring .= " where tstime = extract(epoch from '$thisdate'::timestamp) ";
  // uses timestamptz now since local postgres install has timezone set 
  // select 
  $listobject->querystring .= " where tstime = extract(epoch from '$tstime'::timestamptz) ";
  if (!($tsendtime === FALSE)) {
    $listobject->querystring .= " and tsendtime = extract(epoch from '$tsendtime'::timestamptz) ";
  }
  $listobject->querystring .= " and varid in (select hydroid from dh_variabledefinition  ";
  $listobject->querystring .= "   WHERE varkey = '$varkey'";
  $listobject->querystring .= " ) ";
  print("$listobject->querystring ; <br>");
  $listobject->performQuery();

  print("Inserting Data into Gridded Database<br>");
  $listobject->querystring = "  insert into dh_timeseries_weather (tstime, tsendtime, rain, featureid, entity_type, varid) ";
  $listobject->querystring .= "  select extract(epoch from '$tstime'::timestamptz), ";
  if (!($tsendtime === FALSE)) {
    $listobject->querystring .= " extract(epoch from '$tsendtime'::timestamptz), ";
  } else {
  $listobject->querystring .= "  NULL, ";
  }
  $listobject->querystring .= "  a.globvalue, b.hydroid, 'dh_feature', c.hydroid ";
  $listobject->querystring .= "  from tmp_precipgrid as a  ";
  $listobject->querystring .= "  left outer join dh_variabledefinition as c  ";
  $listobject->querystring .= "  on ( ";
  $listobject->querystring .= "    c.varkey = '$varkey' ";
  $listobject->querystring .= "  ) ";
  $listobject->querystring .= "  left outer join dh_feature as b  ";
  $listobject->querystring .= "  on ( ";
  $listobject->querystring .= "    (a.hrapx || '-' || a.hrapy) = b.hydrocode ";
  $listobject->querystring .= "    and b.bundle = 'weather_sensor' ";
  $listobject->querystring .= "    and b.ftype = 'nws_precip' ";
  $listobject->querystring .= "  ) ";
  $listobject->querystring .= "  where b.hydroid is not null ";
  print("$listobject->querystring ; <br>");
  $listobject->performQuery();
  #$listobject->showList();



  # clean up after ourselves
  if ($listobject->tableExists('tmp_precipgrid')) {
    $listobject->querystring = "drop table tmp_precipgrid ";
    if ($debug) { print("$listobject->querystring ; <br>"); }
    $listobject->performQuery();
  }
  return $message;
}

function dh_weather_import_noaa_vector_precip ($config = array(), $overwrite = 1) {
  // Old format deprecated by NOAA in June 2017 in favor of a raster format
  $year = $thisyear;
  $month = str_pad($thismo, 2, '0', STR_PAD_LEFT);
  $day = str_pad($thisday, 2, '0', STR_PAD_LEFT);
  $wateryear = $thisyear;
  if ($thismo < 10) {
   $wateryear = $wateryear - 1;
  }

  if (empty($config)) {
    return FALSE;
  }

  print("Downloading Data for $year-$month-$day<br>");
  $filebase = $config['filebase'];
  $varkey = $config['varkey'];

  // New file name structure: nws_precip_1day_observed_shape_20110304.tar.gz
  $thisbase = $basedataurl . '/' . $year . '/' . $month . '/' . $day;
  $filename = $filebase;
  $filename .= $year;
  $filename .= $month;
  $filename .= $day;
  $filename .= '.tar.gz';

  // creates and populates tmp_precipvector
  $results = dh_weather_get_noaa_vector_precip ($listobject, $config['scratchdir'], $thisbase, $filename, $overwrite, $debug);
  if ($results['numrecs'] == 0) {
    return $results['error'];
  }

  # process the new data
  $thisdate = "$year-$month-$day";
  $tstime = $thisdate;
  $tsendtime = FALSE;
  if (!(strpos($varkey, 'wy2') === FALSE)) {
    $tsendtime = $tstime;
    $tstime = "$wateryear-10-01";
  }
  
  print("Clearing Old Data from Gridded Database<br>");
  $listobject->querystring = "  delete from dh_timeseries_weather ";
  //$listobject->querystring .= " where tstime = extract(epoch from '$thisdate'::timestamp) ";
  // uses timestamptz now since local postgres install has timezone set 
  // select 
  $listobject->querystring .= " where tstime = extract(epoch from '$tstime'::timestamptz) ";
  if (!($tsendtime === FALSE)) {
    $listobject->querystring .= " and tsendtime = extract(epoch from '$tsendtime'::timestamptz) ";
  }
  $listobject->querystring .= " and varid in (select hydroid from dh_variabledefinition  ";
  $listobject->querystring .= "   WHERE varkey = '$varkey'";
  $listobject->querystring .= " ) ";
  print("$listobject->querystring ; <br>");
  $listobject->performQuery();

  print("Inserting Data into Gridded Database<br>");
  $listobject->querystring = "  insert into dh_timeseries_weather (tstime, tsendtime, rain, featureid, entity_type, varid) ";
  $listobject->querystring .= "  select extract(epoch from '$tstime'::timestamptz), ";
  if (!($tsendtime === FALSE)) {
    $listobject->querystring .= " extract(epoch from '$tsendtime'::timestamptz), ";
  } else {
  $listobject->querystring .= "  NULL, ";
  }
  $listobject->querystring .= "  a.globvalue, b.hydroid, 'dh_feature', c.hydroid ";
  $listobject->querystring .= "  from tmp_precipvector as a  ";
  $listobject->querystring .= "  left outer join dh_variabledefinition as c  ";
  $listobject->querystring .= "  on ( ";
  $listobject->querystring .= "    c.varkey = '$varkey' ";
  $listobject->querystring .= "  ) ";
  $listobject->querystring .= "  left outer join dh_feature as b  ";
  $listobject->querystring .= "  on ( ";
  $listobject->querystring .= "    (a.hrapx || '-' || a.hrapy) = b.hydrocode ";
  $listobject->querystring .= "    and b.bundle = 'weather_sensor' ";
  $listobject->querystring .= "    and b.ftype = 'nws_precip' ";
  $listobject->querystring .= "  ) ";
  $listobject->querystring .= "  where b.hydroid is not null ";
  print("$listobject->querystring ; <br>");
  $listobject->performQuery();
  #$listobject->showList();



  # clean up after ourselves
  if ($listobject->tableExists('tmp_precipvector')) {
    $listobject->querystring = "drop table tmp_precipvector ";
    if ($debug) { print("$listobject->querystring ; <br>"); }
    $listobject->performQuery();
  }
  return $message;
}

?>
