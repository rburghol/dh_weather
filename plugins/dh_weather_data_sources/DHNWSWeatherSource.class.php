<?php

class DHNWSWeatherSource extends DHDefaultWeatherSource {
  
  function __construct($config = array()) {
    
  }
  
  function importWeatherFeatures($config = array()) {
  
  }
  
  function importWeatherTSData ($thisdate, $config = array()) {
    // based on function importNOAAGriddedPrecip ($listobject, $projectid, $thisyear, $thismo, $thisday, $scratchdir, $basedataurl, $debug, $overwrite = 1) {
    $ts = strtotime($thisdate);
    $year = date('Y', $ts);
    $month = date('m', $ts);
    $day = date('d', $ts);
    
    // config options:
    // $config['sensor_hydroids'] = array()
    // $config['entity_field_coverage'] = array('field' => 'dh_geofield', 'entity_type' => 'dh_feature', 'entity_id' = array(1,2,3,...))

     dpm("Downloading Data for $year-$month-$day<br>");
     # old school, gets most recent real-time
     #getNOAAGriddedPrecip ($listobject, $scratchdir, $year, $month, $day, $debug);
     # new school - daily format:
     # http://www.srh.noaa.gov/rfcshare/p_download_new/2007/200706/nws_precip_20070601.tar.gz
     // file structure for this format
     // $thisbase = $basedataurl . '/' . $year . '/' . $year . $month;
     //$filename = 'nws_precip_';
     //$filename .= $year;
     //$filename .= $month;
     //$filename .= $day;
     //$filename .= '.tar.gz';
     // http://water.weather.gov/precip/p_download_new 
     // New file name structure: nws_precip_1day_observed_shape_20110304.tar.gz
     $thisbase = $basedataurl . '/' . $year . '/' . $month . '/' . $day;
     $filename = 'nws_precip_1day_observed_shape_';
     $filename .= $year;
     $filename .= $month;
     $filename .= $day;
     $filename .= '.tar.gz';
     
     // creates and populates tmp_precipgrid
     $results = $this->getNOAAGriddedPrecipHTTP ($scratchdir, $thisbase, $filename, $overwrite, $debug);
     if ($results['numrecs'] == 0) {
        return $results['error'];
     }
     # process the new data
     $thisdate = "$year-$month-$day";
     print("Clearing Old Data from Gridded Database<br>");
     // DELETE tbl1.* FROM tbl1 LEFT JOIN tbl2 USING(fld) WHERE tbl2.fld IS NULL;
     $q = "  delete dhw.* ";
     $q .= " from {dh_timeseries_weather} as dhw ";
     $q .= " left outer join {dh_feature} as dhf ";
     $q .= " on ( ";
     $q .= "   dhw.featureid = dhf.hydroid ";
     if (count($config)) {
       // check for extra
     }
     $q .= " ) ";
     $q .= " where tstime = extract(epoch from '$thisdate'::timestamp) ";
     $q .= "   and varid = $varid ";
     $q .= "   and entity_type = 'dh_feature'";
     $q .= "   dhf.hydroid is not null ";
     dpm("$q ; <br>");
     

     print("Inserting Data into Gridded Database<br>");
     $q = "  insert into {dh_timeseries_weather} (featureid, entity_type, tstime, precip, varid ) ";
     $q .= " select a.hydroid, 'dh_feature', extract(epoch from '$thisdate'::timestamp), globvalue, $varid ";
     # assumes that the file came in as decimal degrees
     $q .= " from dh_feature as a, tmp_precipgrid as b ";
     // 
     $q .= " where (hrapx || '-' || hrapy = hydrocode) ";
     $q .= "   and a.ftype = 'nws_precip' ";
     $q .= "   and a.bundle = 'weather_sensor' ";
    // $config['sensor_hydroids'] = array()
    if (isset($config['wkt']) and strlen($config['wkt'])) {
     // @todo: use this as a spatial filter
    }
    if (isset($config['sensor_hydroids']) and count($config['sensor_hydroids'])) {
     // @todo: check to see if sensor_hydroids is passed in
     //        if so, only join on (hrapx || '-' || hrapy = hydrocode) for the select hydroids
     //        if not, grab all based on the (hrapx || ...) match
    }
    if (isset($config['entity_field_coverage']) 
      and isset($config['entity_field_coverage']['field'])
      and isset($config['entity_field_coverage']['entity_type'])
      and isset($config['entity_field_coverage']['entity_id'])
    ) {
     // @todo: do a containment join on the field indicated
    }
    // $config['entity_field_coverage'] = array('field' => 'dh_geofield', 'entity_type' => 'dh_feature', 'entity_id' = array(1,2,3,...))
     dpm("$q ; <br>");

     # clean up after ourselves
     if ($listobject->tableExists('tmp_precipgrid')) {
        $q = "drop table tmp_precipgrid ";
        if ($debug) { print("$q ; <br>"); }
        $listobject->performQuery();
     }

     return $message;
     #mxcl_mail( $subject, $message, 'robert.burgholzer@deq.virginia.gov' );
     #mailIMAP($mailobj, $mail_headers, 'robert.burgholzer@deq.virginia.gov', 'robert.burgholzer@deq.virginia.gov' , $message, $debug);
  }
  
  function getNOAAGriddedPrecipHTTP ($scratchdir, $thisbase, $filename, $overwrite, $debug) {
    $shp2pg_cmd = 'shp2pgsql'; // windows, use shp2pgsql.exe
    if (strlen($baseurl) == 0) {
      $baseurl = "http://www.srh.noaa.gov/rfcshare/p_download_new";
    }

    $fileURL = $baseurl . '/' . $filename;

    $result = array();
    $results['fileURL'] = $fileURL;
    $results['numrecs'] = 0;

    $getfile = 0;
    if (fopen($scratchdir . '/' . $filename, "r")) {
      if ($overwrite) {
         $getfile = 1;
         print("File $filename exists locally, but refresh from network requested.<br>");
         print("Attempting retrieval from network.<br>");
      } else { 
         print("File $filename exists locally, no refresh requested.<br>");
      }
      fclose($scratchdir . '/' . $filename);
    } else {
      print("File $filename does not exist locally, attempting retrieval from network.<br>");
      $getfile = 1;
    }

    if ($getfile) {
      if ($debug) { print("Initializing server info.<br>"); }
      if ($debug) { print("Trying to retrieve $fileURL.<br>"); }
      if (!copy($fileURL, $scratchdir . '/' . $filename)) {
          $results['error'] .= "failed to retrieve $fileURL...\n";
          $ftpfile = 'ftp://63.77.98.88/pub/rfcshare/precip_new/' . $filename;
          $results['error'] .= "trying ftp file $fileURL...\n";
          if (!copy($ftpfile, $scratchdir . '/' . $filename)) {
             $results['error'] .= "failed to retrieve $ftpfile...\n";
          }
      }
    }

    # unzipping the archive
    if ($debug) { print("Unpacking file $filename.<br>"); }
    #gunzip($tarfile, $filename);
    // @todo: module_load_include system module
    // https://api.drupal.org/api/drupal/modules%21system%21system.tar.inc/class/Archive_Tar/7.x
    $tar = new Archive_Tar($scratchdir . '/' . $filename);
    @$tar->extract($scratchdir);

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

    $shapefilename = "$scratchdir/" . $shapename;

    print("Creating PostGIS loadable data from file.<br>");
    print("Using command: $shp2pg_cmd $shapefilename tmp_precipgrid > $shapefilename.sql <br>");
    exec("$shp2pg_cmd $shapefilename tmp_precipgrid > $shapefilename.sql", $cout);


    # assumes 8k line lenght maximum. This should be OK for these data records
    # since they are only point data, but would be much larger if it were shape data

    if ($listobject->tableExists('tmp_precipgrid')) {
      $q = "drop table tmp_precipgrid ";
      if ($debug) { print("$q ; <br>"); }
      $listobject->performQuery();
    }
    print("Reading the contents of $shapefilename.sql into PG database.<br>");
    $shphandle = fopen("$shapefilename.sql","r");
    while ($thisline = fgets($shphandle) ) {
      $q = $thisline;
      while (substr(rtrim($thisline), -1, 1) <> ';') {
        # keep looking for more, this is a multi-line query
        $thisline = fgets($shphandle);
        $q .= $thisline;
      }
      # Can't uncomment this one, or will end up with a billion records printed out
      #if ($debug) { print("$q ; <br>"); }
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

    $q = "select count(*) as recs from tmp_precipgrid ";
    $listobject->performQuery();
    if ($debug) {
      print("$q ; <br>");
      $listobject->showList();
    }
    $imps = $listobject->getRecordValue(1,'recs');
    $results['error'] .= "<b>Total Records Imported</b> = $imps.<br>";

    return $results;
  }
}