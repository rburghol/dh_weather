<?php
  module_load_include('inc', 'dh', 'plugins/dh.display');
  $v = $view->args ;
  $uid = 343;
  dpm($v, "<br>View args: ");
  $obs_var = array_shift(dh_varkey2varid('weather_obs'));
  $sum_var = array_shift(dh_varkey2varid('weather_obs_daily_sum'));
  echo "Usage: drush scr modules/om_agman/src/summarize-all-daily-weather.php\n";
  // sql to get records with redundant erefs
  // this allows postgresql to handle the date which might offset by 1 day
  // but this should be tolerable since the most recent days will all be updated 
  // by cron every hour, and the only possible redundancy will be an additional prepended date
  // to the beginning of the historical record with all zeroes (easy to detect)
  $q = "  select featureid, entity_type, $sum_var as varid, date_trunc('day', to_timestamp(tstime) at time zone 'EST') as tstime from dh_timeseries_weather ";
  $q .= " where varid = $obs_var ";
  $q .= " and tstime <> 0 ";
  $q .= " group by featureid, entity_type, date_trunc('day', to_timestamp(tstime) at time zone 'EST') ";
  $q .= " order by date_trunc('day', to_timestamp(tstime) at time zone 'EST') ";
  //$q .= " LIMIT 40 ";
  error_log("$q");
  $result = db_query($q);
  // If we want to do a single one uncomment these lines:
  /*
  $result = array(
    0 => new STDClass,
  );
  $result[0]->adminid = 299;
  */
  while ($record = $result->fetchAssoc()) {
    // get events
    // Load some entity.
    $tid = dh_update_timeseries_weather($record, 'tstime_date_singular');
    //echo "Object" . print_r($form,1) . "\n";
    echo "saved $record[featureid] ($tid)" . $record['tstime'] . " \n";
  }

?>