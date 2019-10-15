#!/user/bin/env drush
<?php
module_load_include('inc', 'dh_weather', 'src/php/lib_noaa');
$projectid = 1;
$ownerid = 1;
$debug = 1;
$indicator_sites = 1; # use only indicator sites?? (indicator_site are marked as 1 in map_group_sites)
$overwrite = 0; # replace if already there?
$scratchdir = './data';

$dateranges = array();

# choose water year boundaries based on the beginning of the current water year, 
# through the current date (convention is to handle the end date as the begining of the month)
$yesterday = date('Y-m-d');
$yesterday_obj = new DateTime($yesterday);
$yesterday_obj->modify("-1 days");
$yesterday = $yesterday_obj->format('Y-m-d');
$single = FALSE;
$overwrite = FALSE;
$date = drush_get_option('date');
$single = drush_get_option('single');
$overwrite = drush_get_option('overwrite');
$bundle = 'landunit';
$hydrocode = 'vahydro_nws_precip_grid';
$ftype = 'nws_precip_grid';

if ($date) {
  if ($date == 'today') {
    // we do this because the dataset is not finalized until noon the following day
    $date = $yesterday;
  }
  $single = $single ? filter_var($single, FILTER_VALIDATE_BOOLEAN) : FALSE;
  $overwrite = $overwrite ? filter_var($overwrite, FILTER_VALIDATE_BOOLEAN) : FALSE;
} else {
  echo "Called: drush scr noaa_daily_precip_retrieval.php --date=$date --single=$single --overwrite=$overwrite\n";
  echo "Usage: drush scr noaa_daily_precip_retrieval.php --date=(yyyy-mm-dd or today) [--single=FALSE] [--overwrite=FALSE]\n";
  echo "Example: Retrieve data for all days since last entry up till today, no overwrite.  \n";
  echo "  php noaa_daily_precip_retrieval.php --date=today \n";
  echo "Example: Refresh data from Jan 1, 2015  \n";
  echo "  drush scr noaa_daily_precip_retrieval.php --date=2015-01-01 --single=FALSE --overwrite=FALSE \n";
  echo "Example: Refresh & force overwrite data for only Mar 15, 2015 \n";
  echo "  drush scr noaa_daily_precip_retrieval.php --date=2015-03-15 --single=TRUE --overwrite=TRUE \n";
  die;
}

print("Retrieving Precip for $date single = $single overwrite = $overwrite .\n");

# get precip till today
// changed 6/28/2017
$basedataurl = "http://water.weather.gov/precip/downloads";

$config = array(
  'date' => $date,
  'featureid' => dh_search_feature($hydrocode, $bundle, $ftype),
  'varkey' => 'noaa_precip_raster',
);
$dates = array();
if (!$single) {
  $thistime = strtotime($date);
  $last_time = dh_timeseries_weather_most_recent($config, $debug);
  error_log("Last data time: $last_time " .date('Y-m-d', $last_time) );
  if (!$last_time) {
    // if there is no data we have to handle as a single date
    $dates[] = $date;
  } else {
    $next_time = $last_time + 86400;
    error_log("Checking next time: $next_time " .date('Y-m-d', $next_time) );
    while ($next_time <= $thistime) {
      $dates[] = date('Y-m-d', $next_time);
      $next_time += 86400;
    }
    error_log("Final time: $next_time");
  }
} else {
  $dates[] = $date;
}
error_log('Dates: ' . print_r($dates,1));

foreach ($dates as $date) {
  $config['date'] = $date;
  $config['dataset'] = 'nws_precip_1day_';
  $config['varkey'] = 'noaa_precip_raster';
  $config['multiplicity'] = 'tstime_singular';
  $result = dh_weather_get_noaa_gridded_precip($config, $overwrite, $single, $debug);
  if ($result) {
    $config['dataset'] = 'nws_precip_wytd_';
    $config['varkey'] = 'precip_obs_wy2date';
    $config['multiplicity'] = 'wy2date_singular';
    dh_weather_get_noaa_gridded_precip($config, $overwrite, $single, $debug);
    dh_weather_grid_summary($config, $overwrite, $single, $debug);
  }
}

?>
