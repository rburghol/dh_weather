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
$today = date('Y-m-d');
$today_obj = new DateTime($today);
$today_obj->modify("-1 days");
$today = $today_obj->format('Y-m-d');
$single = FALSE;
$overwrite = FALSE;
$date = drush_get_option('date');
$single = drush_get_option('single');
$overwrite = drush_get_option('overwrite');
if ($date) {
  if ($date == 'today') {
    $today_obj = new DateTime();
    $date = $today_obj->format('Y-m-d');
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
);
if (!$single) {
  $config['dataset'] = 'nws_precip_1day_';
  $config['varkey'] = 'noaa_precip_raster';
  dh_weather_get_noaa_gridded_precip_to_date($config, $overwrite, $single, $debug);
} else {
  $config['dataset'] = 'nws_precip_1day_';
  dh_weather_get_noaa_gridded_precip($config, $overwrite, $single, $debug);
  $config['dataset'] = 'nws_precip_wytd_';
  $config['varkey'] = 'precip_obs_wy2date';
  $config['multiplicity'] = 'wy2date_singular';
  dh_weather_get_noaa_gridded_precip($config, $overwrite, $single, $debug);
  dh_weather_grid_summary($config, $overwrite, $single, $debug);
}

?>
