#!/user/bin/env drush
<?php

function get_weather_sensor(&$entities, $hydrocode) {
  $efq = new EntityFieldQuery;
  $efq->entityCondition('entity_type', 'dh_feature');
  $efq->propertyCondition('hydrocode', $hydrocode, '=');
  $efq->propertyCondition('bundle', 'weather_sensor', '=');
//dpm($efq);
  $result = $efq->execute();
  if (isset($result['dh_feature'])) {
    $rez = array_shift($result['dh_feature']);
    if (property_exists($rez, 'hydroid')) {
      $entities[$hydrocode] = $rez->hydroid;
      return $rez->hydroid;
    }
  }
  return FALSE;
}

$file = '/var/www/incoming/weather/formatted/csci_weather.csv';
$handle = fopen($file, 'r');
$varid = dh_varkey2varid('weather_obs');
$varid = is_array($varid) ? array_shift($varid) : $varid;
// actual $keys = array('begintime', 'records', 'battV_min', 'airTC_avg', 'relav_humid', 'LWmV_avg', 'LWMdry_tot', 'LWMcon_tot', 'LWMwet_tot', 'rain_mm_tot', 'time_interval', 'hydrocode');
$keys = array('tstime', 'records', 'battV_min', 'temp', 'rh', 'LWmV_avg', 'LWMdry_tot', 'LWMcon_tot', 'wet_time', 'rain', 'time_interval', 'hydrocode');
$i = 0;
echo "Processed ";
$summaries = array(); 
$sensors = array(); // a list of individual sensors handled
// $summaries = array(
//   2 => array(
//     '2017-06-01' => '2017-05-01',
// if current date does not match last date, do summary using tstime_date_singular setting
while ($values = fgetcsv($handle)) {
  $i++;
  if ($i == 1) {
    continue;
  }
  $weather = array_combine($keys, $values);
  $weather['varid'] = $varid;
  //dpm($weather,'weather');
  $sensor = isset($entities[$weather['hydrocode']]) ? $entities[$weather['hydrocode']] : get_weather_sensor($entities, $weather['hydrocode']);
  if ($sensor) {
    $sensors[$sensor] = $sensor; // add to list for later summary
    $weather['featureid'] = $sensor;
    $weather['entity_type'] = 'dh_feature';
    //echo print_r($values, 1) . "\n";
    //dpm($values, 'values');
    dh_update_timeseries_weather($weather);
    // just save one entry for each date
    if (!isset($summaries[$sensor])) {
      $summaries[$sensor] = array();
    }
    $summaries[$sensor][date('Y-m-d', dh_handletimestamp($weather['tstime']))] = date('Y-m-d', dh_handletimestamp($weather['tstime']));
  }  
  if ( ($i/500) == intval($i/500)) {
    echo "... $i ";
  }
}
//$sumvar = array_shift($varids = dh_varkey2varid('weather_obs_daily_sum'));
$sumvar = array_shift(dh_varkey2varid('weather_obs_daily_sum'));
foreach ($summaries as $sensor => $dates) {
  foreach ($dates as $thisdate) {
    $values = array(
      'featureid' => $sensor,
      'entity_type' => 'dh_feature',
      'tstime' => $thisdate,
      'varid' => $sumvar
    );
    echo "Updating $sensor - $thisdate for varid = $sumvar \n";
    dh_update_timeseries_weather($values, 'tstime_date_singular');
  }
}
echo " - total $i records ";
foreach ($sensors as $sensor) {
  $values = array(
    'featureid' => $sensor,
    'entity_type' => 'dh_feature',
    'tstime' => date('Y-m-d'), // this will be overwritten by the plugin as there is only 1
    'varid' => 'weather_pd_last24hrs'
  );
  $tid = dh_update_timeseries_weather($values, 'singular');
  echo "Updated $sensor - $thisdate for varid = weather_pd_last24hrs = tid $tid\n";
}
?>
