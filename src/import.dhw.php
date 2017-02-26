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
    $weather['featureid'] = $sensor;
    $weather['entity_type'] = 'dh_feature';
    //echo print_r($values, 1) . "\n";
    //dpm($values, 'values');
    dh_update_timeseries_weather($weather);
  }  
  if ( ($i/500) == intval($i/500)) {
    echo "... $i ";
  }
}
echo " - total $i records ";

?>