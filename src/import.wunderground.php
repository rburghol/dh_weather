<?php
// verified, this works, for 
$uri = "https://api.weather.com/v2/pws/observations/hourly/7day";
// Forecast sems to use different parameters... not user station based, but rather location based
// Need to test this 5 day forecast -- should work
//  But how do we get the location code?
$furi = "https://api.weather.com/v3/wx/forecast/daily/5day";
// Will this work? Can we get an hourly 5day? I made this URI up.
$furi = "https://api.weather.com/v3/wx/forecast/hourly/5day";
// maybe use this lat/lon based approach?
// https://api.weather.com/v3/wx/forecast/daily/5day?geocode=33.74,-84.39&format=json&units=e&language=en-US&apiKey=yourApiKey
$vars = array(
  'stationId' => '',
  'format' => 'json',
  'units' => 'e',
  'apiKey' => ''
);
 
$vars['apiKey'] = '9cf8f8f4b17c4d24b8f8f4b17c1d2443';
$vars['stationId'] = 'KVACROZE25';

// get 7 day history
$feed_url = url($uri, array('query' => $vars));
// get 5 day forecast
//$feed_url = url($furi, array('query' => $vars));
echo "Trying ... " . $feed_url . "\n";
$feed = file_get_contents($feed_url);

$data = json_decode($feed);

error_log("JSON:" . print_r($data,1));
// create function dh_weather_wund2dh($wunder_rec, bundle = 'weather_sensor', ftype = '') returns a dh_timeseries_weather

?>