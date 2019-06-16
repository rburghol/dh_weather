<?php

$uri = "https://api.weather.com/v2/pws/observations/hourly/7day";
$vars = array(
  'stationId' => '',
  'format' => 'json',
  'units' => 'e',
  'apiKey' => ''
);
 
$vars['apiKey'] = '9cf8f8f4b17c4d24b8f8f4b17c1d2443';
$vars['stationId'] = 'KVACROZE25';
 
$feed_url = url($uri, array('query' => $vars));
echo "Trying ... " . $feed_url . "\n";
$feed = file_get_contents($feed_url);

$data = json_decode($feed);

error_log("JSON:" . print_r($data,1));

?>