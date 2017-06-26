<?php

$plugin = array(
  'label' => t('Retrieves data from NWS gridded precip data and inserts to dh_timeseries_weather.'),
  'handler' =>  array(
    'class' => 'DHNWSWeatherSource',
    'file' => 'DHNWSWeatherSource.class.php',
  ),
);
