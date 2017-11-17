<?php

$plugin = array(
  'label' => t('Last 24 Hours dH Weather Timeseries Summary'),
  'handler' =>  array(
    'class' => 'dHVPLast24Weather',
    'file' => 'dHVariableWeatherSummaries.class.php',
  ),
);

?>