<?php

$plugin = array(
  'label' => t('Daily dH Weather Timeseries Summary'),
  'handler' =>  array(
    'class' => 'dHVPDailyWeatherSummary',
    'file' => 'dHVariableWeatherSummaries.class.php',
  ),
);

?>