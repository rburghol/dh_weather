<?php

$plugin = array(
  'label' => t('NOAA Normal Precip Summary'),
  'handler' =>  array(
    'class' => 'dHNOAAMonthlySummary',
    'file' => 'dHVariableNOAA.class.php',
  ),
);

?>