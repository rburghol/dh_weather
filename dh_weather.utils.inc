<?php

global $dhw_entityreferences;
$dhw_entityreferences = array(
  'weather_station' => array('user' => 'dh_link_feature_mgr_id'),
  'weather_sensor' => array(
    'weather_station' => 'dh_link_station_sensor', 
    'user' => 'dh_link_feature_mgr_id'
  ),
  'om_model' => array(
    'weather_station' => 'dh_om_link_model_feature', 
    'user' => 'dh_link_feature_mgr_id',
    'weather_sensor' => 'dh_om_link_model_feature',
  ),
);

// **************************************************
// ***********    dHW Tree Functions    *************
// **  return array of form
// **  array(
// **   'weather_station' => array('ids' => array()), 
// **   'weather_sensor' => array('ids' => array()),
// **   'om_model' => array('ids' => array()), 
// **   'user' => array('ids' => array()), 
// **  );
// **************************************************


// Weather Station/Sensor/Model Tree by user
// Usage in Contextual Filter Validation PHP:
//   module_load_include('inc', 'dh_weather', 'dh_weather.utils');
//   $models = dh_weather_usertree($uid);
//   $handler->argument = implode($models, '+');
//   return true;
function dh_weather_usertree($uid) {
  global $dhw_entityreferences;
  // query for user, then station, then sensor, then model links
  // foreach as $bundle => $target => $eref_col
  $etype = 'dh_feature';
  $bundle = 'weather_station';
  $target_type = 'user';
  $target_id = $uid;
  $target_pk = 'hydroid';
  $ret = array(
    'weather_station'=>array('ids'=>array()),
    'weather_sensor'=>array('ids'=>array()),
    'om_model'=>array('ids'=>array()),
    'user'=>array('ids'=>array($uid)),
  );
  $models = array();
  $sensors = array();
  $ret = dh_weather_tree_query($etype, $bundle, $target_type, $target_pk, $target_id);
  //drupal_set_message("result of dh_weather_tree_query($etype, $bundle, $target_type, $target_id) " . print_r($ret ,1) );
  $stations = $ret['weather_station'];
  foreach ($stations['ids'] as $sta) {
    $sens = dh_weather_tree_query($etype, 'weather_sensor', 'weather_station', $target_pk, $sta);
    foreach ($sens as $thissen) {
      $sensors = array_merge($sensors, $thissen['ids']);
      foreach ($thissen['ids'] as $thisid) {
        $mods = dh_weather_tree_query($etype, 'om_model', 'weather_sensor', $target_pk, $thisid);
        foreach ($mods as $thismod) {
          $models = array_merge($models, $thismod['ids']);
        }
      }
    }
  }
  $ret['om_model']['ids'] = $models;
  $ret['weather_station']['ids'] = $stations;
  $ret['weather_sensor']['ids'] = $sensors;
  //drupal_set_message("Models: " . print_r($ret,1));
  return $ret;
}
  
function dh_weather_tree_query($etype, $bundle, $target_type, $target_pk, $target_id) {
  global $dhw_entityreferences;
  $ret = array();
  $efq = new EntityFieldQuery(); 
  $efq->entityCondition('entity_type', $etype);
  $efq->entityCondition('bundle', $bundle);
  $refname = $dhw_entityreferences[$bundle][$target_type];
  $efq->fieldCondition($refname, 'target_id', $target_id, '=');
  $ret[$bundle]['ids'] = array();
  $result = $efq->execute();
  //drupal_set_message("efq for [$bundle][$target_type] $target_id: " . print_r($result ,1) );
  if (isset($result[$etype])) {
    foreach ($result[$etype] as $rez) {
      if (property_exists($rez, $target_pk)) {
        $ret[$bundle]['ids'][] = $rez->$target_pk;
      } else {
        drupal_set_message("Prop $target_pk not on " . print_r((array)$rez,1));
      }
    }
  }
  return $ret;
}

function dh_weather_stationtree($featureid) {
  // query for station user links
  // query for station sensor links
  // query for sensor model links
  // return array of form
  // return array(
  //  'sensors' => array('ids' => array()),
  //  'models' => array('ids' => array()),
  //  'users' => array('ids' => array()),
  // );
}

function dh_weather_sensortree($featureid) {
  // query for station user links
  // query for station sensor links
  // query for sensor model links
  // return array of form
  // return array(
  //  'stations' => array('ids' => array()),
  //  'models' => array('ids' => array()),
  //  'users' => array('ids' => array()),
  // );
}

function dh_weather_modeltree($featureid) {
  // query for station user links
  // query for station sensor links
  // query for sensor model links
  // return array of form
  // return array(
  //  'stations' => array('ids' => array()),
  //  'sensors' => array('ids' => array()),
  //  'users' => array('ids' => array()),
  // );
}
?>