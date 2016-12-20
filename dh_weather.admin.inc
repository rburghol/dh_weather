<?php

/**
 * @file
 * Model editing UI.
 *
 */
 
/**
 * UI controller.
 */
 
//****************************
// TimeSeries table
//****************************

class dHTimeSeriesWeatherTableUIController extends EntityDefaultUIController {
  public function hook_menu() {
    $items = parent::hook_menu();

    // Change the overview menu type for the list of models.
    $items[$this->path]['type'] = MENU_LOCAL_TASK;

	
  // Extend the 'add' path.
  $items[$this->path . '/add'] = array(
    'title' => 'Add Time Series Weather Data',
	'description' => 'A form to enter Time Series Weather data',
    'page callback' => 'entity_ui_get_bundle_add_form',
    'page arguments' => array($this->entityType,$this->id_count + 1),
    'access callback' => 'entity_access',
    'access arguments' => array('create', $this->entityType),
    'type' => MENU_LOCAL_ACTION,
  );
      
    return $items;
  }	
}


function dh_weather_field_default_field_instances() {
  $field_instances = array();

  // Exported field_instance: 'om_data_table-om_data_table-field_om_data_starttime'
  $field_instances['dh_timeseries_weather-tstime'] = array(
    'bundle' => 'dh_timeseries_weather',
    'deleted' => 0,
    'description' => '',
    'display' => array(
      'default' => array(
        'label' => 'above',
        'module' => 'date',
        'settings' => array(
          'format_type' => 'long',
          'fromto' => 'both',
          'multiple_from' => '',
          'multiple_number' => '',
          'multiple_to' => '',
        ),
        'type' => 'date_default',
        'weight' => 1,
      ),
      'teaser' => array(
        'label' => 'above',
        'settings' => array(),
        'type' => 'hidden',
        'weight' => 0,
      ),
    ),
    'entity_type' => 'dh_timeseries_weather',
    'field_name' => 'tstime',
    'label' => 'Time',
    'required' => 0,
    'settings' => array(
      'default_value' => 'now',
      'default_value2' => 'same',
      'default_value_code' => '',
      'default_value_code2' => '',
      'user_register_form' => FALSE,
    ),
    'widget' => array(
      'active' => 1,
      'module' => 'date',
      'settings' => array(
        'display_all_day' => 0,
        'increment' => 15,
        'input_format' => 'm/d/Y - H:i:s',
        'input_format_custom' => '',
        'label_position' => 'above',
        'text_parts' => array(),
        'year_range' => '-3:+3',
      ),
      'type' => 'date_text',
      'weight' => 1,
    ),
  );

  $field_instances['dh_timeseries_weather-tsendtime'] = array(
    'bundle' => 'dh_timeseries_weather',
    'deleted' => 0,
    'description' => '',
    'display' => array(
      'default' => array(
        'label' => 'above',
        'module' => 'date',
        'settings' => array(
          'format_type' => 'long',
          'fromto' => 'both',
          'multiple_from' => '',
          'multiple_number' => '',
          'multiple_to' => '',
        ),
        'type' => 'date_default',
        'weight' => 1,
      ),
      'teaser' => array(
        'label' => 'above',
        'settings' => array(),
        'type' => 'hidden',
        'weight' => 0,
      ),
    ),
    'entity_type' => 'dh_timeseries_weather',
    'field_name' => 'tsendtime',
    'label' => 'Time',
    'required' => 0,
    'settings' => array(
      'default_value' => 'now',
      'default_value2' => 'same',
      'default_value_code' => '',
      'default_value_code2' => '',
      'user_register_form' => FALSE,
    ),
    'widget' => array(
      'active' => 1,
      'module' => 'date',
      'settings' => array(
        'display_all_day' => 0,
        'increment' => 15,
        'input_format' => 'm/d/Y - H:i:s',
        'input_format_custom' => '',
        'label_position' => 'above',
        'text_parts' => array(),
        'year_range' => '-3:+3',
      ),
      'type' => 'date_text',
      'weight' => 1,
    ),
  );
  // Included for use with string extractors like potx.
  t('Start Time');

  return $field_instances;
}


// FORM Stuff

/**
 * dH Time Series editing form.
 */

function dh_timeseries_weather_form($form, &$form_state, $dh_timeseries_weather, $op = 'edit') {
  if ($op == 'clone') {
    $dh_timeseries_weather->name .= ' (cloned)';
    $dh_timeseries_weather->bundle = '';
  }

/*  $form['tid'] = array(
    '#title' => t('Time Series ID'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->tid,
    '#description' => t('Primary key for Variables'),
    '#required' => TRUE,
    '#size' => 64,
  );*/

  $form['tstime'] = array(
    '#title' => t('TimeSeries Time'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->tstime,
    '#description' => t('Timestamp for monitored value.'),
    '#required' => TRUE,
    '#size' => 64,
  );
  
  $form['utcoffset'] = array(
    '#title' => t('UTC Offset'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->utcoffset,
    '#description' => t('UTC Offset for timestamp.'),
    '#required' => FALSE,
    '#size' => 64,
  );
  
  $form['temp'] = array(
    '#title' => t('Temperature (C)'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->temp,
    '#description' => t('Temperature in degrees C.'),
    '#required' => FALSE,
    '#size' => 16,
  );
  
  $form['tmin'] = array(
    '#title' => t('Temperature (C)'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->tmin,
    '#description' => t('Min Temperature in degrees C.'),
    '#required' => FALSE,
    '#size' => 16,
  );
  
  $form['tmax'] = array(
    '#title' => t('Temperature (C)'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->tmax,
    '#description' => t('Max Temperature in degrees C.'),
    '#required' => FALSE,
    '#size' => 16,
  );
  
  $form['wet_time'] = array(
    '#title' => t('Wet Time'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->wet_time,
    '#description' => t('The duration of wetness during interval (min).'),
    '#required' => FALSE,
    '#size' => 16,
  );
  
  $form['rh'] = array(
    '#title' => t('RH Value'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->rh,
    '#description' => t('Relative Humidity.'),
    '#required' => FALSE,
    '#size' => 16,
  );
  
  $form['precip'] = array(
    '#title' => t('Precipiation'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->precip,
    '#description' => t('Precipiation (mm).'),
    '#required' => FALSE,
    '#size' => 16,
  );
  
  $form['wind_spd'] = array(
    '#title' => t('Wind Speed'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->wind_spd,
    '#description' => t('Wind Speed.'),
    '#required' => FALSE,
    '#size' => 16,
  );
  
  $form['wind_dir'] = array(
    '#title' => t('Wind Direction'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->wind_dir,
    '#description' => t('Wind Direction.'),
    '#required' => FALSE,
    '#size' => 16,
  );
  
  $form['solar_rad'] = array(
    '#title' => t('Precipiation'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->precip,
    '#description' => t('Precipiation (mm).'),
    '#required' => FALSE,
    '#size' => 16,
  );
  
  $form['tsendtime'] = array(
    '#title' => t('Period End'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->tsendtime,
    '#description' => t('Period End (for time spans).'),
    '#required' => FALSE,
    '#size' => 64,
  );
  
  $form['dew_pt'] = array(
    '#title' => t('Dew Point Temp (C)'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->dew_pt,
    '#description' => t('Dew Point Temp (C)'),
    '#required' => FALSE,
    '#size' => 16,
  );
  
  $form['pres'] = array(
    '#title' => t('Atmospheric Pressure (Pa)'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->pres,
    '#description' => t('Atmospheric Pressure'),
    '#required' => FALSE,
    '#size' => 16,
  );
  
  $form['featureid'] = array(
    '#title' => t('Feature ID'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->featureid,
    '#description' => t('Unique numerical identifier of a feature within the geodatabase. Usually, the FeatureID of a TimeSeries record is equal to the HydroID of a feature (e.g., well, monitoring point).'),
    '#required' => TRUE,
    '#size' => 64,
  );   
  
  $form['varid'] = array(
    '#title' => t('Variable ID'),
    '#type' => 'textfield',
    '#default_value' => $dh_timeseries_weather->varid,
    '#description' => t('Unique numerical identifier of a variable within the geodatabase. Variables are defined in the VariableDefinition table.'),
    '#required' => TRUE,
    '#size' => 255,
  );   
    
  $form['actions'] = array('#type' => 'actions');
  $form['actions']['submit'] = array(
    '#type' => 'submit',
    '#value' => t('Save Time Series data'),
    '#weight' => 40,
  );
  return $form;
}

/* Form API submit callback for the type form.
 */
function dh_timeseries_weather_form_submit(&$form, &$form_state) {
  $dh_timeseries_weather = entity_ui_form_submit_build_entity($form, $form_state);
  $dh_timeseries_weather->save();
  $form_state['redirect'] = 'admin/content/dh_timeseries_weather/';
}

?>