<?php
module_load_include('inc', 'dh', 'plugins/dh.display');
module_load_include('module', 'dh');

class dHVPWeatherSummary extends dHVariablePluginDefault {
  var $obs_varkey = 'weather_obs';
  var $last24_varkey = 'weather_pd_last24hrs';
  var $darkness_varkey = 'weather_daily_dark_sum';
  var $daily_varkey = 'weather_obs_daily_sum';
  var $weekly_varkey = 'weather_obs_weekly_sum';
  var $monthly_varkey = 'weather_obs_monthly_sum';
  
  public function summarizeTimePeriod($entity_type, $featureid, $varkey, $begin, $end) {
    $begin = dh_handletimestamp($begin);
    $end = dh_handletimestamp($end);
    $varid = implode(',', dh_varkey2varid($varkey));
    $q = "  select featureid, entity_type, ";
    $q .= "   $begin as tstime, $end as tsendtime, avg(temp) as temp, ";
    $q .= "   sum(wet_time) as wet_time, avg(rh) as rh, sum(rain) as rain, ";
    //$q .= "   avg(wind_spd), avg(wind_dir), CASE sum(solar_rad), avg(pres), avg(dew_pt),  ";
    // we do this on the temp column instead of tmax and tmin because these are not always available on realtime recs
    //$q .= "   min(tmin) as tmin, max(tmax) as tmax ";
    $q .= "   min(temp) as tmin, max(temp) as tmax ";
    $q .= " from {dh_timeseries_weather}  ";
    $q .= " where featureid = $featureid ";
    $q .= "   and entity_type = '$entity_type' ";
    $q .= "   and tstime >= $begin ";
    $q .= "   and tstime < $end ";
    $q .= "   and varid = $varid ";
    $q .= " group by featureid, entity_type ";
    //dpm($q,"query for var = $varkey");
    $result = db_query($q);
    $record = $result->fetchAssoc();
    return $record;
  }
  
  public function darkVarinfo(&$entity){
    $starthour = 21; // 9 pm, could later calculate this algorithmically based on julian day
    $endhour = 5; // a summertime default
    list($yesteryear, $yestermonth, $yesterday) = explode ('-', date('Y-m-d', (dh_handletimestamp($entity->tstime) - 86400)));
    $begin = implode('-', array($yesteryear, $yestermonth, $yesterday)) . " $starthour:00:00";
    $end = date('Y-m-d', dh_handletimestamp($entity->tstime)) . " $endhour:00:00";
    $varids = dh_varkey2varid($this->darkness_varkey);
    $darkinfo = array(
      'featureid' => $entity->featureid,
      'tstime' => dh_handletimestamp($begin),
      'tsendtime' => dh_handletimestamp($end),
      'entity_type' => $entity->entity_type,
      'varid' => array_shift( $varids),
    );
    return $darkinfo;
  }
  
  public function summarizeDaily($entity) {
    // $entity is the dh_timeseries_weather entity in question
    $date = date('Y-m-d', dh_handletimestamp($entity->tstime));
    $begin = $date . " 00:00:00";
    $end = $date . " 23:59:59";
    //dpm('range'," $begin, $end");
    $summary = $this->summarizeTimePeriod($entity->entity_type, $entity->featureid, $this->obs_varkey, $begin, $end);
    $varids = dh_varkey2varid($this->daily_varkey);
    //dpm($varids, $this->daily_varkey);
    $summary['varid'] = array_shift( $varids);
    return $summary;
  }
  
  public function summarizeWeekly($entity) {
    // $entity is the dh_timeseries_weather entity in question
    $ts = dh_handletimestamp($entity->tstime);
    $day = date('w', $ts);
    $begin = date('Y-m-d', $ts - $day * 86400);
    $end = date('Y-m-d', $ts + (7-$day) * 86400);
    //dpm('range'," $begin, $end");
    $summary = $this->summarizeTimePeriod($entity->entity_type, $entity->featureid, $this->obs_varkey, $begin, $end);
    //dpm($varids, $this->daily_varkey);
    $summary['varid'] = dh_varkey2varid($this->weekly_varkey, TRUE);
    return $summary;
  }
  
  public function summarizeMonthly($entity) {
    // $entity is the dh_timeseries_weather entity in question
    $ts = dh_handletimestamp($entity->tstime);
    $begin = date('Y-m', $ts) . '-01';
    $end = date('Y-m', $ts) . '-' . date("t", $ts);
    //dpm('range'," $begin, $end");
    $summary = $this->summarizeTimePeriod($entity->entity_type, $entity->featureid, $this->obs_varkey, $begin, $end);
    //dpm($varids, $this->daily_varkey);
    $summary['varid'] = dh_varkey2varid($this->monthly_varkey, TRUE);
    //dpm($summary, 'summarizeMonthly');
    return $summary;
  }
  
  public function summarizeDarknessTimePeriod($entity) {
    // $entity is the dh_timeseries_weather entity in question
    $starthour = 21; // 9 pm, could later calculate this algorithmically based on julian day
    $endhour = 5; // a summertime default
    list($yesteryear, $yestermonth, $yesterday) = explode ('-', date('Y-m-d', (dh_handletimestamp($entity->tstime) - 86400)));
    //dpm(array($yesteryear, $yestermonth, $yesterday),'yesterday');
    $begin = implode('-', array($yesteryear, $yestermonth, $yesterday)) . " $starthour:00:00";
    $end = date('Y-m-d', dh_handletimestamp($entity->tstime)) . " $endhour:00:00";
    //dpm('range'," $begin, $end");
    $summary = $this->summarizeTimePeriod($entity->entity_type, $entity->featureid, $this->obs_varkey, $begin, $end);
    $varids = dh_varkey2varid($this->darkness_varkey);
    //dpm($varids, $this->darkness_varkey);
    $summary['varid'] = array_shift( $varids);
    return $summary;
  }
}

// Daily Weather Summary including:
// * Daily averages
// * Night time conditions
// we could employ 2 strategies here:
//   1. Recalculate this as a result of insert/update of a realtime observation
//   2. Recalculate this as a result of explicit call to Daily Summary
// Method #1 would eliminate any potential for the summary to be out of synch with the observed
// Method #2 would be more time efficient, since method #1 would execute 96 times per day on 15 min data
// Also, this could be linked as part of the save() event of realtime data if we desired essentially 
// merging #1 and #2.  So #2 is best for now.
// don't recalculate if the given timestamp is not inside the darkness window
// this will reduce redundant saves
/* 
// Method #1 - run as plugin on realtime variable
list($year, $month, $day, $hour) = explode ('-', date('Y-m-d', $tstime));
list($year, $month, $day, $hour) = explode ('-', date('Y-m-d', $tstime));
if ( ($hour <= $endhour) or ($hour >= $starthour) ) {
  return FALSE;
}
if ($hour <= $endhour) {
  list($yesteryear, $yestermonth, $yesterday) = explode ('-', date('Y-m-d', ($tstime - 86400)));
}
if ($hour >= $starthour) {
  list($morrowyear, $morrowmonth, $morrowday) = explode ('-', date('Y-m-d', ($tstime + 86400)));
}
*/

// Method #2 - run as summary that looks at other variable
// See below


class dHVPLast24Weather extends dHVPWeatherSummary {
  // Create a Most recent data summary
  var $obs_varkey = 'weather_obs';
  var $last24_varkey = 'weather_pd_last24hrs';
  var $darkness_varkey = 'weather_daily_dark_sum';
  var $ok_cols = array('tstime', 'tsendtime', 'temp', 'wet_time', 'rh', 'rain', 'wind_spd', 'wind_dir', 'solar_rad', 'pres', 'dew_pt', 'tmin', 'tmax');
  
  public function __construct($conf = array()) {
    parent::__construct($conf);
    $hidden = array('tid', 'tstime', 'tsendtime', 'featureid', 'entity_type', 'bundle');
    foreach ($hidden as $hide_this) {
      $this->property_conf_default[$hide_this]['hidden'] = 1;
    }
  }
  
  public function formRowEdit(&$rowform, $entity) {
    // apply custom settings here
    parent::formRowEdit($rowform, $entity);
  }
  
  public function summarizeLast24Hours($entity) {
    // $entity is the dh_timeseries_weather entity in question
    $varids = dh_varkey2varid($this->obs_varkey);
    //dpm($varids, $this->darkness_varkey);
    $varid = array_shift( $varids);
    $end = db_query("select max(tstime) from dh_timeseries_weather where featureid = :fid  and varid = :varid", array(':fid' => $entity->featureid, ':varid' => $varid))->fetchField();
    $begin = $end - 86400;
    //dpm('range'," $begin, $end");
    $summary = $this->summarizeTimePeriod($entity->entity_type, $entity->featureid, $this->obs_varkey, $begin, $end);
    $varids = dh_varkey2varid($this->last24_varkey);
    //dpm($varids, $this->darkness_varkey);
    $summary['varid'] = array_shift( $varids);
    return $summary;
  }

  public function save(&$entity){
    // Find the last 24 hours in this entities time series
    $summary = $this->summarizeLast24Hours($entity);
    if (empty($summary)) {
      return;
    }
    // update this entity (ts weather) to have the last 24 data in it
    foreach($summary as $key => $val) {
      if (in_array($key, $this->ok_cols)) {
        $entity->{$key} = $val;
      }
    }
    // this entity will automatically be saved 
    // save a summary of nighttime periods
    $summary = $this->summarizeDarknessTimePeriod($entity);
    if (is_array($summary)) {
      dh_update_timeseries_weather($summary, 'tstime_enddate_singular');
      $local_summary = array(
        'entity_type' => 'dh_timeseries_weather', 
        'featureid' => $entity->tid, 
        'bundle' =>'dh_properties',
        'propvalue' => $summary['wet_time'] / 60.0,
        'propname' => 'wet_hrs',
      ) + $summary;
      //dpm($local_summary,'local sum');
      dh_update_properties($local_summary, 'name');
      $local_summary = array(
        'entity_type' => 'dh_timeseries_weather', 
        'featureid' => $entity->tid, 
        'bundle' =>'dh_properties',
        'propvalue' => 32.0 + $summary['temp'] * 9.0 / 5.0,
        'propname' => 'temp_f',
      ) + $summary;
      //dpm($local_summary,'local sum');
      dh_update_properties($local_summary, 'name');
      $local_summary = array(
        'entity_type' => 'dh_timeseries_weather', 
        'featureid' => $entity->tid, 
        'bundle' =>'dh_properties',
        'propvalue' => $summary['rh'],
        'propname' => 'rh',
      ) + $summary;
      dh_update_properties($local_summary, 'name');
    }
  }
}


class dHVPDailyWeatherSummary extends dHVPWeatherSummary {
  // @todo: enable t() for varkey, for example, this is easy, but need to figure out how to 
  //        handle in views - maybe a setting in the filter or jumplists itself?
  //  default: agchem_apply_fert_ee
  //       fr: agchem_apply_fert_fr 
  // test with: http://www.grapeipm.org/d.dev/admin/content/dh_timeseries_weather/manage/44967169 
  
  public function __construct($conf = array()) {
    parent::__construct($conf);
    $hidden = array('tid', 'tstime', 'tsendtime', 'featureid', 'entity_type', 'bundle');
    foreach ($hidden as $hide_this) {
      $this->property_conf_default[$hide_this]['hidden'] = 1;
    }
  }
  
  public function formRowEdit(&$rowform, $entity) {
    // apply custom settings here
    //dpm($entity,'entity');
    $summary = $this->summarizeDarknessTimePeriod($entity);
    //dpm($summary,'summary');
    //dpm($rowform,'form');
    //dpm(date_default_timezone_get(),'date_default_timezone_get()');
    // @todo: include a select list in edit form to allow user to force over-ride the summary
    //        otherwise, the default behavior on save will be to derive the summary from the weather_obs
    //        data in the database table.
    parent::formRowEdit($rowform, $entity);
  }
  
  public function formRowSave(&$rowvalues, &$row) {
    //dpm($rowvalues,'values');
    parent::formRowSave($rowvalues, $row);
  }

  public function save(&$entity){
    // save a summary of the whole day
    // @todo: we need to do a check here in the event that 
    //   we are forcing an override of the daily summary
    //   perhaps we could set a switch on the form handler to add a property to the object?
    //   For now it forces over-write any time a save is done regardless of data submitted
    
    // Check to make sure this is not plugged into something other than observed by mistake
    $varids = dh_varkey2varid($this->obs_varkey);
    $obs_varid = array_shift( $varids);
    // this check is obsolete now that the summary is triggered standalone instead
    // of at every single 15 minute save
    //if ($entity->varid <> $obs_varid) {
    //  return;
    //}
    $weekly = $this->summarizeWeekly($entity);
    $tid = dh_update_timeseries_weather($weekly, 'tspan_singular');
    $weekly['tid'] = $tid;
    //dpm($weekly,'weekly sum');
    $monthly = $this->summarizeMonthly($entity);
    $tid = dh_update_timeseries_weather($monthly, 'tspan_singular');
    $monthly['tid'] = $tid;
    //dpm($monthly,'monthly sum');
    $summary = $this->summarizeDaily($entity);
    //dpm($summary,'summary at save()');
    // apply summary values to entity properties and they will get saved by the controller
    if (is_array($summary)) {
      // we need to
      $vars = array('tstime', 'tsendtime', 'temp', 'wet_time', 'rh', 'rain', 'wind_spd', 'wind_dir', 'solar_rad', 'pres', 'dew_pt', 'tmin', 'tmax');
      foreach ($summary as $key => $val) {
        if (in_array($key, $vars)) {
          $entity->$key = $val;
        }
      }
    }
    //dpm($entity,'entity');
    // save a summary of nighttime periods
    $summary = $this->summarizeDarknessTimePeriod($entity);
    //dpm($summary,'dark summary at save()');
    if (is_array($summary)) {
      dh_update_timeseries_weather($summary, 'tstime_enddate_singular');
      $local_summary = array(
        'entity_type' => 'dh_timeseries_weather', 
        'featureid' => $entity->tid, 
        'bundle' =>'dh_properties',
        'propvalue' => $summary['wet_time'] / 60.0,
        'propname' => 'wet_hrs',
      ) + $summary;
      //dpm($local_summary,'local sum');
      dh_update_properties($local_summary, 'name');
      $local_summary = array(
        'entity_type' => 'dh_timeseries_weather', 
        'featureid' => $entity->tid, 
        'bundle' =>'dh_properties',
        'propvalue' => 32.0 + $summary['temp'] * 9.0 / 5.0,
        'propname' => 'temp_f',
      ) + $summary;
      //dpm($local_summary,'local sum');
      dh_update_properties($local_summary, 'name');
      $local_summary = array(
        'entity_type' => 'dh_timeseries_weather', 
        'featureid' => $entity->tid, 
        'bundle' =>'dh_properties',
        'propvalue' => $summary['rh'],
        'propname' => 'rh',
      ) + $summary;
      //dpm($local_summary,'rh sum');
      //dpm($summary,'summary');
      dh_update_properties($local_summary, 'name');
    }
  }
  
  public function buildContent(&$content, &$entity, $view_mode) {
    // @todo: handle teaser mode and full mode with plugin support
    //        this won't happen till we enable at module level however, now it only 
    //        is shown when selecting "plugin" in the view mode in views
    $content['#view_mode'] = $view_mode;
    // hide all to begin then allow individual mode to control visibility
    $hidden = array('varname', 'tstime', 'tid', 'tsvalue', 'tscode', 'entity_type', 'featureid', 'tsendtime', 'modified', 'label', 'rain', 'rh', 'tmin', 'tmax', 'wet_time');
    foreach ($hidden as $col) {
      $content[$col]['#type'] = 'hidden';
    }
    
    //$summary = $this->summarizeDarknessTimePeriod($entity);
    //dpm($summary,'summary realtime');
    $sumrecs = dh_get_timeseries_weather($this->darkVarinfo($entity));
    if (isset($sumrecs['dh_timeseries_weather'])) {
      //dpm($result,"found records - checking singularity settings");
      $data = entity_load('dh_timeseries_weather', array_keys($summary['dh_timeseries_weather']));
      $summary = array_shift($data);
    }
    //dpm($summary,'summary saved');
    // @todo: fix this up 
    $uri = "ipm-live-events/$vineyard/sprayquan/$feature->adminid";
    $link = array(
      '#type' => 'link',
      '#prefix' => '&nbsp;',
      '#suffix' => '<br>',
      '#title' => 'Go to ' . $uri,
      '#href' => $uri,
      'query' => array(
        'finaldest' => 'ipm-home/all/all/',
      ),
    );
    switch ($view_mode) {
      case 'teaser':
      case 'summary':
      // summary is like teaser except that Drupal adds label as title to teaser regardless
        $content['last_updated'] = array(
          '#type' => 'item',
          '#markup' => date('Y-m-d h:m', $entity->tsendtime),
        );
        $header = array('Temp (lo / hi / dark)',	'RH',	'Wet hrs. (all/dark)');
        $rows = array(
          0=>array(
              round($entity->temp, 1) . '°F (' 
              . round($entity->tmax, 1) . ' / '
              . round($entity->tmin, 1) . ' / '
              . round( $summary->temp, 1) . ')'
            ,
            round($entity->rh,1) . ' %',
            round($entity->wet_time/60,1) . " / " . round($summary->wet_time/60.0,1)),
        );
        $content['table'] = array(
          '#theme' => 'table',
          '#header' => $header,
          '#rows' => $rows,
          '#attributes' => array (
            'class' => array('views-table', 'cols-3', 'table', 'table-hover', 'table-striped'),
          ),
        );
        $content['link'] = $link; 
        $content['modified']['#markup'] = '(modified on ' . date('Y-m-d', $entity->tstime) . ")"; 
      break;
      
      // @todo: develop weather summary suitable for iCal???
      case 'ical_summary':
        unset($content['title']['#type']);
        #$content['body']['#type']= 'item'; 
        $content['body']['#markup'] = $title; 
        $content = array();
      break;
      
      case 'full':
      case 'plugin':
      default:
      // @todo: what should the full view look like??
        $content['title'] = array(
          '#type' => 'item',
          '#markup' => $title,
        );         
        $content['blocks'] = array(
          '#type' => 'item',
          '#markup' => '<b>Blocks:</b> ' .implode(', ', $block_names),
        );         
        $content['materials'] = array(
          '#type' => 'item',
          '#markup' => '<b>Materials:</b> ' .implode(', ', $chem_names),
        );
        $content['link'] = $link; 
        $entity->title = $title;
        $content['modified']['#markup'] = '(modified on ' . date('Y-m-d', $feature->modified) . ")"; 
      break;
    }
  }
}

?>