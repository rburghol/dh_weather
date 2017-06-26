<?php
module_load_include('inc', 'dh', 'plugins/dh.display');
module_load_include('module', 'dh');

class dHVPWeatherSummary extends dHVariablePluginDefault {
  
  public function summarizeTimePeriod($entity_type, $featureid, $varkey, $begin, $end) {
    $begin = dh_handletimestamp($begin);
    $end = dh_handletimestamp($end);
    $varid = implode(',', dh_varkey2varid($varkey));
    $q = "  select featureid, entity_type, ";
    $q .= "   $begin as tstime, $end as tsendtime, avg(temp) as temp, ";
    $q .= "   sum(wet_time) as wet_time, avg(rh) as rh, sum(rain) as rain, ";
    //$q .= "   avg(wind_spd), avg(wind_dir), CASE sum(solar_rad), avg(pres), avg(dew_pt),  ";
    $q .= "   min(tmin) as tmin, max(tmax) as tmax ";
    $q .= " from {dh_timeseries_weather}  ";
    $q .= " where featureid = $featureid ";
    $q .= "   and entity_type = '$entity_type' ";
    $q .= "   and tstime >= $begin ";
    $q .= "   and tstime < $end ";
    $q .= "   and varid = $varid ";
    $q .= " group by featureid, entity_type ";
    //dpm($q,'query');
    $result = db_query($q);
    $record = $result->fetchAssoc();
    return $record;
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


class dHVPDailyWeatherSummary extends dHVPWeatherSummary {
  // @todo: enable t() for varkey, for example, this is easy, but need to figure out how to 
  //        handle in views - maybe a setting in the filter or jumplists itself?
  //  default: agchem_apply_fert_ee
  //       fr: agchem_apply_fert_fr 
  var $realtime_varkey = 'weather_obs';
  var $darkness_varkey = 'weather_daily_dark_sum';
  
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
  
  public function summarizeDarknessTimePeriod($entity) {
    // $entity is the dh_timeseries_weather entity in question
    $starthour = 21; // 9 pm, could later calculate this algorithmically based on julian day
    $endhour = 5; // a summertime default
    list($yesteryear, $yestermonth, $yesterday) = explode ('-', date('Y-m-d', (dh_handletimestamp($entity->tstime) - 86400)));
    //dpm(array($yesteryear, $yestermonth, $yesterday),'yesterday');
    $begin = implode('-', array($yesteryear, $yestermonth, $yesterday)) . " $starthour:00:00";
    $end = date('Y-m-d', dh_handletimestamp($entity->tstime)) . " $endhour:00:00";
    //dpm('range'," $begin, $end");
    $summary = $this->summarizeTimePeriod($entity->entity_type, $entity->featureid, $this->realtime_varkey, $begin, $end);
    $varids = dh_varkey2varid($this->darkness_varkey);
    //dpm($varids, $this->darkness_varkey);
    $summary['varid'] = array_shift( $varids);
    return $summary;
  }

  public function save(&$entity){
    // save a summary of nighttime periods
    $summary = $this->summarizeDarknessTimePeriod($entity);
    //dpm($summary,'summary at save()');
    if (is_array($summary)) {
      dh_update_timeseries_weather($summary, 'tstime_enddate_singular');
    }
  }

}
?>