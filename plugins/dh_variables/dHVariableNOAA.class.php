<?php
module_load_include('inc', 'dh', 'plugins/dh.display');
module_load_include('module', 'dh');
module_load_include('module', 'dh_wsp');
// make sure that we have base plugins 
$plugin_def = ctools_get_plugins('dh', 'dh_variables', 'dHVarWithTableFieldBase');
$class = ctools_plugin_get_class($plugin_def, 'handler');

class dHNOAASummary extends dHVarWithTableFieldBase {
  public function summarizeTimePeriod($entity_type, $featureid, $varkey, $begin, $end) {
    
  }
  
  public function hiddenFields() {
    return array('pid', 'varid', 'featureid', 'entity_type', 'bundle' );
  }
  
  public function tableDefault($entity) {
    // Returns associative array keyed table (like is used in OM)
    // This format is not used by Drupal however, so a translation 
    //   with tablefield_parse_assoc() is usually in order (such as is done in load)
    // set up defaults - we can sub-class this to handle each version of the model land use
    // This version is based on the Chesapeake Bay Watershed Phase 5.3.2 model land uses
    // this brings in an associative array keyed as $table[$luname] = array( $year => $area )
    $table = array();
    $table[0] = array('mon', 'nml_daily', 'obs_daily', 'nml', 'obs', 'days', 'modays');
    for ($mon = 1; $mon <= 12; $mon++) {
      $modate = strtotime("2000/$mon/01");
      $nml = 0.0;
      $nml_daily = 0.0;
      $obs = 0.0;
      $obs_daily = 0.0;
      $days = ($mon == 2) ? 28.25 : date('t', $modate);
      $modays = ($mon == 2) ? 28.25 : date('t', $modate);
      $table[$mon] = array(
        $mon, $nml_daily, $obs_daily, $nml, $obs, $days, $modays 
      );
    }
    return $table;
  }

}

class dHNOAAMonthlySummary extends dHNOAASummary {
  var $srckey = 'noaa_precip_raster';
  
  public function create(&$entity) {
    // set up defaults?
    if ($this->default_bundle) {
      $entity->bundle = $this->default_bundle;
    }
    if ($entity->propcode <> 'manual') {
      $tbl = $this->summarizeTimePeriodByMonth($entity, $this->srckey, '2017-10-01', '2018-09-30');
      $this->setCSVTableField($entity, $tbl);
    }
  }
  
  public function insert(&$entity) {
    if ($entity->propcode == 'refresh') {
      $tbl = $this->summarizeTimePeriodByMonth($entity, $this->srckey, '2017-10-01', '2018-09-30');
      dsm('Refreshing monthly period summary from NOAA precip rasters.');
      $this->setCSVTableField($entity, $tbl);
    }
  }
  
  public function save(&$entity) {
    // set up defaults?
    if ($this->default_bundle) {
      $entity->bundle = $this->default_bundle;
    }
    if ($entity->propcode <> 'manual') {
      $tbl = $this->summarizeTimePeriodByMonth($entity, $this->srckey, '2017-10-01', '2018-09-30');
      dsm('Refreshing monthly period summary from NOAA precip rasters.');
      $this->setCSVTableField($entity, $tbl);
    }
  }
	
  public function formRowEdit(&$rowform, $entity) {
    // call parent class to insure proper bundle and presence of tablefield
    //dpm($entity,'ent');
    parent::formRowEdit($rowform, $entity);
    if ( ($entity->is_new === TRUE) and ($entity->propcode <> 'manual')) {
      //dpm($entity->is_new,'ent->is_new');
      $tbl = $this->summarizeTimePeriodByMonth($entity, $this->srckey, '2017-10-01', '2018-09-30');
      $this->setCSVTableField($entity, $tbl);
    }
    $rowform[$this->matrix_field]['#description'] = t('Monthly Trends');
	  $opts = array(
	    'automatic' => 'Automatic (from NOAA db at create)',
	    'refresh' => 'Refresh on Save (from NOAA db)',
      'manual' => 'Manual',
    );
    $rowform['startdate']['#title'] = t('Period Beginning');
    $rowform['enddate']['#title'] = t('Period Ending');
    $rowform['propcode'] = array(
      '#title' => t('Summary Data Entry Mode'),
      '#type' => 'select',
      '#options' => $opts,
      '#default_value' => $entity->propcode,
      '#size' => 1,
      '#weight' => -1,
	    '#description' => 'Select Manual to set custom values for these period averages',
    );
  }
  
  public function summarizeTimePeriodByMonth($entity, $varkey, $begin, $end) {
    $varid = implode(',', dh_varkey2varid($varkey));
    $begin = dh_handletimestamp($begin);
    $end = dh_handletimestamp($end);
    $q = "  select hydroid, name, mo, sum(obs) as obs, ";
    $q .= "    max(nml) as nml_precip_in, count(mo) as numdays ";
    $q .= " from ( ";
    $q .= "   select a.hydroid, a.name,  ";
    $q .= "     to_timestamp(c.tstime) as tstime,  ";
    $q .= "     to_timestamp(c.tsendtime) as tsendtime,  ";
    $q .= "     extract(month from to_timestamp(c.tstime)) as mo,  ";
    $q .= "    (ST_summarystats(st_clip(c.rast, b.dh_geofield_geom, 0.0), 1, TRUE)).mean as obs,  ";
    $q .= "     (ST_summarystats(st_clip(c.rast, b.dh_geofield_geom, 0.0), 2, TRUE)).mean as nml,  ";
    $q .= "     (ST_summarystats(st_clip(c.rast, b.dh_geofield_geom, 0.0), 2, TRUE)).count as num ";
    $q .= "     from dh_feature as a  ";
    $q .= "     left outer join field_data_dh_geofield as b  ";
    $q .= "     on ( ";
    $q .= "       entity_id = a.hydroid    ";
    $q .= "         and b.entity_type = 'dh_feature'  ";
    $q .= "     ) ";
    $q .= "     left outer join dh_timeseries_weather as c  ";
    $q .= "     on (  ";
    $q .= "       c.varid in (select hydroid from dh_variabledefinition where varkey = '$varkey' )  ";
    $q .= "     ) ";
    $q .= "     where a.hydroid in ( $entity->featureid) ";
    $q .= "     and c.featureid in ( ";
    $q .= "       select hydroid  ";
    $q .= "       from dh_feature  ";
    $q .= "       where hydrocode = 'vahydro_nws_precip_grid'  ";
    $q .= "         and bundle = 'landunit'  ";
    $q .= "         and ftype = 'nws_precip_grid'  ";
    $q .= "     )  ";
    $q .= "     and (ST_summarystats(st_clip(c.rast, b.dh_geofield_geom), 2, TRUE)).min >= 0 ";
    $q .= "     and c.tstime >= $begin ";
    $q .= "     and c.tstime <= $end ";
    $q .= "   order by c.tstime ";
    $q .= " ) as foo  ";
    $q .= " group by hydroid, name, mo ";
    $q .= " order by hydroid, mo ";
    //dpm($q,"Query");
    $result = db_query($q);
    $table = $this->tableDefault(FALSE);
    $total = 0.0;
    while ($record = $result->fetchAssoc()) {
      $mon = $record['mo'];
      $modate = strtotime("2000/$mon/01");
      $modays = $table[$mon][6];
      $days = $record['numdays'];
      $nml_daily = $record['nml_precip_in'];
      $nml = $modays * $nml_daily;
      $total += $nml;
      $obs = 0.0;
      $obs_daily = $record['obs'] / $modays;
      $table[$mon] = array(
        $mon, $nml_daily, $obs_daily, $nml, $obs, $days, $modays 
      );
    }
    $entity->propvalue = $total;
    return $table;
  }
}

?>