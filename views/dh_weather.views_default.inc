<?php

/**
 * Implements hook_views_default_views().
 */
function dh_weather_views_default_views() {
   $views = array();
   $views['dh_admin_content_properties_tsweather'] = dh_weather_tabbed_detail();
   return $views;
}

function dh_weather_tabbed_detail() {
  $view = new view();
  $view->name = 'dh_admin_content_properties_tsweather';
  $view->description = '';
  $view->tag = 'default';
  $view->base_table = 'dh_properties';
  $view->human_name = 'dH Admin Content Properties';
  $view->core = 7;
  $view->api_version = '3.0';
  $view->disabled = FALSE; /* Edit this to true to make a default view disabled initially */

  /* Display: Master */
  $handler = $view->new_display('default', 'Master', 'default');
  $handler->display->display_options['title'] = 'dH Admin Content Properties';
  $handler->display->display_options['use_more_always'] = FALSE;
  $handler->display->display_options['access']['type'] = 'none';
  $handler->display->display_options['cache']['type'] = 'none';
  $handler->display->display_options['query']['type'] = 'views_query';
  $handler->display->display_options['exposed_form']['type'] = 'basic';
  $handler->display->display_options['pager']['type'] = 'full';
  $handler->display->display_options['pager']['options']['items_per_page'] = '10';
  $handler->display->display_options['style_plugin'] = 'table';
  /* Header: Global: View (Views field view) */
  $handler->display->display_options['header']['view_field']['id'] = 'view_field';
  $handler->display->display_options['header']['view_field']['table'] = 'views';
  $handler->display->display_options['header']['view_field']['field'] = 'view_field';
  $handler->display->display_options['header']['view_field']['label'] = '';
  $handler->display->display_options['header']['view_field']['element_label_colon'] = FALSE;
  $handler->display->display_options['header']['view_field']['view'] = 'add_dh_properties_jump_list';
  $handler->display->display_options['header']['view_field']['display'] = 'page_1';
  $handler->display->display_options['header']['view_field']['arguments'] = '!1';
  /* Field: DH Properties: Label */
  $handler->display->display_options['fields']['pid']['id'] = 'pid';
  $handler->display->display_options['fields']['pid']['table'] = 'dh_properties';
  $handler->display->display_options['fields']['pid']['field'] = 'pid';
  $handler->display->display_options['fields']['pid']['label'] = 'Pid';
  $handler->display->display_options['fields']['pid']['alter']['make_link'] = TRUE;
  $handler->display->display_options['fields']['pid']['alter']['path'] = 'admin/content/dh_properties/manage/[pid]';
  $handler->display->display_options['fields']['pid']['separator'] = '';
  /* Field: DH Properties: Property Name */
  $handler->display->display_options['fields']['propname']['id'] = 'propname';
  $handler->display->display_options['fields']['propname']['table'] = 'dh_properties';
  $handler->display->display_options['fields']['propname']['field'] = 'propname';
  /* Field: DH Properties: propcode */
  $handler->display->display_options['fields']['propcode']['id'] = 'propcode';
  $handler->display->display_options['fields']['propcode']['table'] = 'dh_properties';
  $handler->display->display_options['fields']['propcode']['field'] = 'propcode';
  /* Field: DH Properties: propvalue */
  $handler->display->display_options['fields']['propvalue']['id'] = 'propvalue';
  $handler->display->display_options['fields']['propvalue']['table'] = 'dh_properties';
  $handler->display->display_options['fields']['propvalue']['field'] = 'propvalue';
  $handler->display->display_options['fields']['propvalue']['precision'] = '0';
  /* Field: DH Properties: Startdate */
  $handler->display->display_options['fields']['startdate']['id'] = 'startdate';
  $handler->display->display_options['fields']['startdate']['table'] = 'dh_properties';
  $handler->display->display_options['fields']['startdate']['field'] = 'startdate';
  $handler->display->display_options['fields']['startdate']['date_format'] = 'short';
  $handler->display->display_options['fields']['startdate']['second_date_format'] = 'long';
  /* Field: DH Properties: Enddate */
  $handler->display->display_options['fields']['enddate']['id'] = 'enddate';
  $handler->display->display_options['fields']['enddate']['table'] = 'dh_properties';
  $handler->display->display_options['fields']['enddate']['field'] = 'enddate';
  $handler->display->display_options['fields']['enddate']['date_format'] = 'short';
  $handler->display->display_options['fields']['enddate']['second_date_format'] = 'long';
  /* Field: DH VariableDefinition: Variable Key (may be short name or = varname) */
  $handler->display->display_options['fields']['varkey']['id'] = 'varkey';
  $handler->display->display_options['fields']['varkey']['table'] = 'dh_variabledefinition';
  $handler->display->display_options['fields']['varkey']['field'] = 'varkey';
  /* Field: DH VariableDefinition: Variable Name */
  $handler->display->display_options['fields']['varname']['id'] = 'varname';
  $handler->display->display_options['fields']['varname']['table'] = 'dh_variabledefinition';
  $handler->display->display_options['fields']['varname']['field'] = 'varname';
  /* Contextual filter: DH Properties: featureid */
  $handler->display->display_options['arguments']['featureid']['id'] = 'featureid';
  $handler->display->display_options['arguments']['featureid']['table'] = 'dh_properties';
  $handler->display->display_options['arguments']['featureid']['field'] = 'featureid';
  $handler->display->display_options['arguments']['featureid']['default_action'] = 'empty';
  $handler->display->display_options['arguments']['featureid']['default_argument_type'] = 'fixed';
  $handler->display->display_options['arguments']['featureid']['summary']['number_of_records'] = '0';
  $handler->display->display_options['arguments']['featureid']['summary']['format'] = 'default_summary';
  $handler->display->display_options['arguments']['featureid']['summary_options']['items_per_page'] = '25';
  $handler->display->display_options['arguments']['featureid']['comparator'] = 'eq';
  $handler->display->display_options['arguments']['featureid']['op_location'] = 'where';
  /* Filter criterion: DH Properties: entity_type */
  $handler->display->display_options['filters']['entity_type']['id'] = 'entity_type';
  $handler->display->display_options['filters']['entity_type']['table'] = 'dh_properties';
  $handler->display->display_options['filters']['entity_type']['field'] = 'entity_type';
  $handler->display->display_options['filters']['entity_type']['value'] = 'dh_feature';
  $handler->display->display_options['filters']['entity_type']['op_location'] = 'where';

  /* Display: Timeseries Properties */
  $handler = $view->new_display('page', 'Timeseries Properties', 'page_1');
  $handler->display->display_options['defaults']['filter_groups'] = FALSE;
  $handler->display->display_options['defaults']['filters'] = FALSE;
  /* Filter criterion: DH Properties: entity_type */
  $handler->display->display_options['filters']['entity_type']['id'] = 'entity_type';
  $handler->display->display_options['filters']['entity_type']['table'] = 'dh_properties';
  $handler->display->display_options['filters']['entity_type']['field'] = 'entity_type';
  $handler->display->display_options['filters']['entity_type']['value'] = 'dh_timeseries_weather';
  $handler->display->display_options['filters']['entity_type']['op_location'] = 'where';
  $handler->display->display_options['path'] = 'admin/content/dh_timeseries_weather/manage/%/dh_properties';
  $handler->display->display_options['menu']['type'] = 'tab';
  $handler->display->display_options['menu']['title'] = 'dH Properties';
  $handler->display->display_options['menu']['weight'] = '10';
  $handler->display->display_options['menu']['context'] = 0;
  $handler->display->display_options['menu']['context_only_inline'] = 0;
  return $view;
}