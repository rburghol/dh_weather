# Download a test file
yr='2017'
mo='08'
da='01'
ext='_conus.tif'
dataset="nws_precip_1day_"
dataset="nws_precip_mtd_"
#dataset="nws_precip_last365days_"
fil="$dataset$yr$mo$da$ext"
url="http://water.weather.gov/precip/downloads/$yr/$mo/$da/$fil"
echo $url

wget $url 

# Convert to projection 4326
gdalwarp $fil -t_srs EPSG:4326 "$fil.conus-4326.tif"
# If you like, you can just download this new file with sftp and test it out on QGIS to see if the transformation was good
# export to postgis format
raster2pgsql "$fil.conus-4326.tif" tmp_precipgrid > tmp_precipgrid.sql

# add to database
echo "drop table tmp_precipgrid;" | psql -h dbase2 drupal.beta 
cat tmp_precipgrid.sql | psql -h dbase2 drupal.beta 
echo "alter table tmp_precipgrid add column dataset varchar(32);" | psql -h dbase2 drupal.beta 
clip="insert into tmp_precipgrid (rast, dataset) "
clip="$clip select st_clip(a.rast, st_envelope(st_setsrid(b.dh_geofield_geom,4326))), 'va_precip_points' "
#clip="$clip from tmp_precipgrid as a left outer join field_data_dh_geofield as b "
clip="$clip from tmp_precipgrid as a "
clip="$clip left outer join ( "
clip="$clip   select max(entity_id) as entity_id, "
clip="$clip   st_envelope(st_extent(dh_geofield_geom)) as dh_geofield_geom "
clip="$clip   from field_data_dh_geofield "
clip="$clip   where bundle = 'weather_sensor' "
clip="$clip ) as b "
clip="$clip on (1 = 1) "
clip="$clip where b.entity_id is not null "
# connect to the database 
# disabled in favor of clipping and inserting into dh_timeseries_weather in module code
echo $clip | psql -h dbase2 drupal.beta 
psql -h dbase2 drupal.beta 
-- verify that the extent looks good
select rid, st_astext(st_envelope(rast)) from tmp_precipgrid group by rid;

-- summarize a point in the noaa weather grid
select a.hydrocode, a.hydroid, st_astext(b.dh_geofield_geom),
   ST_Value(rast, b.dh_geofield_geom) as obs,
   ST_Value(rast, 4, b.dh_geofield_geom) as pct
from dh_feature as a 
left outer join field_data_dh_geofield as b 
on (
  b.entity_id = a.hydroid 
  and b.entity_type = 'dh_feature' 
)
left outer join tmp_precipgrid as c 
on (
  -- does this slow us down? Oh yes! dramatically
  --ST_intersects(c.rast, b.dh_geofield_geom)
  1 = 1 
)
where a.bundle = 'weather_sensor'
  -- uncomment to do a single one, comment it and all will be returned
  --and a.hydrocode = '1014-526' 
  --and c.rid = 2 
  and c.dataset = 'va_precip_points' 
;

select a.hydrocode, to_timestamp(c.tstime), a.hydroid, st_astext(b.dh_geofield_geom),
   ST_Value(rast, 1, b.dh_geofield_geom) as obs,
   ST_Value(rast, 2, b.dh_geofield_geom) as pct
from dh_feature as a 
left outer join field_data_dh_geofield as b 
on (
  b.entity_id = a.hydroid 
  and b.entity_type = 'dh_feature' 
)
  left outer join dh_timeseries_weather as c 
  on (
    -- does this slow us down? Oh yes! dramatically
    --ST_intersects(c.rast, b.dh_geofield_geom)
    1 = 1 
  )
where a.bundle = 'weather_sensor'
  -- uncomment to do a single one, comment it and all will be returned
  and a.hydrocode = '1014-526' 
  --and c.rid = 2 
    and c.featureid in (select hydroid from dh_feature where hydrocode = 'vahydro_nws_precip_grid'  and bundle = 'landunit'  and ftype = 'nws_precip_grid')
    and c.entity_type = 'dh_feature'
    and varid in (select hydroid from dh_variabledefinition where varkey = 'noaa_precip_raster')
;


-- summarize a point in the noaa weather grid stored in the dh_timeseries_weather table
select hydroid, 'dh_feature', avg(pct) as pct_normal, count(*) 
from (
  select a.hydrocode, to_timestamp(tstime), a.hydroid, st_astext(b.dh_geofield_geom),
     ST_Value(rast, b.dh_geofield_geom) as obs,
     CASE
       WHEN ST_Value(rast, 4, b.dh_geofield_geom) is null THEN 0
       ELSE ST_Value(rast, 4, b.dh_geofield_geom)
     END as pct
  from dh_feature as a 
  left outer join field_data_dh_geofield as b 
  on (
    b.entity_id = a.hydroid 
    and b.entity_type = 'dh_feature' 
  )
  left outer join dh_timeseries_weather as c 
  on (
    -- does this slow us down? Oh yes! dramatically
    --ST_intersects(c.rast, b.dh_geofield_geom)
    1 = 1 
  )
  where a.bundle = 'weather_sensor'
    -- uncomment to do a single one, comment it and all will be returned
    --and a.hydrocode = '1014-526' 
    --and c.rid = 2 
    and c.tstime >= extract(epoch from '2016-10-01'::date)
    and c.tstime <= extract(epoch from '2017-09-30'::date)
    and c.tstime >= extract(epoch from '2016-10-15'::date)
    and c.tstime <= extract(epoch from '2016-10-20'::date)
    and c.featureid in (select hydroid from dh_feature where hydrocode = 'vahydro_nws_precip_grid'  and bundle = 'landunit'  and ftype = 'nws_precip_grid')
    and c.entity_type = 'dh_feature'
    and varid in (select hydroid from dh_variabledefinition where varkey = 'noaa_precip_raster')
) as foo 
group by hydroid
;

select hydroid, 'dh_feature', 
  CASE 
    WHEN SUM(nml) > 0 THEN SUM(obs) / sum(nml) 
    ELSE 0
  END as pct_normal, 
  count(*) 
from (
  select a.hydrocode, to_timestamp(tstime), a.hydroid, st_astext(b.dh_geofield_geom),
     CASE
       WHEN ST_Value(rast, 1, b.dh_geofield_geom) is null THEN 0
       ELSE ST_Value(rast, 1, b.dh_geofield_geom)
     END as obs,
     CASE
       WHEN ST_Value(rast, 2, b.dh_geofield_geom) is null THEN 0
       ELSE ST_Value(rast, 2, b.dh_geofield_geom)
     END as nml
     CASE
       WHEN ST_Value(rast, 4, b.dh_geofield_geom) is null THEN 0
       ELSE ST_Value(rast, 4, b.dh_geofield_geom)
     END as pct
  from dh_feature as a 
  left outer join field_data_dh_geofield as b 
  on (
    b.entity_id = a.hydroid 
    and b.entity_type = 'dh_feature' 
  )
  left outer join dh_timeseries_weather as c 
  on (
    -- does this slow us down? Oh yes! dramatically
    --ST_intersects(c.rast, b.dh_geofield_geom)
    1 = 1 
  )
  where a.bundle = 'weather_sensor'
    -- uncomment to do a single one, comment it and all will be returned
    --and a.hydrocode = '1014-526' 
    --and c.rid = 2 
    and c.featureid in (select hydroid from dh_feature where hydrocode = 'vahydro_nws_precip_grid'  and bundle = 'landunit'  and ftype = 'nws_precip_grid')
    and c.entity_type = 'dh_feature'
    and varid = -99999
) as foo 
group by hydroid
;

insert into dh_timeseries_weather (featureid, entity_type, varid, rast) 
select featureid, entity_type, -99999, st_union(rast) from dh_timeseries_weather 
where varid in (select hydroid from dh_variabledefinition where varkey = 'noaa_precip_raster')
  and tstime >= extract(epoch from '2016-10-15'::date)
  and tstime <= extract(epoch from '2016-10-20'::date)
group by featureid, entity_type, varid
;

select st_astext(st_envelope(st_extent(dh_geofield_geom))) from field_data_dh_geofield where bundle = 'weather_sensor';
--POLYGON((-83.6753762502731 36.1185663433469,-83.6753762502731 40.2384540257181,-75.2276211227505 40.2384540257181,-75.2276211227505 36.1185663433469,-83.6753762502731 36.1185663433469))

--  2 | POLYGON((-83.7070323515323 39.4936610268204,-75.250751186152 39.4936610268204,-75.250751186152 36.5273215604304,-83.7070323515323 36.5273215604304,-83.7070323515323 39.4936610268204))

-- sample output from 7/5/2017 if it goes properly
-- hydrocode | hydroid |               st_astext                |     st_value
-------------+---------+----------------------------------------+------------------
-- 1014-526  |  258831 | POINT(-75.27522146899 37.269463244714) | 41.0511817932129
--(1 row)


-- inventory
select count(*) 
from dh_feature as a 
left outer join field_data_dh_geofield as b 
on (
  b.entity_id = a.hydroid 
  and b.entity_type = 'dh_feature' 
)
left outer join tmp_precipgrid as c 
on (
  ST_intersects(c.rast, b.dh_geofield_geom)
)
where a.bundle = 'weather_sensor'
and b.dh_geofield_geo_type = 'point' 
and ST_Value(rast, b.dh_geofield_geom) > 0;

-- non-null inventory
select count(*) from (
  select a.hydrocode, a.hydroid, b.dh_geofield_geo_type, st_astext(b.dh_geofield_geom),
     ST_Value(rast, b.dh_geofield_geom) as globvalue 
  from dh_feature as a 
  left outer join field_data_dh_geofield as b 
  on (
    b.entity_id = a.hydroid 
    and b.entity_type = 'dh_feature' 
  )
  left outer join tmp_precipgrid as c 
  on (
    ST_intersects(c.rast, b.dh_geofield_geom)
  )
  where a.bundle = 'weather_sensor'
  and b.dh_geofield_geo_type = 'point'
) as foo 
where globvalue > 0;


-- compare/QA with old data
noaa=# select max(thisdate) from precip_gridded;
         max
---------------------
 2017-06-27 00:00:00
(1 row)

noaa=# select count(*) from precip_gridded where thisdate = '2017-06-27';
 count
-------
  3366
(1 row)
