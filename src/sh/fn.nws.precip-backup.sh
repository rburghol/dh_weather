# Download a test file
yr='2017'
mo='06'
da='27'
ext='_conus.tif'
dataset="nws_precip_1day_"
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
clip="insert into tmp_precipgrid (rast) "
clip="$clip select st_clip(a.rast, st_envelope(b.dh_geofield_geom)) "
clip="$clip from tmp_precipgrid as a left outer join field_data_dh_geofield as b "
clip="$clip on ( b.entity_id in (select hydroid from dh_feature where bundle = 'landunit' "
clip="$clip   and ftype = 'usastate_00' "
clip="$clip   and hydrocode = '0400000US51' "
clip="$clip   ) "
clip="$clip ) "
clip="$clip where b.entity_id is not null "
# connect to the database 
echo $clip  | psql -h dbase2 drupal.beta 
psql -h dbase2 drupal.beta 
-- verify that the extent looks good
select rid, st_astext(st_envelope(rast)) from tmp_precipgrid group by rid;

-- summarize a point in the noaa weather grid
select a.hydrocode, a.hydroid, st_astext(b.dh_geofield_geom),
   ST_Value(rast, b.dh_geofield_geom)
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
where a.hydrocode = '1014-526' 
  and a.bundle = 'weather_sensor'
  and c.rid = 2 
;

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
