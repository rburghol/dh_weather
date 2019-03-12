# Download RTMA ndfd rasters
# filename: rtma2p5.YYYYMMDD/rtma2p5.t10z.2dvarges_ndfd.grb2_wexp
wget https://ftp.ncep.noaa.gov/data/nccf/com/rtma/prod/rtma2p5.20190312/rtma2p5.t10z.2dvarges_ndfd.grb2_wexp

# what's in it?
gdalinfo rtma2p5.t10z.2dvarges_ndfd.grb2_wexp > rtma_raster_info

# Convert to projection 4326
fil="rtma2p5.t10z.2dvarges_ndfd.grb2_wexp"
gdalwarp rtma2p5.t10z.2dvarges_ndfd.grb2_wexp -t_srs EPSG:4326 "$fil.conus-4326.tif"

# Convert to postgis
raster2pgsql -t 1000x1000 rtma2p5.t10z.2dvarges_ndfd.grb2_wexp.conus-4326.tif tmp_weather > tmp_wexp.sql

# Add top postgis database
echo "drop table tmp_weather" | psql drupal.dev
cat tmp_wexp.sql | psql drupal.dev

# Clip
drop table tmp_rtma_clip;
create table tmp_rtma_clip as (
select rid, 
  ST_Union(
    ST_Clip(
      rast,
      ST_GeomfromText('POLYGON((-85 34, -85 41, -74 34, -74 34, -85 34))',4326)
    )
  ) as rast 
  from tmp_weather 
  group by rid
);

# 
create table tmp_pts_rtma (gid bigint, temp float8, dp_temp float8, sp_hum float8);
insert into tmp_pts_rtma(geom, temp, dp_temp, sp_hum) 
select a.hydroid, c.rid, b.dh_geofield_geom, st_srid(b.dh_geofield_geom),
  st_value( c.rast, 3, b.dh_geofield_geom) as temp,
  st_value( c.rast, 4, b.dh_geofield_geom) as dp_temp,
  st_value( c.rast, 7, b.dh_geofield_geom) as sp_hum
 from dh_feature as a 
 left outer join field_data_dh_geofield as b 
 on (
   a.hydroid = b.entity_id 
   and b.entity_type = 'dh_feature'
 )
 left outer join tmp_rtma_clip as c 
 on (st_centroid(b.dh_geofield_geom) && c.rast)
 WHERE a.bundle = 'weather_sensor'   
;

select c.rid, a.hydroid, b.dh_geofield_geom,
  st_value( c.rast, 3, b.dh_geofield_geom) as temp,
  st_value( c.rast, 4, b.dh_geofield_geom) as dp_temp,
  st_value( c.rast, 7, b.dh_geofield_geom) as sp_hum
 from dh_feature as a
 left outer join field_data_dh_geofield as b
 on (
   a.hydroid = b.entity_id
   and b.entity_type = 'dh_feature'
 )
 left outer join tmp_weather as c
 on (st_centroid(b.dh_geofield_geom) && c.rast)
 WHERE a.bundle = 'weather_sensor'
;
