create table obs_pts_tmp (geom geometry, rh float8);

-- only select points

insert into obs_pts_tmp(geom, rh) 
select a.geom,st_value( b.rast, 1, a.x, a.y)
 from (
   SELECT (ST_PixelAsCentroids(rast, 1)).*
   FROM (
     select obs.* 
     from obs, (
       select st_transform(st_envelope(st_setsrid(st_extent(the_geom),4326)),3277) as envelope
       from va_counties
     ) as foo 
     where st_intersects( foo.envelope, st_envelope(st_setsrid(rast,3277))) 
       and rid = 1
 ) as a,
 obs as b
;

insert into obs_pts_tmp(geom, rh) 

-- all points 
select a.geom,st_value( b.rast, 1, a.x, a.y)
 from (SELECT (ST_PixelAsCentroids(rast, 1)).*
 FROM obs WHERE rid = 1) as a,
 obs as b
;

-- view points
SELECT (ST_PixelAsCentroids(rast, 1)).*
 FROM obs_pts_tmp WHERE rid = 1;
-- view points - nws_precip_last365days_20170705_conus
SELECT (ST_PixelAsCentroids(rast, 1)).*
 FROM nws_precip_last365days_20170705_conus WHERE rid = 1;
 
-- from vahydro
/c drupal.beta
select a.hydrocode, a.hydroid, st_astext(b.dh_geofield_geom) from dh_feature as a left outer join field_data_dh_geofield as b on (b.entity_id = a.hydroid and b.entity_type = 'dh_feature' ) where a.hydrocode = '1014-525' and a.bundle = 'weather_sensor';
 hydrocode | hydroid |                st_astext
-----------+---------+------------------------------------------
 1014-525  |  258018 | POINT(-75.2981658542622 37.237462177298)
(1 row)


select ST_Value(rast, st_geomfromtext('POINT(-75.2981658542622 37.237462177298)',4326)) from "nws_precip_last365days_20170705_conus-4326" where rid = 1;

-- view centroid
-- Note: srid 3277 is just a guess - and it's not actually correct
select st_astext(st_transform(st_setsrid(st_envelope(rast),3277),4326)) from nws_precip_ where rid = 1;

create index tmp_rh_gix on obs_pts_tmp using GIST (geom);
create table temp_pts_tmp (geom geometry, temp float8);
insert into temp_pts_tmp(geom, temp) 
select a.geom,st_value( b.rast, 1, a.x, a.y)
 from (SELECT (ST_PixelAsCentroids(rast, 1)).*
 FROM temp_tmp WHERE rid = 1) as a,
 temp_tmp as b
;
create index tmp_temp_gix on temp_pts_tmp using GIST (geom);
