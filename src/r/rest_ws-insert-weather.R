library(httr)

#Cross-site Request Forgery protection (Token needed for POST and PUT)
csrf <- GET(
  url='http://www.grapeipm.org/d.dev/restws/session/token/',
  authenticate("restws_wadmin", "We@therD@t@")
);
token <- content(csrf)

x <- POST("http://www.grapeipm.org/d.dev/dh_timeseries_weather/", 
  add_headers(X_CSRF_TOKEN = token),
  body = list(bundle = 'submittal',
    featureid = 13,
    entity_type = 'dh_timeseries_weather',
    varkey = 'weather_obs',
    tstime = '2017-01-01',
    rh = 0.99,
    temp = 23.0,
    wet_time = 15,
    rain = 1.5,
  encode = "json",
  verbose()
);
content(x)
