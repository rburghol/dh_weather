library(httr)

#Cross-site Request Forgery protection (Token needed for POST and PUT)
csrf <- GET(
  url='http://www.grapeipm.org/d.dev/restws/session/token/',
  authenticate("restws_wadmin", "We@therD@t@")
);
token <- content(csrf)
token

# search for Feature 
feature  <- GET(
  "http://www.grapeipm.org/d.dev/dh_timeseries_weather.json", 
  add_headers(X_CSRF_TOKEN = token),
  query = list(
    tstime = 1424926800,
    featureid = 13
  ),
  encode = "json",
  verbose()
);

f <- content(feature  );
