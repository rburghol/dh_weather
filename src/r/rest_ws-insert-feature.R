library(httr)

#Cross-site Request Forgery protection (Token needed for POST and PUT)
csrf <- GET(
  url='http://www.grapeipm.org/d.dev/restws/session/token/',
  authenticate("restws_wadmin", "@dmin123REST")
);
token <- content(csrf)

x <- POST("http://www.grapeipm.org/d.dev/dh_feature/", 
  add_headers(X_CSRF_TOKEN = token),
  body = list(
    bundle = 'monitoringpoint',
    hydrocode = 'rwb_feature_rest_test_2',
    name = 'REST Feature test 2',
    fstatus = 'active',
    ftype = 'fe_quantreg'
  ), 
  encode = "json",
  verbose()
);
