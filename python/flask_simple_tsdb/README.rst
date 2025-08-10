flask_simple_tsdb
=================
This package provides a library for interacting with a simple_tsdb server to
fetch and write points from a flask app.  This package only works with flask
versions prior to 2.2 (so, the most-recently-supported flask version is 2.1.3).
This is because the way to access app context variables had a breaking change
at 2.2.
