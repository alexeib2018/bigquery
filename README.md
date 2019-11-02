# BigQuery project #


### Project structure ###

cgi/ - perl and python scripts  
create_tables/ - SQL for create tables  
css/ - frontend css  
js/ - frontend javascript  
jupyter/ - jupyter notebook scripts  
bigquery_key.json - key file to access (should be placed to the root of the project)  
index.html - frontend  
requirements.txt - python libraries  
httpd.conf - apache config


### Install ###

Run sql scripts with psql from create_tables directory

Create python environment by commands:  
virtualenv .venv3 --python=python3  
.venv3/bin/pip install -r requirements.txt

Setup apache httpd server with httpd.conf configuration

### Run ###

Import raw data from BigQuery:  
import_bigquery_events.py 20191101

Prepare data for reports:  
preprocess_events.py

Open link http://big-data.freshgrillfoods.com in the web browser
