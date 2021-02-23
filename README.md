# techtestdatagouv

Scraping data from an open data pool.

## Not done
- dockerization not working. The issue is that I seem to have a shadow name "api" instead of
 "app" stuck somewhere I can't find.
 
## How to run without docker
- install requirements
- start postgresql service
- `sudo -iu postgres`  

in another terminal
- `python setup_db/scraping.py` downloading some files may take a while.
- `python webpage/app.py`
- go to http://127.0.0.1:5000/filesmetadatalist
