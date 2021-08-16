# pywws2weewx
Data converter from pywws to weewx

pywws2weewx scans a specified directory (possibly recursively) for pywws raw files and creates one or more .csv and import config file pairs for use with weewx importer.

## Installation
Just download the `pywws2weewx.py` to any location and run it

## Usage
`python3 pywws2weewx.py -r /path/to/pywws/data/raw`

Most likely you will want to use the `-r` option for a recursive run from the base raw data directory.  
For all options see  
`python3 pywws2weewx.py --help`

## Notes
For large datasets or datasets with short update intervals the update interval may not be consistent throughout the dataset. As an example: in the data from my pywws installation the report interval was reset from shortest possible to 30 minutes after a longer power outtake. Also, at shortest possible the interval varies between 3 and 4 minutes. (I suppose it was around 3.5 minutes in real life, but pywws only registers hour and minute.)
As weewx importer requres all datapoints in the dataset to have the same interval pywws2weewx will create one csv/config combination for each detected update interval.

pywws raw data file format can be seen in the `RawStore()` function of https://github.com/jim-easterbrook/pywws/blob/master/src/pywws/filedata.py  
*NOTE:* Older versions of pywws does not contain the `illuminance` or `uv` fields.

WeeWX import configuration file is documented at https://www.weewx.com/docs/utilities.htm#import_config
