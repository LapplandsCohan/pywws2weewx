#!/usr/bin/python3
# encoding: utf-8
'''
pywws2weewx -- WeeWX importer for pywws data

Creates one or many configuration and csv file pairs from pywws raw data files for use with the weewx_importer 

@author:	 Konrad Skeri Ekblad

@copyright:  2021 Konrad Skeri Ekblad

@license:	GPL 3.0

@contact:	https://github.com/LapplandsCohan/pywws2weewx
@deffield	updated: Updated
'''

import sys
import os
import re

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from functools import total_ordering
from datetime import datetime

__all__ = []
__version__ = 1.0
__date__ = '2021-08-10'
__updated__ = '2021-10-28'

DEBUG = 0
TESTRUN = 0
PROFILE = 0

PYWWS_RAW_DATA = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,-?(?:\d+(?:\.\d+)?)*){11,13}$"
MINUTES_IN_A_DAY = 24*60

class CLIError(Exception):
	'''Generic exception to raise and log different fatal errors.'''
	def __init__(self, msg):
		super(CLIError).__init__(type(self))
		self.msg = "E: %s" % msg
	def __str__(self):
		return self.msg
	def __unicode__(self):
		return self.msg

@total_ordering
class pywwsDatapoint():
	def decode_status(self):
		result = {}
		for key, mask in (('invalid_wind_dir', 0b100000000000),	#0x800
	                      ('rain_overflow',    0b000010000000), #0x080
	                      ('lost_connection',  0b000001000000), #0x040
	                      ('unknown',          0b011100111111), #0x73f
	                      ):
			result[key] = self.status & mask
		return result

	'''A data point in pywws.
		See https://github.com/jim-easterbrook/pywws/blob/master/src/pywws/filedata.py '''
	def __init__(self, idx, delay, hum_in, temp_in, hum_out, temp_out, abs_pressure, wind_ave, wind_gust, wind_dir, rain, status, illuminance=None, uv=None):
		global args
		global delay_values
		
		if DEBUG or args.verbose:
			self.date_human           = idx
		self.idx                      = datetime.strptime(idx + "+0000", "%Y-%m-%d %H:%M:%S%z").timestamp()	# time stamp, idx in UTC
		self.status                   = int(status)
		if (args.verbose and args.verbose >= 2 and self.status > 0 and self.status != 0x800) or (args.verbose and args.verbose >= 4 and self.status > 0):
			print("Data point %s has a status flag! %s" %(idx, status))
		self.delay                    = int(delay)					# record interval in minutes. Conveniently provided by pywws
		if  self.delay not in delay_values:
			delay_values.append(self.delay)
		self.hum_in                   = hum_in
		self.temp_in                  = temp_in
		self.hum_out                  = hum_out
		self.temp_out                 = temp_out
		self.abs_pressure             = abs_pressure
		self.wind_ave                 = wind_ave
		self.wind_gust                = wind_gust
		self.wind_dir                 = wind_dir					# integer 0=N, 1=NNE..., 4=E..., 15=NNW
		if self.decode_status()["invalid_wind_dir"]:				# The error code 0x800 (2048)
			self.wind_dir_deg         = -9999						# -9999 is invalid wind in weewx_import
		else:
			self.wind_dir_deg         = int(wind_dir)*22.5			# Convert indexed direction to degrees
		self.rain_since_station_start = float(rain)					# Measured since the weather station was started!
		self.rain_today               = None
		self.illuminance              = illuminance					# Currently not handled as I don't have any data with uv/luminance
		self.uv                       = uv							# Currently not handled as I don't have any data with uv/luminance
		return
	
	def date(self):
		return datetime.fromtimestamp(self.idx).date()
	
	def __eq__(self, other):
		return self.idx == other.idx
	
	def __lt__(self, other):
		return self.idx < other.idx
	
	def __str__(self):
		'''Returns the data point formatted as CSV row'''
		return "%0.0f,%s,%s,%s,%s,%s,%s,%s,%s,%s,%0.1f" %(self.idx, self.delay, self.hum_in, self.temp_in, self.hum_out, self.temp_out, self.abs_pressure, self.wind_ave, self.wind_gust, self.wind_dir_deg, self.rain_today)

def sanitize_rain_data():
	global dataset, args
	
	if args.verbose:
		print("Sanitizing rain data…")
	previous1_datapoint = None
	previous2_datapoint = None
	for datapoint in dataset:
		if previous2_datapoint:
			# Sometimes there is a rain difference in one record whereafter the rain returns to the previous value.
			# Set the different record to the value of the surrounding records, but only if the three records are not too far apart. 
			minutes_diff = (datapoint.idx - previous2_datapoint.idx) / 60.0
			if minutes_diff <= MINUTES_IN_A_DAY:
				if datapoint.rain_since_station_start == previous2_datapoint.rain_since_station_start != previous1_datapoint.rain_since_station_start:
					previous1_datapoint.rain_since_station_start = datapoint.rain_since_station_start
					if args.verbose:
						print("Single record rain data anomaly found at %s" %previous1_datapoint.date_human)
		previous2_datapoint = previous1_datapoint
		previous1_datapoint = datapoint
	return

def calculate_rain():
	global dataset, args
	
	sanitize_rain_data()
	
	if args.verbose:
		print("Calculating rain…")
	first_datapoint_today = None
	previous_datapoint = None
	for datapoint in dataset:
		if previous_datapoint:
			# previous_datapoint is of same date as current datapoint.
			if datapoint.date() == previous_datapoint.date(): 
				# Set rain_today to amount of rain since midnight
				datapoint.rain_today = datapoint.rain_since_station_start - first_datapoint_today.rain_since_station_start + first_datapoint_today.rain_today
				# Apply correction if rain overflow or system restart
				if datapoint.decode_status()["rain_overflow"]:
					datapoint.rain_today = previous_datapoint.rain_today
				# Sanity check
				if datapoint.rain_today < previous_datapoint.rain_today or datapoint.rain_today > args.max_rain:
					datapoint.rain_today = previous_datapoint.rain_today
					if args.verbose:
						print("Insane rain data found at %s (%0.1f -> %0.1f)" %(datapoint.date_human, previous_datapoint.rain_since_station_start, datapoint.rain_since_station_start))
			# previous_datapoint is of other date than current datapoint.
			else:
				# Check if time difference from last datapoint is to big
				minutes_diff = (datapoint.idx - previous_datapoint.idx) / 60.0
				if minutes_diff > int(args.rain_record_age) or datapoint.rain_since_station_start < previous_datapoint.rain_since_station_start:
					datapoint.rain_today = 0
				else:
					datapoint.rain_today = datapoint.rain_since_station_start - previous_datapoint.rain_since_station_start
				first_datapoint_today = datapoint
		# No previous datapoint. Should only occur for the first record.
		else:
			datapoint.rain_today = 0
			first_datapoint_today = datapoint
		previous_datapoint = datapoint
	return

def main(argv=None): # IGNORE:C0111
	'''Command line options.'''

	if argv is None:
		argv = sys.argv
	else:
		sys.argv.extend(argv)

	program_name = os.path.basename(sys.argv[0])
	program_version = "v%s" % __version__
	program_build_date = str(__updated__)
	program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
	program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
	program_license = '''%s

  Created by Konrad Skeri Ekblad on %s.
  Copyright 2021 Konrad Skeri Ekblad.

  Licensed under the GNU General Public License 3.0
  
  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))

	try:
		# Setup argument parser
		parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
		parser.add_argument("-r", "--recursive", dest="recursive", action="store_true", help="also check files in sub folders [default: %(default)s]")
		parser.add_argument("-v", "--verbose", dest="verbose", action="count", help="set verbosity level [default: %(default)s]")
		parser.add_argument("-i", "--include", dest="inclusive_regex", help="only include files matching this regex pattern. Note: exclude is given preference over include. [default: %(default)s]", metavar="RE", default=r"^\d{4}-\d{2}-\d{2}\.txt$" )
		parser.add_argument("-e", "--exclude", dest="exclusive_regex", help="exclude files matching this regex pattern. [default: %(default)s]", metavar="RE" )
		parser.add_argument('-V', '--version', action='version', version=program_version_message)
		parser.add_argument("-c", "--config-file", dest="config_file", help="specifies path to wee_import configuration file to be created [default: %(default)s]", metavar="config_file", nargs="?", default="pywws.cnf")
		parser.add_argument("-f", "--force", dest="force_overwrite", action="store_true", help="Overwrite existing output files")
		parser.add_argument("--qc", dest="qc", action="store_true", help="Sets qc=true in the output config file")
		parser.add_argument("--calc_missing", dest="calc_missing", action="store_true", help="Sets calc_missing=true in the output config file")
		parser.add_argument("--rain_record_age", dest="rain_record_age", action="store", metavar="min", default=1440, help="If the first record of the day is more than min minutes newer than the previous record, set rain since last record to 0")
		parser.add_argument("--max_rain", dest="max_rain", action="store", metavar="mm", default=300, help="Rain data of datapoint is ignored if it will cause daily rain to exceed this value")
		parser.add_argument(dest="paths", help="paths to folder(s) with pywws file(s) [default: %(default)s]", metavar="path", nargs='+')

		# Process arguments
		global args
		args = parser.parse_args()

		if args.inclusive_regex:	# Include file regex, remove leading whitespace
			args.inclusive_regex = args.inclusive_regex.lstrip()
		if args.exclusive_regex:	# Exclude file regex, remove leading whitespace
			args.exclusive_regex = args.exclusive_regex.lstrip()
		
		global dataset
		dataset = []
		
		global delay_values
		delay_values = []
		
		if args.verbose:
			print("Verbose mode " + str(args.verbose))
			print("Specified paths: %s" %args.paths)
			print("Recursive mode " + str(args.recursive))
			if args.inclusive_regex:
				print("Infile include regex: %s" %args.inclusive_regex)
			if args.exclusive_regex:
				print("Infile exclude regex: %s" %args.exclusive_regex)

		if args.inclusive_regex and args.exclusive_regex and args.inclusive_regex == args.exclusive_regex:
			raise CLIError("include and exclude pattern are equal! Nothing will be processed.")

		for inpath in args.paths:
			if not os.path.exists(inpath):
				raise CLIError("Path does not exist: " + inpath)
				continue
			else:	# Path exists
				if os.path.isfile(inpath):
					process_file(inpath)
				else:
					process_directory(inpath)
				if dataset.__len__() == 0:
					print("Dataset empty! No files created!")
					if not args.recursive:
						print("Did you forget the -r (recursive) flag?")
					return 1
				dataset.sort()
				calculate_rain()
				if args.verbose:
					print("Delay values found: %s" %delay_values)
				config_file_splitted = os.path.splitext(args.config_file)
# Seems like this part is not needed.
# wee_import documentation states that all datapoints in the import set must have the same interval, but it seems like the delay column of a import file
# and uses that value, so there is no need to split the import files into one file pair for each delay value.
#				if delay_values.__len__() > 1:		# Create separate output files for each delay_value
#					for delay_value in delay_values:
#						config_file_with_value = "%s-%s%s" %(config_file_splitted[0], delay_value, config_file_splitted[1])
#						datafile = "%s-%s.csv" %(config_file_splitted[0], delay_value)
#						write_config_file(config_file_with_value, delay_value, datafile)
#						delay_value_dataset = [datapoint for datapoint in dataset if datapoint.delay == delay_value]
#						write_data_file(datafile, delay_value_dataset)
#				else:
				datafile = "%s.csv" %(config_file_splitted[0])
#				write_config_file(args.config_file, delay_values[0], args.datafile)
				write_config_file(args.config_file, datafile)
				write_data_file(datafile, dataset)
					
		return 0
	except KeyboardInterrupt:
		### handle keyboard interrupt ###
		return 0
	except Exception as e:
		if DEBUG or TESTRUN:
			raise(e)
		indent = len(program_name) * " "
		sys.stderr.write(program_name + ": " + repr(e) + "\n")
		sys.stderr.write(indent + "  for help use --help")
		return 2

def process_directory(inpath):
	'''Walks through the files in a directory processing file by file.'''
	for entry in os.scandir(inpath):
		if entry.is_dir() and args.recursive:
			if args.verbose:
				print("Scanning directory %s recursively" %entry.name)
			process_directory(entry.path)
		if entry.is_file():
			process_file(entry.path)
	return

def process_file(infile):
	'''Reads a pywws raw data file and adds the data points to a collection.'''
	#pywws raw data file format can be seen in the RawStore() at https://github.com/jim-easterbrook/pywws/blob/master/src/pywws/filedata.py
	global dataset
	filename = os.path.basename(infile)
	if (not (args.inclusive_regex) or args.inclusive_regex and re.search(args.inclusive_regex, filename)) and (not (args.exclusive_regex) or args.exclusive_regex and not re.search(args.exclusive_regex, filename)):
		if args.verbose and args.verbose >= 3:
			print("Starting processing file: %s" %filename)
		file = open(infile, "r")
		firstline = file.readline()
		
		# If first line does not contain pywws data, assume file not to be a pywws file
		if not re.search(PYWWS_RAW_DATA, firstline):
			if args.verbose:
				print("%s does not seem to be a pywws raw data file. Skipping file." %filename)
			return
		
		file.seek(0)	# Return to beginning of file
		linereader = file.readlines()
		for line in linereader:
			datapoint = line.replace('\n', '').split(",")
			dataset.append(pywwsDatapoint(*datapoint))
	else:
		if args.verbose:
			print("The filename \"%s\" did not pass regex filter and will not be processed." %filename)
	return

#def write_config_file(config_file, delay_value, datafile):
def write_config_file(config_file, datafile):
	#WeeWX import configuration file is documented at https://www.weewx.com/docs/utilities.htm#import_config
	global args
	
	if os.path.exists(config_file):
		if os.path.isdir(config_file):
			raise CLIError("Path is a directory: " + os.path.abspath(config_file))
		else:
			if not args.force_overwrite:
				raise CLIError("File already exists: " + os.path.abspath(config_file))	
	with open(config_file, "w") as f:
		f.write("source = CSV\n")
		f.write("[CSV]\n")
		f.write("\tfile = %s\n" %datafile)
#		f.write("\tinterval = %s\n" %delay_value)
		f.write("\tinterval = derive\n")
		f.write("\tqc = %s\n" %args.qc)
		f.write("\tcalc_missing = True\n")
		f.write("\tUV_sensor = False\n")
		f.write("\tsolar_sensor = False\n")
		f.write("\t" + r"raw_datetime_format = %Y-%m-%d %H:%M:%S" + "\n")
		f.write("\train = cumulative\n")
		f.write("\t[[FieldMap]]\n")
		f.write("\t\tdateTime    = idx\n")
		f.write("\t\tinterval    = interval\n")
		f.write("\t\tinHumidity  = hum_in, percent\n")
		f.write("\t\tinTemp      = temp_in, degree_C\n")
		f.write("\t\toutHumidity = hum_out, percent\n")
		f.write("\t\toutTemp     = temp_out, degree_C\n")
		f.write("\t\tbarometer   = abs_pressure, mbar\n")
		f.write("\t\twindSpeed   = wind_ave, meter_per_second\n")
		f.write("\t\twindGust    = wind_gust, meter_per_second\n")
		f.write("\t\twindDir     = wind_dir_deg, degree_compass\n")
		f.write("\t\train        = rain_today, mm")
	if args.verbose:
		print("File %s created." %config_file)
	return

def write_data_file(datafile, dataset):
	global args
	
	if os.path.exists(datafile):
		if os.path.isdir(datafile):
			raise CLIError("Path is a directory: " + os.path.abspath(datafile))
		else:
			if not args.force_overwrite:
				raise CLIError("File already exists: " + os.path.abspath(datafile))	
	with open(datafile, "w") as f:
		# Write header
		f.write("idx,interval,hum_in,temp_in,hum_out,temp_out,abs_pressure,wind_ave,wind_gust,wind_dir_deg,rain_today\n")
		# Write data
		for datapoint in dataset:
			f.write(str(datapoint) + "\n")
	if args.verbose:
		print("File %s created." %datafile)
	return

if __name__ == "__main__":
	if DEBUG:
		#sys.argv.append("-h")
		sys.argv.append("-vvv")
		sys.argv.append("-r")
		#sys.argv.append(r"-i ^2015-01-2[7]\.txt$")
	if TESTRUN:
		import doctest
		doctest.testmod()
	if PROFILE:
		import cProfile
		import pstats
		profile_filename = 'pywws2weewx_profile.txt'
		cProfile.run('main()', profile_filename)
		statsfile = open("profile_stats.txt", "wb")
		p = pstats.Stats(profile_filename, stream=statsfile)
		stats = p.strip_dirs().sort_stats('cumulative')
		stats.print_stats()
		statsfile.close()
		sys.exit(0)
	sys.exit(main())