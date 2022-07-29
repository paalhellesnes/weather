# *******************************************
# **  I N I T I A L I Z E   P R O G R A M  **
# *******************************************

import os
import sys
import json
import requests
import pandas as pd
import fastparquet as fp

# clear_the_screen
os.system('clear')

# display welcome message
print("Welcome to the weather forecast\n")

verbose = False
progress = True

# *****************************
# **  G A T H E R   D A T A  **
# *****************************

# Kompakt versjon av værmelding for posisjon (60.10, 9.58) i JSON format: 
request_localforecast_compact_lat60_lon9 = 'https://api.met.no/weatherapi/locationforecast/2.0/compact?lat=60.10&lon=9.58'

# Kompakt versjon av værmelding for Ytre Eikjo (61.3306958, 7.326719) i JSON format:
request_localforecast_compact_ytre_eikjo = 'https://api.met.no/weatherapi/locationforecast/2.0/compact?lat=61.3306958&lon=7.326719'

# gather weather forecast from met 
if progress: print('Gathering weather forecast from Meteorologisk institutt...\n')
base_url = request_localforecast_compact_ytre_eikjo
response = requests.get(base_url, headers={"User-Agent": None,})
data = response.json()

# ***************************************
# **  I N V E S T I G A T E   D A T A  **
# ***************************************

# investigate json structure
if verbose:
	print(f"Response ok: {response.ok}\n")
	print(f"Response status code: {response.status_code}\n")
	print(f"Response text: {response.text}\n")
	print(f"Response keys: {data.keys()}")
	print(f"Response json: {json.dumps(data, indent = 4)}")

# ***********************************
# **  T R A N S F O R M   D A T A  **
# ***********************************

# parse timeseries 
if progress: print('Parsing timeseries...\n')
dataset = pd.DataFrame()
for timeserie in data['properties']['timeseries']:
	time = timeserie['time']
	air_pressure_at_sea_level = timeserie['data']['instant']['details']['air_pressure_at_sea_level']
	air_temperature = timeserie['data']['instant']['details']['air_temperature']
	cloud_area_fraction = timeserie['data']['instant']['details']['cloud_area_fraction']
	wind_from_direction = timeserie['data']['instant']['details']['wind_from_direction']
	wind_speed = timeserie['data']['instant']['details']['wind_speed']
	dict = {
		'time': time,
		'air_pressure_at_sea_level': air_pressure_at_sea_level,
		'air_temperature': air_temperature,
		'cloud_area_fraction': cloud_area_fraction,
		'wind_from_direction': wind_from_direction,
		'wind_speed': wind_speed
		}
	row = pd.DataFrame(dict, index=[0])
	dataset = pd.concat([dataset, row], axis=0, ignore_index=True)

# *****************************
# **  O U T P U T   D A T A  **
# *****************************

# Save to parquet file
if progress: print('Saving to parquet file...\n')
dataset.to_parquet('~/Downloads/weather-forecast.parquet', engine='fastparquet')

print(f"{dataset}\n")

