
class Weather:
  def __init__(self, location, latitude, longitude):
    self.location = location
    self.latitude = latitude
    self.longitude = longitude

  def LocationForecast(self, latitude, longitude):
    print("Hello my location is " + self.location)



weather = Weather.LocationForecast("YtreEikjo")
weather.to_parquet('~/Downloads/weather-forecast.parquet', engine='fastparquet')



class LocationForecast:

  BASE_URL = "https://api.met.no/weatherapi/locationforecast/2.0/complete"

  def __init__(self, latitude, longitude):
    self.latitude = latitude
    self.longitude = longitude

  def myfunc(self):
    print("Hello my location is " + self.location)



request_localforecast_complete_ytre_eikjo = 'https://api.met.no/weatherapi/locationforecast/2.0/complete?lat=61.3306958&lon=7.326719'






#    **      **    **********      ******      **********    **      **    **********    ********
#    **      **    **            **      **        **        **      **    **            **      **
#    **      **    **            **      **        **        **      **    **            **      **
#    **      **    ********      **********        **        **********    ********      ********
#    **  **  **    **            **      **        **        **      **    **            **  **
#    **  **  **    **            **      **        **        **      **    **            **    **
#      **  **      **********    **      **        **        **      **    **********    **      **
#
#                                                                         (C) 2022 BY PÅL HELLESNES




#    **      **    **********      ******      **********    **      **    **********    ********
#    **      **    **            **      **        **        **      **    **            **      **
#    **      **    **            **      **        **        **      **    **            **      **
#    **      **    ********      **********        **        **********    ********      ********
#    **  **  **    **            **      **        **        **      **    **            **  **
#    **  **  **    **            **      **        **        **      **    **            **    **
#      **  **      **********    **      **        **        **      **    **********    **      **
#
#                                                                         (c) 2022 by Pål Hellesnes




#    **      **    **********      ******      **********    **      **    **********    ********
#    **      **    **            **      **        **        **      **    **            **      **
#    **      **    **            **      **        **        **      **    **            **      **
#    **      **    ********      **********        **        **********    ********      ********
#    **  **  **    **            **      **        **        **      **    **            **  **
#    **  **  **    **            **      **        **        **      **    **            **    **
#      **  **      **********    **      **        **        **      **    **********    **      **
#
#    U N I T   T E S T   S U I T E                                        (C) 2022 BY PÅL HELLESNES


"""
Weather API Library
~~~~~~~~~~~~~~~~~~~
Weather is an API library, written in Python, for data scientists.
Basic usage:
   >>> import weather
   >>> forecast = weather.get_location_forecast('Ytre Eikjo')
   >>> forecast.status_code
   200
   >>> b'Temp' in forecast.content
   True
   >>> print(forecast.text)
   {
     ...
     "form": {
       "key1": "value1",
       "key2": "value2"
     },
     ...
   }
:copyright: (c) 2022 by Pål Hellesnes.
:license: Apache 2.0, see LICENSE for more details.
"""


import os
import sys
import json
import requests
import pandas as pd
import geopandas as gpd
import fastparquet as fp


# ***********************************
# **  L O C A T I O N   C L A S S  **
# ***********************************

class Location:

  def __init__(self, name, latitude, longitude):
    self.name = name
    self.latitude = latitude
    self.longitude = longitude

  @property
  def coordinates(self):
    return '({},{})'.format(self.latitude, self.longitude)


# *****************************************
# **  I N I T I A L I Z E   M O D U L E  **
# *****************************************

locations = [
    Location('Ytre Eikjo', 61.3306958, 7.326719),
    Location('Bergen', 60.4102999, 5.3223733)
    ]

location_for_ytreeikjo = {'name': 'Ytre Eikjo', 'latitude': 61.3306958, 'longitude': 7.326719}
location_for_bergen = {'name': 'Bergen', 'latitude': 60.4102999, 'longitude': 5.3223733}
locations = [
    location_for_ytreeikjo,
	location_for_bergen
    ]

locations = [
    {'name': 'Ytre Eikjo', 'latitude': 61.3306958, 'longitude': 7.326719},
	{'name': 'Bergen', 'latitude': 60.4102999, 'longitude': 5.3223733}
    ]

locations_df = pd.DataFrame({
	'Name': ['Ytre Eikjo', 'Bergen'],
	'Latitude': [61.3306958, 60.4102999],
	'Longitude': [7.326719, 5.3223733]
	})
locations_gdf = gdf.GeoDataFrame(
	locations_df,
	geometry = gdf.points_from_xy(locations_df.Longitude, locations_df.Latitude)
	)



# *********************************
# **  W E A T H E R   C L A S S  **
# *********************************

class Weather:

  raise_amount = 1.05

  def __init__(self, first, last, pay):
    self.first = first
    self.last = last
    self.pay = pay

  @property
  def email(self):
    return '{}.{}@email.com'.format(self.first, self.last)

  @property
  def fullname(self):
    return '{} {}'.format(self.first, self.last)

  def apply_raise(self):
    self.pay = int(self.pay * self.raise_amount)




class Weather:
  def __init__(self, location, latitude, longitude):
    self.location = location
    self.latitude = latitude
    self.longitude = longitude

  def LocationForecast(self, latitude, longitude):
    print("Hello my location is " + self.location)



weather = Weather.LocationForecast("YtreEikjo")
weather.to_parquet('~/Downloads/weather-forecast.parquet', engine='fastparquet')



class LocationForecast:

  BASE_URL = "https://api.met.no/weatherapi/locationforecast/2.0/complete"

  def __init__(self, latitude, longitude):
    self.latitude = latitude
    self.longitude = longitude

  def myfunc(self):
    print("Hello my location is " + self.location)



request_localforecast_complete_ytre_eikjo = 'https://api.met.no/weatherapi/locationforecast/2.0/complete?lat=61.3306958&lon=7.326719'




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

# Komplett versjon av værmelding for Ytre Eikjo (61.3306958, 7.326719) i JSON format:
request_localforecast_complete_ytre_eikjo = 'https://api.met.no/weatherapi/locationforecast/2.0/complete?lat=61.3306958&lon=7.326719'

# gather weather forecast from Meteorologisk institutt
if progress: print('Gathering weather forecast from Meteorologisk institutt...\n')
base_url = request_localforecast_complete_ytre_eikjo
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
list = []
for timeserie in data['properties']['timeseries']:
	dict = {
		'time': timeserie['time'],
		'air_pressure_at_sea_level': timeserie['data']['instant']['details']['air_pressure_at_sea_level'],
		'air_temperature': timeserie['data']['instant']['details']['air_temperature'],
		'air_temperature_percentile_10': timeserie['data']['instant']['details']['air_temperature_percentile_10'],
		'air_temperature_percentile_90': timeserie['data']['instant']['details']['air_temperature_percentile_90'],
		'cloud_area_fraction': timeserie['data']['instant']['details']['cloud_area_fraction'],
		'cloud_area_fraction_high': timeserie['data']['instant']['details']['cloud_area_fraction_high'],
		'cloud_area_fraction_low': timeserie['data']['instant']['details']['cloud_area_fraction_low'],
		'cloud_area_fraction_medium': timeserie['data']['instant']['details']['cloud_area_fraction_medium'],
		'dew_point_temperature': timeserie['data']['instant']['details']['dew_point_temperature'],
		'relative_humidity': timeserie['data']['instant']['details']['relative_humidity'],
		'wind_from_direction': timeserie['data']['instant']['details']['wind_from_direction'],
		'wind_speed': timeserie['data']['instant']['details']['wind_speed'],
		'wind_speed_percentile_10': timeserie['data']['instant']['details']['wind_speed_percentile_10'],
		'wind_speed_percentile_90': float(timeserie['data']['instant']['details']['wind_speed_percentile_90'])
		}
	list.append(dict)
dataset = pd.DataFrame(list)

# *****************************
# **  O U T P U T   D A T A  **
# *****************************

# Save to parquet file
if progress: print('Saving to parquet file...\n')
dataset.to_parquet('~/Downloads/weather-forecast.parquet', engine='fastparquet')

print(f"{dataset}\n")

