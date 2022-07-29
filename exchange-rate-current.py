# *******************************************
# **  I N I T I A L I Z E   P R O G R A M  **
# *******************************************

import requests
import json
import os
import pandas as pd

# clear_the_screen
os.system('clear')

# display welcome message
print("Welcome to the exchange rate converter\n")

# *****************************
# **  G A T H E R   D A T A  **
# *****************************

# gather exchange rates from norges bank
base_url = 'https://data.norges-bank.no/api/data/EXR/B..NOK.SP?lastNObservations=1&format=sdmx-json'
response = requests.get(base_url)
response.ok
response.status_code
response.text
data = response.json()

# ***************************************
# **  I N V E S T I G A T E   D A T A  **
# ***************************************

# investigate json structure
#print(data.keys())
#print(json.dumps(data, indent = 4))

# ***********************************
# **  T R A N S F O R M   D A T A  **
# ***********************************

# parse dimensions
for dimension in data['data']['structure']['dimensions']['series']:
	if dimension['id'] == 'FREQ':
		frequency = pd.DataFrame(dimension['values'])
		frequency.columns = ['frequency_code', 'frequency_name', 'frequency_description']
	elif dimension['id'] == 'BASE_CUR':
		base_currency = pd.DataFrame(dimension['values'])
		base_currency.columns = ['base_currency_code', 'base_currency_name']
	elif dimension['id'] == 'QUOTE_CUR':
		quote_currency = pd.DataFrame(dimension['values'])
		quote_currency.columns = ['quote_currency_code', 'quote_currency_name']
	elif dimension['id'] == 'TENOR':
		tenor = pd.DataFrame(dimension['values'])
		tenor.columns = ['tenor_code', 'tenor_name']
for observation in data['data']['structure']['dimensions']['observation']:
	if observation['id'] == 'TIME_PERIOD':
		time_period = pd.DataFrame(observation['values'])
		time_period.columns = ['time_period_start', 'time_period_end', 'time_period_code', 'time_period_name']

# parse attributes
for attribute in data['data']['structure']['attributes']['series']:
	if attribute['id'] == 'DECIMALS':
		decimals = pd.DataFrame(attribute['values'])
		decimals.columns = ['decimals_code', 'decimals_name']
	elif attribute['id'] == 'CALCULATED':
		calculated = pd.DataFrame(attribute['values'])
		calculated.columns = ['calculated_code', 'calculated_name']
	elif attribute['id'] == 'UNIT_MULT':
		unit_multiplier = pd.DataFrame(attribute['values'])
		unit_multiplier.columns = ['unit_multiplier_code', 'unit_multiplier_name']
	elif attribute['id'] == 'COLLECTION':
		collection_indicator = pd.DataFrame(attribute['values'])
		collection_indicator.columns = ['collection_indicator_code', 'collection_indicator_name']

def extract_attribute_id(row, index):
	attributes = row['attributes']
	return attributes[index]

def extract_time_period_id(row):
        observations = row['observations']
        return list(observations.keys())[0]

def extract_exchange_rate(row):
	observations = row['observations']
	return list(observations.values())[0][0]

# parse exchange rate facts
dataset = pd.DataFrame(data['data']['dataSets'][0]['series']).transpose()
dataset["index"] = dataset.index
dataset['quote_currency_id'] = dataset['index'].str.split(':', expand=True)[0].astype(int)
dataset = dataset.join(quote_currency, on='quote_currency_id', rsuffix='_right')
dataset['base_currency_id'] = dataset['index'].str.split(':', expand=True)[1].astype(int)
dataset = dataset.join(base_currency, on='base_currency_id', rsuffix='_right')
dataset['decimals_id'] = dataset.apply(lambda row: extract_attribute_id(row,0), axis=1).astype(int)
dataset = dataset.join(decimals, on='decimals_id', rsuffix='_right')
dataset['calculated_id'] = dataset.apply(lambda row: extract_attribute_id(row,1), axis=1).astype(int)
dataset = dataset.join(calculated, on='calculated_id', rsuffix='_right')
dataset['unit_multiplier_id'] = dataset.apply(lambda row: extract_attribute_id(row,2), axis=1).astype(int)
dataset = dataset.join(unit_multiplier, on='unit_multiplier_id', rsuffix='_right')
dataset['collection_indicator_id'] = dataset.apply(lambda row: extract_attribute_id(row,3), axis=1).astype(int)
dataset = dataset.join(collection_indicator, on='collection_indicator_id', rsuffix='_right')
dataset['time_period_id'] = dataset.apply(lambda row: extract_time_period_id(row), axis=1).astype(int)
dataset = dataset.join(time_period, on='time_period_id', rsuffix='_right')
dataset['exchange_rate'] = dataset.apply(lambda row: extract_exchange_rate(row), axis=1).astype(float)

# display all parsed data frames
#print(f"frequency-dimension:\n{frequency}\n")
#print(f"base_currency-dimension:\n{base_currency}\n")
#print(f"quote_currency-dimension:\n{quote_currency}\n")
#print(f"tenor-dimension:\n{tenor}\n")
#print(f"decimals-attribute:\n{decimals}\n")
#print(f"calculated-attribute:\n{calculated}\n")
#print(f"unit_multiplie-attribute:\n{unit_multiplier}\n")
#print(f"collection_indicator-attribute:\n{collection_indicator}\n")
#print(f"time_period-observation:\n{time_period}\n")
#print(f"dataset ({type(dataset)}) =\n{dataset}")

# *****************************
# **  O U T P U T   D A T A  **
# *****************************

dataset.drop(axis=1, inplace=True, columns = [
	'attributes',
	'observations',
	'index',
	'quote_currency_id',
	'quote_currency_name',
	'base_currency_id',
	'base_currency_name',
	'decimals_id',
	'decimals_code',
	'decimals_name',
	'calculated_id',
	'calculated_code',
	'calculated_name',
	'unit_multiplier_id',
	'unit_multiplier_code',
	'unit_multiplier_name',
	'collection_indicator_id',
	'collection_indicator_code',
	'collection_indicator_name',
	'time_period_id',
	'time_period_start',
	'time_period_end',
	'time_period_name',
	])
dataset.rename(inplace = True, columns = {
	'quote_currency_code': 'quote_currency',
	'base_currency_code': 'base_currency',
	'time_period_code': 'date'
	})
print(dataset)

