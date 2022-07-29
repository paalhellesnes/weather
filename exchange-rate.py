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
print("Welcome to the exchange rate converter\n")

verbose = False
progress = True

# *****************************
# **  G A T H E R   D A T A  **
# *****************************

# Sist publiserte (lastNObservations=1) virkedag (B) kurs for alle valuta (tom verdi signaliserer "alle") i JSON format (format=sdmx-json) fra EXR dataflyten:
request_current_exchange_rates = 'https://data.norges-bank.no/api/data/EXR/B..NOK.SP?lastNObservations=1&format=sdmx-json'
request_current_exchange_rates_3_days = 'https://data.norges-bank.no/api/data/EXR/B..NOK.SP?lastNObservations=3&format=sdmx-json'

# Månedlig (M) kurs for sveitsiske franc (CHF) og (+) amerikanske dollar (USD) siden 2010 (startPeriod=2010) som XML (format=sdmx-compact-2.1) fra EXR dataflyten:
request_monthly_exchange_rates_chf_usd_since_2010 = 'https://data.norges-bank.no/api/data/EXR/M.CHF+USD.NOK.SP?startPeriod=2010&format=sdmx-json'

# Alle (ingen spesifikasjon av tidsperiode i parametere) månedlige (M) noteringer for sveitsiske franc (CHF) som tegnseparert fil (csv) med norsk oppsett (locale=no): komma som desimaltegn og semikolon som skilletegn. I tillegg tas det med ByteOrderMark for at Excel skal forstå de norske tegnene. Denne vises med tid som rader:
request_monthly_exchange_rates_chf_all_years = 'https://data.norges-bank.no/api/data/EXR/M.CHF.NOK.SP?format=sdmx-json'

request_monthly_exchange_rates_all_currencies_all_years = 'https://data.norges-bank.no/api/data/EXR/B..NOK.SP?format=sdmx-json'

# gather exchange rates from norges bank
if progress: print('Gathering exchange rates from norges bank...\n')
base_url = request_monthly_exchange_rates_chf_usd_since_2010
response = requests.get(base_url)
response.ok
response.status_code
response.text
data = response.json()

# ***************************************
# **  I N V E S T I G A T E   D A T A  **
# ***************************************

# investigate json structure
if verbose:
	print(data.keys())
	print(json.dumps(data, indent = 4))

# ***********************************
# **  T R A N S F O R M   D A T A  **
# ***********************************

# parse dimensions
if progress: print('Parsing dimensions...\n')
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
if progress: print('Parsing attributes...\n')
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
if progress: print('Parsing exchange rate facts...\n')
dataset = pd.DataFrame()
for serie in data['data']['dataSets'][0]['series']:
	quote_currency_id = int(serie.split(':')[0])
	base_currency_id = int(serie.split(':')[1])
	if progress: print(f"  currency {base_currency_id} of {len(data['data']['dataSets'][0]['series'])}")
	decimals_id = data['data']['dataSets'][0]['series'][serie]['attributes'][0]
	calculated_id = data['data']['dataSets'][0]['series'][serie]['attributes'][1]
	unit_multiplier_id = data['data']['dataSets'][0]['series'][serie]['attributes'][2]
	collection_indicator_id = data['data']['dataSets'][0]['series'][serie]['attributes'][3]
	for observation in data['data']['dataSets'][0]['series'][serie]['observations']:
		time_period_id = int(observation)
		exchange_rate = float(data['data']['dataSets'][0]['series'][serie]['observations'][observation][0])
		dict = {
			'quote_currency_id': quote_currency_id,
			'base_currency_id': base_currency_id,
			'decimals_id': decimals_id,
			'calculated_id': calculated_id,
			'unit_multiplier_id': unit_multiplier_id,
			'collection_indicator_id': collection_indicator_id,
			'time_period_id': time_period_id,
			'exchange_rate': exchange_rate
			}
		row = pd.DataFrame(dict, index=[0])
		dataset = pd.concat([dataset, row], axis=0, ignore_index=True)
dataset = dataset.join(quote_currency, on='quote_currency_id', rsuffix='_right')
dataset = dataset.join(base_currency, on='base_currency_id', rsuffix='_right')
dataset = dataset.join(decimals, on='decimals_id', rsuffix='_right')
dataset = dataset.join(calculated, on='calculated_id', rsuffix='_right')
dataset = dataset.join(unit_multiplier, on='unit_multiplier_id', rsuffix='_right')
dataset = dataset.join(collection_indicator, on='collection_indicator_id', rsuffix='_right')
dataset = dataset.join(time_period, on='time_period_id', rsuffix='_right')

# display all parsed data frames
if verbose:
	print(f"frequency-dimension:\n{frequency}\n")
	print(f"base_currency-dimension:\n{base_currency}\n")
	print(f"quote_currency-dimension:\n{quote_currency}\n")
	print(f"tenor-dimension:\n{tenor}\n")
	print(f"decimals-attribute:\n{decimals}\n")
	print(f"calculated-attribute:\n{calculated}\n")
	print(f"unit_multiplie-attribute:\n{unit_multiplier}\n")
	print(f"collection_indicator-attribute:\n{collection_indicator}\n")
	print(f"time_period-observation:\n{time_period}\n")
	print(f"dataset ({type(dataset)}) =\n{dataset}")

# *****************************
# **  O U T P U T   D A T A  **
# *****************************

# Transform output
if progress: print('\nTransforming output...\n')
dataset.drop(axis=1, inplace=True, columns = [
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

# Save to parquet file
if progress: print('Saving to parquet file...\n')
dataset.to_parquet('~/Downloads/exchange-rate.parquet', engine='fastparquet')
#fp.write('~/Downloads/exchange-rate.parq.snappy', dataset ,compression='SNAPPY')

print(f"{dataset}\n")

