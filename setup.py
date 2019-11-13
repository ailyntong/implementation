from setuptools import setup
import os

setup(
	name='orpheus',
	version='1.0.2',
	description='OrpheusDB command line tool',
	packages=['orpheus', 'orpheus.clt', 'orpheus.core'],
	url='http://orpheus-db.github.io/',
    # py_modules=['db',
	 		# 	'encryption',
	 		# 	'metadata',
	 		# 	'orpheus_const',
	 		# 	'orpheus_exceptions',
	 		# 	'orpheus_sqlparse',
	 		# 	'relation',
	 		# 	'orpheus_schema_parser',
	 		# 	'user_control',
	 		# 	'version',
	 		# 	'access',
	 		# 	'click_entry'],
	#py_modules=['click_entry'],
	install_requires=[
	    'Click', 'psycopg2-binary', 'PyYAML', 'pandas', 'pyparsing', 'sqlparse', 'django'
		#'Click'
	],
	license='MIT',
	entry_points='''
		[console_scripts]
		orpheus=orpheus.clt.click_entry:cli
	'''
)
import yaml
# Setting up orpheus home and data directory
orpheus_dir = os.path.dirname(os.path.realpath(__file__))
orpheus_data = os.path.join(orpheus_dir, 'data')
orpheus_config = os.path.join(orpheus_dir, 'config.yaml')

try:
	with open(orpheus_config, 'w') as f:
		data = yaml.safe_load(f)
		data['orpheus']['home'] = orpheus_dir
		data['orpheus']['data'] = orpheus_data
		yaml.dump(data, f, default_flow_style=False, allow_unicode=True) 
except IOError as e:
	print("Failed to load config.yaml")