from orpheus.core.attribute import AttributeManager
from orpheus.core.exception import NotImplementedError

# This file is static file for parsing user given schema information
# Expected comma seperated file as default

class FormatError(Exception):
	def __init__(self, filename):
		self.filename = filename
	def __str__(self):
		return "Error parsing %s, please check format" % self.filename

class ReservedFieldError(Exception):
	def __init__(self, field):
		self.field = field
	def __str__(self):
		return "Error parsing field %s, reserved field" % self.field	

class Parser(object):

	@staticmethod
	def get_attributes_from_file(abs_path, delimiter=','):
		# Postgresql supported type
		PREDEFINED_TYPE = AttributeManager.valid_types

		# Reserved attribute names
		RESERVED_ATTRIBUTES = set(['rid', 'vid', 'aid'])


		attribute_name, attribute_type = [],[]
		with open(abs_path, 'r') as f:
			for line in f:
				try:
					[cur_attribute, cur_attribute_type] = line.rstrip().split(delimiter)
					if cur_attribute_type not in PREDEFINED_TYPE:
						raise NotImplementedError("Type %s not supported" % cur_attribute_type)
						return
					if cur_attribute in RESERVED_ATTRIBUTES:
						raise ReservedFieldError(cur_attribute)
						return

					# use generator if file is really large	
					attribute_name.append(cur_attribute)
					attribute_type.append(cur_attribute_type)
				except ValueError:
					raise FormatError(abs_path)
					return
		return attribute_name, attribute_type
