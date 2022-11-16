import re

def sqlvalid(string):
	# Remove spaces
	nospaces = re.sub("\s", "", string)
	# Remove special chars
	norspecials = re.sub("([^1-z])+", "", nospaces)
	return norspecials

def insertvalid(string):
	# Remove spaces
	# nospaces = re.sub("\s", "", string)
	# Remove special chars
	norspecials = re.sub("'", "", string)
	return norspecials

	# print(sqlvalid(">Hola ! que / t%al"))
	# Los 0's tambien se tienen que ir
	# print(sqlvalid('Total Digital Population_Total Unique Visitors/Viewers (000)'))

