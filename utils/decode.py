def decode(source: dict, key, value=None):
	try:
		return source[key]
	except:
		return value