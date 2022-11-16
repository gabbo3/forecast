import traceback
import numpy as np
import pyodbc
import pandas as pd
import logging
from utils.sqlvalid import insertvalid

class SQLServer():
	def __init__(self):
		try:
			self.conn = pyodbc.connect('Driver={SQL Server};'
										'Server=192.168.100.18;'
										'Database=ANALITICA;'
										'UID=python_externo;'
										'PWD=MazzPythonNew'
										# 'Trusted_Connection=yes;'
										)
			# self.conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+'BUEANA03'+';DATABASE='+database+';UID='+username+';PWD='+ password)
			logging.info('Conectado a SQL Server')
		except pyodbc.OperationalError:
			logging.error('Error de conexion')
			exit()

	def truncate(self,table):
		cursor = self.conn.cursor()
		statement = "truncate table " + table
		cursor.execute(statement)
		self.commit()
			
	def commit(self):
		cursor = self.conn.cursor()
		cursor.execute('commit')
			
	def insert(self,df: pd.DataFrame, table):
		
		columns = self.validate_columns(df,table)

		if columns == None:
			print('Columns unmatched')
			return
		print('Columns matched')

		for i,r in df.iterrows():
			statement = 'insert into '+ table + '(' + columns + ')' + ' VALUES ('
			for value in r:
				if type(value) == str:
					statement += "'" + insertvalid(value) + "',"	
				elif type(value) == int:
					statement += str(value) + ","
				elif type(value) == bool:
					statement += "'" + insertvalid(str(value)) + "',"	
				elif str(value) == 'nan' or str(value) == 'NaN' or value is None:
					statement += "NULL,"
				elif type(value) == float:
					statement += str(value) + ","
			statement = statement[:-1]
			statement += ')'
			# logging.info(statement)
			
			try:
				print(statement)
				# cursor = self.conn.cursor()
				# cursor.execute(statement)
			except Exception as e:
				# logging.error(e.args + ':' + e)
				print(e.args + ' : ' + e)
		
		self.commit()
	
	def validate_columns(self,df: pd.DataFrame, table):
		cursor = self.conn.cursor()
		statement = "SELECT column_name FROM analitica.information_schema.columns where UPPER(table_name) = '"
		statement += table +"' ORDER BY table_name,ORDINAL_POSITION"
		cursor.execute(statement)
		table_columns = str()
		quoted_columns = []
		for i in cursor:
			quoted_columns.append(i)
			table_columns += i[0]
			table_columns += ','
		table_columns = table_columns[:-1]

		df_columns = str()
		for i in df.columns:
			df_columns += i
			df_columns += ','
		df_columns = df_columns[:-1]

		logging.debug(table)
		
		if table_columns != df_columns:
			logging.warning('Columns unmatched in: ' + table)
			logging.warning(table_columns)
			logging.warning(df_columns)
			return None
		else:
			retval = str()
			for i in quoted_columns:
				retval += '['
				retval += i[0]
				retval += ']'
				retval += ','
			retval = retval[:-1]
			return retval

	def upsert(self,dataframe: pd.DataFrame,table):
		columns = self.validate_columns(dataframe,table)
		if columns == None:
			return

		sets = self.getSets(dataframe)
		insert_values = self.getInsertValues(dataframe)
		for i,r in dataframe.iterrows():
			statement = 'MERGE ' + table + ' AS TARGET USING (VALUES ('
			for value in r:
				if str(value) == 'nan' or str(value) == 'NaN' or value is None or value == np.NaN:
					statement += "NULL,"
				elif type(value) == str:
					statement += "'" + insertvalid(value) + "',"	
				elif type(value) == int:
					statement += str(value) + ","
				elif type(value) == bool:
					statement += "'" + insertvalid(str(value)) + "',"	
				elif type(value) == float:
					statement += str(value) + ","
			statement = statement[:-1]
			statement += ')'
			statement += ') AS SOURCE (' + columns +')'
			statement += ' on (TARGET.UKEY = SOURCE.UKEY) WHEN MATCHED THEN UPDATE SET ' 
			statement += sets
			statement += ' WHEN NOT MATCHED THEN '
			statement += ' INSERT (' + columns + ') VALUES (' + insert_values + ');'
			try:
				cursor = self.conn.cursor()
				cursor.execute(statement)
			except Exception as e:
				logging.info(statement)
				print(statement)
				logging.error(e, exc_info=True)
				traceback.print_exc()
		
		self.commit()
	
	def getSets(self,df: pd.DataFrame):
		df_columns = str()
		for i in df.columns:
			if i == 'UKEY':
				continue
			df_columns += 'TARGET.'
			df_columns += '['
			df_columns += i
			df_columns += ']'
			df_columns += ' = SOURCE.'
			df_columns += '['
			df_columns += i
			df_columns += ']'
			df_columns += ','
		df_columns = df_columns[:-1]
		return df_columns
		
	def getInsertValues(self,df: pd.DataFrame):
		df_columns = str()
		for i in df.columns:
			df_columns += 'SOURCE.'
			df_columns += '['
			df_columns += i
			df_columns += ']'
			df_columns += ','
		df_columns = df_columns[:-1]
		return df_columns

	def query(self, query) -> pd.DataFrame:
		cursor = self.conn.cursor()
		cursor.execute(query)
		result = cursor.fetchall()
		df = pd.DataFrame.from_records(result, columns=[x[0] for x in cursor.description])
		return df

    