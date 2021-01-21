import os
import mysql.connector
import portvakt as pk

cnx = mysql.connector.connect(user='root', password='nyan', host='127.0.0.1')
cursor = cnx.cursor()


def database_setup():
	database_creation = 'create database if not exists weather_raw;'
	database_usage = 'use weather_raw;'
	drop_table_check = 'drop table if exists weather_ingest_raw;'
	create_table = '''create table if not exists weather_ingest_raw
		(
		event_id varchar(10),
		weather_type varchar(15),
		severity varchar(10),
		start_time varchar(20),
		end_time varchar(20),
		timezone varchar(30),
		airport_code varchar(20),
		location_lat varchar(25),
		location_long varchar(25),
		city varchar(30),
		county varchar(30),
		state varchar(20),
		zipcode varchar(20)
		);'''

	cursor.execute(database_creation)
	cursor.execute(database_usage)
	cursor.execute(drop_table_check)
	cursor.execute(create_table)
	cnx.commit()


def file_parsing():
	values_string = ''
	insertion_string = 'insert into weather_ingest_raw values '
	line_num = 1

	data_file = 'C:/Users/helto/OneDrive/Desktop/Portfolio/Weather Data/US_WeatherEvents_2016-2019.csv'
	data = open(data_file, 'r')

	for line in data:
		line = str(line).replace(",","','").replace('\n','')
		print(line)
		values_string = values_string+"('"+str(line).replace("'",'').replace(",","','").replace('\n','')+"'), "

		if line_num == 1000:
			values_string = values_string[:-2]
			full_string = insertion_string+values_string+';'
			cursor.execute(full_string)
			cnx.commit()
			values_string = ''
			line_num = 1

		line_num = line_num + 1


database_setup()
file_parsing()