# -*- coding: utf-8 -*-
import csv,codecs
'''with open("example.csv", "rb") as f:
    reader = csv.reader(f)
    for row in reader:
        print row[1]'''


def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data),
                            dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')

with codecs.open('tomatos.csv', 'rb') as csvfile:
	reader = unicode_csv_reader(csvfile, delimiter=',')
	for count, row in enumerate(reader):
		print count
		try:
			print({"Year": row[0],"Score": row[1],"Title":row[2]})
		except IndexError:
			print "Bad record, skip."
