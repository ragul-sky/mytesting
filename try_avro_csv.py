import avro.schema, csv,codecs
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


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

schema = avro.schema.parse(open("tomatos.avsc", "rb").read())

with open('tomatos.csv', 'rb') as csvfile:
	reader = unicode_csv_reader(csvfile, delimiter=',')
	writer = DataFileWriter(open("tomatos.avro", "wb"), DatumWriter(), schema, codec='deflate')
	for count, row in enumerate(reader):
		print count
		try:
			writer.append({"Year": int(row[0]), "Score": int(row[1]), "Title": row[2]})
		except IndexError:
			print "Bad record, skip."
	writer.close()

reader = DataFileReader(open("tomatos.avro", "rb"), DatumReader())
for user in reader:
    print user
reader.close()
