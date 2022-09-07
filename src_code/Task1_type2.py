import apache_beam as beam
import apache_beam as beam
import os
from google.cloud import storage
import json

os.environ['Google_APPLICATION_CREDENTIALS'] = 'credentials.json'
storage_client = storage.Client()


def to_jason(data):
    with open("output/data_file.json", "a") as write_file:
        json.dump(data, write_file)


def main():
    print("Dataflow")

    p = beam.Pipeline()
    transform_coll = (
            p
            | 'ReadMyFile' >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
            | 'Composite Transform' >>  beam.Map(lambda record: record.split(','))
            | beam.Filter(lambda record: float(record[3]) > 20)
            | beam.Filter(lambda record: int(record[0][0:4]) > 2010)
            | beam.GroupBy(lambda record: record[0][0:10]) \
            .aggregate_field(lambda record: float(record[3]), sum, 'total_by_date')
            | 'Write to File1' >> beam.Map(print)
    )

    write = (
            transform_coll
            | 'Write_File' >> beam.io.WriteToText('output/results.csv')
            | 'Write_File2' >> beam.Map(to_jason)
    )


if __name__ == "__main__":
    main()
