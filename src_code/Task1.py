import apache_beam as beam
import os
from google.cloud import storage
import json

import csv

os.environ['Google_APPLICATION_CREDENTIALS'] = 'credentials.json'
storage_client = storage.Client()


def to_jason(data):
    with open("output/data_file.json", "a") as write_file:
        json.dump(data, write_file)


def main():
    print("Dataflow")
    #
    with beam.Pipeline() as pipeline:
        # 1 ReadFromText
        # lines = pipeline | 'ReadMyFile' >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv',skip_header_lines=1)
        lines = pipeline | 'ReadMyFile' >> beam.io.ReadFromText('./input/transactions.csv', skip_header_lines=1)
        # 2. Split by comma
        split = lines | beam.Map(lambda record: record.split(','))

        # 3. Filter: Find all transactions have a `transaction_amount` greater than `20`
        filter_1 = split | beam.Filter(lambda record: float(record[3]) > 20)

        # 4. Filter: Exclude all transactions made before the year `2010`
        filter_2 = filter_1 | beam.Filter(lambda record: int(record[0][0:4]) > 2010)

        # 5. Sum the total by `date`
        total_by_date = filter_2 | beam.GroupBy(lambda record: record[0][0:10])\
            .aggregate_field(lambda record: float(record[3]), sum, 'total_by_date')
        #| 'Make key value pairs' >> beam.Map(lambda elements: (elements[0] + ', ' + elements[1]+' '+elements[2], int(elements[9])) )
        #(thisTuple[0], thisTuple[1:]) 1st as key and rest as value => Dictionary


        # 6. Save the output into `output/results.jsonl.gz` and make sure all files in the `output/` directory is git ignored
        write = total_by_date | 'Write_File' >> beam.io.WriteToText('output/results.csv', compression_type='gzip')
        #         FileIO.write().to("output.txt").withCompression(Compression.GZIP)

        # write = total_by_date | 'Write_File2' >> beam.Map(lambda record: to_jason())
        write = total_by_date | 'Write_File2' >> beam.Map(to_jason)


if __name__ == "__main__":
    main()
