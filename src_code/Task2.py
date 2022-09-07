import apache_beam as beam
import apache_beam as beam
import os
from google.cloud import storage


# os.environ['Google_APPLICATION_CREDENTIALS'] = 'credentials.json'
# storage_client = storage.Client()

# Expand method to be overridden : Takes Pcollection as input


class MyTransform(beam.PTransform):
    def expand(self, pcoll):
        # Transform logic goes here.
        pcoll_return = pcoll | beam.Map(lambda record: record.split(',')) \
                       | beam.Filter(lambda record: float(record[3]) > 20) \
                       | beam.Filter(lambda record: int(record[0][0:4]) > 2010)
        return pcoll_return


def main():
    print("Dataflow: Composite Transform")
    p = beam.Pipeline()
    transform_coll = (
            p
            | 'ReadMyFile' >> beam.io.ReadFromText('beam_input.csv', skip_header_lines=1)
            | 'composite Transform' >> MyTransform()
            | 'Write to File1' >> beam.Map(print)
    )

    p.run()


if __name__ == "__main__":
    main()
