import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery

import json

def parse_message(message):
    data = json.loads(message.decode('utf-8'))
    records = data['data']
    for record in records:
        # Convert fields to appropriate types
        record['age'] = int(record['age'])
        record['average_order_value'] = float(record['average_order_value'])
        record['lifetime_total_value'] = float(record['lifetime_total_value'])
        record['total_order_count'] = int(record['total_order_count'])
        record['user_id'] = int(record['user_id'])
    return records

def main():
    pipeline_options = PipelineOptions([
        '--project', 'thelook-product-recom-1',
        '--region', 'us-south1',
        '--runner', 'DataflowRunner',
        '--temp_location', 'gs://customer_streaming_data/temp',
        '--streaming'  # Enable streaming mode
    ])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'Read from PubSub' >> ReadFromPubSub(subscription='projects/thelook-product-recom-1/subscriptions/customer_stream_topic-sub')
            | 'Parse JSON Messages' >> beam.Map(parse_message)
            | 'Flatten Records' >> beam.FlatMap(lambda x: x)
            | 'Write to BigQuery' >> WriteToBigQuery(
                'thelook-product-recom-1.theLook.customers_features_API',
                schema='age:INT64, average_order_value:FLOAT64, country:STRING, favorite_browser:STRING, favorite_category:STRING, gender:STRING, lifetime_total_value:FLOAT64, total_order_count:INT64, traffic_source:STRING, user_id:INT64',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                method="STREAMING_INSERTS"
            )
        )

if __name__ == '__main__':
    main()
