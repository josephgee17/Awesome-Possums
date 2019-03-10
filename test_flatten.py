import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# Project ID is needed for bigquery data source, even with local execution.
options = {
    'project': 'healthy-kayak-216403'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:
    
    RACE_pcoll = p | 'Read Race' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM Current_Population_Data.EncryptionCode1'))

    # write PCollection to a log file
    RACE_pcoll | 'Write to File 1' >> WriteToText('race_query_results.txt')
    
    HISPANIC_pcoll = p | 'Read Hispanic' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM PopulationData.Formatted_HISPANIC'))

    # write PCollection to a log file
    HISPANIC_pcoll | 'Write to File 2' >> WriteToText('hispanic_query_results.txt')

    # Flatten the two PCollections 
    merged_pcoll = (RACE_pcoll, HISPANIC_pcoll) | 'Merge Race and Hispanic' >> beam.Flatten()
    
    # write PCollection to a file
    merged_pcoll | 'Write to File 3' >> WriteToText('output_flatten1.txt')
    
    qualified_table_name = 'healthy-kayak-216403:PopulationData.Merged_Race'
    table_schema = 'PTDTRACE:INTEGER,RACE:STRING,PEHSPNON:INTEGER,HISPANIC:STRING'
    
    merged_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                     schema=table_schema,  
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)
