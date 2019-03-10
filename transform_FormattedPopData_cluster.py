import logging
import apache_beam as beam
import os
import datetime
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class FormatPopFn(beam.DoFn):
    def process(self, element):
        record = element
        input_HHID = record.get('HRHHID')
        input_Gender = record.get('PESEX')
        input_His = record.get('PEHSPNON')
        if input_His == 1:
            input_His = 100
        elif input_His == 2:
            input_His = 200
        input_Race = record.get('PTDTRACE')
        if input_Race > 6:
            input_Race = 6
        dictionary = {
            "HRHHID": input_HHID,
            "PESEX": input_Gender,
            "PEHSPNON": input_His,
            "PTDTRACE": input_Race
        }
        return [dictionary]

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# run pipeline on Dataflow 
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-application-table',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-8',
    'num_workers': 12
}

opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DataflowRunner', options=opts) as p:
    
    query_results = p | beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM Current_Population_Data.PopulationData'))

    # write PCollection to a log file
    query_results | 'Write to File 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    out_pcoll = query_results | 'Format PopData' >> beam.ParDo(FormatPopFn())
    
    # write PCollection to a log file
    out_pcoll | 'Write to File 2' >> WriteToText('output_pardo.txt')
    
    qualified_table_name = 'healthy-kayak-216403:PopulationData.Formatted_PopDataAll'
    table_schema = 'HRHHID:INTEGER,PESEX:INTEGER,PEHSPNON:INTEGER,PTDTRACE:INTEGER'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                     schema=table_schema,  
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)
