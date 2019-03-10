import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs processing on each element from the input PCollection.
class FormatHISFn(beam.DoFn):
  def process(self, element):
    record = element
    input_his = record.get('PEHSPNON')
    input_val = record.get('HISPANIC')
    
    # desired PTDTRACE format: "race_value" (e.g. "White")
    # input date formats: integer_value 
    if input_his == 1:
    	input_his = 100
    elif input_his == 2:
    	input_his = 200
      
    dictionary = {
      "PEHSPNON": input_his,
      "HISPANIC": input_val
    }
    return [dictionary]

options = {
    'project': 'healthy-kayak-216403'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:
    
    query_results = p | beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM Current_Population_Data.Hispanic'))

    # write PCollection to a log file
    query_results | 'Write to File 1' >> WriteToText('query_results3.txt')

    # apply a ParDo to the PCollection 
    out_pcoll = query_results | 'Format RACE' >> beam.ParDo(FormatHISFn())
    
    # write PCollection to a log file
    out_pcoll | 'Write to File 2' >> WriteToText('output_pardo3.txt')
    
    qualified_table_name = 'healthy-kayak-216403:PopulationData.Formatted_HISPANIC'
    table_schema = 'PEHSPNON:INTEGER,HISPANIC:STRING'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                     schema=table_schema,  
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)
