import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class FormatPopFn(beam.DoFn):
    def process(self, element):
        record = element
        input_HHID = record.get('HRHHID')
        input_Gender = record.get('PESEX')
        input_His = record.get('PEHSPNON')
        input_Race = record.get('PTDTRACE')
        dictionary = {
            "HRHHID": input_HHID,
            "PESEX": input_Gender,
            "PEHSPNON": input_His,
            "PTDTRACE": input_Race
        }
        return [dictionary]

options = {
    'project': 'healthy-kayak-216403'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:
    
    query_results = p | beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM Current_Population_Data.PopulationData'))

    # write PCollection to a log file
    query_results | 'Write to File 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    out_pcoll = query_results | 'Format PopData' >> beam.ParDo(FormatPopFn())
    
    # write PCollection to a log file
    out_pcoll | 'Write to File 2' >> WriteToText('output_pardo.txt')
    
    qualified_table_name = 'healthy-kayak-216403:PopulationData.Formatted_PopData'
    table_schema = 'HRHHID:INTEGER,PESEX:INTEGER,PEHSPNON:INTEGER,PTDTRACE:INTEGER'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                     schema=table_schema,  
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)
