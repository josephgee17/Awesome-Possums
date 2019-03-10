import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class FormatDemoFn(beam.DoFn):
    def process(self, element):
        record = element
        input_person = record.get('person')
        input_race = record.get('race_ethnicity')
        input_raceConf = record.get('race_ethnicity_confidence')
        input_awardYear = record.get('year_of_award')
        input_award = record.get('award')
        dictionary = {
            "person": input_person,
            "race": input_race,
            "race_confidence": input_raceConf,
            "award_year": input_awardYear,
            "award": input_award
        }
        return [dictionary]
options = {
    'project': 'healthy-kayak-216403'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:
    
    query_results = p | beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM Oscar_Academy_Awards_Demographics.Demographics'))

    # write PCollection to a log file
    query_results | 'Write to File 1' >> WriteToText('query_results1.txt')

    # apply a ParDo to the PCollection 
    out_pcoll = query_results | 'Format PopData' >> beam.ParDo(FormatDemoFn())
    
    # write PCollection to a log file
    out_pcoll | 'Write to File 2' >> WriteToText('output_pardo1.txt')
    
    qualified_table_name = 'healthy-kayak-216403:PopulationData.Formatted_OscarDemo'
    table_schema = 'person:STRING,race:STRING,race_confidence:FLOAT,award_year:INTEGER,award:STRING'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                     schema=table_schema,  
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)
