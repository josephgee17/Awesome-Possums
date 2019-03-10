import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


class MakeAwardsTuple(beam.DoFn):
    def process(self, element):
        record = element
        awards_tuple = (record, '')
        return [awards_tuple]


class MakeAwardsRecord(beam.DoFn):
    def process(self, element):
        record, val = element
        return [record]

options = {
    'project': 'healthy-kayak-216403'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:
    query_results = p | beam.io.Read(beam.io.BigQuerySource(query='SELECT person, year_of_award, race_ethnicity FROM `healthy-kayak-216403.Oscar_Academy_Awards_Demographics.Demographics`
))

    query_results | 'Write to File 1' >> WriteToText('query_results.txt')

    tuple_pcoll = query_results | 'Create Awards Tuple' >> beam.ParDo(MakeAwardsTuple())

    tuple_pcoll | 'Write to File 2' >> WriteToText('output_pardo_student_tuple.txt')

    deduped_pcoll = tuple_pcoll | 'Dedup Awards Records' >> beam.GroupByKey()

    deduped_pcoll | 'Write to File 3' >> WriteToText('output_group_by_key.txt')

    out_pcoll = deduped_pcoll | 'Create Awards Record' >> beam.ParDo(MakeAwardsRecord())

    out_pcoll | 'Write to File 4' >> WriteToText('output_pardo_student_record.txt')

    qualified_table_name = 'cs327e-fa2018:college_split2.Deduped_Student'
    table_schema = 'sid:STRING,fname:STRING,lname:STRING,dob:DATE'

    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                                          schema=table_schema,
                                                                          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                          write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)
