import argparse
import requests
import logging
from datetime import datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText



class calVisitDuration(beam.DoFn):
    def process(self,element):
        dt_frmt = '%Y-%m-%dT%H:%M:%S'
        start_dt = datetime.strptime(element[1], dt_frmt)
        end_dt = datetime.strptime(element[2], dt_frmt)
        diff = end_dt - start_dt
        
        yield [element[0], diff.total_seconds()]

    
class calCountryOrigin(beam.DoFn):
    def process(self, element):
        
        ip = element[0]
        resp = requests.get(f'http://ip-api.com/json/{ip}?fields=country')
        country = resp.json()['country']
        
        yield [ip, country]


def mapIpToCountry(element, ip_map):
    ip = element[0]
    return [ip_map[ip], element[1]]
    
    
def run(argv = None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input')
    parser.add_argument('--output')
    args, beam_args = parser.parse_known_args(argv)
    
    with beam.Pipeline(argv=beam_args) as p:
        
        lines = (
            p
            | 'Read data' >> ReadFromText(args.input, skip_header_lines = 1)
            | 'Parse lines ' >> beam.Map(lambda row:row.split(','))
        )
            
        duration = lines | 'VisitDuration' >> beam.ParDo(calVisitDuration())
        ip_map = lines | 'GetIpCountryOrigin' >> beam.ParDo(calCountryOrigin())
            
        result = (
            duration
            | 'MapIpToCountry' >> beam.Map(mapIpToCountry, ip_map=beam.pvalue.AsDict(ip_map))
            | 'AvgByCountry' >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
            | 'Format Output' >> beam.Map(lambda row: ','.join(map(str, row)))
            | 'WriteToFile' >> WriteToText(args.output, file_name_suffix = '.csv')
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.WARNING)
    run()
            