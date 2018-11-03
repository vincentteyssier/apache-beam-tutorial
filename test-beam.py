import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import numpy as np

# defining custom arguments
class MyOptions(PipelineOptions):    
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--input',
                        help='Input for the pipeline',
                        default='./data/')
    parser.add_argument('--output',
                        help='Output for the pipeline',
                        default='./output/')

# class to split a csv line by elements 
class Split(apache_beam.DoFn):
    def process(self, element):
        Date,Open,High,Low,Close,Volume = element.split(",")
        return [{
            'Open': float(Open),
            'Close': float(Close)
        }]

# class to calculate the standard deviation over an entire PCollection
class Standard_deviation(apache_beam.DoFn):
    def create_accumulator(self):
        return (0.0, 0.0, 0) # x, x^2, count

    def add_input(self, sum_count, input):
        (sum, sumsq, count) = sum_count
        return sum + input, sumsq + input*input, count + 1

    def merge_accumulators(self, accumulators):
        sums, sumsqs, counts = zip(*accumulators)
        return sum(sums), sum(sumsqs), sum(counts)

    def extract_output(self, sum_count):
        (sum, sumsq, count) = sum_count
        if count:
        mean = sum / count
        variance = (sumsq / count) - mean*mean
        # -ve value could happen due to rounding
        stddev = np.sqrt(variance) if variance > 0 else 0
        return {
            'mean': mean,
            'variance': variance,
            'stddev': stddev,
            'count': count
        }
        else:
        return {
            'mean': float('NaN'),
            'variance': float('NaN'),
            'stddev': float('NaN'),
            'count': 0
        }

# setting input and output files
input_filename = "./data/sp500.csv"
output_filename = "./output/result.txt"

# instantiate the pipeline
options = PipelineOptions()
p = beam.Pipeline(options=options)

# reading the csv and splitting lnies by elements we want to retain
csv_lines = (
        p | beam.io.ReadFromText(input_filename) |
        beam.ParDo(Split())
    )

# calculate the mean
mean = (
    csv_lines | "Calculating mean" >> beam.CombineValues(
        beam.combiners.MeanCombineFn()
        )
    )

# calculate the standard deviation
std = (
    csv_lines | "Calculating standard deviation" >> beam.ParDo(Standard_deviation())
    )

# writing results to file
output = (
    mean | beam.io.WriteToText(output_filename)
)