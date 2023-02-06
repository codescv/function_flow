# coding=utf-8
# Copyright 2022 Google LLC..
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Example dataflow job."""

import json
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


class ContentExtractor(beam.DoFn):

  def process(self, element):
    # input schema: {'id': '1', 'content': 'foo', 'value': 3}
    logging.info('element: %s', element)
    item = json.loads(element.strip())
    # raise Exception('error!')
    yield item['content']


def run_pipeline():

  class AppOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      # Use add_value_provider_argument for arguments to be templatable
      # Use add_argument as usual for non-templatable arguments
      parser.add_value_provider_argument(
          '--input_path', type=str, help='Path of the file to read from')

      parser.add_value_provider_argument(
          '--output_path', type=str, help='Path of the output file')

  options = PipelineOptions()
  p = beam.Pipeline(options=options)

  app_options = options.view_as(AppOptions)

  (p
   | 'read' >> ReadFromText(app_options.input_path)
   | 'batch' >> beam.ParDo(ContentExtractor())
   | 'write' >> WriteToText(app_options.output_path))

  p.run()


if __name__ == '__main__':
  run_pipeline()
