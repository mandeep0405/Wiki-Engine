#!/usr/bin/python
# Copyright 2009-2010 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The classic MapReduce job: count the frequency of words.
"""
from mrjob.job import MRJob
import re
import itertools
from mrjob.protocol import JSONProtocol

WORD_RE = re.compile(r"[\w']+")

class inverted(MRJob):
  INPUT_PROTOCOL = JSONProtocol  # read the same format we write
	
	def mapper(self, url, page_links):
		for p in page_links[:1]:
			rank = p			
		yield (url,p)


		

		
if __name__ == '__main__':
    inverted.run()
