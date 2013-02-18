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
WORD_RE = re.compile(r"[\w']+")

#linkss=[]
class inverted(MRJob):
  def mapper(self, _, v):
		if v!='':
			url_counts={}
			url,content = v.split('\t',1)
			if 	len(WORD_RE.findall(content)) > 390:	
				l=len(WORD_RE.findall(content))-350
				
				for word in WORD_RE.findall(content)[40:l]:
					if url_counts.has_key(word):
						url_counts[word]=url_counts[word]+1
					else:
						url_counts[word]=1	
				for k,v in url_counts.items():							
					yield (k.lower(),(url,v))

	

	
	def combiner(self, word, url):		
		links=[]
		url =[x for x in url]	
		#doc = [item for sublist in url for item in sublist]	
		for link in url:
			links.append(link)	


		yield (word,links)

	def reducer(self, word, url):
		
		links=[]
		for l in url:
			links.append(l)
		doc = [item for sublist in links for item in sublist]		
		yield (word, doc)
		


		
if __name__ == '__main__':
    inverted.run()
