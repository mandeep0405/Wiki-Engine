#!/usr/bin/python

# Import modules for CGI handling 
import cgi, cgitb 

import sys
import re
import copy
from collections import defaultdict
sys.stderr = sys.stdout
# Create instance of FieldStorage 
form = cgi.FieldStorage() 

# Get data from fields
query = form.getvalue('query')
class QueryIndex:
  
    def __init__(self):
        self.index={}
        self.pagerank={}

    def getStopwords(self):
        f=open(self.stopwordsFile, 'r')
        stopwords=[line.rstrip() for line in f]
        self.sw=dict.fromkeys(stopwords)
        f.close()
            
    def intersectLists(self,lists):
        if len(lists)==0:
            return []
        #start intersecting from the smaller list
        lists.sort(key=len)
        return list(reduce(lambda x,y: set(x)&set(y),lists))
                
    def getTerms(self, line):
        line=line.lower()
        line=re.sub(r'[^a-z0-9 ]',' ',line) #put spaces instead of non-alphanumeric characters
        line=line.split()
        line=[x for x in line if x not in self.sw]
#        line=[ porter.stem(word, 0, len(word)-1) for word in line]
        return line

    def readRanks(self):
		f=open("/home/mandeep/pageranks",'r')
		for line in f:
			line= line.rstrip()
			url, rank =line.split('\t')
			url = re.search(r'\"(.+?)\"',url)
			url = url.group().strip('"')
			rank = re.search(r'\"(.+?)\"',rank)
			rank = rank.group().strip('"') 
			self.pagerank[url]=rank
		f.close()
                       			    
    def readIndex(self):
        f=open("/home/mandeep/believe", 'r');
        for line in f:
            line=line.rstrip()
            termID, postings = line.split('\t')
            terms = re.search(r'\"(.+?)\"',termID)
            term = terms.group().strip('"')

            self.index[term]=postings
        f.close()

    def queryType(self,q):

        if len(q.split()) > 1:
            return 'FTQ'
        else:
            return 'OWQ'

    def owq(self,q):
        '''One Word Query'''
        originalQuery=q
        q=self.getTerms(q)
        if len(q)==0:
          #  print ''
            return
        elif len(q)>1:
            self.ftq(originalQuery)
            return
        
        #q contains only 1 term 
        q=q[0]
        links=[]
        if q not in self.index:
         #   print 'sorry cant find it'
            return
        else:
            p=self.index[q]
            #print p
            p=[x[0] for x in p]
            p=''.join(p)  #docid's are integers
           # print p
            top_most=""
            links = re.findall(r'\"(.+?)\"',p)
            for m in links:
				if re.search(q,m.lower()):
					#match=re.search(q,m)
					top_most=m
					continue
            #print links
            tf = re.findall(r'(?<=\,)[+ -]?\d+',p)
            #print tf
            page_tf=dict(zip(links,tf))	
            #print page_tf			
            ranks = {}
            for page in links:
                if self.pagerank.has_key(page):
					ranks[page]=float(self.pagerank[page])
					#print ranks
                else:
					ranks[page]=0.15			
            final_pages ={}
            for page in links:
				final_pages[page] = float(ranks[page]*int(page_tf[page]))
            count=0
            final_result=[]
            for k, v in sorted(final_pages.items(), key=lambda kv: kv[1], reverse=True):
				if count < 20:
					if top_most!="":						
						final_result.append(top_most)
						top_most=""
					else:
						final_result.append(k)
					#	print("%s => %s" % (k,v))
						count +=1
            return final_result
            #result = sorted(ranks , key= lambda x : ranks[x])
            #count=0
            #final_pages=[]
            ##print result
            #for urls in reversed(result):
				#if count < 20:
					#print urls
					#count +=1 
					
			#	print x
            #for link in p:
				#print link
				#link = re.findall(r'\"(.+?)\"',link)
				#links.append(link)	     
				
          

    def ftq(self,q):
        """Free Text Query"""
        q=self.getTerms(q)
        if len(q)==0:
        #    print ''
            return
        
        all_the_pages=[]
        all_the_pages_tf=[]
        all_the_pages_tf_dic=defaultdict(list)
        for term in q:
            try:
                p=self.index[term]
                p=[x[0] for x in p]
                p=''.join(map(str,p)) 
                links = re.findall(r'\"(.+?)\"',p)
                all_the_pages.append(links)
                tf = re.findall(r'(?<=\,)[+ -]?\d+',p)
                for i in range(len(links)):
					all_the_pages_tf_dic[links[i]].append(tf[i])
            except:
                #term not in index
                pass
#        print all_the_pages
        comman_pages=self.intersectLists(all_the_pages)        
       # print comman_pages
        comman_pages_tf={}
        for res in comman_pages:
			s=0
			for med in all_the_pages_tf_dic[res]:
				s+=int(med)
			comman_pages_tf[res]=s/len(all_the_pages_tf_dic[res])
        comman_ranks={}
#        for page in comman_pages:
#			comman_ranks[page]=self.pagerank[page]
        count=0			
        final_result=[]
        for k, v in sorted(comman_pages_tf.items(), key=lambda kv: kv[1], reverse=True):
			if count < 20:
				final_result.append(k)
		#		print("%s => %s" % (k,v))
				count +=1    						
        return final_result   
    
    def getParams(self):
        self.stopwordsFile="/home/mandeep/Downloads/mrjob-master/mrjob/examples/stop.txt"
        self.indexFile="/home/mandeep/believe"
        self.rankFile="/home/mandeep/pageranks"   
                
    def queryIndex(self):
        self.getParams()
     #   print "params done"
     #   print self.indexFile
        self.readIndex()  
      #  print 'readIndex done'
        self.getStopwords() 
      #  print "stop done"
        self.readRanks()
        q=query
        qt=self.queryType(q)
        if qt=='OWQ':
			final=self.owq(q)
			return final
        elif qt=='FTQ':
            final=self.ftq(q)
            return final
        #while True:
      #      q=sys.stdin.readline()
     #       print q
            #if q=='':
             #   break

            #qt=self.queryType(q)
      #      print qt
            #if qt=='OWQ':
           #     final=self.owq(q)
       #         print final
         #   elif qt=='FTQ':
          #      self.ftq(q)
        #print "end!"        
   
if __name__=="__main__":
	WORD_RE = re.compile(r"[\w']+")
	q=QueryIndex()
	vv=q.queryIndex()

print "Content-type:text/html\r\n\r\n"
print "<html>"
print "<head>"
print "<title>Hello - Second CGI Program</title>"
print "</head>"
print "<body>"
print"<p><h1>Results</h1></p>"
#print "<h4>%s</h4>" %(vv) 
#print vv
for part in vv:
	print "<h4>%s</h4>" % (part)
	
print "</body>"
print "</html>"
