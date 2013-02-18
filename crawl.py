import urllib2
import json
from bs4 import BeautifulSoup 
from urlparse import urljoin
from bs4 import UnicodeDammit
from StringIO import StringIO
import gzip
from cookielib import CookieJar
import sqlite3dbm
db = sqlite3dbm.sshelve.open('/media/New Volume/craw.sqlite3')
db_page = sqlite3dbm.sshelve.open('/media/New Volume/page1.sqlite3')
# Create a list of words to ignore
ignorewords=set(['the','of','to','and','a','in','is','it'])
filename="bhai.json"
l=[]

with open(filename, mode='w') as f:
    json.dump([], f)
class crawler:
  
  
  # Initialize the crawler with the name of database
  def __init__(self,dbname):
    pass
    
  def __del__(self):
    pass
  def dbcommit(self):
    pass
  # Auxilliary function for getting an entry id and adding
  # it if it's not present
  def getentryid(self,table,field,value,createnew=True):
    return None
  # Index an individual page
  def addtoindex(self,url,soup):
	  print url
#	  print soup
	  print "\n"
	  page_con = soup
	  db[url]=page_con
	
  # Extract the text from an HTML page (no tags)
  def gettextonly(self,soup):
    return None

# Return true if this url is already indexed
  def isindexed(self,url):
    if url in url_index:
		return True
    else:
		url_index.add(url)
		return False
		
  def crawl(self,pages,depth=4):
    count = 0  
    for i in range(depth):
      newpages=[]
#      print newpages
      for page in pages:
  

        try:

			opener = urllib2.build_opener()
			opener.addheaders = [('User-agent', 'Mozilla/5.0')]
			request = opener.open(page)

        except urllib2.HTTPError, urllib2.URLError:		
			continue				
        count=count+1											
        print page + '\t'+str(count) +'\t'+str(i)
        outlink=["1"]
        soup=BeautifulSoup(request.read( ))
        soup_text=soup.body.get_text()
        self.addtoindex(page,soup_text)
        links=soup('a')
        for link in links:
          if ('href' in dict(link.attrs)):
			  val=link['href'].split('/')
			  s=set(val)
			  if 'wiki' in s and val[1]=='wiki' and str(val[2]).find(":")==-1:
				  url=urljoin('http://en.wikipedia.org',link['href'])
				  if url.find("'")!=-1: continue
				  url=url.split('#')[0]  # remove location portion
				  if url[0:4]=='http':
					#  print url + "           inside"
					  outlink.append(url)
					  if not self.isindexed(url):
						  newpages.append(url)		
    	db_page[page] = outlink
    				
      pages=newpages
  # Create the database tables
  def createindextables(self):
    pass
    
pagelist=['http://en.wikipedia.org/wiki/Finance']
url_index=set()
url_index.add('http://en.wikipedia.org/wiki/Finance')	
c=crawler('')
c.crawl(pagelist)
    
