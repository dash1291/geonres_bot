import urllib2
import urllib
import xml.dom.minidom
import _mysql
import string
import sys
import os,signal
DOM=xml.dom.minidom
URL=urllib2
db=None
country_list=[]
logfile=None
def exit_process():
	pidfile=open('process.pid','r')
	pid=int(pidfile.read())
	pidfile.close()
	os.kill(pid,signal.SIGKILL)
	print 'killed '+str(pid)
	os.remove('process.pid')
	return 1
def get_process_state():
	try:
		pidfile=open('process.pid','r')
		pid=int(pidfile.read())
	except:
		create_process(current_pid)
	return 1
def create_process():
	global db
	global logfile
	s=os.fork()
	if(s!=0):
		sys.exit()
	pid=os.getpid()
	pidfile=open('process.pid','w')
	pidfile.write(str(pid))
	pidfile.close()	
	logfile=open('log.txt','a')
	db=_mysql.connect(host="hostname", user="username", passwd="password",db="geonres")
	populateCountryList()
	getSimilarArtists()
	return 1
def populateCountryList():
	file_contents=open("countries.xml").read()
	xml=DOM.parseString(file_contents)
	document=xml.documentElement
	countries=document.getElementsByTagName("country");
	count=countries.length	
	for country in countries:
		country_name=country.childNodes[0].nodeValue;
		country_list.append(country_name)
	return

def addArtist(artist,country,listeners):
	global db
	global logfile
	query="INSERT INTO artists (name, country, listeners) VALUES ('"+artist+"', '"+country+"', '"+listeners+"')"
	db.query(query)
	logfile.write('added '+artist+' from '+country+' listeners '+listeners)
	return

def getSimilarArtists():
	global db
	global logfile
	f=open("lastOffset",'r')
	lastOffset=int(f.read())
	f.close()
	query="SELECT * FROM artists ORDER BY ID"
	db.query(query)
	result=db.store_result()
	row=result.fetch_row()
	index=0
	while(row):
		artist_id=row[0][0]
		artist_name=row[0][1]
		logfile.write(artist_name)
		if(index==lastOffset):
			break
		index=index+1
		row=result.fetch_row()
	data={'artist': artist_name}
	data=urllib.urlencode(data)
	url="http://ws.audioscrobbler.com/2.0/?method=artist.getsimilar&"+data+"&api_key=<api key here>"
	file_contents=URL.urlopen(url).read()
	xml=DOM.parseString(file_contents)
	document=xml.documentElement
	similar_artists=document.getElementsByTagName("artist")
	for artist in similar_artists:
		artist_name=artist.getElementsByTagName("name")[0].childNodes[0].nodeValue
		try:
			getArtistCountry(artist_name)
		except:
			logfile.write(str(sys.exc_info()[0]))
			continue
	f=open("lastOffset",'w')
	lastOffset=lastOffset+1
	f.write(str(lastOffset))
	f.close()
	#keep track of the artist index in the database
	getSimilarArtists()
	return

def getArtistCountry(artist_name):
	global db
	query="SELECT * FROM artists WHERE name = '"+artist_name+"'"
	db.query(query)
	result=db.store_result()
	row=result.fetch_row()
	if(row):
		return
	if(len(artist_name)<1):
		return
	data={'artist': artist_name}
	data=urllib.urlencode(data)
	url="http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&"+data+"&api_key=<api key here>"
	file_contents=URL.urlopen(url).read()
	xml=DOM.parseString(file_contents)
	document=xml.documentElement
	listeners=document.getElementsByTagName("listeners")[0].childNodes[0].nodeValue;
	data={'documentContent':file_contents,'documentType':'text/plain','outputType':'rss','appid':'<app id here>'}
	data=urllib.urlencode(data)
	url="http://wherein.yahooapis.com/v1/document"
	req=URL.Request(url,data)
	response=URL.urlopen(req).read()
	response_xml=DOM.parseString(response)
	document=response_xml.documentElement
	location=document.getElementsByTagName("item")
	if(not location):
		return
	location_title=location[0].getElementsByTagName("title")[0].childNodes[0].nodeValue
	for country in country_list:
		if(string.find(location_title,country)!=-1):
			if(country=='India'):
				if(string.find(location_title,'Indiana')!=-1):
					country='United States'
			addArtist(artist_name,country,listeners)
			break	
	return
command=sys.argv[1]
if command=='start':
	create_process()
if command=='stop':
	exit_process()

