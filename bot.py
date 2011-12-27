import MySQLdb
import _mysql
import os
import signal
import string
import sys
import urllib
import urllib2
import xml.dom.minidom

DOM = xml.dom.minidom
URL = urllib2
db = None
country_list = []
logfile = None
lastOffset = 0

def get_lastOffset():
    global lastOffset
    f = open("lastOffset", 'r')
    lastOffset = int(f.read())
    f.close()

def incr_lastOffset():
    global lastOffset
    f = open("lastOffset",'w')
    lastOffset = lastOffset + 1
    f.write(str(lastOffset))
    f.close()
    
def exit_process():
    pidfile = open('process.pid', 'r')
    pid = int(pidfile.read())
    pidfile.close()
    os.kill(pid, signal.SIGKILL)
    print 'killed ' + str(pid)
    os.remove('process.pid')
    return 1

def get_process_state():
    try:
        pidfile = open('process.pid', 'r')
        pid = int(pidfile.read())
    except:
        create_process(current_pid)
	return 1

def create_process():
    global logfile
    s = os.fork()
    if(s!=0):
        sys.exit()
    pid = os.getpid()
    pidfile = open('process.pid', 'w')
    pidfile.write(str(pid))
    pidfile.close()	
    connectDB()
    return 1

def connectDB():
    global db, logfile
    logfile = open('log.txt', 'a')
    db = MySQLdb.connect(host="geonres.db.8046490.hostedresource.com",
         user="geonres", passwd="Dubey@111", db="geonres")
    db = db.cursor()
    populateCountryList()
    get_lastOffset()
    incr_lastOffset()
    getArtistInformation()
    return 1

def populateCountryList():
	file_contents = open("countries.xml").read()
	xml = DOM.parseString(file_contents)
	document = xml.documentElement
	countries = document.getElementsByTagName("country");
	count = countries.length	
	for country in countries:
            country_name = country.childNodes[0].nodeValue;
            country_list.append(country_name)
	return

def addArtist(artist):
	global db
	global logfile
      	query = "SELECT * FROM artists WHERE name='" + artist + "'"
        try:
	    db.execute(query)
        except:
            return
	row = db.fetchone()
	if(row):
	    return
	if(len(artist)<1):
	    return
	query = "INSERT INTO artists (name, country, listeners) VALUES ('"
        query = query + artist + "', '" + "notset" + "', '" + "0" + "')"
	db.execute(query)
        log = 'added ' + artist
        logfile.write(log)
	return

def updateArtist(artist, location, listeners):
	global db
	global logfile
      	query = "UPDATE artists SET listeners='" + listeners + "', country='"
        query = query + location + "' WHERE name='" + artist + "'"
        db.execute(query)
        log = 'updated ' + artist + ' country ' + location + ' listeners '
        log = log + listeners
        logfile.write(log)
	return

def needs_update(artist):
    global db
    query = "SELECT * FROM artists WHERE name='" + artist + "'"
    db.execute(query)
    row = db.fetchone()
    if(row[2]=='notset'):
        return True
    else:
        return False
    
def getArtistInformation():
	global db, lastOffset
        get_lastOffset()
	query = "SELECT * FROM artists ORDER BY ID"
	db.execute(query)
	row = db.fetchone()
	index = 0
	while(row):
            artist_id = row[0]
            artist_name = row[1]
            if(index==lastOffset):
                break
            index = index + 1
            row = db.fetchone()

	data = {'artist': artist_name}
	data = urllib.urlencode(data)
	url = "http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&"
        url = url + data + "&api_key=b25b959554ed76058ac220b7b2e0a026"
	file_contents = URL.urlopen(url).read()
	xml = DOM.parseString(file_contents)
	document = xml.documentElement
	listeners = document.getElementsByTagName("listeners")[0]
        listener_count = listeners.childNodes[0].nodeValue

        similar = document.getElementsByTagName('similar')[0]
        similar_artists = similar.getElementsByTagName('artist')
        for similar_artist in similar_artists:
            similar_artist_name = similar_artist.getElementsByTagName('name')
            node = similar_artist_name[0].childNodes[0]
            addArtist(node.toxml('utf-8'))
                    
        if(needs_update(artist_name)):
            data = {'documentContent':file_contents, 'documentType':'text/plain',
                   'outputType':'rss', 'appid':'cm5bOt7c'}
	    data = urllib.urlencode(data)
	    url = "http://wherein.yahooapis.com/v1/document"
	    req = URL.Request(url, data)
	    response = URL.urlopen(req).read()
	    response_xml = DOM.parseString(response)
	    document = response_xml.documentElement
	    location = document.getElementsByTagName("item")
	    if(not location):
                return
	    l = location[0].getElementsByTagName("title")[0]
            location_title = l.childNodes[0].nodeValue
	    for country in country_list:
                if(string.find(location_title, country)!=-1):
                    if(country=='India'):
                        if(string.find(location_title, 'Indiana')!=-1):
                            country = 'United States'
                    updateArtist(artist_name, country, listener_count)
	            break
        
        incr_lastOffset()       
	getArtistInformation()
        """except:
            incr_lastOffset()
            getArtistInformation()"""
    
        return

command = sys.argv[1]
if command=='start':
    create_process()
if command=='stop':
    exit_process()
if command=='nd':
    connectDB()

