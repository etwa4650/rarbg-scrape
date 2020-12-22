import requests
import urllib3
from bs4 import BeautifulSoup
import lxml.html as lh
import pandas as pd
import mysql.connector
import re

db = mysql.connector.connect(
  host="localhost",
  user="root",
  database="scrape",
  password="99Gbdyprnnv76*"
)



cursor = db.cursor(buffered=True)


site = requests.get('https://www.1337x.to/top-100-movies')

soup = BeautifulSoup(site.text.encode('unicode-escape'), 'lxml')

col1 = soup.find_all("td", {"class": "coll-1 name"})
col2 = soup.find_all("td", {"class": "coll-2 seeds"})
col3 = soup.find_all("td", {"class": "coll-3 leeches"})
col4= soup.find_all("td", {"class": "coll-4"})

torrents = []

for i  in range(0, len(col1)):
  print(i)
  torrent = {}
  torrent['torrentName'] = col1[i].find('a', class_=lambda x: x != 'icon').text
  torrent['torrentSeeders'] = col2[i].text
  torrent['torrentLeechers'] = col3[i].text
  torrent['torrentSize'] = col4[i].text.split("B",1)[0] +'B'

  torrentHref = col1[i].find('a', class_=lambda x: x != 'icon')['href']
  torrentLink = 'https://www.1337x.to{}'.format(torrentHref)
  torrentPage = requests.get(torrentLink).text.encode('unicode-escape')
  torrentSoup = BeautifulSoup(torrentPage, 'lxml')
  imdbID = torrentSoup.find('a', href=lambda href: href and "imdb" in href)
  if(imdbID):
    torrent['imdbID'] = imdbID['href'].split('title/',1)[1].replace('/','')
  else:
    torrent['imdbID'] = 'unknown'
  infoHash = torrentSoup.find('div', {"class": "infohash-box"})
  if(infoHash):
    torrent['infoHash'] = infoHash.find('span').text
  else:
    torrent['infoHash'] = 'unknown'
  torrents.append(torrent)



for torrent in torrents:
    sql = "INSERT INTO torrents (torrent_name, imdb_id, torrent_size, torrent_seeders, torrent_leechers, info_hash) VALUES (%s, %s, %s, %s, %s, %s)"
    values = (torrent["torrentName"], torrent["imdbID"], torrent["torrentSize"], torrent["torrentSeeders"], torrent["torrentLeechers"], torrent["infoHash"])
    cursor.execute(sql, values)
db.commit()

