# BeatuifulSoup comes with many parsing functions•See documentation 
# for the full list:https://www.crummy.com/software/BeautifulSoup/bs4/doc/

import requests as req
from bs4 import BeautifulSoup

page = req.get("http://www.alawini.com")
soup = BeautifulSoup(page.text,'html.parser')
for link in soup.find_all('a'):
	print(link.get('href'))