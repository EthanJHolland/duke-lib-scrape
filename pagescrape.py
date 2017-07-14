# -*- coding: utf-8 -*-

import sys
import traceback
from time import sleep
import math
import time
from urllib.request import ProxyHandler
from urllib.request import urlopen
import urllib.request
from datetime import timedelta
from bs4 import BeautifulSoup
from pymongo import MongoClient
from timeit import default_timer as timer
import re

class PageScraper:    
    def __init__(self):
        self.BASE_LINK='http://search.library.duke.edu/search?Nao=START&Nty=1&Nr=OR%28210969%2cOR%28206474%29%29&Ne=2+200043+206474+210899+210956&N=206437'
        
        db=MongoClient('localhost:27017').lib
        self.ebookcoll=db.ebooks
        self.bookcoll=db.bookids

    def start(self):
        start=timer()
        file=open('pagenum.txt', 'r')

        pagenum=int(file.read())
        print("getting page "+str(pagenum))
        file.close()

        data = urlopen(self.BASE_LINK.replace('START', str(pagenum*20))).read()
        soup=BeautifulSoup(data, 'html.parser')

        # href=re.compile("^\?id=DUKE")
        # print(soup.prettify(), file=open("out.txt", "a"))
        for row in soup.find_all("table", "itemRecord"):
            doc={}

            #get id and title
            titletag=row.find('h3', 'recordTitle')
            doc['_id']=titletag.attrs['recordid']
            doc['title']=titletag.a.string.strip()

            #get raw author, published, etc. (no parsing)
            for field in row.find_all('td', 'lightText'):
                strings=list(field.parent.stripped_strings)
                key=strings[0].strip().replace(':','').lower()
                if 'format' not in key and 'online access' not in key: #ignore format (book/ebook/etc) because gotten elsewhere
                    val=strings[1].strip()
                    doc[key]=val

            #get libraries
            libraries=[]
            for libtag in row.find_all('tr', attrs={'data-sublibrary':True}):
                libraries.append(libtag.attrs['data-sublibrary'])
            if libraries:
                doc['libraries']=libraries

            #get form
            for formtag in row.find_all("img", "fmtIcon"):
                src=formtag.attrs['src']
                if 'icon-Book' in src:
                    #book
                    print("book")
                    self.writeToMongoDB(self.bookcoll, doc)
                if "icon-eBook" in src:
                    #ebook
                    print("ebook")
                    self.writeToMongoDB(self.ebookcoll, doc)
        
        file=open('pagenum.txt', 'w')
        file.write(str(pagenum+1))
        file.close()
        end=timer()
        tstring = str(timedelta(seconds=(end - start)))
        print("time elapsed: {}".format(tstring))
        time.sleep(1)

    #helper methods
    #write a document to the given mongo database
    def writeToMongoDB(self, coll, doc):
        #self.printDoc(doc)
        coll.insert_one(doc)
        
    #print a doc in a nice format
    def printDoc(self, doc, indent=0):
        print('\t'*indent+"{")
        for key in doc.keys():
            try:
                doc[key].keys()
                #dictionary so print recursively with an extra indent
                print('\t'*(indent+1)+str(key)+":")
                self.printDoc(doc[key], indent+1)
            except:
                #not a dict
                if isinstance(doc[key], list):
                    #this is an array
                    print('\t'*(indent+1)+str(key)+": [")
                    for elem in doc[key]:
                        print('\t'*(indent+2)+str(elem)+",")
                    print('\t'*(indent+2)+"]")
                else:
                    #either a string or None
                    print('\t'*(indent+1)+str(key)+": "+str(doc[key]))
        print('\t'*indent+"}")

    #print a beautifulsoup object in prettified form after removing unprintable characters
    def printSoup(self, soup):
        #replace unprintable characters with printable characters
        non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)
        print(str(soup.prettify()).translate(non_bmp_map))


if __name__ == '__main__':
    ps=PageScraper()
    ps.start()