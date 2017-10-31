# -*- coding: utf-8 -*-

import sys
import traceback
from time import sleep
import math
import time
from tqdm import tqdm
from random import randint
from urllib.request import urlopen
import urllib.request
from datetime import timedelta
from bs4 import BeautifulSoup
from pymongo import MongoClient
from timeit import default_timer as timer
import re
from threading import Thread
from queue import Queue
from pymongo.errors import DuplicateKeyError

class PageScraper(Thread):
    def __init__(self, master):
        Thread.__init__(self)
        self.BASE_LINK='http://search.library.duke.edu/search?Nao=START&Nty=1&Nr=OR%28210969%2cOR%28206474%29%29&Ne=2+200043+206474+210899+210956&N=206437'
        db=MongoClient('localhost:27017').sample
        self.ebookcoll=db.ebooks
        self.bookcoll=db.books
        self.samplebookcoll=db.sbooks
        self.sampleebookcoll=db.sebooks
        self.pagecoll=db.pages
        self.master=master

    def run(self):
        while True:
            start=timer()
            booknum=self.randBook()
            print("getting book "+str(booknum))
            self.writeToMongoDB(self.pagecoll, {'booknum': booknum, 'success': False})
            try:
                data = urlopen(self.BASE_LINK.replace('START', str(booknum))).read()
                soup = BeautifulSoup(data, 'html.parser')
            except:
                print(traceback.format_exc())
                print("trying again")
                time.sleep(10)
                data = urlopen(self.BASE_LINK.replace('START', str(booknum))).read()
                soup = BeautifulSoup(data, 'html.parser')

            # href=re.compile("^\?id=DUKE")
            #print(soup.prettify(), file=open("out.txt", "w"))
            first=True
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
                        self.writeToMongoDB(self.bookcoll, doc)
                        if first:
                            print("book")
                            first=False
                            self.writeToMongoDB(self.samplebookcoll, doc)
                            self.pagecoll.find_one_and_replace({'booknum': booknum}, {'booknum': booknum, 'success': True})
                    if "icon-eBook" in src:
                        #ebook
                        self.writeToMongoDB(self.ebookcoll, doc)
                        if first:
                            print("ebook")
                            first=False
                            self.writeToMongoDB(self.sampleebookcoll, doc)
                            self.pagecoll.find_one_and_replace({'booknum': booknum}, {'booknum': booknum, 'success': True})

            end=timer()
            tstring = str(timedelta(seconds=(end - start)))
            print("\ntime elapsed: {}".format(tstring))
            self.master.updateProgress()


    def randBook(self):
        return randint(0,Master.NumBooks-1)

    #helper methods
    #write a document to the given mongo database
    def writeToMongoDB(self, coll, doc):
        #self.printDoc(doc)
        try:
            coll.insert_one(doc)
        except DuplicateKeyError:
            print("dup key error")
        
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

class Master:
    NWorkers = 10
    NumBooks = 4928093

    def __init__(self):
        self.scrapers=[]

    def updateProgress(self):
        self.pbar.update(1)

    def start(self):   
        self.pbar = tqdm(total=100, position=0,ncols=80, mininterval=1.0)

        for i in range(Master.NWorkers):
            ps=PageScraper(self)
            self.scrapers.append(ps)
        for ps in self.scrapers:
            ps.start()
        for ps in self.scrapers:
            ps.join()

if __name__ == '__main__':
    m=Master()
    m.start()