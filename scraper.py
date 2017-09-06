import sys
import traceback
from time import sleep
import math
import time
from urllib.request import urlopen
from urllib.request import Request
import urllib.request
from bs4 import BeautifulSoup
from urllib.error import HTTPError
from datetime import timedelta
from pymongo import MongoClient
from timeit import default_timer as timer

class Scraper:
    def __init__(self):
        self.BASE_LINK='http://search.library.duke.edu/search?id=DUKEID'
        db=MongoClient('localhost:27017').sample
        self.ebookcoll=db.ebooks
        self.bookcoll=db.books
        self.samplebookcoll=db.sbooks
        self.sampleebookcoll=db.sebooks
        self.pagecoll=db.pages

    def start(self):
        start=timer()
        link = 'http://firstsearch.oclc.org.proxy.lib.duke.edu/WebZ/FSFETCH?fetchtype=fullrecord:next=html/record.html:bad=error/badfetch.html:resultset=12:format=FI:recno=1:numrecs=2:entitylibrarycount=6383:sessionid=fsapp5-60190-j79fxg6w-21anzc:entitypagenum=63:3:sessionid=fsapp5-60190-j79fxg6w-21anzc:entitypagenum=63:3'
        request = Request(link, method='GET', headers={'Cookie':'ezproxy=MM3huP6lGJWChkj'})
        data = urlopen(request).read()
        soup = BeautifulSoup(data, 'html.parser')
        print(soup.prettify(), file=open("out.txt", "w"))

        end=timer()
        tstring = str(timedelta(seconds=(end - start)))            
        print("\ntime elapsed: {}".format(tstring))


        # go=True
        # while go:
        #     go=False
        #     start=timer()

            
        #     bookid='DUKE003139904'
        #     print("getting book "+str(bookid))
            

        #     try:
        #         data = urlopen(self.BASE_LINK.replace('DUKEID', bookid)).read()
        #         soup = BeautifulSoup(data, 'html.parser')
        #     except:
        #         print(traceback.format_exc())
        #         print("trying again")
        #         time.sleep(10)
        #         data = urlopen(self.BASE_LINK.replace('DUKEID', bookid)).read()
        #         soup = BeautifulSoup(data, 'html.parser')

        #     # # href=re.compile("^\?id=DUKE")
        #     # print(soup.prettify(), file=open("out.txt", "w"))

            # end=timer()
            # tstring = str(timedelta(seconds=(end - start)))            
            # # print("\ntime elapsed: {}".format(tstring))
        
        # aa=['Description: xii, 285 pages : illustrations, map ; 23 cm',
        # 'Description: xv, 180 p. : ill. ; 24 cm.',
        # 'Description: 301 p. ; 21 cm.',
        # 'Description: 301 p. 19 cm.',
        # 'Description: 277 pages ; 21 cm',
        # 'Description: viii, 336 p. ; 21 cm.',
        # 'Description: xv, 275 p. : ill. ; 24 cm.',
        # 'Description: x, 283 p. : ill., maps ; 24 cm.',
        # 'Description: 3 v. : ill. ; 29 cm.',
        # 'Description: xx, 380 p. ; 24 cm.']

        # for a in aa:
        #     ill, length, pages = self.extractDescription(a)
        #     print(str(ill)+"\t"+str(length)+"\t"+str(pages))

    #given a description, return a tuple with whether or not the book has illustrations, the length in cm, and the number of pages
    def extractDescription(self, desc):
        '''
        examples
        Description: xii, 285 pages : illustrations, map ; 23 cm
        Description: xv, 180 p. : ill. ; 24 cm.
        Description: 301 p. ; 21 cm.
        Description: 301 p. 19 cm.
        Description: 277 pages ; 21 cm
        Description: viii, 336 p. ; 21 cm.
        Description: xv, 275 p. : ill. ; 24 cm.
        Description: x, 283 p. : ill., maps ; 24 cm.
        Description: 3 v. : ill. ; 29 cm.
        Description: xx, 380 p. ; 24 cm.
        '''

        desc=desc.lower()

        #get is illustrated
        ill= 'ill' in desc

        #get length
        desc=desc[:desc.index('cm')].rstrip()
        try:
            length=int(self.lw(desc))
        except: 
            #write err
            pass

        #get number of pages
        if 'description' in desc:
            desc=desc[desc.index('description')+len('description')+1:].lstrip()

        if desc[0] in 'lvxi':
            #roman numeral
            start=-1*self.interperetRomanNumerals(self.fw(desc).replace(',',''))
            desc=desc[desc.index(' '):].lstrip()

            try:
                end=int(self.fw(desc))
            except:
                #write err
                end=0
                pass

            pages=end-start
        else:
            try:
                pages=int(self.fw(desc))
            except:
                #write err
                pass

        return ill, length, pages

    #get the first word (all characters until the first space) of a string
    def fw(self, s):
        return s[:s.index(' ')]

    #get the last word of a string
    def lw(self, s):
        return s[s.rindex(' ')+1:]

    #convert a roman numeral lowercase string to an integer
    #checks for invalid characters but assumes the roman numeral is valid (ex iviviv will return 12)
    def interperetRomanNumerals(self, rn):
        rnToInt = {'l': 50, 'x': 10, 'v': 5, 'i': 1}
        sum = 0

        for c in rn:
            if c not in 'lxvi':
                #not a valid rn string
                return False

        for i in range(len(rn)):
            c=rn[i]
            val=rnToInt[c]
            #check next char
            if i < len(rn)-1 and val < rnToInt[rn[i+1]]:
                #next char is greater so subtract this one
                sum-=val
            else:
                sum+=val

        return sum

    #helper methods
    #write a document to the given mongo database
    def writeToMongoDB(self, db, doc):
        #print("writing")
        self.printDoc(doc)
        #db.insert_one(doc)
        
    def removeTags(self, s):
        while '<' in s and '>' in s:
                indLess=s.index('<')
                indGreat=s.index('>')
                if indGreat==len(s)-1:
                        s=s[:indLess]
                else:
                        s=s[:indLess]+s[indGreat+1:]
        return s.strip().replace("  "," ") #remove all extra spacing caused by tags

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


# headers = {
#     'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
#     'Accept-Encoding':'gzip, deflate',
#     'Accept-Language':'en-US,en;q=0.8',
#     'Cache-Control':'max-age=0',
#     'Connection':'keep-alive',
#     'Cookie':'ezproxy=MM3huP6lGJWChkj',
#     'Host':'firstsearch.oclc.org.proxy.lib.duke.edu',
#     'Upgrade-Insecure-Requests':1,
#     'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'
# }
if __name__ == '__main__':
    sc=Scraper()
    sc.start()