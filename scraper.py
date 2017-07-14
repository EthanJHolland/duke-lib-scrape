import sys
import traceback
from time import sleep
import math
import time
from shortid import ShortId
from urllib.request import ProxyHandler
from urllib.request import urlopen
import urllib.request
from bs4 import BeautifulSoup
from urllib.error import HTTPError

class Scraper:    
   def __init__(self):
        start=time.clock()

        end=time.clock()
        elapsed=end-start
        print("total time: "+str(math.floor(elapsed/60))+" min "+str(math.floor(10*(elapsed%60))/10)+ " sec")

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
            desc=desc[s.index(' '):].lstrip()

            try:
                end=int(self.fw(desc))
            except:
                #write err
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
        return s[s.rindex(' '):]

    #convert a roman numeral lowercase string to an integer
    #checks for invalid characters but assumes the roman numeral is valid (ex iviviv will return 12)
    def interperetRomanNumerals(self, rn):
        rnToInt = ['l': 50, 'x': 10, 'v': 5, 'i': 1]
        sum=0

        for c in rn:
            if c not in 'lxvi':
                #not a valid rn string
                #TODO: write error
                return False

        for i in range(len(rn)):
            c=rn[i]
            val=rnToInt[c]
            #check next char
            if i<len(input)-1 && val<rnToInt[rn[i+1]]:
                #next char is greater so subtract this one
                sum-=val
            else:
                sum+=val

        return sum


    #helper methods
    #write a document to the given mongo database
    def writeToMongoDB(self, db, doc):
        print("writing")
        #self.printDoc(doc)
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


if __name__ == '__main__':
    sc=Scraper.init()