#encoding:utf-8
import sys
import pycurl
import StringIO
#import MySQLdb
import os

class FeatureHandle:
    host = ""
    port = ""

    def __init__(self):
        pass

    def download(self,url,timeout = 10):
        try:
            curl = pycurl.Curl()
            curl.setopt(pycurl.URL,url)
            curl.setopt(pycurl.FOLLOWLOCATION, 1)
            curl.setopt(pycurl.MAXREDIRS, 5)
            curl.setopt(pycurl.CONNECTTIMEOUT, 10)
            curl.setopt(pycurl.TIMEOUT, timeout)
            content = StringIO.StringIO()
            curl.setopt(pycurl.WRITEFUNCTION, content.write)
            curl.setopt(pycurl.FOLLOWLOCATION, 1)

            curl.perform()
            if (curl.getinfo(pycurl.HTTP_CODE) == 200):
                return content.getvalue()
            return ""
        except BaseException,e:
            return ""
      
