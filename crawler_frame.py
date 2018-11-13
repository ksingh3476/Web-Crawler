
import logging
from datamodel.search.Ketans1BfullamLucc2_datamodel import Ketans1BfullamLucc2Link, OneKetans1BfullamLucc2UnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4

from urlparse import urlparse, parse_qs
from uuid import uuid4


#-----my imports---------

from bs4 import BeautifulSoup
from urllib2 import urlopen
import lxml.html
#----global data set-----

crawled_urls = set()
crawled_analytics = dict()

max_url_links = 0
max_url = ''
total_url_download_count = 0


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

@Producer(Ketans1BfullamLucc2Link)
@GetterSetter(OneKetans1BfullamLucc2UnProcessedLink)


class CrawlerFrame(IApplication):
    app_id = "Ketans1BfullamLucc2"

    def __init__(self, frame):
        self.app_id = "Ketans1BfullamLucc2"
        self.frame = frame
        self.starttime = 0
    


    def initialize(self):
        self.count = 0
        links = self.frame.get_new(OneKetans1BfullamLucc2UnProcessedLink)
        if len(links) > 0:
            print "Resuming from the previous state."
            self.download_links(links)
        else:
            l = Ketans1BfullamLucc2Link("http://www.ics.uci.edu/")
            print l.full_url
            self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get_new(OneKetans1BfullamLucc2UnProcessedLink)
        if unprocessed_links:
            self.download_links(unprocessed_links)

    def shutdown(self):
        print (
               " Total time spent this session: ",
               time() - self.starttime, " seconds.")

    def download_links(self, unprocessed_links):
        global total_url_download_count
        global crawled_analytics
        if total_url_download_count == 3000:
            max_sub_domain_url = max(crawled_analytics.keys())
            max_sub_domain_num = crawled_analytics[max_sub_domain_url]
            
            file = open("analytics.txt","w")
            file.write("CRAWL ANALYSIS\n Max number of outlinks URL: {} Amount: {}".format(max_sub_domain_url,max_sub_domain_num,max_url, max_url_links))
            for k,v in crawled_analytics.items():
                file.write("\n Domain: {} Amount of Subdomains: {}".format(k,v))
            file.close()
            self.shutdown()
            
        for link in unprocessed_links:
            print "Got a link to download:", link.full_url
            total_url_download_count += 1
            print(total_url_download_count)
            downloaded = link.download()
            links = extract_next_links(downloaded)
            
            for l in links:
                if is_valid(l):
                    data = urlparse(l)
                    
                    
                    if data.netloc not in crawled_analytics.keys():
                        crawled_analytics[data.netloc] = 1
                                
                    else:
                        crawled_analytics[data.netloc] += 1
                    
                    print(l)
                    
                    
                    self.frame.add(Ketans1BfullamLucc2Link(l))
        print(crawled_analytics)


    
def extract_next_links(rawDataObj):
    outputLinks = []
    global crawled_urls
    global max_url
    global max_url_links
    crawled_urls.add(rawDataObj.url)        # add the url being crawled into list of crawled_urls
    
    
    #type(soup)
    if rawDataObj.error_message != None:
        print("MESSAGE")
        print(rawDataObj.error_message)
        print(rawDataObj.url)
        pass
    
    else:
        if rawDataObj.is_redirected == True:                 # for analytics
            print()
            print()
            print("fck")
            print(rawDataObj.url)
            url_crawled = rawDataObj.final_url
            print(url_crawled)
            #print(url_crawled)
            crawled_urls.add(url_crawled)                    # not sure if should do or not
        else:
            url_crawled = rawDataObj.url
    try:
        dom = lxml.html.fromstring(rawDataObj.content)
        dom.make_links_absolute(url_crawled)
        for link in dom.xpath('//a/@href'):
                outputLinks.append(link)
    except:
        print("error with document")



        '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.
    
    Suggested library: lxml
    '''
    if len(outputLinks) > max_url_links:
        max_url_links = len(outputLinks)
        max_url = rawDataObj.url
    
    
    return outputLinks

def is_valid(url):
    
    '''
        Function returns True or False based on whether the url has to be
        downloaded or not.
        Robot rules and duplication rules are checked separately.
        This is a great place to filter out crawler traps.
        '''
    global crawled_urls
    if url in crawled_urls:                                 # if this url has already been crawled, return False
        # print(1)
        return False
    
    mailto_string = "mailto:"                               # if this url include "mailto" it is not real url
    if url.find(mailto_string) != -1:
        #  print(2)
        return False
    
    parsed = urlparse(url)
    stuff_in_path = parsed.path.split("/")                  #if there are repeats in directory path, don't parse
    if '' in stuff_in_path:
        stuff_in_path.remove('')
    if len(stuff_in_path) != len(set(stuff_in_path)):
        #  print(3)
        return False
    
    if url.find('?') != -1:                                 # if there is a '?' then don't parse, is a query
        #  print(4)
        return False
    
    if url.find('../') != -1:                               # if there is "../" then don't parse
        #   print(5)
        return False
    
    # if url.find('.php') != -1:                                 # DONT THINK WE CAN DO THIS if there is a '.php' then don't parse
    #   return False
    
    if len(url) > 100:                                      # if lenght of url is greater than 100, don't parse
        # print(6)
        return False
    
    parsed = urlparse(url)


    if parsed.scheme not in set(["http", "https"]):
        #  print(7)
        return False
    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
                             + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                             + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                             + "|thmx|mso|arff|rtf|jar|csv"\
                             + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())
    
    except TypeError:
        print ("TypeError for ", parsed)
        return False

