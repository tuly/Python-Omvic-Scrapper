import urllib
import gc
from db.DbHelper import DbHelper
from logs.LogManager import LogManager
from utils.Csv import Csv
from utils.Utils import Utils
from spiders.Spider import Spider
from utils.Regex import Regex
from bs4 import BeautifulSoup

__author__ = 'Tuly'

''' Latest PID: 147805 https://www.omvic.on.ca/RegistrantSearch/Profile/Dealer.aspx?PID=146576
sudo scp -r rabbituly@ekushit.com:/home/content/03/11273303/spiders/Omvic/omvic.csv /home/rabbi/
Last run 149770'''


class OmvicScrapper:
    isFinished = False
    im_data = []

    def __init__(self):
        self.logger = LogManager(__name__)
        self.spider = Spider()
        self.regex = Regex()
        self.utils = Utils()
        self.initScrapper()


    def initScrapper(self):
        try:
            dupCsvReader = Csv()
            dupCsvRows = dupCsvReader.readCsvRow('omvic.csv')
            self.dbHelper = DbHelper('omvic.db')
            self.dbHelper.createTable('omvic')
            self.totaldata = self.dbHelper.getTotalProduct('omvic')
            self.csvWriter = Csv('omvic.csv')
            csvDataHeader = ['URL', 'Legal Name', 'Business Name', 'Status', 'Class of Registration', 'Subclass',
                             'Operating Status',
                             'Business Address', 'Email', 'Phone Number', 'Salesperson(s) Names']
            if len(dupCsvRows) == 0:
                self.csvWriter.writeCsvRow(csvDataHeader)
            del dupCsvReader
            del dupCsvRows
            gc.collect()
            del gc.garbage[:]
            gc.collect()
        except Exception, x:
            print x


    def scrapData(self, start, end):
        try:
            self.scrapBruteforce(start, end)
        except Exception, x:
            self.logger.error(x.message)
            print x

    def scrapBruteforce(self, start, end):
        try:
            url_base = 'https://www.omvic.on.ca/RegistrantSearch/Profile/Dealer.aspx?PID='
            for pid in range(start, end):
                if self.dbHelper.searchProduct(pid, 'omvic'):
                    self.logger.debug('Skip PID: %d' % pid)
                    continue
                url = url_base + str(pid)
                self.scrapDetails(url, pid)
                if pid % 500 == 0:
                    gc.collect()
                    del gc.garbage[:]
                    gc.collect()
        except Exception, x:
            print x


    def scrapDetails(self, url, pid):
        try:
            self.logger.debug(url)
            data = self.spider.fetchData(url)
            if data and len(data) > 0:
                data = self.regex.reduceNewLine(data)
                data = self.regex.reduceBlankSpace(data)
                data = self.regex.getSearchedData('(?i)<body[^>]*?>(.*?)</body>', data)
                soup = BeautifulSoup(data)

                div = soup.find('div', id='ctl00_ctl00_Main_consumerMain_divProfile')
                if div is not None:
                    legal_name = ''
                    business_name = ''
                    status = ''
                    registration_class = ''
                    subclass = ''
                    operation_status = ''
                    business_address = ''
                    email = ''
                    phone = ''
                    sales_person = ''

                    for row in div.find_all('tr'):
                        cols = row.find_all('td')
                        for i in range(0, len(cols), 2):
                            if self.regex.isFoundPattern('(?i)^Legal Name$', cols[i].text.strip()):
                                legal_name = cols[i + 1].text
                            if self.regex.isFoundPattern('(?i)^Business Name$', cols[i].text.strip()):
                                business_name = cols[i + 1].text
                            if self.regex.isFoundPattern('(?i)^Status$', cols[i].text.strip()):
                                status = cols[i + 1].text
                            if self.regex.isFoundPattern('(?i)^Class of Registration$', cols[i].text.strip()):
                                registration_class = cols[i + 1].text
                            if self.regex.isFoundPattern('(?i)^Subclass$', cols[i].text.strip()):
                                subclass = cols[i + 1].text
                            if self.regex.isFoundPattern('(?i)^Operating Status$', cols[i].text.strip()):
                                op_status = cols[i + 1].text
                                operation_status = 'Terminated' if op_status.strip() == 'N/A' else op_status.strip()
                            if self.regex.isFoundPattern('(?i)^Business Address$', cols[i].text.strip()):
                                business_address = cols[i + 1].text
                            if self.regex.isFoundPattern('(?i)^Email$', cols[i].text.strip()):
                                email = cols[i + 1].text
                            if self.regex.isFoundPattern('(?i)^Phone #$', cols[i].text.strip()):
                                phone = cols[i + 1].text
                            if self.regex.isFoundPattern('(?i)Salesperson.*?', cols[i].text.strip()):
                                s_person = cols[i + 1].find('table')
                                person = []
                                if s_person is not None:
                                    trs = s_person.find_all('tr')
                                    if trs is not None and len(trs) > 0:
                                        for tr in trs:
                                            tds = tr.find_all('td')
                                            if tds is not None and len(tds) > 1 and not self.regex.isFoundPattern(
                                                    '(?i)^\d+$', tds[1].text.strip()):
                                                person.append(tds[1].text.strip())
                                    sales_person = ', '.join(person)
                                    # print sales_person
                            ## todo
                            if len(cols[i].text.strip()) == 0:
                                business_address += ' ' + cols[i + 1].text

                    csvdata = [url, legal_name, business_name, status, registration_class, subclass, operation_status,
                               business_address,
                               email, phone, sales_person]

                    self.csvWriter.writeCsvRow(csvdata)
                    self.totaldata += 1
                    self.logger.debug('Total Data: %d' % self.totaldata)
                    self.dbHelper.saveProduct(pid, 'omvic')
                del data
                del soup
                gc.collect()
                del gc.garbage[:]
                gc.collect()
        except Exception, x:
            self.logger.error(x.message)
            print x

