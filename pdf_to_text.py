import urllib.request

from tika import parser
import mysql.connector
from datetime import date
import logging
#import schedule
import time
import re


today = date.today()
states_list= ['\nDelhi','\nHaryana','\nHimachal','\nKashmir','\nMadhya','\nPunjab','\nChandigarh','\nUttar','\nUttarakhand','\nAndaman','\nAssam','\nBihar','\nChhattisgarh','\nJharkhand','\nManipur','\nNagaland','\nOdisha','\nTripura','\nMeghalaya' ,'\nMizoram','\nKolkata'  ,'\nBagdogra','\nGoa','\nGujarat','\nRajasthan','\nAurangabad','\nPune' ,'\nShirdi','\nNagpur','\nMumbai','\nKolhapur','\nAndhra','\nTelangana','\nKarnataka','\nKerala','\nTamilnadu']

download_url = 'https://www.civilaviation.gov.in/sites/default/files/State_wise_quarantine_regulation-converted.pdf'
filename='State_wise_quarantine_regulation-converted'+str(f"{today}"+'.pdf')
#filename = 'State_wise_quarantine_regulation-converted.pdf'
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(levelname)s:%(name)s:%(asctime)s:%(message)s')

file_handler = logging.FileHandler(f'completeprocess.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


def download_file(download_url, filename):
    response = urllib.request.urlopen(download_url)    
    file = open(filename , 'wb')
    file.write(response.read())
    logger.info(f"PDF file downloaded,{filename}")
    file.close()


def infoextraction(filename):
    print(filename)
    parsedPDF = parser.from_file(filename)
    print(filename)
    bb5 = parsedPDF['content']
    states_list= ['\nDelhi','\nHaryana','\nHimachal','\nKashmir','\nMadhya','\nPunjab','\nChandigarh','\nUttar','\nUttarakhand','\nAndaman','\nAssam','\nBihar','\nChhattisgarh','\nJharkhand','\nManipur','\nNagaland','\nOdisha','\nTripura','\nMeghalaya' ,'\nMizoram','\nKolkata'  ,'\nBagdogra','\nGoa','\nGujarat','\nRajasthan','\nAurangabad','\nPune' ,'\nShirdi','\nNagpur','\nMumbai','\nKolhapur','\nAndhra','\nTelangana','\nKarnataka','\nKerala','\nTamilnadu']
    start_string = "STARTEDHERE "
    end_string = "ENDEDHERE "
    for i in states_list:
        #print(i)
        le =len(i)
        #print(le)
        dels = bb5.find(i)
        bef =dels-1
        af = dels+le
        bb5 = bb5[:bef]+end_string+bb5[bef:af]+start_string+bb5[af:]
    logger.info(f"Added startedhere ,endedhere,{bb5}")
    return bb5
    
def starthereendhere(bb5):
    a_list1 = []
    for i in states_list:
        t = ()
        pos1 = bb5.find(i)
        #print(pos1)
        pos2 = bb5.find('STARTEDHERE',pos1)
        #print(pos2)
        pos3 = bb5.find('Institutional Quarantine',pos2)
        #print(pos3)
        pos4 = bb5.find('ENDEDHERE',pos3)
        #print(pos4)
        #print (bb5[pos2+11: pos3])
        res = bb5[pos3: pos4]
        res = re.sub("\n{2,30}","\n",res)
        i =i.strip('\n')
        t = (i,res)
        a_list1.append(t)
    logger.info(f"list of all states and details,{a_list1}")
    return a_list1 

    
def db_insertion(a_list1):
    mydb = mysql.connector.connect(
            host="novigidba.cdk8b0fuarvc.ap-south-1.rds.amazonaws.com",
            user="dbadmin",
            password="8CuMZcRnjYSWsyde",
            database="covidguidlines",
            auth_plugin='mysql_native_password'
        )
    mycursor = mydb.cursor()
    sql = "truncate table statewise" 
    sql1 = "INSERT INTO statewise(STATE,DETAILS) VALUES (%s, %s)"
    mycursor.execute(sql)
    mydb.commit()
    print("a")
    mycursor.executemany(sql1, a_list1)
    mydb.commit()
    print("b")
    logger.info(f"updated content to db statewise")
    return None

def start_running():
    logger.info(f"Started running Program")
    a1 = download_file(download_url, filename)
    b = infoextraction(filename)
    b1 = starthereendhere(b)
    d1 = db_insertion(b1)
    logger.info(f"Finished process")
a= start_running()

# def cron_job():
#     print("cronjob started")
#     start_running()


# n=["01:28","03:28","04:58"]
# #n = ["10:40"]
# for i in range(len(n)):
#     schedule.every().day.at(n[i]).do(cron_job)

# while True:
#     schedule.run_pending()
#     time.sleep(1)