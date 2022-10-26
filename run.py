import argparse
from scrapy import cmdline
import datetime
import schedule
import time


def start_scraper(start, end, resume):
    cmdline.execute(f"scrapy crawl sonora_spider -a start_date={start} -a end_date={end} -a resume={resume}".split())

parser =argparse.ArgumentParser()
parser.add_argument('-s','--start',dest='start',help='write start date e.g 2022/01/01',required=True) 
parser.add_argument('-e','--end',dest='end',help='write end date e.g 2022/01/30 (left blank for today)', default='') 
parser.add_argument('-r','--resume',dest='resume',help='Enter a boolean value e.g True/False', default='') 
values = parser.parse_args()  
args_dict = vars(values)
start = args_dict.get("start")
end = args_dict.get("end")
resume = args_dict.get("resume")
start_scraper(start, end, resume)
