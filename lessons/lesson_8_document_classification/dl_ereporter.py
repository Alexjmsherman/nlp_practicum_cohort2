import io
import os
import requests
import zipfile
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

REPORTER_PROJECT_DATA = r'C:\Users\alsherman\Desktop\NLP\nlp_practicum_health_baseline\raw_data\pubmed\RePORTER_project_data'
EXPORTER_URL = 'https://exporter.nih.gov/ExPORTER_Catalog.aspx?sid=0&index=0'
NUM_FILES_TO_DOWNLOAD = None  # limit downloads
 
def main():
    exporter = ExporterLoader(EXPORTER_URL, REPORTER_PROJECT_DATA)
    exporter.collect_data()

class ExporterLoader:
    """
    Collect and store data from NIH ExPORTER data catalog
    SOURCE: https://exporter.nih.gov/ExPORTER_Catalog.aspx?sid=0&index=0
    """
    
    def __init__(self, url, source):
        self.url = url
        self.source = source
    
    def collect_data(self):
        self.rows = self.collect_html()
        self.zip_urls = self.extract_urls()

        if NUM_FILES_TO_DOWNLOAD:
            self.zip_urls = self.zip_urls[0:NUM_FILES_TO_DOWNLOAD]

        self.download_zips()
        self.combine_data()
    
    def collect_html(self):
        """ collect html from exporter site """
        r = requests.get(self.url)
    
        # find all table rows with csv urls
        b = BeautifulSoup(r.text, 'lxml')
        rows = b.find_all('tr', attrs={'class':'row_bg'})
        
        return rows
    
    def extract_urls(self):
        """ extract all csv zip urls from table """
        zip_urls = []
        for r in self.rows:
            fname = r.find('td').text.strip()
            url_end = r.find_all('a')[1]['href'].strip()
            zip_url = '/'.join([r'https://exporter.nih.gov',url_end])
            # ignore extra links that are not zip files
            if '.zip' in zip_url:
                zip_urls.append(zip_url)
        
        return zip_urls
    
    def download_zips(self):
        """ download new csv files to local machine """
        source = self.source
        zip_urls = self.zip_urls
        
        for url in zip_urls:
            # download zip to local machine
            print('Downloading: {}'.format(url))
            r = requests.get(url)    
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall(source)
    
    def combine_data(self):
        """ combine all project csvs into a single file """

        for fname in os.listdir(self.source):
            csv_file = os.path.join(REPORTER_PROJECT_DATA, fname) 
            print(csv_file)

            df = pd.read_csv(csv_file, encoding='latin-1')
            df['source'] = csv_file

            output_file = os.path.join(REPORTER_PROJECT_DATA, 'reporter_project_data.csv') 
            df.to_csv(output_file, mode='a', index=False)            
            os.remove(csv_file)


if __name__ == "__main__": main()