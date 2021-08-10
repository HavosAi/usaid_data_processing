import requests
from requests import get
from bs4 import BeautifulSoup
import time
from pathlib import Path
import json
import re
import os
import pandas as pd
import datetime
from utilities import excel_writer
import sys
import pickle
from webscrapping import webcrawler_logger

class WebCrawlerBase:

    def __init__(self, headers = None):
        self.headers = headers
        self.start_page = 1

    def sleep(self, duration):
        print(f'sleep {duration}s')
        time.sleep(duration)
        
    def parse(self, text, parser='html.parser'):
        return BeautifulSoup(text, parser)

    def fetch(self, url, file_path=None, session=None, max_attempts=10, timeout = 100, custom_headers=None,
            return_json=False):
        if custom_headers is None:
            custom_headers = {}
        
        if file_path is not None:
            print(f'Fetch {url} to {file_path}')
            if Path(file_path).exists():
                print('Already exists. Skipped.')
                return None
        else: 
            print(f'Fetch {url}')
        try:
            for attempt in range(max_attempts):
                if session is not None:
                    response = session.get(url, headers=custom_headers)
                else:
                    response = get(url, headers=custom_headers, timeout = timeout)

                # handle HTTP 429 Too Many Requests
                if response.status_code == 429:
                    delay = (attempt + 1) * 10
                    print(f'Retrying in {delay} seconds')
                    time.sleep(delay)
                else:   
                    break
            response.raise_for_status()
            print("Url was searched")
        
            if session is not None:
                print(f'Session cookies: {len(session.cookies)}')

            if return_json:
                print("Response converted to json")
                return response.json()
                
            if file_path is not None:
                with open(file_path, 'wb') as file:
                    file.write(response.content)

            return response.content

        except:
            print("The problem with url %s"%url)
            try:
                self._webcrawler_logger.update_status_for_query(url, "Finished with errors", "The problem with url %s"%url)
            except:
                pass
        print("Some error url")
        return ""

    def process_page(self, page, query, folder_to_save, added_ids_from_previous_page):
        print(f'\n\nPage {page}')

        doc = self.parse(self.fetch(
            self.prepare_query(query, page), custom_headers = self.headers
        ))
        
        articles =  self.extract_links(doc)
        processed_links = []
        
        for article in articles:
            if article in added_ids_from_previous_page:
                continue
            self.process_article(article, folder_to_save)
            added_ids_from_previous_page.add(article)
            processed_links.append(article)
            self.sleep(1)
        
        self.sleep(3)
        
        return processed_links, len(articles) if len(processed_links) > 0 else 0, added_ids_from_previous_page

    def crawl_query(self, query, folder_to_save, start_page = None, log_status_filename = ""):

        if not os.path.exists(folder_to_save):
            os.makedirs(folder_to_save)

        self._webcrawler_logger = webcrawler_logger.WebcrawlerLogger(log_status_filename)

        if len(self.check_url(query)) == 0:
            added_ids_from_previous_page = set()
            page = self.start_page if start_page == None else start_page
            while True:
                processed_links, articles_in_page, added_ids_from_previous_page = self.process_page(page, query, folder_to_save, added_ids_from_previous_page)
                if len(processed_links) == 0:
                    break
                self._webcrawler_logger.update_webscrapping_results(query, processed_links, articles_in_page)
                page += 1

            print('Done!')
        else:
            self._webcrawler_logger.update_status_for_query(query, "Finished with errors", self.check_url(query))

    def prepare_initial_dataset(self, folder):
        df = pd.DataFrame(data= {'article_name': os.listdir(folder)})
        df["abstract"] = ""
        df["title"] = ""
        df["keywords"] = ""
        df["authors"] = ""
        df["journal_name"] = ""
        df["publisher"] = ""
        df["year"] = ""
        df["url"] = ""
        df["affiliation"] = ""
        df["source_name"] = ""
        return df

    def prepare_dataset(self, folder, filename):
        df = self.prepare_initial_dataset(folder)
        for i in range(len(df)):
            with open(os.path.join(folder, df["article_name"].values[i]), encoding="utf-8") as f:
                try:
                    meta_doc = self.process_file(f)
                    df = self.fill_df_fields(meta_doc, df, i)
                    df = self.fill_year_if_not_found(df, i)
                except Exception as e:
                    print(e)
                    print(df["article_name"].values[i]," ### ", i)
        df = self.process_the_whole_dataset(df)
        df = self.append_more_data(df)
        if len(df) > 0:
            excel_writer.ExcelWriter().save_df_in_excel(df, filename)

    def append_more_data(self, df):
        return df

    def process_file(self, f):
        return self.parse(f.read())

    def add_to_keywords(self, old_val, new_val):
        if old_val != "":
            return old_val + ";" + new_val
        return new_val

    def fill_df_fields(self, meta_doc, df, i):
        pass

    def prepare_query(self, query, page):
        pass

    def extract_links(self, doc):
        pass

    def process_article(self, article_url, folder_to_save):
        pass

    def process_the_whole_dataset(self, df):
        return df

    def check_url(self, url):
        return ""

    def extract_year(self, year_text, old_year):
        now = datetime.datetime.now()
        for it in re.finditer("\d{4}", year_text):
            year = int(it.group(0))
            if type(old_year) == str or old_year == 0:
                old_year = year
            if year > 1800 and year <= now.year:
                old_year = min(year, old_year)
        return old_year if type(old_year) != str else 0

    def fill_year_if_not_found(self, df, i):
        now = datetime.datetime.now()
        if df["year"].values[i] > 1800 and df["year"].values[i] <= now.year:
            return df 
        year = self.extract_year(df["title"].values[i] + "." + df["abstract"].values[i], 0)
        df["year"].values[i] = (year if year > 0 else 2018)
        return df

