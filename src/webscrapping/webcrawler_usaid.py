from webscrapping.webcrawler_base import WebCrawlerBase
import re
import os
import requests
import json
import pickle
from webscrapping import webcrawler_logger
from utilities import excel_writer

class WebCrawlerUsaid(WebCrawlerBase):
    def __init__(self):
        WebCrawlerBase.__init__(self)
        self.domain_name = "https://dec.usaid.gov"

    def crawl_query(self, query, folder_to_save, start_page = None, log_status_filename = ""):
        if not os.path.exists(folder_to_save):
            os.makedirs(folder_to_save)

        self._webcrawler_logger = webcrawler_logger.WebcrawlerLogger(log_status_filename)

        try:
            api_query = "https://dec.usaid.gov/api/qsearch.ashx?q="+query.split("?q=")[1]+"&rtype=JSON"
            json_result = self.fetch(api_query, return_json=True)
            total_cnt = len(json_result["Records"])
            pickle.dump(json_result, open(os.path.join(folder_to_save, "results.pickle"), "wb"))
            self._webcrawler_logger.update_webscrapping_results(query, [1]*total_cnt, total_cnt)
            print('Done!')
        except:
            self._webcrawler_logger.update_status_for_query(query, "Finished with errors", self.check_url(query))

    def prepare_dataset(self, folder, filename):
        df = self.prepare_initial_dataset(folder)
        json_results = pickle.load(open(os.path.join(folder, "results.pickle"), "rb"))
        df = df.reindex(df.index.tolist() + list(range(1, len(json_results["Records"]))))
        for i, article_doc in enumerate(json_results["Records"]):
            df = self.fill_df_fields(article_doc, df, i)
            df = self.fill_year_if_not_found(df, i)
        if len(df) > 0:
            excel_writer.ExcelWriter().save_df_in_excel(df, filename)

    def fill_df_fields(self, meta_doc, df, i):
        df["source_name"].values[i] = "USAID"
        df["abstract"].values[i] = "\n".join(meta_doc["Abstract"]["value"])
        df["title"].values[i] = "\n".join(meta_doc["Title"]["value"])
        df["keywords"].values[i] = ";".join(meta_doc["Descriptors_Topical"]["value"]+meta_doc["Descriptors_Geographic"]["value"])
        df["authors"].values[i] = ";".join(meta_doc["Personal_Author"]["value"])
        df["year"].values[i] = self.extract_year(" ".join(meta_doc["Date_Resource_Created"]["value"]), df["year"].values[i])
        df["url"].values[i] = " ".join(meta_doc["URI"]["value"])
        df["affiliation"].values[i] = ";".join(meta_doc["Inst_Author"]["value"])
        df["journal_name"].values[i] = ";".join(meta_doc["Series_Title"]["value"])
        df["publisher"].values[i] = ";".join(meta_doc["Inst_Publisher"]["value"])
        return df
        