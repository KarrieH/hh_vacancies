import requests
import json
import os
import tqdm
from io import StringIO
from html.parser import HTMLParser
import pandas as pd
import time
import queue
import threading

PATH = "files"


class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.text = StringIO()

    def handle_data(self, d):
        self.text.write(d)

    def get_data(self):
        return self.text.getvalue()


def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


class MultiThread(threading.Thread):
    def __init__(self):
        super().__init__()

    def run(self):
        pass


class Parser():
    def __init__(self, text, area, per_page):
        self.text = "NAME:" + text
        self.area = area
        self.per_page = per_page

    def getPage(self, p=0):
        params = {
            'text': self.text,  # Текст фильтра. В имени должно быть слово "Аналитик"
            'area': self.area,  # Поиск ощуществляется по вакансиям города Москва
            'page': p,  # Индекс страницы поиска на HH
            'per_page': self.per_page  # Кол-во вакансий на 1 странице
        }

        req = requests.get('https://api.hh.ru/vacancies', params)  # Посылаем запрос к API
        data = req.content.decode()  # Декодируем его ответ, чтобы Кириллица отображалась корректно
        req.close()
        return data

    def process_key_skills(self, skills):
        return ", ".join([skill["name"] for skill in skills])

    def append_to_df(self, df, row):
        try:
            lat = row["address"]["lat"]
        except TypeError:
            lat = 0
        try:
            lng = row["address"]["lng"]
        except TypeError:
            lng = 0
        return df.append(
            pd.DataFrame(
                {"id": row["id"], "name": row["name"],
                 "key_skills": self.process_key_skills(row["key_skills"]),
                 "description": strip_tags(row["description"]), "address_lat": lat,
                 "address_lng": lng}, index=[0]))

    def df_to_csv(self, df, name):
        df.to_csv(name, index=False)

    def getPages(self, n_pages):
        q = queue.Queue()
        for i in range(n_pages):
            q.put(self.getPage(i))
        return q

    def parse(self, pages, df):
        for page in range(pages):
            before = time.time()
            jsObj = json.loads(parser.getPage(page))
            print(time.time() - before)
            for jsonbj in tqdm.tqdm(jsObj["items"]):
                req = requests.get(jsonbj["url"])
                data = req.content.decode()
                req.close()
                data = json.loads(data)
                df = self.append_to_df(df, data)
        self.df_to_csv(df, "data.csv")


parser = Parser("Аналитик", 1, 10)
data = pd.DataFrame()
parser.parse(3, data)
