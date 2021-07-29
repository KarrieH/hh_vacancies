import requests
import json
import aiohttp
import asyncio
from io import StringIO
from html.parser import HTMLParser
import pandas as pd
import time
from tqdm import tqdm

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
    if not html:
        return ""
    s.feed(html)
    return s.get_data()


class Parser:
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

    async def fetch(self, session, url):
        async with session.get(url) as response:
            if response.status != 200:
                response.raise_for_status()
            return await response.text()

    async def fetch_all(self, session, urls):
        tasks = []
        for url in urls:
            task = asyncio.create_task(self.fetch(session, url))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        return results

    async def get(self, urls, loop):
        l = []
        async with aiohttp.ClientSession(loop=loop) as session:
            htmls = await self.fetch_all(session, urls)
        l.append(htmls)
        return l

    def process_key_skills(self, skills):
        return ",".join([skill["name"] for skill in skills])

    def make_save_df(self, pages, name):
        df = self.make_df(pages)
        df.to_csv(name, index=False)

    def getPages(self, n_pages):
        urls = []
        snippets = []
        for i in range(n_pages):
            data = json.loads((self.getPage(i)))
            urls = urls + [elem["url"] for elem in data["items"]]
            snippets = snippets + [elem["snippet"] for elem in data["items"]]
        return urls, snippets

    def Parse(self, pages):
        urls, snippets = self.getPages(pages)
        # loop = asyncio.get_event_loop()
        # htmls = loop.run_until_complete(self.get(urls, loop))
        # loop.close()
        htmls = []
        for url in urls:
            req = requests.get(url)
            html = req.content.decode()
            req.close()
            htmls.append(html)
        htmls = [json.loads(elem) for elem in htmls]
        return htmls, snippets

    def make_df(self, pages):
        df = pd.DataFrame(columns=['id', 'name', 'skills', 'desc', 'exp', 'spec', "area","resp","req"],
                          index=range(pages * self.per_page))
        rows, snippets = self.Parse(pages)
        for i, row in enumerate(rows):
            df.iloc[i]["id"] = row["id"]
            df.iloc[i]["name"] = row["name"]
            df.iloc[i]["skills"] = self.process_key_skills(row["key_skills"])
            df.iloc[i]["desc"] = strip_tags(row["description"])
            df.iloc[i]["ex"] = row["experience"]["name"]
            df.iloc[i]["spec"] = self.process_key_skills(row["specializations"])
            df.iloc[i]["area"] = row["area"]["name"]
            df.iloc[i]["resp"] = strip_tags(snippets[i]["responsibility"])
            df.iloc[i]["req"] = strip_tags(snippets[i]["requirement"])

        return df


parser_ml_spb = Parser("machine learning", 2, 20)
parser_de_spb = Parser("data engineering", 2, 20)
parser_ds_spb = Parser("data science", 2, 20)
parser_ml_msk = Parser("machine learning", 1, 20)
parser_de_msk = Parser("data engineering", 1, 20)
parser_ds_msk = Parser("data science", 1, 20)
parser_ml_random = Parser("machine learning", 1202, 20)
parser_de_random = Parser("data engineering", 1202, 20)
parser_ds_random = Parser("data science", 1202, 20)

time_before = time.time()
parsers = [parser_ml_spb, parser_de_spb, parser_ds_spb,
           parser_ml_msk, parser_de_msk, parser_ds_msk,
           parser_ml_random, parser_de_random, parser_ds_random]

for i, parser in tqdm(enumerate(parsers)):
    parser.make_save_df(1, f'data_{i}.csv')

time_after = time.time()
print(time_after - time_before)
