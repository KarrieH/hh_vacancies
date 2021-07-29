import requests
import json
import aiohttp
import asyncio
from io import StringIO
from html.parser import HTMLParser
import pandas as pd
import time

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

    def make_save_df(self, pages, name):
        df = self.make_df(pages)
        df.to_csv(name, index=False)

    def getPages(self, n_pages):
        urls = []
        for i in range(n_pages):
            data = json.loads((self.getPage(i)))
            urls = urls + [elem["url"] for elem in data["items"]]
        return urls

    def Parse(self, pages):
        urls = self.getPages(pages)
        loop = asyncio.get_event_loop()
        htmls = loop.run_until_complete(self.get(urls, loop))
        loop.close()
        htmls = [json.loads(elem) for elem in htmls[0]]
        return htmls

    def make_df(self, pages):
        df = pd.DataFrame(columns=['id', 'name', 'skills', 'desc', 'lat', 'lng'],
                          index=range(pages * self.per_page))
        rows = self.Parse(pages)
        for i, row in enumerate(rows):
            try:
                lat = row["address"]["lat"]
            except TypeError:
                lat = 0
            try:
                lng = row["address"]["lng"]
            except TypeError:
                lng = 0
            df.iloc[i]["id"] = row["id"]
            df.iloc[i]["name"] = row["name"]
            df.iloc[i]["skills"] = self.process_key_skills(row["key_skills"])
            df.iloc[i]["desc"] = strip_tags(row["description"])
            df.iloc[i]["lat"] = lat
            df.iloc[i]["lng"] = lng
        return df


parser = Parser("Аналитик", 1, 20)
time_before = time.time()
parser.make_save_df(1, 'data.csv')
time_after = time.time()
print(time_after - time_before)
