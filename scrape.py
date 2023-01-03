import re

import pandas as pd
import requests
import unidecode
from bs4 import BeautifulSoup

ARMY_LINK = "https://devenir-aviateur.fr/rejoindre-la-communaute-des-aviateurs?famille=All&niveau=7"
STRONG_KEYWORDS = [
  "intelligence artificielle",
  "artificiel intelligence",
  " ai ",
  "ai.",
  " ia ",
  " ia.",
  "deep learning",
  "machine learning",
  "data science",
  "data scientist",
  "science de la donnee",
  "science des donnees",
  "scientifique de la donnee",
  "scientifique des donnees",
]


def get_jobs_info() -> pd.DataFrame:
  """
  Scrape the "Armée de l'air website and return a df with information
  about each job offered.
  """

  # Get soup
  r = requests.get(ARMY_LINK)
  html = r.text
  soup = BeautifulSoup(html, "html.parser")

  # Get each job card
  job_cards = soup.find_all(class_="col-12 col-sm-6 col-md-4 p-3")

  # Initialize result df
  jobs = pd.DataFrame()

  # Get info on each job
  for job_card in job_cards:
    job_title = job_card.a.h3.text
    job_link = "https://devenir-aviateur.fr" + job_card.a.get("href")
    job_level = job_card.a.find(class_="col-niveau py-1").text

    data = {"title": [job_title.title()], "link": [job_link], "level": [job_level]}
    jobs = pd.concat([jobs, pd.DataFrame(data)], ignore_index=True)

  return jobs


def get_job_text(job_link: str) -> str:
  """
  Get the description of a job.
  """
  global _calls_counter
  if "_calls_counter" in vars() or "_calls_counter" in globals():
    _calls_counter += 1
  else:
    _calls_counter = 0

  # Check that job is running
  print(f"Getting job description {_calls_counter}")
  r = requests.get(job_link)
  html = r.text
  soup = BeautifulSoup(html, "html.parser")
  content_class = soup.find_all(class_="content")
  job_text: str = content_class[3].text
  job_text = job_text.strip()
  return job_text


def process_descr(descr: str) -> str:
  if pd.isna(descr):
    return descr
  descr = unidecode.unidecode(descr)
  descr = descr.lower()
  descr = descr.replace("\n", " ")
  descr = descr.replace("\t", " ")
  descr = re.sub(" +", " ", descr)
  return descr


def contains_strong_kw(descr: str):
  if pd.isna(descr):
    return []
  terms_contained = []
  for term in STRONG_KEYWORDS:
    if term in descr:
      terms_contained.append(term)

  # if terms_contained == []:
  #   return None
  return terms_contained