from bs4 import BeautifulSoup
import scraperwiki
from datetime import datetime
import time
import re
import unicodedata

import firebase_admin
from firebase_admin import credentials

# define the order our columns are displayed in the datastore
scraperwiki.sqlite.save_var('data_columns', ['air_date','episode', 'category', 'dollar_value', 'text', 'answer','uid'])

seasons_url = 'http://www.j-archive.com/listseasons.php'
base_url = 'http://www.j-archive.com/'

# store into firebase
cred = credentials.Certificate()

def scrape_all_seasons(url):

    soup = BeautifulSoup(scraperwiki.scrape(url), features='lxml')
    
    #Grab all of the seasons listed
    seasons = soup.find('div', {"id":"content"}).findAll('a')
    print(len(seasons))
    count = 0
    for season in seasons:
        count += 1
        if count > 20:
            scrape_season(base_url+season['href'])
            break

def scrape_season(url):
    
    soup = BeautifulSoup(scraperwiki.scrape(url), features='lxml')
    
    #Grab the div that contains the content and search for any links
    episodes = soup.find('div', {"id":"content"}).findAll('a',{"href":re.compile('showgame\.php')})
    for episode in episodes:
        ep_data = unicodedata.normalize('NFKC', episode.text).split(',')
        ep_num = re.search('#(\d+)', ep_data[0]).group(1)

        #Fuck this is messy
        air_data = re.search('(\d{4}-\d{2}-\d{2})', ep_data[1]).group(1)
        air_date = datetime.strptime(air_data, '%Y-%m-%d')
        timestamp = time.mktime(air_date.timetuple())

        scrape_episode(episode['href'], ep_num, timestamp)
        break


def scrape_episode(url, episode, air_date):
    
    try:
        soup = BeautifulSoup(scraperwiki.scrape(url), features='lxml')

        #only scrape full episodes
        allCategories = soup.findAll('td', {"class" : "category_name"})
        if len(allCategories) > 0:
    
            cats = [] # List of categories without any html
            for cat in allCategories:
                cats.append(cat.text)
    
            allClues = soup.findAll(attrs={"class" : "clue"})
            for clue in allClues:
        
                clue_attribs = get_clue_attribs(clue, cats)
                if clue_attribs:
                    clue_attribs['air_date'] = air_date
                    clue_attribs['episode'] = episode
        
                    #a shitty unique id but it should do
                    clue_attribs['uid'] = str(episode)+clue_attribs['category']+str(clue_attribs['dollar_value'])
                    scraperwiki.sqlite.save(['uid'], clue_attribs)

    except RuntimeError:
        exception = 1


def get_clue_attribs(clue, cats):
    #Because of the way jarchive hides the answers to clues
    #this is here to keep things a bit more tidy
    div = clue.find('div')
    
    if div:
        #Split the JS statement into it's arguments so we can extract the html from the final argument
        mouseover_js = div['onmouseover'].split(",",2)
        answer_soup = BeautifulSoup(mouseover_js[2], features='lxml') #We need to go... deeper
        answer = answer_soup.find('em', {"class" : "correct_response"}).text
        
        clue_props = mouseover_js[1].split("_") #contains the unique ID of the clue for this specific game
                                                #format: clue_["DJ"||"J"]_[Category(1-6]]_[Row(1-5)]
                                                
        #Now to figure out the category
        if clue_props[1] == "FJ":
            cat = cats[12]
        elif clue_props[1] == "DJ":
            cat = cats[int(clue_props[2])+5]
        else:
            cat = cats[int(clue_props[2])-1]

        #Are we in double jeopardy?
        dj = clue_props[1] == "DJ"

        #The class name for the dollar value varies if it's a daily double
        dollar_value = clue.find(attrs={"class" : re.compile('clue_value*')}).text
        clue_text = clue.find(attrs={"class" : "clue_text"}).text
        
        clue_dict = {
            'answer': answer,
            'category': cat,
            'text': clue_text,
            'dollar_value': dollar_value
        }

        print(clue_dict)
        return clue_dict



scrape_all_seasons(seasons_url)