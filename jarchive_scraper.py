from bs4 import BeautifulSoup
import scraperwiki
from datetime import datetime
import re
import unicodedata

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# define the order our columns are displayed in the datastore
# scraperwiki.sqlite.save_var('data_columns', ['air_date','episode', 'category', 'dollar_value', 'text', 'answer','uid'])

seasons_url = 'http://www.j-archive.com/listseasons.php'
base_url = 'http://www.j-archive.com/'

# store into firebase
cred = credentials.Certificate('./firebase_key.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

def scrape_all_seasons(url):

    soup = BeautifulSoup(scraperwiki.scrape(url), features='lxml')
    
    #Grab all of the seasons listed
    seasons = soup.find('div', {"id":"content"}).findAll('a')
    count = 0
    for season in seasons:
        season_name = unicodedata.normalize('NFKC', season.text)
        print('Scraping ' + season_name + ' from ' + season['href'])
        scrape_season(base_url+season['href'], season_name)

def scrape_season(url, season):
    
    soup = BeautifulSoup(scraperwiki.scrape(url), features='lxml')
    
    #Grab the div that contains the content and search for any links
    episodes = soup.find('div', {"id":"content"}).findAll('a',{"href":re.compile('showgame\.php')})
    for episode in episodes:
        ep_data = unicodedata.normalize('NFKC', episode.text).split(',')
        ep_num = re.search('#(\d+)', ep_data[0]).group(1)

        # Get the Date
        air_data = re.search('(\d{4}-\d{2}-\d{2})', ep_data[1]).group(1) + ' UTC'
        air_datetime = datetime.strptime(air_data, '%Y-%m-%d %Z')
        air_date = air_datetime.strftime('%Y/%m/%d')

        print('Scraping Episode {} from {}'.format(ep_num, episode['href']))
        scrape_episode(episode['href'], season, ep_num, air_date)


def scrape_episode(url, season, episode, air_date):
    
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

                # The Final Jeopardy Div is not located in the same place
                # as other questions so it must be found seperately
                fj_div = None
                if not clue.find('div') and clue.find(id='clue_FJ'):
                    fj_div = clue.parent.parent.find('div')

                clue_attribs = get_clue_attribs(clue, cats, fj_div)
                if clue_attribs:
                    clue_attribs['air_date'] = air_date
                    clue_attribs['season'] = season
                    clue_attribs['episode'] = episode
        
                    #a shitty unique id but it should do
                    uid = ': '.join([str(episode), clue_attribs['category'], str(clue_attribs['dollar_value'])])

                    doc_ref = db.collection(u'clues').document(uid)
                    doc_ref.set(clue_attribs)

    except RuntimeError:
        exception = 1


def get_clue_attribs(clue, cats, fj_div=None):
    #Because of the way jarchive hides the answers to clues
    #this is here to keep things a bit more tidy
    div = fj_div if fj_div else clue.find('div')
    
    if div:
        #Split the JS statement into it's arguments so we can extract the html from the final argument
        mouseover_js = div['onmouseover'].split(",",2)
        answer_soup = BeautifulSoup(mouseover_js[2], features='lxml') #We need to go... deeper
        answer = answer_soup.find('em').text

        clue_props = mouseover_js[1].split("_") #contains the unique ID of the clue for this specific game
                                                #format: clue_["DJ"||"J"]_[Category(1-6]]_[Row(1-5)]
        j_type = clue_props[1]
        cat_num = 1 if j_type == 'FJ' else int(clue_props[2])
        cat_order = 1 if j_type == 'FJ' else int(clue_props[3])

        #Now to figure out the category
        if j_type == "FJ":
            cat = cats[12]
        elif j_type == "DJ":
            cat = cats[cat_num+5]
        else:
            cat = cats[cat_num-1]

        #Are we in double jeopardy?
        dj = clue_props[1] == "DJ"

        #The class name for the dollar value varies if it's a daily double
        dollar_value = "FJ: $0" if j_type == 'FJ' else clue.find(attrs={"class" : re.compile('clue_value*')}).text
        clue_text = clue.find(attrs={"class" : "clue_text"}).text
        
        clue_dict = {
            'answer': answer,
            'category': cat,
            'question': clue_text,
            'dollar_value': dollar_value,
            'type': j_type,
            'order': cat_order
        }

        return clue_dict



scrape_all_seasons(seasons_url)