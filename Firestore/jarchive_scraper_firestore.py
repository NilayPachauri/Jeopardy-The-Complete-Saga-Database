from bs4 import BeautifulSoup
import scraperwiki

from datetime import datetime
import pickle
import re
import sys
import unicodedata

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

seasons_url = 'http://www.j-archive.com/listseasons.php'
base_url = 'http://www.j-archive.com/'

# store into firebase
cred = credentials.Certificate('./firestore_key.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

# Load the already processed episodes
pickle_file = './firestore_processed.p'
processed = pickle.load(open(pickle_file, 'rb'))

def scrape_all_seasons(url):

    soup = BeautifulSoup(scraperwiki.scrape(url), features='lxml')

    #Grab all of the seasons listed
    seasons = soup.find('div', {"id":"content"}).findAll('a')
    for season in seasons:

        season_name = unicodedata.normalize('NFKC', season.text)

        print('Scraping ' + season_name + ' from ' + season['href'])
        scrape_season(base_url+season['href'], season_name)
        print('Finished scraping ' + season_name)
        print()

def scrape_season(url, season):
    
    soup = BeautifulSoup(scraperwiki.scrape(url), features='lxml')
    
    #Grab the div that contains the content and search for any links
    episodes = soup.find('div', {"id":"content"}).findAll('a',{"href":re.compile('showgame\.php')})
    for episode in episodes:
        try:
            ep_data = unicodedata.normalize('NFKC', episode.text).split(',')
            ep_num = int(re.search('#(\d+)', ep_data[0]).group(1))

            # Get the Date
            air_data = re.search('(\d{4}-\d{2}-\d{2})', ep_data[1]).group(1) + ' UTC'
            air_datetime = datetime.strptime(air_data, '%Y-%m-%d %Z')
            air_date = air_datetime.strftime('%Y/%m/%d')

            print('\tScraping Episode {} from {}'.format(ep_num, episode['href']))
            scrape_episode(episode['href'], season, ep_num, air_date)
        except Exception as e:
            print('\tCould not correctly parse ' + unicodedata.normalize('NFKC', episode.text).strip())
        sys.stdout.flush()


def scrape_episode(url, season, episode, air_date):
    # Warm Start Due to Errors
    if season in processed and episode in processed[season]:
        return
    
    try:
        soup = BeautifulSoup(scraperwiki.scrape(url), features='lxml')

        jeopardy_round_exists = soup.find('div', {'id': 'jeopardy_round'}) != None
        double_jeopardy_round_exists = soup.find('div', {'id': 'double_jeopardy_round'}) != None
        final_jeopardy_round_exists = soup.find('div', {'id': 'final_jeopardy_round'}) != None

        #only scrape full episodes
        allCategories = soup.findAll('td', {"class" : "category_name"})
        if len(allCategories) > 0:
    
            cats = [] # List of categories without any html
            for cat in allCategories:
                cats.append(cat.text)

            # Populate the Category Dictionary
            categories = {}
            if jeopardy_round_exists:
                categories['J'] = cats[:6]

            if double_jeopardy_round_exists:
                categories['DJ'] = cats[6:12] if jeopardy_round_exists else cats[:6]

            if final_jeopardy_round_exists:
                categories['FJ'] = [(cats[12] if double_jeopardy_round_exists else cats[6]) if jeopardy_round_exists else cats[1]]
            
            # Perform a Batch Write for all Clues in an Episode
            batch = db.batch()

            allClues = soup.findAll(attrs={"class" : "clue"})
            for clue in allClues:

                # The Final Jeopardy Div is not located in the same place
                # as other questions so it must be found seperately
                fj_div = None
                if not clue.find('div') and clue.find(id='clue_FJ'):
                    fj_div = clue.parent.parent.find('div')

                clue_attribs = get_clue_attribs(clue, categories, fj_div)
                if clue_attribs:
                    clue_attribs['air_date'] = air_date
                    clue_attribs['season'] = season
                    clue_attribs['episode'] = episode
        
                    # Create Unique Identification
                    uid = ': '.join([season, str(episode), clue_attribs['category'], str(clue_attribs['dollar_value'])])

                    # Replace potential forward slashes with backward slashes
                    # Note: Firebase forward slash represeents a new collection
                    uid = uid.replace('/', '\\')

                    doc_ref = db.collection(u'clues').document(uid)
                    batch.set(doc_ref, clue_attribs)
                    
            # Commit the batch
            batch.commit()

            # Update the Processed Dictionary with the most recently processed episode
            if season in processed:
                processed[season].append(episode)
            else:
                processed[season] = [episode]

            # Write out the pickle file
            with open(pickle_file, 'wb') as file:
                pickle.dump(processed, file)

    except RuntimeError as re:
        print(re)


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
        cat = cats[j_type][cat_num - 1]

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