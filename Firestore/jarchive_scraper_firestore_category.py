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
cred = credentials.Certificate('./firestore_category_key.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

# Load the already processed episodes
pickle_file = './firestore_category_processed.p'
processed = {}
try:
    processed = pickle.load(open(pickle_file, 'rb'))
except Exception as e:
    print(e)

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
            print(type(e))
            print(e)
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
            categories_by_jtype = {}
            if jeopardy_round_exists:
                categories_by_jtype['J'] = cats[:6]

            if double_jeopardy_round_exists:
                categories_by_jtype['DJ'] = cats[6:12] if jeopardy_round_exists else cats[:6]

            if final_jeopardy_round_exists:
                categories_by_jtype['FJ'] = [(cats[12] if double_jeopardy_round_exists else cats[6]) if jeopardy_round_exists else cats[1]]

            categories_clues = {}
            for jtype, categories in categories_by_jtype.items():
                categories_clues[jtype] = {}
                for category in categories:
                    categories_clues[jtype][category] = {'category': category}

            allClues = soup.findAll(attrs={"class" : "clue"})
            for clue in allClues:

                # The Final Jeopardy Div is not located in the same place
                # as other questions so it must be found seperately
                fj_div = None
                if not clue.find('div') and clue.find(id='clue_FJ'):
                    fj_div = clue.parent.parent.find('div')

                clue_attribs = get_clue_attribs(clue, categories_by_jtype, fj_div)
                if clue_attribs:
                    clue_attribs['air_date'] = air_date
                    clue_attribs['season'] = season
                    clue_attribs['episode'] = episode

                    clue_jtype = clue_attribs['type']
                    clue_cat = clue_attribs['category']
                    clue_order = str(clue_attribs['order'])

                    categories_clues[clue_jtype][clue_cat][clue_order] = clue_attribs
                    
            # Number of Questions Per Category
            CAT_QUESTIONS = 6

            # Remove All Categories that weren't completed
            for jtype in categories_clues:

                # Final Jeopardy will always be asked
                if jtype == 'FJ':
                    continue

                # List of Categories to delete
                delete = [cat for cat, questions in categories_clues[jtype].items() if len(questions) != CAT_QUESTIONS]

                # Delete the categories
                for cat in delete:
                    del categories_clues[jtype][cat]

            # Perform a Batch Write for all Categories in Episode
            batch = db.batch()

            counters_ref = db.collection(u'count').document(u'counters')
            counters_dict = counters_ref.get().to_dict()

            # Dict to associate Jeopardy Type with Collection Name and counter variable
            jtype_to_ref_details = {
                'J': {
                    'collection': u'jeopardy',
                    'counter': 'jcount'
                },
                'DJ': {
                    'collection': u'double_jeopardy',
                    'counter': 'djcount'
                },
                'FJ': {
                    'collection': u'final_jeopardy',
                    'counter': 'fjcount'
                }
            }

            for jtype in categories_clues:
                for cat in categories_clues[jtype]:
                    collection_name = jtype_to_ref_details[jtype]['collection']
                    counter_name = jtype_to_ref_details[jtype]['counter']

                    doc_ref = db.collection(collection_name).document(str(counters_dict[counter_name]))
                    batch.set(doc_ref, categories_clues[jtype][cat])
                    counters_dict[counter_name] += 1

            batch.update(counters_ref, counters_dict)

            # Commit Batch to the Firebase
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
        print('Error')
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