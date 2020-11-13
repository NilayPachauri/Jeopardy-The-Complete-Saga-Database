import pickle
import re

# Get all the lines from the log file
lines = [line.strip() for line in open('./dynamodb.log').readlines()]

# Create the Regex to detect the season, episode
season_regex = r'Scraping (.+) from showseason\.php'
episode_regex = r'Scraping Episode (\d+) from \S+showgame\.php'

season = None

# Empty Dictionary for the Pickle File
processed_dict = {}

for line in lines:

	# Store potential match objects into variables
	season_match = re.search(season_regex, line)
	episode_match = re.search(episode_regex, line)

	if season_match:
		season = season_match.group(1)

	if episode_match:
		episode = int(episode_match.group(1))
		if season in processed_dict:
			processed_dict[season].append(episode)
		else:
			processed_dict[season] = [episode]

with open('dynamodb_processed.p', 'wb') as file:
	pickle.dump(processed_dict, file)

print(processed_dict)