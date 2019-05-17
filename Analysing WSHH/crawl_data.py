import datetime
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

import general_helper_functions as help

# The name of the file we want to save to
name_of_csv_file = "data_full"

# Get a timestamp to save the time of data being crawled
ts = datetime.datetime.now().strftime('%Y-%m-%d')

# Create a counter to loop through all the pages
number_of_current_page = 1

# Accumulate the data to save as csv later
data = []

# If something goes wrong during crawling we want to save the data
# we have already accumulated
while True:
    # Get HTML
    url = "https://www.worldstarhiphop.com/videos/?start=" + str(number_of_current_page)
    print("Page: " + str(number_of_current_page))
    code = requests.get(url)
    plain = code.text
    site_html = BeautifulSoup(plain, "html.parser")

    # Check if there are videos to crawl on the site
    # If not end the crawler
    if len(site_html.findAll('section', {'class': 'videos'})) == 0:
        print("No videos found on page.")
        break
    else:
        # If Videos are found we extract data
        print("Found Videos")
        for big_video_box in site_html.findAll('section', {'class': 'videos'}):
            time_of_post = big_video_box.parent.header.time['datetime']
            for child in big_video_box.findAll('section', 'box'):
                try:
                    title = child.strong.string
                    views = child.find('span', "views").string.strip(',')
                    views = int(str(views).replace(',', ''))
                    time_crawled = ts
                    data.append({"title": title, "views": views, 'time_posted': time_of_post, 'time_crawled': ts})
                except:
                    pass
        number_of_current_page += 1
        # Don't send too many requests at once
        time.sleep(0.4)

# Save to csv file
df = pd.DataFrame(data)
df['views'] = pd.to_numeric(df['views'])

# Write to csv
df.to_csv(name_of_csv_file + ".csv", index=False)

# Create sub frame and write down some infos
print("Saved data to .csv file")

# Make a smaller sub-sample for testing
testing_subframe = help.create_small_subframe(df.copy(), 1000)

# Get infos about both sets
help.write_df_infos_to_file(df, "data_info.txt")
help.write_df_infos_to_file(testing_subframe, "data_small_info.txt")
