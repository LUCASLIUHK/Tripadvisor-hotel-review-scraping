# -v2 update log
#   --solved problems with users with 0 contribution by introducing new class = "rNZKv"
#   --introduced page groups, scrape 5 pages each time, with time delays in-between
#   --added new function clean_text to clean text data by removing emojis, escape sequences and leading & trailing whitespace etc
#   --introduce auto-export per certain num of pages feature (func cache)

# -v3 update log
#   --solved bug which can't parse number with separator

# -v4 update log
#   --solved bug caused by different length of matched date of stay list (due to website problem), try filling with nan


from bs4 import BeautifulSoup as soup
import requests
import re
import json
import numpy as np
import pandas as pd
import random
import time
import os
import math
from tqdm.notebook import tqdm


def header_list(user_agent):
    return [
        {
            'authority': 'httpbin.org',
            'cache-control': 'max-age=0',
            'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
            'sec-ch-ua-mobile': '?0',
            'upgrade-insecure-requests': '1',
            'user-agent': user_agent,
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'sec-fetch-site': 'none',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-user': '?1',
            'sec-fetch-dest': 'document',
            'accept-language': 'en-US,en;q=0.9',
        }
    ]

def get_full_text(response):
    html_text = response.text
    data = re.search(r'window\.__WEB_CONTEXT__=(.*?});', html_text).group(1)
    data = data.replace('pageManifest', '"pageManifest"')
    data_json = json.dumps(json.loads(data), indent=4)
    pattern = r"""\\"additionalRatings\\":\[.*?\],\\"text\\":\\"(.*?)\\",\\"username\\"""
    full_text = re.findall(pattern, data_json)
    return full_text

def url_join(head, body):
    return "/".join([head, body])

def list_to_df(list, columns=None):
    dataframe = None
    for i in range(len(list)):
        dataframe = pd.concat([dataframe, pd.DataFrame(list[i])], ignore_index=True)
    dataframe.columns = columns
    return dataframe

def convert_ratings(dataframe):
    columns = dataframe.columns
    dataframe["rating_stars"] = dataframe.ratings.str[-3]
    dataframe.drop("ratings", axis=1, inplace=True)
    dataframe = dataframe.rename(columns={"rating_stars": "ratings"})
    dataframe = dataframe[columns]
    return dataframe

# def unicode_char():
#     num = list(range(2018, 2060))
#     encode = []
#     for i in range(len(num)):
#         p = r"\u" + str(num[i])
#         encode.append(p)
#     encode_sequence = "|".join(encode)
#     pattern = f"""u"({encode_sequence})"""
#     return pattern

def format_num(num_str):
    return eval(num_str.replace(" ", "").replace(",", ""))

def clean_text(string):
    #deal with emoji
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U0001F600-\U0001F64F"
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               u"\U0001f926-\U0001f937"
                               u"\U0001F1F2"
                               u"\U0001F1F4"
                               u"\U0001F620"
                               u"\u200d"
                               u"\u2640-\u2642"
                               "]+", flags=re.UNICODE)

    string = emoji_pattern.sub(r"", string)
    #remove \n\t\r
    string = string.translate(string.maketrans("\n\t\r", "   "))
    string = string.replace("\\n", " ").replace("\\r", " ").replace("\\t", " ") #.replace("\\", "")
    #trim
    string = string.strip()
    string = string.encode('utf-8', errors="ignore").decode('utf-8', errors="ignore").replace("\\", " ")
    return string.replace("u2019", "'").replace("u2018", "'").replace("u2026", " ")

def final_df_clean(final_df, col_list):
    for col in col_list:
        final_df[col] = final_df[col].apply(clean_text)
    return final_df

def make_subdir(subdir):
    current_dir = os.getcwd()
    sub_dir = os.path.join(current_dir, subdir)
    if os.path.exists(sub_dir) is False:
        os.makedirs(sub_dir)
        print(f"Directory created: {sub_dir}")
    return sub_dir

def cache(dataframe, filename, ref, interval=100):
    if ref % interval == 0:
        cache_dir = make_subdir("cache")
        file = f"{cache_dir}/cache_page{pair[1]}_" + filename
        dataframe.to_csv(file, header=True, index=False)
        print(f"{file} saved")
    return None

def fill_null(list, stdlen):
    length = len(list)
    if length < stdlen:
        for _ in range(stdlen - length):
            list.append(str(np.nan))
    return list

def if_empty(list): #for lists whose length is supposed to be 1
    if len(list) == 0:
        return str(np.nan)
    else:
        return list[0]

if __name__ == "__main__":
    start = time.time()
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
        'Mozilla/5.0 (Linux; Android 11; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Mobile Safari/537.36'
    ]

    url = "https://www.tripadvisor.com.sg/Hotel_Review-g294265-d1770798-Reviews-or{}-Marina_Bay_Sands-Singapore.html#REVIEWS"
    url_head = "https://www.tripadvisor.com.sg"

    final = []
    final_df = None
    total_reviews_eng = 18105  # should be >= 60 # change here for ad-hoc loop
    # original total = 19154
    review_per_page = 10
    #total_pages = math.ceil(total_reviews_eng/review_per_page) - 1
    total_pages = math.floor(total_reviews_eng / review_per_page)
    last_page = total_pages
    regular_page = total_pages - 1
    range_pair = []
    groups_num = math.floor(regular_page / 5)
    operated_page = 105 # change here for ad-hoc loop

    for n in range(groups_num):
        start_page = 5 * n + operated_page
        end_page = 5 * n + operated_page + 5
        range_pair.append((start_page, end_page))
        last_group = (end_page, last_page)
    range_pair.append(last_group)

    for t in tqdm(range(len(range_pair)), desc="Total progress"):
        time.sleep(random.randint(1, 3))
        pair = range_pair[t]
        #print(pair[0], pair[1])
        for n in tqdm(range(pair[0], pair[1]), desc=f"Request progress [Page {pair[0] + 1}-{pair[1]}]"):

            user_agent = random.choice(user_agents)
            headers_list = header_list(user_agent)
            headers = random.choice(headers_list)
            time.sleep(random.randint(1, 3))
            page = str(n) + "0"
            url = "https://www.tripadvisor.com.sg/Hotel_Review-g294265-d1770798-Reviews-or" + str(
                page) + "-Marina_Bay_Sands-Singapore.html#REVIEWS"
            response = requests.get(url, headers=headers, timeout=60)

            user_class = "ui_header_link uyyBf"
            pattern_user = """<a class="ui_header_link uyyBf" href=".*">(.*?)</a>"""
            pattern_user_profile = """<a class="ui_header_link uyyBf" href="(.*?)">.*</a>"""

            contributions_class1 = "etCOn b Wc _S"
            contributions_class2 = "rNZKv"
            pattern_contributions = """<a class="(?:etCOn b Wc _S|rNZKv)">(.*?)</a>"""

            title_class = "KgQgP MC _S b S6 H5 _a"
            pattern_title = """<div class="KgQgP MC _S b S6 H5 _a".*<span><span>(.*?)</span></span>"""

            review_class = "QewHA H4 _a"
            pattern_review = """<q class="QewHA H4 _a"><span>(.*?)</span>"""

            date_class = "teHYY _R Me S4 H3"
            pattern_date = """<span class="teHYY _R Me S4 H3"><span class="usajM">Date of stay:</span> (.*?)</span>"""

            rating_class = "Hlmiy F1"
            pattern_rating = """<div class="Hlmiy F1" data-test-target="review-rating"><span class=(.*?)></span></div>"""

            if response.status_code == 200:
                print(f"{n + 1} Request Successful by {user_agent}")
                html = soup(response.content, "html.parser")
                user_list = [str(item) for item in html.find_all(class_=user_class)]
                review_per_page_new = len(user_list)
                contributions_list = []

                #for k in tqdm(range(review_per_page_new), desc=f"Sub-request progress"):
                for k in range(review_per_page_new):
                    user_profile_url = re.findall(pattern_user_profile, user_list[k])[0]
                    profile_url = url_join(url_head, user_profile_url)
                    user_agent = random.choice(user_agents)
                    headers_list = header_list(user_agent)
                    headers = random.choice(headers_list)
                    time.sleep(random.randint(0, 2))
                    profile_html = requests.get(profile_url, headers=random.choice(headers_list), timeout=60)

                    if profile_html.status_code == 200:
                        print(f"\t{n+1}-{k+1} Sub-request Successful by {user_agent}")
                        profile = soup(profile_html.content, "html.parser")
                        if len(profile.find_all(class_=contributions_class1)) != 0:
                            contributions_string = str(profile.find_all(class_=contributions_class1)[0])
                            contributions_num = format_num(re.findall(pattern_contributions, contributions_string)[0])
                            contributions_list.append(contributions_num)
                        else:
                            contributions_list.append(0)
                    else:
                        print(f"\t{n + 1}-{k + 1} Sub-request Failed")
                        contributions_list.append(np.nan)
                        continue

                title_list = [str(item) for item in html.find_all(class_=title_class)]
                #review_list = [str(item) for item in html.find_all(class_=review_class)]
                review_list = get_full_text(response)
                date_list = [str(item) for item in html.find_all(class_=date_class)]
                rating_list = [str(item) for item in html.find_all(class_=rating_class)]
                result_list = []

                for i in range(review_per_page_new):
                    #a = review_list[i]
                    result_list.append([if_empty(re.findall(pattern_user, fill_null(user_list, review_per_page_new)[i])),
                                        contributions_list[i],
                                        if_empty(re.findall(pattern_date, fill_null(date_list, review_per_page_new)[i])),
                                        if_empty(re.findall(pattern_rating, fill_null(rating_list, review_per_page_new)[i])),
                                        if_empty(re.findall(pattern_title, fill_null(title_list, review_per_page_new)[i])),
                                        review_list[i]])
                final.append(result_list)

            else:
                print(f"{n + 1} Request Failed")
                continue
            operated_page += 1
            print(f"{operated_page} page(s) have been processed. Next: {operated_page+1}\n")

        columns = ["user_id", "contributions", "date_of_stay", "ratings", "title", "content"]
        dataframe = convert_ratings(list_to_df(final, columns))
        #final_df = pd.concat([final_df, dataframe], ignore_index=True)
        #print(pair[1], pair[1] % 5)
        cache(final_df_clean(dataframe, col_list=["title", "content"]), "tripadvisor_mbs_review.csv", pair[1], 5) # x*10 records of reviews in each cache
    final_df = final_df_clean(dataframe, col_list=["title", "content"])
    output_dir = make_subdir("output")
    final_df.to_csv(output_dir + f"/tripadvisor_mbs_review_page1_{total_pages}.csv", header=True, index=False)
    print(f"Completed after {time.time() - start}s")
