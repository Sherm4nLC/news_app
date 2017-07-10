from urllib2 import urlopen, build_opener
import re
from bs4 import BeautifulSoup as bs
import pandas as pd
from datetime import datetime as dt
import time

#  opener = urllib2.build_opener()
# url = 'http://www.coto.com.ar/hoy/'
list_url = ['http://www.clarin.com/','https://www.pagina12.com.ar/','http://chequeado.com/','http://www.eldestapeweb.com','http://www.enorsai.com.ar/','http://www.minutouno.com','http://www.infonews.com/','http://elpais.com/']


def ws(list_url):

    initial_time = dt.now()
    print "staring at: ",initial_time
    # df = pd.DataFrame({"tags":["0"],"url":["0"],"date":["0"]})
    df = pd.read_csv("file_2.csv")
    df = df[["date","tags","url"]]
    df.drop_duplicates(subset=['tags'],inplace=True)

    initial_len = len(df)
    print "initial df: ",len(df)

    for i, url in enumerate(list_url):
        print "len of urls: ", len(list_url)
        if i > 10: break
        elif url.endswith("pdf"): continue
        print "processing: ",url

        ### first grab a and append

        #try:
        # site = urlopen(url)

        opener = build_opener()
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        try:
            site = opener.open(url)


        except:
            print "skipping", url
            continue
        data = site.read()
        soup = bs(data, 'html5lib')
        all_a = soup.find_all("a", {"href": True})

        # temp_a_list = []

        for a in all_a:
            # print a["href"]
            if a["href"].startswith("http"):
                list_url.append(a["href"])

        #temp_a_list = list(set(temp_a_list)) # get uniques
        #list_url = list_url + temp_a_list # supposedly here I have added a to my list for later iterations

        #  now I work on h

        all_h = soup.find_all(['h5','h4','h3','h2','h1','p'])
        # print type(str(all_h[0]))
        # print str(all_h[0])
        all_h = [re.sub('<[^<]+?>', '', unicode(x)).strip() for x in all_h]
        df_temp = pd.DataFrame({"tags":all_h})
        # print df_temp.head()
        df_temp['url'] = url
        df_temp['date'] = dt.now()

        print "lines harvested: ",len(df_temp)
        df = df.append(df_temp)

        # print "done with: ",url

    print "Total lines harvested: ", len(df)
    print df.columns

    # deduplicating
    # df.sort(['A', 'B'], ascending=[1, 0])
    df["date"] = df["date"].astype(str)
    # df.sort(['date'],ascending=True,inplace=True)
    df.sort_values(by='date',ascending=True,inplace=True)

    print df.head()
    df.drop_duplicates("tags", inplace=True)
    print len(df.tags.unique())




    df.to_csv("file_2.csv",index=False,encoding='utf-8')

    df = pd.read_csv("file_2.csv")
    df = df[["date","tags","url"]]
    df.drop_duplicates(subset=['tags'],inplace=True)

    df.to_csv("file_2.csv",index=False,encoding='utf-8')

    print df.tail()

    done_len = len(df)
    done_time = dt.now()
    print "after df: ", done_len
    print "entries added: ", done_len - initial_len
    print "in time: ", done_time - initial_time

    print "Done."
    time.sleep(5)

ws(list_url)



# msg = raw_input(" ")
