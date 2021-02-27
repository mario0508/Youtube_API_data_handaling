from api_key import key1,key2,key3,key4,key5,key6,key7,key8,key9,key10,key12,key13,key14,key15,key16,key17
import json
import pandas as pd
import csv
import time
a=time.localtime()
yy=str(a[0])[:2]
mm=str(a[1])
dd=str(a[2])
if len(mm)<2:
    mm='0'+mm
if len(dd)<2:
    dd='0'+dd
date=mm+dd+yy

def main_filter(filename):
    with open(('Unfiltered_csv/{}.csv'.format(filename)), 'r')as f:
        fieldnames = ['index', 'channel title', 'subscriber count', 'video count', 'video id', 'original post',
                      'upload timestamp', 'category',
                      'view count', 'video likes', 'video dislikes', 'comment count',
                      'comment author id', 'comment author name', 'comment author img',
                      'comment like count', 'comment dislike count', 'comment timestamp',
                      'comment text', 'reply count', 'reply author id', 'reply author name',
                      'reply author img', 'reply like count', 'reply dislike count',
                      'reply timestamp', 'reply text']
    r = csv.DictReader(f, fieldnames=fieldnames)
    print(r)
    k = 0
    for i in r:
        print(i)
        k +=1
        if k == 2:
            print(i['channel title'])
            l=i['channel title']
            break
    l=l.split()
    if len(l)<2:
        l.append('influencer')
    with open('{}.csv'.format(filename), 'r', newline='', encoding="UTF-") as infile, open('Filtered_csv/YouTube.API.v1.{}.{}.{}.csv'.format(l[1],l[0],date), 'w', newline='',
                                                                                     encoding="UTF-8 ") as outfile:
        fieldnames = ['index', 'channel title', 'subscriber count', 'video count', 'video id', 'original post',
                      'upload timestamp', 'category',
                      'view count', 'video likes', 'video dislikes', 'comment count',
                      'comment author id', 'comment author name', 'comment author img',
                      'comment like count', 'comment dislike count', 'comment timestamp',
                      'comment text', 'reply count', 'reply author id', 'reply author name',
                      'reply author img', 'reply like count', 'reply dislike count',
                      'reply timestamp', 'reply text']
        writer = csv.DictWriter(outfile,fieldnames)
        b = True

        for row in csv.DictReader((l.replace('\0', '') for l in infile),fieldnames):
            print(row)
            if b:
                b = False
                writer.writerow(row)
                continue
            for j in fieldnames:
                l = []
                print(row[j])
                if len(row[j])>255:
                    row[j]=row[j][:255]
                for i in row[j]:
                    if ord(i) <= 31:
                        l.append(' ')
                    elif ord(i) >126:
                        l.append(chr("~"))
                    else:
                        l.append(i)
                row[j] = "".join(l)
            writer.writerow(row)
            print(row)

l_k = [key1,key2,key3,key4,key5,key6,key7,key8,key9,key10,key12,key13,key14,key15,key16,key17]
l_k.reverse()
category={'2' : 'Autos & Vehicles',
'1' :  'Film & Animation',
'10' : 'Music',
'15' : 'Pets & Animals',
'17' : 'Sports',
'18' : 'Short Movies',
'19' : 'Travel & Events',
'20' : 'Gaming',
'21' : 'Videoblogging',
'22' : 'People & Blogs',
'23' : 'Comedy',
'24' : 'Entertainment',
'25' : 'News & Politics',
'26' : 'Howto & Style',
'27' : 'Education',
'28' : 'Science & Technology',
'29' : 'Nonprofits & Activism',
'30' : 'Movies',
'31' : 'Anime/Animation',
'32' : 'Action/Adventure',
'33' : 'Classics',
'34' : 'Comedy',
'35' : 'Documentary',
'36' : 'Drama',
'37' : 'Family',
'38' : 'Foreign',
'39' : 'Horror',
'40' : 'Sci:Fi/Fantasy',
'41' : 'Thriller',
'42' : 'Shorts',
'43' : 'Shows',
'44': 'Trailers'}

def beta_filter(s):
    s=str(s)
    if "\n"  or "'"  or "'"  or "\t" or ";" or "," in s:
        s=s.replace("'","")
        s=s.replace('"',"")
        s=s.replace("\n","")
        s = s.replace(",", "")
        s = s.replace("\t", "")
        s = s.replace(",", "")
        s = s.replace(";", "")
        return s
    else:
        return s

k_call = 0
global key
def key_selector():
    global k_call
    if k_call<6:
        key = l_k[k_call]
        k_call += 1
        return key
    else:
        k_call=k_call%6
        key = l_k[k_call]
        k_call += 1
        return key




def v_info_finder(channelid,pagewrite,rec):
    z=0
    import os
    import csv
    import googleapiclient.discovery
    import googleapiclient.errors
    with open(('Unfiltered_csv/' + pagewrite + '.csv'), 'a', newline='', encoding='utf-8') as f:
        fieldnames = ['index', 'channel title', 'subscriber count', 'video count', 'video id', 'original post',
                      'upload timestamp', 'category',
                      'view count', 'video likes', 'video dislikes', 'comment count',
                      'comment author id', 'comment author name', 'comment author img',
                      'comment like count', 'comment dislike count',
                      'comment timestamp', 'comment text', 'reply count',
                      'reply author id', 'reply author name', 'reply author img',
                      'reply like count', 'reply dislike count', 'reply timestamp',
                      'reply text']
        wt = csv.DictWriter(f, fieldnames=fieldnames)
        wt.writerow({'index':'index','channel title':'channel title', 'subscriber count': 'subscriber count', 'video count': 'video count', 'video id': 'video id','original post':'original post','upload timestamp':'upload timestamp','category':'category','view count':'view count', 'video likes': 'video likes', 'video dislikes': 'video dislikes', 'comment count': 'comment count','comment author id':'comment author id', 'comment author name': 'comment author name', 'comment author img': 'comment author img','comment like count':'comment like count', 'comment dislike count': 'comment dislike count','comment timestamp':'comment timestamp', 'comment text': 'comment text', 'reply count': 'reply count','reply author id':'reply author id', 'reply author name': 'reply author name', 'reply author img': 'reply author img','reply like count':'reply like count', 'reply dislike count': 'reply dislike count', 'reply timestamp': 'reply timestamp','reply text':'reply text',})


    key = key_selector()

    scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

    def channel_details(id):
        # Disable OAuthlib's HTTPS verification when running locally.
        # *DO NOT* leave this option enabled in production.
        api_service_name = "youtube"
        api_version = "v3"
        youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=key)
        # print(youtube)

        request = youtube.channels().list(
            part="snippet,statistics,contentDetails",
            id=id
        )
        response = request.execute()
        # print(response)
        with open('Dump/Youtube_scraping_channel_json.txt','a') as outfile:
            json.dump(response,outfile)
        with open('Dump/'+pagewrite+' json.txt','a') as outfile:
            json.dump(response,outfile)
        return response

    def playlist_details(id, token):
        # Disable OAuthlib's HTTPS verification when running locally.
        # *DO NOT* leave this option enabled in production.

        api_service_name = "youtube"
        api_version = "v3"

        # Get credentials and create an API client
        youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=key)
        if token == '':
            request = youtube.playlistItems().list(
                part="contentDetails",
                maxResults=50,
                playlistId=id
            )
        else:
            request = youtube.playlistItems().list(
                part="contentDetails",
                maxResults=50,
                playlistId=id,
                pageToken=token
            )
        response = request.execute()
        # print('1',response)
        with open('Dump/Youtube_scraping_channel_json.txt', 'a') as outfile:
            json.dump(response, outfile)
        with open('Dump/' + pagewrite + ' json.txt', 'a') as outfile:
            json.dump(response, outfile)
        return response

    def video_stats(id):
        # Disable OAuthlib's HTTPS verification when running locally.
        # *DO NOT* leave this option enabled in production.

        api_service_name = "youtube"
        api_version = "v3"
        youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=key)
        request = youtube.videos().list(
            part="snippet,statistics",
            id=id
        )
        response = request.execute()
        with open('Dump/Youtube_scraping_channel_json.txt', 'a') as outfile:
            json.dump(response, outfile)
        with open('Dump/' + pagewrite + ' json.txt', 'a') as outfile:
            json.dump(response, outfile)
        return response

    def video_comments(id, token):
        # Disable OAuthlib's HTTPS verification when running locally.
        # *DO NOT* leave this option enabled in production.
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

        api_service_name = "youtube"
        api_version = "v3"

        youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=key)
        if token == '':
            request = youtube.commentThreads().list(
                part="snippet,replies",
                maxResults=50,
                videoId=id
            )
        else:

            request = youtube.commentThreads().list(
                part="snippet,replies",
                maxResults=50,
                videoId=id,
                pageToken=token

            )

        response = request.execute()

        with open('Dump/Youtube_scraping_channel_json.txt', 'a') as outfile:
            json.dump(response, outfile)
        with open('Dump/' + pagewrite + ' json.txt', 'a') as outfile:
            json.dump(response, outfile)
        return response

    if __name__ == "__main__":
        channel_details_var = channel_details(channelid)

        c_title = channel_details_var['items'][0]['snippet']['title']
        c_stats = channel_details_var['items'][0]['statistics']

        c_uploadid = channel_details_var['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        token = ''

        v_details = playlist_details(channel_details_var['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                                     token)

        try:
            j = 0
            """only remove i from while"""
            while True :

                if j == 0:
                    token = v_details['nextPageToken']

                k = playlist_details(channel_details_var['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                                     token)
                v_details['items'] = v_details['items'] + k['items']
                token = k['nextPageToken']
                j += 1

        except:
            print("video details exception")
        k=0
        if k or k==0:
            for i in range(len(v_details['items'])):
                if k or k==0:
                    k += 1
                    v_info = v_details['items'][i]['contentDetails']['videoId']
                    original_post='www.youtube.com/watch?v='+v_info
                    v_stats = video_stats(v_info)['items'][0]
                    v_category=category[v_stats['snippet']['categoryId']]
                    print(v_category)


                    v_publish=v_stats['snippet']['publishedAt']
                    print(v_publish)
                    v_stats = v_stats['statistics']
                    try:
                        print(v_stats['viewCount'],
                                     v_stats['likeCount'], v_stats['dislikeCount'], v_stats['commentCount'],)
                    except:
                        v_stats['viewCount']=-1
                        v_stats['likeCount']=-1
                        v_stats['dislikeCount']=-1
                        v_stats['commentCount']=-1

                    token = ''
                    try:
                        p = video_comments(v_info, token)
                    except:
                        p={'items':[]}
                    v_comments_detail = p['items']
                    j = 0

                    try:
                        """remove i from loop and if"""
                        while True and j < 20:

                            if p['nextPageToken']:
                                token = p['nextPageToken']
                                try:
                                    p = video_comments(v_info, token)
                                except:
                                    p={'items':[]}
                                v_comments_detail = v_comments_detail + p['items']
                                j += 1
                                """i limiter"""
                            else:
                                print("no next page token for comments")
                    except:
                        print("comment exception")

                    for i in v_comments_detail:
                        comment = i['snippet']['topLevelComment']['snippet']['textOriginal']
                        # print(comment)
                        try:
                            comment_author_channel_id = i['snippet']['topLevelComment']['snippet']['authorChannelId'][
                                'value']
                        except:
                            comment_author_channel_id = 'NULL'
                        comment_author_name = i['snippet']['topLevelComment']['snippet']['authorDisplayName']
                        comment_author_img = i['snippet']['topLevelComment']['snippet']["authorProfileImageUrl"]
                        comment_timestamp = i['snippet']['topLevelComment']['snippet']['publishedAt']
                        try:
                            comment_like_count = i['snippet']['topLevelComment']['snippet']['likeCount']
                            # print(comment_like_count)
                        except:
                            comment_like_count = 0
                            pass
                        try:
                            comment_dislike_count = i['snippet']['topLevelComment']['snippet']['dislikeCount']

                        except:
                            comment_dislike_count = 0
                            pass
                        try:
                            comment_replies_count = i['snippet']['totalReplyCount']


                        except:
                            comment_replies_count = 0
                            pass
                        if comment_replies_count != 0:
                            print(i)
                            print(range(len(i['replies']['comments'])))
                            for j in range(len(i['replies']['comments'])):
                                reply_comment = i['replies']
                                reply_comment = reply_comment['comments']
                                reply_comment = reply_comment[j]
                                reply_comment = reply_comment['snippet']
                                reply_comment = reply_comment['textOriginal']
                                reply_comment_author_channel_id = i['replies']['comments'][j]['snippet']['authorChannelId']['value']
                                reply_comment_author_name = i['replies']['comments'][j]['snippet']['authorDisplayName']
                                reply_comment_img = i['replies']['comments'][j]['snippet']["authorProfileImageUrl"]
                                reply_comment_timestamp = i['replies']['comments'][j]['snippet']['publishedAt']

                                try:
                                    reply_comment_like_count = i['replies']['comments'][j]['snippet']['likeCount']

                                except:
                                    reply_comment_like_count = 0
                                    pass
                                try:
                                    reply_comment_dislike_count = i['replies']['comments'][j]['snippet']['dislikeCount']
                                except:
                                    reply_comment_dislike_count = 0
                                    pass
                                if z < rec:
                                    print("x0")
                                    with open(('Unfiltered_csv/'+pagewrite + '.csv'), 'a', newline='',encoding='utf-8') as f:
                                        fieldnames = ['index','channel title', 'subscriber count', 'video count', 'video id','original post','upload timestamp','category',
                                                      'view count', 'video likes', 'video dislikes', 'comment count',
                                                      'comment author id', 'comment author name', 'comment author img',
                                                      'comment like count', 'comment dislike count',
                                                      'comment timestamp', 'comment text', 'reply count',
                                                      'reply author id', 'reply author name', 'reply author img',
                                                      'reply like count', 'reply dislike count', 'reply timestamp',
                                                      'reply text']
                                        wt = csv.DictWriter(f, fieldnames=fieldnames)
                                        wt.writerow({'index':z+1,'channel title':beta_filter(c_title), 'subscriber count':c_stats['subscriberCount'], 'video count':c_stats['videoCount'], 'video id':v_info,'original post':original_post,'upload timestamp':v_publish, 'category':v_category ,'view count':v_stats['viewCount'], 'video likes':v_stats['likeCount'],'video dislikes': v_stats['dislikeCount'],'comment count': v_stats['commentCount'], 'comment author id':comment_author_channel_id, 'comment author name':beta_filter(comment_author_name), 'comment author img':comment_author_img, 'comment like count':comment_like_count, 'comment dislike count':comment_dislike_count, 'comment timestamp':beta_filter(comment_timestamp), 'comment text':beta_filter(comment), 'reply count':comment_replies_count, 'reply author id':reply_comment_author_channel_id, 'reply author name':beta_filter(reply_comment_author_name), 'reply author img':reply_comment_img, 'reply like count':beta_filter(reply_comment_like_count), 'reply dislike count':beta_filter(reply_comment_dislike_count), 'reply timestamp':beta_filter(reply_comment_timestamp), 'reply text':beta_filter(reply_comment)})
                                        print("row print",z)
                                        z += 1
                                    print("x0")
                                else:

                                    return 0

                        else:
                            reply_comment = 'NULL'
                            reply_comment_author_channel_id = 'NULL'
                            reply_comment_author_name = 'NULL'
                            reply_comment_img = 'NULL'
                            reply_comment_timestamp = 'NULL'
                            reply_comment_like_count = 'NULL'
                            reply_comment_dislike_count = 'NULL'

                            if z < rec:
                                print("x1")
                                with open(('Unfiltered_csv/'+pagewrite + '.csv'), 'a', newline='',encoding='utf-8') as f:

                                    fieldnames = ['index','channel title','subscriber count','video count','video id','original post','upload timestamp','category','view count','video likes','video dislikes','comment count','comment author id','comment author name','comment author img','comment like count','comment dislike count','comment timestamp','comment text','reply count','reply author id','reply author name','reply author img','reply like count', 'reply dislike count','reply timestamp','reply text']
                                    wt = csv.DictWriter(f, fieldnames=fieldnames)
                                    wt.writerow({'index':z+1,'channel title':beta_filter(c_title), 'subscriber count':c_stats['subscriberCount'], 'video count':c_stats['videoCount'], 'video id':v_info,'original post':original_post,'upload timestamp':v_publish,'category':v_category ,  'view count':v_stats['viewCount'], 'video likes':v_stats['likeCount'],'video dislikes': v_stats['dislikeCount'],'comment count': v_stats['commentCount'], 'comment author id':comment_author_channel_id, 'comment author name':beta_filter(comment_author_name), 'comment author img':comment_author_img, 'comment like count':comment_like_count, 'comment dislike count':comment_dislike_count, 'comment timestamp':beta_filter(comment_timestamp), 'comment text':beta_filter(comment), 'reply count':comment_replies_count, 'reply author id':reply_comment_author_channel_id, 'reply author name':beta_filter(reply_comment_author_name), 'reply author img':reply_comment_img, 'reply like count':beta_filter(reply_comment_like_count), 'reply dislike count':beta_filter(reply_comment_dislike_count), 'reply timestamp':beta_filter(reply_comment_timestamp), 'reply text':beta_filter(reply_comment)})
                                    print("row print", z)
                                    z += 1
                                print("x1")
                            else:
                                return 0


                    else:
                        comment = 'NULL'
                        comment_author_channel_id = 'NULL'
                        comment_author_name = 'NULL'
                        comment_author_img = 'NULL'
                        comment_timestamp = 'NULL'
                        comment_like_count = 'NULL'
                        comment_dislike_count = 'NULL'
                        comment_replies_count = 'NULL'
                        reply_comment = 'NULL'
                        reply_comment_author_channel_id = 'NULL'
                        reply_comment_author_name = 'NULL'
                        reply_comment_img = 'NULL'
                        reply_comment_timestamp = 'NULL'
                        reply_comment_like_count = 'NULL'
                        reply_comment_dislike_count = 'NULL'
                        if z < rec:
                            print("x2")
                            with open(('Unfiltered_csv/'+pagewrite + '.csv'), 'a', newline='',encoding='utf-8') as f:
                                fieldnames = ['index','channel title', 'subscriber count', 'video count', 'video id','original post','upload timestamp','category',
                                              'view count', 'video likes', 'video dislikes', 'comment count',
                                              'comment author id', 'comment author name', 'comment author img',
                                              'comment like count', 'comment dislike count', 'comment timestamp',
                                              'comment text', 'reply count', 'reply author id', 'reply author name',
                                              'reply author img', 'reply like count', 'reply dislike count',
                                              'reply timestamp', 'reply text']
                                wt = csv.DictWriter(f, fieldnames=fieldnames)
                                wt.writerow({'index':z+1,'channel title':beta_filter(c_title), 'subscriber count':c_stats['subscriberCount'], 'video count':c_stats['videoCount'], 'video id':v_info,'original post':original_post,'upload timestamp':v_publish,'category':v_category ,  'view count':v_stats['viewCount'], 'video likes':v_stats['likeCount'],'video dislikes': v_stats['dislikeCount'],'comment count': v_stats['commentCount'], 'comment author id':comment_author_channel_id, 'comment author name':beta_filter(comment_author_name), 'comment author img':comment_author_img, 'comment like count':comment_like_count, 'comment dislike count':comment_dislike_count, 'comment timestamp':beta_filter(comment_timestamp), 'comment text':beta_filter(comment), 'reply count':comment_replies_count, 'reply author id':reply_comment_author_channel_id, 'reply author name':beta_filter(reply_comment_author_name), 'reply author img':reply_comment_img, 'reply like count':beta_filter(reply_comment_like_count), 'reply dislike count':beta_filter(reply_comment_dislike_count), 'reply timestamp':beta_filter(reply_comment_timestamp), 'reply text':beta_filter(reply_comment)})
                                print("x2")
                                print("row print", z)
                                z += 1
                        else:
                            return 0
        else:
            pass


rec = int(input("Please enter Number of records that you want to mine per influencer :-"))
a = input("Please enter Channel ids of the influencer(use space as separator between ids :- ").split()

for i in a:
    page = i
    try:
        v_info_finder(i, page, rec)
        if rec > 40000:
            key = key_selector()
            print("next")

    except Exception as e:
        print("error", e)
        print("error fetching ", i)
        e = str(e)
        if 'quota' in e or 'HttpError' in e or 'Network is unreachable' in e:
            try:
                key = key_selector()
                print("re-run")
                print(key)
                v_info_finder(i, page, rec)
            except Exception as e:
                print("error", e)
                print("error fetching ", i)
                e = str(e)
                if 'quota' in e or 'HttpError' in e or 'Network is unreachable' in e:
                    key = key_selector()
                    print(key)
                    print("re-run2")
                    v_info_finder(i, page, rec)
            main_filter(i)


