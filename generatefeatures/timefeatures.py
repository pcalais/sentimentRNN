from collections import defaultdict
import gzip
import json as simplejson
import numpy as np
import datetime
import utils
import operator


outputDir = 'trainingdata/'

def dict_int():
    return defaultdict(int)

def dict_list():
    return defaultdict(list)

##################################
def stats(side, tweets, authors, rt_reaction_times, num_replies, totalTweet_per_tracked_rts):
    count = len(tweets)
    count_unique_authors = len(set(authors))
    count_rt = 0
    count_hashtags = 0
    count_urls = 0
    len_char = 0
    len_words = 0

    truncated_rts = [min(int(rt), 600) for rt in rt_reaction_times]
    if len(truncated_rts) == 0:
        most_common_rt_reaction_time = 0
    else:
        most_common_rt_reaction_time = max(set(truncated_rts), key=truncated_rts.count)

    for tweet in tweets:
        len_char += len(tweet)
        len_words += len(tweet.split())
        if "RT @" in tweet:
            count_rt += 1
        if "#" in tweet:
            count_hashtags += 1
        if "http" in tweet:
            count_urls += 1

    if len(totalTweet_per_tracked_rts) > 0:
        avgRtTotalTweets = np.sum(totalTweet_per_tracked_rts) / float(len(totalTweet_per_tracked_rts))
        stdRtTotalTweets = np.std(totalTweet_per_tracked_rts)
    else:
        avgRtTotalTweets = len(tweets)
        stdRtTotalTweets = len(tweets)

    # print(sorted(truncated_rts))
    # print(most_common_rt_reaction_time)
    # print("#################\n")

    print("side " + side + "\t" + str(len(tweets)))

    cols = ["totalTweets",
            "avgRtTotalTweets",
            "deltaAvgRtTotalTweetsAndTotalTweets",
            "stdRtTotalTweets",
            "mostCommonRtReactionTime",
            "totalRts",
            "pct_RTs",
            "stdRTReactionTime",
            "meanRTReactionTime",
            "medianRTReactionTime",
            "pctReplies",
            "tweetsPerAuthor",
            "charsPerTweet",
            "wordsPerTweet",
            "hashtagsPerTweet",
            "urlsPerTweet",
            "numDistinctRtedMsgs",
            "deltaRts"]

    s = [
        len(tweets),
        avgRtTotalTweets,
        avgRtTotalTweets - len(tweets),
        stdRtTotalTweets,
        most_common_rt_reaction_time,
        count_rt,
        count_rt / float(count),
        np.std(rt_reaction_times),
        np.mean(rt_reaction_times),
        np.median(truncated_rts),
        num_replies / float(count),
        count / float(count_unique_authors),
        len_char / float(count),
        len_words / float(count),
        count_hashtags / float(count),
        count_urls / float(count)
    ]
    return s, cols


##################################

inputFileName = "/home/pedro/PESQUISA/tweets_processados/tweets_processados_cruzeiro_flamengo.csv.gz"
inputFileName = "/home/pedro/PESQUISA/tweets_processados/tweets_processados_palmeiras_corinthians.csv.gz"
inputFileName = "/home/pedro/PESQUISA/tweets_processados/tweets_processados.txt.gz"
dic_bias = utils.loadBias("/var/tmp/FUTEBOL_BR/BIPARTITE/bias.txt")
outputRtFile = "rt.csv"

print("Loaded bias of " + str(len(dic_bias)) + " users.")

maxCount = 100 * 1000 * 1000  # 6 * 1000 * 1000
count = 0

tweets_per_timestamp = defaultdict(dict_int)
metadata_per_timestamp = defaultdict(dict_int)
dic_num_tracked_rts_per_timestamp = defaultdict(dict_int)
dic_totalTweet_per_tracked_rts = defaultdict(dict_list)

current_tweets = defaultdict(list)
current_authors = defaultdict(list)
current_rt_reaction_times = defaultdict(list)
current_num_replies = 0

dic_current_tweets = defaultdict(list)
dic_current_rts = defaultdict(dict_int)
dic_current_rt_reaction_times = defaultdict(list)

ff_output_rt_file = open(outputRtFile, "w")

datetimestr_anterior = ""
with gzip.open(inputFileName, 'r') as fin:
    for line in fin:

        count += 1
        # if count % 1000000 == 0:
        #    print(str(count/1000000)+ "M")

        if count > maxCount and maxCount != -1:
            break

            # try:
        tweet = simplejson.loads(line.decode('utf-8'))
        text = tweet['text']
        author = tweet['author']
        topicsAndEntities = tweet['topics'] if 'topics' in tweet else ['TOPIC', 'NO_ENTITY']

        if topicsAndEntities[0][0] != 'FUTEBOL_BR':
            continue

        entities = topicsAndEntities[0][1]
        if 'CORINTHIANS' not in entities and 'PALMEIRAS' not in entities and \
                'FLAMENGO' not in entities and 'CRUZEIRO' not in entities:
            continue

        # if 'CRUZEIRO' not in topicsAndEntities[0][1]:
        #   continue

        is_reply = tweet['is_reply_button']
        if is_reply:
            current_num_replies += 1

        # print(topicsAndEntities)
        side_author = dic_bias[author] if author in dic_bias else "NULL"

        # Sun May 07 04:51:56 +0000 2017
        (dt_object, datetimestr) = utils.dateTimeToMinuteStr(tweet['datetime'])
        if datetimestr_anterior > datetimestr:
            datetimestr = datetimestr_anterior
        tweets_per_timestamp['ALL'][datetimestr] += 1
        tweets_per_timestamp[side_author][datetimestr] += 1

        # print(" HA " + dt)

        current_tweets['ALL'].append(text)
        current_tweets[side_author].append(text)
        current_authors['ALL'].append(author)
        current_authors[side_author].append(text)

        for entity in topicsAndEntities[0][1]:
            dic_current_tweets[entity].append(text)

        if 'retweet_reaction_time_sec' in tweet:
            retweetReactionTimeSec = tweet['retweet_reaction_time_sec']
            dic_current_rts[datetimestr][tweet['retweeted_msg_id']] += 1
            current_rt_reaction_times['ALL'].append(float(retweetReactionTimeSec) / 60)
            current_rt_reaction_times[side_author].append(float(retweetReactionTimeSec) / 60)
            for entity in topicsAndEntities[0][1]:
                dic_current_rt_reaction_times[entity].append(retweetReactionTimeSec / 60)
                dt_rt_object = dt_object - datetime.timedelta(seconds=retweetReactionTimeSec)
                rtdatetimestr = dt_rt_object.strftime('%Y_%m_%d_%H:%M')
                if rtdatetimestr in metadata_per_timestamp['ALL']:
                    # print("RT foi em " + rtdatetimestr + " com stats " + str(metadata_per_timestamp[rtdatetimestr]))
                    dic_num_tracked_rts_per_timestamp['ALL'][datetimestr] += 1
                    dic_totalTweet_per_tracked_rts['ALL'][datetimestr].append(
                        tweets_per_timestamp['ALL'][rtdatetimestr])

                if rtdatetimestr in metadata_per_timestamp[side_author]:
                    dic_num_tracked_rts_per_timestamp[side_author][datetimestr] += 1
                    dic_totalTweet_per_tracked_rts[side_author][datetimestr].append(
                        tweets_per_timestamp[side_author][rtdatetimestr])

        if datetimestr != datetimestr_anterior:
            print("Finished processing " + datetimestr)

            for side in current_tweets:
                s, cols = stats(side, current_tweets[side], current_authors[side], current_rt_reaction_times[side],
                                current_num_replies, dic_totalTweet_per_tracked_rts[side][datetimestr_anterior])
                # print(dt + "\t" + datetimestr) #+ "\t" +str(s) + "\t" + str(current_tweets))

                if datetimestr_anterior in metadata_per_timestamp[side]:
                    raise Exception("Fatal error: timestamp anterior " + datetimestr_anterior + " already exists")
                metadata_per_timestamp[side][datetimestr_anterior] = s

            current_tweets = defaultdict(list)
            current_authors = defaultdict(list)
            current_rt_reaction_times = defaultdict(list)
            current_num_replies = 0

        datetimestr_anterior = datetimestr

###################################
cols = ["totalTweets", "avgRtTotalTweets", "deltaAvgRtTotalTweetsAndTotalTweets", "stdRtTotalTweets",
        "mostCommonRtReactionTime", "totalRts", "pct_RTs", "stdRTReactionTime", "meanRTReactionTime",
        "medianRTReactionTime", "pctReplies", "tweetsPerAuthor", "charsPerTweet", "wordsPerTweet", "hashtagsPerTweet",
        "urlsPerTweet", "numDistinctRtedMsgs", "deltaRts"]
for side in ['ALL', 'PALMEIRAS', 'CORINTHIANS', 'CRUZEIRO', 'FLAMENGO']:
    with open("trainingdata/time_" + side + ".csv", 'w') as ff:
        ff.write("timeCount,timestamp,")
        cols_side = list()
        for col in cols:
            cols_side.append(col)
        ff.write(','.join(cols_side))
        ff.write("\n")

        timeUnityAnterior = ""
        timeCount = 0
        sAnterior = 0
        print("side " + side + " has " + str(len(tweets_per_timestamp[side])) + " timestamps")
        #  always iterate through 'ALL', which does not have missing datapoints.
        for timeUnity in sorted(tweets_per_timestamp['ALL']):
            print("TIME UNITY " + side + " " + timeUnity)
            totalTweets = tweets_per_timestamp[side][timeUnity]
            print(totalTweets)
            # if totalTweets < 10:
            #   print("skipping interval " + str(timeCount) + " due only " + str(totalTweets) + " tweets.")
            #   timeCount += 1
            #   continue

            s = metadata_per_timestamp[side][timeUnity]
            # if empty datapoint, take the last one.
            if s == 0:
                s = sAnterior
            if s != 0 and sAnterior != 0:
                for i in range(len(s)):
                    if i == 0:
                        print("s[i] = " + str(s[i]))
                    s[i] = 0.87 * sAnterior[i] + 0.13 * s[i]
                    if i == 0:
                        print("smoothed is " + str(s[i]))

            if timeUnityAnterior == "":
                timeUnityAnterior = timeUnity

            print("s: ")
            print(s)

            deltaRts = utils.computeDelta(dic_current_rts[timeUnityAnterior], dic_current_rts[timeUnity])
            num_distinct_rted_msgs = len(dic_current_rts[timeUnity].items())
            ff.write(str(timeCount) + "," + timeUnity)
            ff.write(',' + ','.join(map(str, s)))
            ff.write(',' + str(num_distinct_rted_msgs) + "," + str(deltaRts))

            ff_output_rt_file.write(str(timeCount) + "\t" + str(timeUnity) + "\t")
            for key, value in sorted(dic_current_rts[timeUnity].items(), key=operator.itemgetter(1), reverse=True):
                ff_output_rt_file.write("\t" + key + "\t" + str(value))
                ff_output_rt_file.write("\n")

            ff.write("\n")
            timeCount += 1
            timeUnityAnterior = timeUnity
            sAnterior = s

print("FINISHED. Processed " + str(len(tweets_per_timestamp)) + " timestamps.")
ff_output_rt_file.close()


