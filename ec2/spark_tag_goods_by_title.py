import ujson
import re
from user_action_help_function import fenci, tagsMatchGoodTitle, pipTag, fenciBatch, uniform, string2List, simpleMatchGoodTitle
import subprocess
from pyspark import SparkContext
from pyspark.sql import SQLContext
import sys
from datetime import date, timedelta
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import ssl

SEARCH_KEY_PATTERN  = re.compile('\"SearchKey\":\"([^\"]+)\"')
KEY_WORDS_PATTERN  = re.compile('\"keyword\":\"([^\"]+)\"')

sc = SparkContext(appName="user_search_profile & goods_title")

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIAJBXQCJWUTBMPDUDA")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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")


if hasattr(ssl, '_create_unverified_context'):
   ssl._create_default_https_context = ssl._create_unverified_context

s3_conn = S3Connection('AKIAJBXQCJWUTBMPDUDA', 'dVyOIHJce0vUvabFRuWRpITsf9Xhz9Z7vh4OLgQu')
b = s3_conn.get_bucket('xhs.emr.bucket')


sql_context = SQLContext(sc)

sc.setLogLevel('WARN')

tag_name = sc.textFile('s3n://xhs.emr.bucket/user_discovery_actions/tag_name/')
tagNameHelper = tag_name.map(lambda line: line.split("|")).filter(lambda x : len(x) == 3).map(lambda x : (uniform(x[1]), [(x[0], x[1], x[2])]))\
    .reduceByKey(lambda x, y : x+y).filter(lambda x : not x[0].isdigit() and not len(x[0])==1).map(lambda x : (x[0], pipTag(x[1]))).collectAsMap()
broadcastTagNameHelper = sc.broadcast(tagNameHelper)

tagNameMatchInTitle = tag_name.map(lambda line: line.split("|")).filter(lambda x : len(x) == 3).map(lambda x : (uniform(x[1]), [(x[0], x[1], x[2])]))\
    .reduceByKey(lambda x, y : x+y).filter(lambda x : not x[0].isdigit() and not len(x[0])==1).map(lambda x : (x[0], pipTag(x[1]))).filter(lambda x: x[1][0][2] != "common_tag").collectAsMap()
broadcastCoreTagHelper = sc.broadcast(tagNameMatchInTitle)


def tag_goods(datestring):
    ### start combine with DiscoveryTagRdd
    # Goods_Tags     = sc.textFile("s3n://xhs.emr.bucket/user_goods_actions/dw_goods_tags/")
    # #
    # TagedGoodsIds = set(Goods_Tags.map(lambda line: line.split("|")).map(lambda line: line[0]).collect())
    # broadcastTagedIds = sc.broadcast(TagedGoodsIds)
    #
    goods_title = sc.textFile('s3n://xhs.emr.bucket/user_discovery_actions/goods_title/')
    goodsTitleNeededMatch = goods_title.map(lambda line: line.split("|")).filter(lambda x: x[1]).map(lambda x: (x[0], x[1], string2List(uniform(x[1]))))
    # goodsTitleNeededMatch = goods_title.map(lambda line: line.split("|")).filter(lambda line: line[0] not in broadcastTagedIds.value).filter(lambda x: x[1]).map(lambda x: (x[0], x[1], string2List(uniform(x[1]))))
    goodsTitleRecords = goodsTitleNeededMatch.collect()
    #
    f = open("/root/title_sep_result.txt", "w")
    f.close()
    fenciBatch(goodsTitleRecords, task_id=1, tasks=128)
    subprocess.call(["s3cmd", "rm", "-rf", "s3://xhs.emr.bucket/user_discovery_actions/goods_title_sep/"])
    for x in range(5):
        if subprocess.call(["s3cmd", "put", "/root/title_sep_result.txt", "s3://xhs.emr.bucket/user_discovery_actions/goods_title_sep/%s/" % datestring]) == 0:
            print "successfully upload to S3"
            break
        print "Error, Try Again"

    goods_sep_title = sc.textFile("s3n://xhs.emr.bucket/user_discovery_actions/goods_title_sep/%s/" % datestring)
    goodsTitleTag = goods_sep_title.map(lambda line: line.split("|")).map(lambda line : (line[0], line[1].strip(), line[2].split(",")))\
        .map(lambda x : (x[0], x[1], list(set(simpleMatchGoodTitle(x[1], broadcastCoreTagHelper.value) + tagsMatchGoodTitle(x[2], broadcastTagNameHelper.value)))))

    Goods_Tags2 = goodsTitleTag.map(lambda x : (x[0], [(x[0] + '|' + ele[0] + '|' + ele[2]) for ele in x[2]])).flatMap(lambda x:x[1])
    #
    Goods_Tags     = sc.textFile("s3n://xhs.emr.bucket/user_goods_actions/dw_goods_tags/")
    GoodsIdTagTextRdd = Goods_Tags.map(lambda x : x.strip().split("|")).map(lambda x: (x[0], [x[-1]])).filter(lambda x: len(x[1]) > 0)
    GoodsIdTagTextFenciRdd = GoodsIdTagTextRdd.filter(lambda x : not x[1][0].isdigit() and not len(x[1][0])==1)\
        .map(lambda x: (x[0], x[1][0], string2List(uniform(x[1][0]))))
    GoodsIdTagText = GoodsIdTagTextFenciRdd.collect()
    f = open("/root/goods_tag_match.txt", "w")
    f.close()
    fenciBatch(GoodsIdTagText, task_id=3, tasks=128)
    subprocess.call(["s3cmd", "rm", "-rf", "s3://xhs.emr.bucket/user_goods_actions/dw_goods_tags_sep/"])
    subprocess.call(["s3cmd", "rm", "s3://xhs.emr.bucket/user_goods_actions/*folder*"])
    for x in range(5):
        if subprocess.call(["s3cmd", "put", "/root/goods_tag_match.txt", "s3://xhs.emr.bucket/user_goods_actions/dw_goods_tags_sep/"]) == 0:
            print "successfully upload to S3"
            break
        print "Error, Try Again"
    GoodsIdTagTextFile = sc.textFile("s3n://xhs.emr.bucket/user_goods_actions/dw_goods_tags_sep/")
    GoodsIdTagTextRdd = GoodsIdTagTextFile.map(lambda line : line.split("|")).map(lambda line : (line[0], [line[1].strip()], line[2].split(",")))
    GoodsIdTagTextRelatedTagRdd = GoodsIdTagTextRdd.map(lambda line : (line[0], tagsMatchGoodTitle(line[1], broadcastTagNameHelper.value) + tagsMatchGoodTitle(line[2], broadcastTagNameHelper.value)))\
        .filter(lambda line : line[1]).map(lambda line: [((line[0], ele[0], ele[1], ele[2]), 6.0/len(line[1])) for ele in line[1]]).flatMap(lambda x:x)  # result as (user_id, tag_id) - score
    Goods_Tags3 = GoodsIdTagTextRelatedTagRdd.reduceByKey(lambda x, y : x + y).map(lambda x: x[0][0] + "|" + x[0][1] + "|" + x[0][3])

    Goods_Tags3 = Goods_Tags.map(lambda x: x.split("|")).map(lambda x: "|".join(x[:-1])).union(Goods_Tags3).distinct()
    Goods_Tags2 = Goods_Tags3.union(Goods_Tags2).distinct()
    subprocess.call(["s3cmd", "rm", "-rf", "s3://xhs.emr.bucket/user_goods_actions/dw_goods_tags2/"])
    subprocess.call(["s3cmd", "rm", "s3://xhs.emr.bucket/user_goods_actions/*folder*"])
    Goods_Tags2.saveAsTextFile("s3n://xhs.emr.bucket/user_goods_actions/dw_goods_tags2/")





def DigUserSearchAction(datestring):
    RawLogs = sc.textFile("s3n://xhs.emr.bucket/%s/*/*" % datestring, 300)    #83204669
    SearchEventRdd = RawLogs.filter(lambda line: "Confirm_Search" in line).map(lambda line:ujson.loads(line)).filter(lambda line: "user_id" in line and "se_action" in line and "contexts" in line).filter(lambda line: line["user_id"] and line["se_action"] == "Confirm_Search")
    userSearchKeywordRdd = SearchEventRdd.map(lambda action: (action["user_id"], SEARCH_KEY_PATTERN.findall(action["contexts"])+ KEY_WORDS_PATTERN.findall(action["contexts"]))).filter(lambda x: len(x[1])>0)
    #
    userSearchKeyFenciRdd = userSearchKeywordRdd.filter(lambda x : not x[1][0].isdigit() and not len(x[1][0])==1)\
        .map(lambda x: (x[0], x[1][0], string2List(uniform(x[1][0]))))
    searchRecords = userSearchKeyFenciRdd.collect()
    f = open("/root/keywords_sep_result.txt", "w")
    f.close()
    fenciBatch(searchRecords)

    subprocess.call(["s3cmd", "rm", "-rf", "s3://xhs.emr.bucket/user_search_actions/%s/" % datestring])
    subprocess.call(["s3cmd", "rm", "s3://xhs.emr.bucket/user_search_actions/%s/*folder*" % datestring[:-3]])
    for x in range(5):
        if subprocess.call(["s3cmd", "put", "/root/keywords_sep_result.txt", "s3://xhs.emr.bucket/user_search_actions/%s/" % datestring]) == 0:
            print "successfully upload to S3"
            break
        print "Error, Try Again"


def userProfileFromSearch(datestring):
    SearchFile = sc.textFile("s3n://xhs.emr.bucket/user_search_actions/%s/" % datestring, 300)
    SearchKeyRdd = SearchFile.map(lambda line : line.split("|")).map(lambda line : (line[0], [line[1].strip()], line[2].split(",")))
    SearchRelatedTagRdd = SearchKeyRdd.map(lambda line : (line[0], tagsMatchGoodTitle(line[1], broadcastTagNameHelper.value) + tagsMatchGoodTitle(line[2], broadcastTagNameHelper.value)))\
        .filter(lambda line : line[1]).map(lambda line: [((line[0], ele[0], ele[1], ele[2]), 6.0/len(line[1])) for ele in line[1]]).flatMap(lambda x:x)  # result as (user_id, tag_id) - score
    userSearchProfile = SearchRelatedTagRdd.reduceByKey(lambda x, y : x + y).map(lambda line : (line[0][0], [(line[0][1], line[1],  line[0][2], line[0][3], "search")])).reduceByKey(lambda x, y : x+y)

    # import user_token - user_id information
    userTokenRdd = sc.textFile("s3n://xhs.emr.bucket/user_discovery_actions/user_id_token/")
    # format: usertoken - user_id
    userIdTokenDict = userTokenRdd.map(lambda x: x.split("|")).map(lambda x: (x[1], x[0]))
    # user token decrypt
    UserIdSearchProfile = userSearchProfile.join(userIdTokenDict).map(lambda x : x[1]).map(lambda x: (x[1], x[0]))

    tstmp = datestring.replace("/", "-")
    # format (user_id, tag_id, score, tag_type, source_tye, tstmp)
    userSearchProfileFlatRdd = UserIdSearchProfile.map(lambda x: [(x[0], ele[0], ele[1], ele[3], ele[4], tstmp) for ele in x[1]])\
        .flatMap(lambda x:x )         #.map(lambda line: "|".join(line))
    subprocess.call(["s3cmd", "rm", "-rf", "s3://xhs.emr.bucket/user_profile/pickle_pool/%s/search/" % datestring])
    subprocess.call(["s3cmd", "rm", "s3://xhs.emr.bucket/user_profile/pickle_pool/%s/*folder*" % datestring])
    userSearchProfileFlatRdd.saveAsPickleFile("s3n://xhs.emr.bucket/user_profile/pickle_pool/%s/search/" % datestring)




if __name__ == '__main__':
    DailyJob = 0
    is_reset = 0
    date_string = sys.argv[1]
    today = date.today()
    deltaday = timedelta(days=1)
    yesterday = today - deltaday
    if date_string == yesterday.strftime("%Y/%m/%d"):
        print "The Input Datestring is yesterday, We will update the MongoDb and merger weekterm profile"
        DailyJob = 1
    if len(sys.argv) > 2:
        is_reset = 1

    print date_string
    print "Finish initization... load tag_goods..."
    print "Start Stage01: tag_goods"
    print ">>>>>>>>>>>>>>>>"

    if DailyJob:
        tag_goods(date_string)
    else:
        print "pass"

    print "Start Stage02 DigUserSearchAction >>>>>>>>>>>"

    if not Key(bucket=b, name="/user_profile/pickle_pool/%s/search/_SUCCESS" % date_string).exists():
        DigUserSearchAction(date_string)
    else:
        print "the SearchUserProfile in DATE: %s is already exists, skip it without recomputing" % date_string

    print "Start Stage03 userProfileFromSearch >>>>>>>>>"

    if not Key(bucket=b, name="/user_profile/pickle_pool/%s/search/_SUCCESS" % date_string).exists():
        userProfileFromSearch(date_string)
    else:
        print "the SearchUserProfile in DATE: %s is already exists, skip it without recomputing" % date_string

    print "Finish ..."



""" View Search Profile Result
tt = userSearchProfile.take(20)
for ele in tt:
    print ele[0]
    for tag in ele[1]:
        print "\t" + "|".join(tag[:-1]) + "|" + str(tag[-1])
"""

""" View Title Extract Tag Result
tt = goodsTitleTag.take(200)
for ele in tt:
    print ele[1]
    for tag in ele[2]:
        print "\t" + "|" + tag[1] + "|" + str(tag[-1])
"""