from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import re
import os
import calendar
import time

def change(list):
    list
def get_in_usa(path_location):
    location = sc.textFile(path_location)
    # print(location.collect())
    in_usa = location.map(lambda a: a.replace('\"', '').split(',')). \
        filter(lambda s: s[3] == 'US'). \
        filter(lambda s: s[4] != ''). \
        map(lambda s: (s[0], s[4]))
    return in_usa
    #get USAF ans STATE
def get_recording(path_recording):
    recordings = []
    path_1 = path_recording +'/2006.txt'
    print(path_1)
    recordings.append(sc.textFile(path_1))

    path_2 = path_recording + '/2007.txt'
    print(path_2)
    recordings.append(sc.textFile(path_2))

    path_3 = path_recording + '/2008.txt'
    print(path_3)
    recordings.append(sc.textFile(path_3))

    path_4 = path_recording + '/2009.txt'
    print(path_4)
    recordings.append(sc.textFile(path_4))

    recordings = sc.union(recordings)
    return recordings

def get_avg_prcp_min(recording, in_usa):
    avg_prcp = in_usa.join(recording.map(lambda s: re.findall('[\S]+', s)). \
                           map(lambda s: (s[0], (s[2][4:6], s[-3])))). \
        filter(lambda s: s[1][1][1] != '99.99'). \
        map(lambda s: ((s[1][0], s[1][1][0]), (s[1][1][1]))). \
        map(lambda x: (x[0][0], (x[0][1], x[1]))). \
        map(lambda s: ((s[0], s[1][0]), s[1][1]))

    # print(avg_prcp.collect())

    list1 = avg_prcp.collect()
    # print(list1)

    length = len(list1)
    # print(length)
    for i in range(0, length):
        if list1[i][1][-1] == 'A':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) / 6 * 24), 2))
            list1[i] = tuple(temp)
        if list1[i][1][-1] == 'B':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) / 12 * 24), 2))
            list1[i] = tuple(temp)

        if list1[i][1][-1] == 'C':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) / 18 * 24), 2))
            list1[i] = tuple(temp)
        if list1[i][1][-1] == 'D':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) / 24 * 24), 2))
            list1[i] = tuple(temp)
        if list1[i][1][-1] == 'E':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) / 12 * 24), 2))
            list1[i] = tuple(temp)
        if list1[i][1][-1] == 'F':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) / 24 * 24), 2))
            list1[i] = tuple(temp)
        if list1[i][1][-1] == 'G':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) / 24 * 24), 2))
            list1[i] = tuple(temp)
        if list1[i][1][-1] == 'H':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) * 0), 2))
            list1[i] = tuple(temp)
        if list1[i][1][-1] == 'I':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) * 0), 2))
            list1[i] = tuple(temp)
    # for i in range(0, length):
    #     list1[i][1] = tuple(list1[i][1])
    #     list1[i] = tuple(list1[i])
    # print(list1)
    avg_prcp = sc.parallelize(list1)
    # avg_prcp = avg_prcp.\
    #     groupByKey(). \
    #     reduceByKey(max)
    avg_prcp = avg_prcp. \
        reduceByKey(min). \
        map(lambda x: (x[0][0], (x[0][1], x[1]))). \
        groupByKey(). \
        map(lambda s: (s[0], sorted(s[1], key=lambda a: a[1])[0]))
    # print(avg_prcp.collect())

    return avg_prcp


def get_avg_prcp_max(recording, in_usa):
    avg_prcp = in_usa.join(recording.map(lambda s: re.findall('[\S]+', s)). \
                           map(lambda s: (s[0], (s[2][4:6], s[-3])))). \
        filter(lambda s: s[1][1][1] != '99.99'). \
        map(lambda s: ((s[1][0], s[1][1][0]), (s[1][1][1]))).\
        map(lambda x: (x[0][0], (x[0][1], x[1]))) .\
        map(lambda s: ((s[0],s[1][0]), s[1][1]))
        # map(lambda s: (s[0], sorted(s[1], key=lambda a: a[1])[0]))
    # groupByKey(). \
        #reduceByKey(max). \


    #print(avg_prcp.collect())

    list1 = avg_prcp.collect()
    # print(list1)

    length = len(list1)
    # print(length)
    for i in range(0, length):
        if list1[i][1][-1] == 'A':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) /6 * 24), 2))
            list1[i] = tuple(temp)
        if list1[i][1][-1] == 'B':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) /12 * 24), 2))
            list1[i] = tuple(temp)

        if list1[i][1][-1] == 'C':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) /18 * 24), 2))
            list1[i] = tuple(temp)
        if list1[i][1][-1] == 'D':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) /24 * 24), 2))
            list1[i] = tuple(temp)
        if list1[i][1][-1] == 'E':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) /12 * 24), 2))
            list1[i] = tuple(temp)
        if list1[i][1][-1] == 'F':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) /24 * 24), 2))
            list1[i] = tuple(temp)
        if list1[i][1][-1] == 'G':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) /24 * 24), 2))
            list1[i] = tuple(temp)
        if list1[i][1][-1] == 'H':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) * 0 ), 2))
            list1[i] = tuple(temp)
        if list1[i][1][-1] == 'I':
            temp = list(list1[i])
            temp[1] = str(round((float(temp[1][0:4]) * 0 ), 2))
            list1[i] = tuple(temp)
    # for i in range(0, length):
    #     list1[i][1] = tuple(list1[i][1])
    #     list1[i] = tuple(list1[i])
    # print(list1)
    avg_prcp = sc.parallelize(list1)
    # avg_prcp = avg_prcp.\
    #     groupByKey(). \
    #     reduceByKey(max)
    avg_prcp= avg_prcp.\
        reduceByKey(max).\
        map(lambda x: (x[0][0], (x[0][1], x[1]))).\
        groupByKey().\
        map(lambda s: (s[0], sorted(s[1], key=lambda a: a[1])[0]))
    # print(avg_prcp.collect())

    return avg_prcp




def get_diff(avg_prcp_min,avg_prcp_max):
    temp = avg_prcp_max.join(avg_prcp_min)
    print(temp.collect())
    difference = temp.join(temp.map(lambda s: (s[0], round(float(s[1][0][1]) - float(s[1][1][1]), 2)))). \
        map(lambda s: (s[0], s[1][0][0][1], calendar.month_name[int(s[1][0][0][0])], s[1][0][1][1], calendar.month_name[int(s[1][0][1][0])], s[1][1])). \
        sortBy(lambda s: s[5])
    # print(difference.collect())

    return difference






def get_result(result):
    string_head = 'STATE' + '\t' + 'AVERAGE RAINFALL OF THE HIGHEST MONTH' + '\t' + 'HIGHEST MONTH' + '\t' + 'AVERAGE RAINFALL OF THE LOWEST MONTH' + '\t' + 'LOWEST MONTH' + '\t' + 'DIFFERENCE BETWEEN THESE TWO MONTHS' + '\r'

    with open(path_output + '/' + 'result.txt', 'w') as file:
        file.write(string_head)
        file.close()

    test = result.sortBy(lambda s: s[5]).collect()
    print(test)
    for x in test:
        string_result = x[0] + '\t' + str(x[1]) + '\t' + x[2] + '\t' + str(x[3]) + '\t' + x[4] + '\t' + str(x[5]) + '\r'
        print(string_result)

        with open(path_output + '/' + 'result.txt', mode='a') as file:
            file.write(string_result)
            file.close()


if __name__ == '__main__':
    start = time.time()
    sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/Users/zhaoyifan/Desktop/project')

    args = sys.argv
    print(args)
    if len(args) != 4:
        print('ERROR: The length of configurations is not right.\n'
              'Please input the path of the Locations File, the Recordings Files and the Output Folder.\n'
              'EXITING')
        sys.exit(1)

    path_location = args[1]
    path_recording = args[2]
    # global path_output
    path_output = args[3]
    # path_location = '/Users/zhaoyifan/Desktop/project/CS236-Dataset/Locations'
    # path_recording = '/Users/zhaoyifan/Desktop/project/CS236-Dataset/Recordings'
    # path_output = '/Users/zhaoyifan/Desktop/project/CS236-Dataset/output'
    spark = SparkSession.builder.master("local").appName("SparkRDD")
    sc = SparkContext.getOrCreate()

    in_usa = get_in_usa(path_location)
    # in_usa_print = in_usa.collect()
    # print(in_usa_print)


    recording = get_recording(path_recording)
    avg_prcp_min = get_avg_prcp_min(recording, in_usa)
    # print(avg_prcp_min.collect())
    avg_prcp_max = get_avg_prcp_max(recording, in_usa)
    # # print(avg_prcp_max.collect())
    diff=get_diff(avg_prcp_min,avg_prcp_max)

    get_result(diff)




    end = time.time()
    run_time = end-start
    print("Run time is:")
    print(run_time)
