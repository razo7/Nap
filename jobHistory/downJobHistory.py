# https://hadoop.apache.org/docs/r2.4.1/hadoop-yarn/hadoop-yarn-site/HistoryServerRest.html
# Hadoop Counters Explained -> http://www.coding-daddy.xyz/node/8
# https://xlsxwriter.readthedocs.io/worksheet.html / https://realpython.com/python-csv/

# install - conda install -c conda-forge selenium , conda install -c conda-forge geckodriver
from selenium import webdriver
import xlsxwriter
import xml.etree.ElementTree as ET


def jobStatistics(htmlPrefix, ID, modeTime, modeBytes, workbook):
    jobInfo = RESTjobHistoryHtmlToXML(
        htmlPrefix)  # 13 values, http://23.22.43.90:19888/ws/v1/history/mapreduce/jobs/job_1544631848492_0013
    mapers = int(jobInfo[8].text)
    reducers = int(jobInfo[10].text)
    jobTime = difftime(jobInfo[1].text, jobInfo[2].text, modeTime)
    list1 = ()
    list2 = ()
    list3 = ()
    list1 = list1 + (
    jobInfo[3].text, jobTime, jobInfo[4].text, jobInfo[8].text, jobInfo[22].text,
    jobInfo[21].text, jobInfo[10].text, jobInfo[19].text, jobInfo[18].text,
    miliSecondsToTime(jobInfo[14].text, modeTime), miliSecondsToTime(jobInfo[16].text, modeTime),
    miliSecondsToTime(jobInfo[17].text, modeTime), miliSecondsToTime(jobInfo[15].text, modeTime))
    html = htmlPrefix + "/counters"  # 8 values
    jobCounters = RESTjobHistoryHtmlToXML(
        html)  # http://23.22.43.90:19888/ws/v1/history/mapreduce/jobs/job_1544631848492_0013/counters
    list1 = list1 + (
    jobCounters[3][1][2].text, bytesToSize(jobCounters[1][6][2].text, modeBytes), jobCounters[3][10][3].text,
    bytesToSize(jobCounters[3][9][3].text, modeBytes), jobCounters[3][11][3].text,
    bytesToSize(jobCounters[6][1][3].text, modeBytes), jobCounters[3][8][3].text, jobCounters[3][13][3].text)

    html = htmlPrefix + "/jobattempts"  # 1 values
    jobAM = RESTjobHistoryHtmlToXML(
        html)  # http://23.22.43.90:19888/ws/v1/history/mapreduce/jobs/job_1544631848492_0013/jobattempts
    AM = jobAM[0][1].text.partition(':')[0]
    #list1 = list1 + tuple(str(AM))
    print "AM Node- " + str(AM)
    taskType = "_m_00000"
    # print "Mappers Info"
    for i in range(mapers):
        if (i > 9):
            taskID = taskType + str(i)
        else:
            taskID = taskType + "0" + str(i)
        html = htmlPrefix + "/tasks/task_" + ID + taskID + "/attempts/attempt_" + ID + taskID + "_0"  # 3 values
        taskInfo = RESTjobHistoryHtmlToXML(
            html)  # http://23.22.43.90:19888/ws/v1/history/mapreduce/jobs/job_1544631848492_0012/tasks/task_1544631848492_0012_r_0000010/attempts/attempt_1544631848492_0012_r_0000010_0
        list2 = list2 + (taskInfo[4].text, miliSecondsToTime(taskInfo[2].text, modeTime), str(taskInfo[8].text.partition(':')[0]))
    taskType = "_r_00000"
    # print "Reducers Info"
    for i in range(reducers):
        if (i > 9):
            taskID = taskType + str(i)
        else:
            taskID = taskType + "0" + str(i)
        html = htmlPrefix + "/tasks/task_" + ID + taskID + "/attempts/attempt_" + ID + taskID + "_0"  # 6 values
        taskInfo = RESTjobHistoryHtmlToXML(html)
        list3 = list3 + (taskInfo[4].text, miliSecondsToTime(taskInfo[2].text, modeTime), str(taskInfo[8].text.partition(':')[0]),
                         miliSecondsToTime(taskInfo[14].text, modeTime), miliSecondsToTime(taskInfo[15].text, modeTime),
                         miliSecondsToTime(taskInfo[16].text, modeTime))
        html = htmlPrefix + "/tasks/task_" + ID + taskID + "/counters"  # 5 values,  http://23.22.43.90:19888/ws/v1/history/mapreduce/jobs/job_1544631848492_0013/tasks/task_1544631848492_0013_r_0000012/counters
        taskCounters = RESTjobHistoryHtmlToXML(html)
        list3 = list3 + (
        taskCounters[2][5][1].text, bytesToSize(taskCounters[2][4][1].text, modeBytes), taskCounters[2][6][1].text,
        bytesToSize(taskCounters[4][1][1].text, modeBytes), taskCounters[2][3][1].text)
    worksheet = workbook.add_worksheet("job_" + ID)  # add a worksheet.
    worksheet.set_column(0, 0, 38)  # TaskID
    worksheet.set_column(1, 1, 11)  # ElapsedTime
    worksheet.set_column(2, 2, 7)   # NodeID
    worksheet.set_column(3, 5, 6)   # shuffle, merge, reduce
    worksheet.set_column(6, 6, 21)  # ReducerInput # records
    worksheet.set_column(7, 7, 12)  # ReducerInput
    worksheet.set_column(8, 8, 22)  # ReducerOutput # records
    worksheet.set_column(9, 10, 14) # ReducerOutput, ReducerGroups
    worksheet.write_row(0, 0,
                        ['TaskID', 'ElapsedTime', 'NodeID', 'shuffle', 'merge', 'reduce', 'ReducerInput # records',
                         'ReducerInput', 'ReducerOutput # records', 'ReducerOutput', 'ReducerGroups'])
    worksheet.write_row(1, 0, ["AM", jobTime, AM, 0, 0, 0, 0, 0, 0, 0, 0])
    row = 2
    for i in range(mapers):
       worksheet.write_row(row, 0, [list2[0 + i * 3], list2[1 + i * 3], list2[2 + i * 3], 0, 0, 0, 0, 0, 0, 0, 0])
       row += 1
    for i in range(reducers):
        worksheet.write_row(row, 0, [list3[0 + i * 11], list3[1 + i * 11], list3[2 + i * 11], list3[3 + i * 11],
                            list3[4 + i * 11], list3[5 + i * 11], list3[6 + i * 11], list3[7 + i * 11],
                            list3[8 + i * 11], list3[9 + i * 11], list3[10 + i * 11]])
        row += 1
    return list1

def RESTjobHistoryHtmlToXML(html):
    driver.get(html)
    page_source = driver.page_source
    return ET.fromstring(page_source)


def miliSecondsToTime(time, format):
    hours = int(float(time) / 3600000)
    if (hours > 0):
        left = int(float(time) % 3600000)
        minutes = int(float(left) / 60000)
        seconds = int((float(left) % 60000 / 1000))
        if (format == "ff"):
            return str(hours) + " h, " + str(minutes) + " m and " + str(seconds) + " s"
        else:
            return str(int(float(time) / 1000))
    else:
        minutes = int(float(time) / 60000)
        if (minutes > 0):
            seconds = int(((float(time) % 60000) / 1000))
            if (format == "ff"):
                return str(minutes) + " m and " + str(seconds) + " s"
            else:
                return str(int(float(time) / 1000))
        else:
            if (format == "ff"):
                return str(int(float(time) / 1000)) + " s"
            else:
                return str(int(float(time) / 1000))


def bytesToSize(byte, format):
    # ff= "FullFormat", f= "Format", n= "NoFormat"
    giga = int(float(byte) / 1000000000)
    if (giga > 0):
        mega = int((float(byte) % 1000000000) / 1000000)
        kilo = int((float(byte) % 1000000)/1000)
        if (format == "ff"):
            return str(giga) + " GB, " + str(mega) + " MB, " + str(kilo) + " KB and " + str(int(float(byte) % 1000)) \
                   + " B"
        elif (format == "f"):
            return str(giga) + "." + str(int(float(mega)/10)) + " GB"
        else:
            return str(byte)
    else:
        mega = int(float(byte) / 1000000)
        if (mega > 0):
            kilo = int((float(byte) % 1000000 / 1000))
            if (format == "ff"):
                return str(mega) + " MB, " + str(kilo) + " KB and " + str(int(float(byte) % 1000)) + " B"
            elif (format == "f"):
                return str(mega) + "." + str(int(float(kilo)/10)) + " MB"
            else:
                return str(byte)
        else:
            kilo = int(float(byte) / 1000)
            if (kilo > 0):
                if (format == "ff"):
                    return str(kilo) + " KB and " + str(int(float(byte) % 1000)) + " B"
                elif (format == "f"):
                    return str(kilo) + "." + str(int((float(byte) % 1000)/10)) + " KB"
                else:
                    return str(byte)
            else:
                if (format == "n"):
                    return str(byte)
                else:
                    return str(byte) + " B"


def difftime(time1, time2, format):
    return miliSecondsToTime(str(int(time2) - int(time1)), format)


def myMain(masterIP, ID, jobsList, name):
    masterIPort = masterIP + ":19888"
    prefixID = ID + "_0"
    jobs = ()
    # jobsList = range(1, Range + 1)
    # Create a workbook and add a worksheet.
    workbook = xlsxwriter.Workbook('jobsHistory_' + name + '.xlsx')
    worksheet = workbook.add_worksheet("Jobs")  # add a worksheet.
    for i in jobsList:
        if (i > 99):
            ID = prefixID + str(i)
        if (i > 9):
            ID = prefixID + "0" + str(i)
        else:
            ID = prefixID + "00" + str(i)
        htmlPrefix = "http://" + masterIPort + "/ws/v1/history/mapreduce/jobs/job_" + ID
        jobs = jobs + jobStatistics(htmlPrefix, ID, "n", "n", workbook)
    worksheet.set_column(0, 0, 22)  # JobID
    worksheet.set_column(1, 1, 11)  # ElapsedTime
    worksheet.set_column(2, 2, 16)  # JobName
    worksheet.set_column(3, 3, 6)  # #Maps
    worksheet.set_column(4, 6, 9)  # #MapsKill, #MapsFail, #Reducers
    worksheet.set_column(7, 8, 12)  # #ReducersKill, #ReducersFail
    worksheet.set_column(9, 9, 8)  # AVGmap
    worksheet.set_column(10, 12, 10)  # AVGshuffle, AVGmerge, AVGreduce
    worksheet.set_column(13, 13, 17)  # MapInput # records
    worksheet.set_column(14, 14, 9)  # MapInput
    worksheet.set_column(15, 15, 20)  # ReduceInput # records
    worksheet.set_column(16, 16, 12)  # ReduceInput
    worksheet.set_column(17, 17, 23)  # ReducerOutput # records
    worksheet.set_column(18, 21, 14)  # ReducerOutput, ReduceGroupts, ShuffledMaps
    # AM Node
    worksheet.write_row(0, 0,
                        ["JobID", "ElapsedTime", "JobName", "#Maps", "#MapsKill", "#MapsFail", "#Reducers",
                         "#ReducersKill",
                         "#ReducersFail", "AVGmap", "AVGshuffle", "AVGmerge", "AVGreduce", "MapInput # records",
                         "MapInput",
                         "ReduceInput # records", "ReduceInput", "ReducerOutput # records", "ReducerOutput",
                         "ReduceGroupts",
                         "ShuffledMaps", "AM Node"])
    row = 1
    for i in range(len(jobsList)):
        #AM = ''.join(jobs[21 + i * 27: 27 + i * 27])

        worksheet.write_row(row, 0,
                            [jobs[0 + i * 21], jobs[1 + i * 21], jobs[2 + i * 21], jobs[3 + i * 21], jobs[4 + i * 21],
                             jobs[5 + i * 21], jobs[6 + i * 21], jobs[7 + i * 21], jobs[8 + i * 21], jobs[9 + i * 21],
                             jobs[10 + i * 21],
                             jobs[11 + i * 21], jobs[12 + i * 21], jobs[13 + i * 21], jobs[14 + i * 21],
                             jobs[15 + i * 21],
                             jobs[16 + i * 21], jobs[17 + i * 21], jobs[18 + i * 21], jobs[19 + i * 21],
                             jobs[20 + i * 21],
                             0])
        row += 1
    workbook.close()

driver = webdriver.Firefox()
myMain("23.22.43.90", "1548932257169", range(63, 73), "10_AMJ_31Jan_131212")
#myMain("23.22.43.90", "1548932257169", range(41, 51), "10_AMJ_31Jan_867")
#myMain("23.22.43.90", "1548932257169", range(51, 61), "10_AMJ_31Jan_967")
#myMain("23.22.43.90", "1548932257169", range(61, 71), "10_AMJ_31Jan_857")
#myMain("23.22.43.90", "1548932257169", range(71, 81), "10_AMJ_31Jan_968")
driver.close()