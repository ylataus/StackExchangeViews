import dbaccess
import stackexchange
import time
import httplib


f = open("../../Access/mysql_access.txt","r")
mysql = f.read().strip().split(",")
f.close()

msqlu = mysql[0]
msqlp = mysql[1]
msqldb = mysql[2]
msqlh = 'localhost'

acceso = dbaccess.get_Connection(msqlh, 3306, msqlu, msqlp, msqldb)


# registered api key for more pulls
g = open("../../Access/seapi_access.txt","r")
mykey = g.read().strip()
g.close()



# need to set dates based on starttime and current time
# times are unix clock times
#originalstart = (2016,3,17,21,40,0,0,0,0) # always do 1 week from now - this will then track questions for a week
originalstart = time.localtime(time.time()- 60*60) 

#startdate = (2016,3,24,10,0,0,0,0,1)

#endtime = time.time()
#print starttime, endtime
#2010-07-19T19:14:43.050
#print time.strftime("%Y-%m-%dT%H:%M:%S",time.localtime(endtime))





## read in big data tags
def readBigData(Site):

    f = open("../bigdata"+Site.lower()+".txt","rU")
    tagnames = f.read().split(",")
    f.close()
    return tagnames

## get posts from so that happened in timeperiod
## get posts from so that happened with tag
## get view for each post


def writeQues(myques,endtime,Site):
    vals = [str(myques.id),
            "'"+str(myques.creation_date)+"'",
            str(myques.view_count),
            "'"+time.strftime("%Y-%m-%dT%H:%M:%S",time.localtime(endtime))+"'"]
    sqlStr = "INSERT IGNORE "+Site.lower()+"_views VALUES ("+",".join(vals)+")"
    acceso[1].execute(sqlStr)
    acceso[0].commit()

def getQuestions(Site,SiteAPI):
    tagnames = readBigData(Site)
    quesdetails = {}
    byn = 0
    callcount = 0
    totalcalls = 0
    startwindow = time.time() - 7*24*60*60
    #print startwindow
    #print time.mktime(originalstart)
    starttime = max(time.mktime(originalstart),startwindow)
    
    for (i,tagn) in enumerate(tagnames[byn:]):
        #print tagn
        #print byn+i,tagn, callcount
        if callcount >= 15:
            print "wait"
            time.sleep(5)
            callcount = 0
        
        while True:
            try:
                endtime = time.time()
                ques = SiteAPI.questions(tagged=tagn,fromdate=starttime,todate=endtime)
                callcount += 1
                totalcalls += 1
                break
            except (IOError, httplib.HTTPException) as e:
                print "Error in Original Loop"
                print e
                print "wait 1 min"
                time.sleep(60)
            except:
                print "Unexpected error Original loop"
                print "wait 1 min"
                time.sleep(60)

        while True:
            #print tagn
            for q in ques:
#                 quesdetails[q.id] = {"id":q.id,
#                                      "views":q.view_count,
#                                      "viewtime":time.strftime("%Y-%m-%dT%H:%M:%S",time.localtime(endtime))}
                writeQues(q,endtime,Site)
            if callcount >= 15:
                time.sleep(5)
                callcount = 0 
                
            if len(ques)>=30:
                try:          
                    ques = ques.fetch_next()
                    callcount += 1
                    totalcalls += 1
                except (IOError, httplib.HTTPException) as e:
                    print "Error in Next Loop"
                    print e
                    print "wait 1 min"
                    time.sleep(60)
                except:
                    print "Unexpected error next loop"
                    print "wait 1 min"
                    time.sleep(60)
            else:
                break
            
            if ques == None or len(ques) == 0:
                break
            

            
    print totalcalls
    #for q in quesdetails.values():
    #    writeQues(q,endtime,Site)
    #    print q

def everyHour(Site):
    
    SiteAPI = None
    if Site.upper()=="SO":
        SiteAPI = stackexchange.Site(stackexchange.StackOverflow,mykey)
    elif Site.upper()=="CV":
        SiteAPI = stackexchange.Site(stackexchange.CrossValidated,mykey) 
#     elif Site.upper()=="DS":
#         SiteAPI = stackexchange.Site(stackexchange.DataScience,mykey)
    SiteAPI.throttle_stop = False   
    
    while True:
        mystart = time.time()  
        print "Start "+Site+" Hour",time.strftime("%Y-%m-%dT%H:%M:%S",time.localtime(mystart))
        getQuestions(Site,SiteAPI)
        myend = time.time()
        print "End "+Site+" Hour",time.strftime("%Y-%m-%dT%H:%M:%S",time.localtime(myend))
        timediff = myend-mystart
        anhour = 3600
        myhour = anhour-timediff
        print "Wait",myhour
        time.sleep(myhour)

#print len(readBigData("SO"))
