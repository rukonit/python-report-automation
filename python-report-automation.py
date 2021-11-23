import pyodbc
import pandas as pd
import datetime as dt

# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
new_order = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
# select 26 rows from SQL table to insert in dataframe.

# df['MonthName']=  pd.DatetimeIndex((pd.to_datetime(df['CreatedDate'],format='%Y-%m-%d'))).strftime('%B')

# df.groupby('MonthName').agg('count')
def generateSingleReport(seriesName, query1, query2):
    
  
    df = pd.read_sql(query1, cnxn)
    
    df['CreatedDate'] = pd.to_datetime(df['CreatedDate'])
    df['Month'] = df['CreatedDate'].dt.month_name()
    df['Day'] = df['CreatedDate'].dt.day
    fltr = df['CreatedDate'].dt.month < dt.datetime.now().month
    dfMonth  = df[fltr].groupby('Month').agg('count')['ApplicationName__c']
    dfMonth.index = pd.CategoricalIndex(dfMonth.index, categories=new_order, ordered=True)
    dfMonth = dfMonth.sort_index()
    
    
    fltr2 = (df['CreatedDate'] >= (dt.datetime.now() - dt.timedelta(9))) & (df['CreatedDate'] < (dt.datetime.now()- dt.timedelta(1)))
    dfDay = df[fltr2].groupby('Day').agg('count')['ApplicationName__c']
    dfDay
    dfMonthDay = dfMonth.append(dfDay)
    mainDF = pd.DataFrame(dfMonthDay)
    
    df2 = pd.read_sql(query2, cnxn)

    df2['CreatedDate'] = pd.to_datetime(df2['CreatedDate'])
    df2['Month'] = df2['CreatedDate'].dt.month_name()
    df2['Day'] = df2['CreatedDate'].dt.day
    fltrDF2 = df2['CreatedDate'].dt.month < dt.datetime.now().month
    dfMonth2  = df2[fltrDF2].groupby('Month').agg('count')
    dfMonth2.index = pd.CategoricalIndex(dfMonth2.index, categories=new_order, ordered=True)
    dfMonth2 = dfMonth2.sort_index()
    
    
    fltrDF22 = (df2['CreatedDate'] >= (dt.datetime.now() - dt.timedelta(9))) & (df2['CreatedDate'] < (dt.datetime.now()- dt.timedelta(1)))
#     fltrDF22 = (df2['CreatedDate'].dt.month >= dt.datetime.now().month) & (df2['CreatedDate'].dt.day >= (dt.datetime.now().day - 8)) 
    dfDay2 = df2[fltrDF22].groupby('Day').agg('count')
    dfDay2
    dfMonthDay2 = dfMonth2.append(dfDay2)
    mainDF2 = pd.DataFrame(dfMonthDay2)


    
    mainDF3 = pd.concat([mainDF2, mainDF], axis=1)
    mainDF3 = mainDF3[['CreatedDate', 'ApplicationName__c']].fillna(0)
    mainDF3[seriesName] = ((mainDF3['ApplicationName__c']/(mainDF3['ApplicationName__c']+mainDF3['CreatedDate']))) 
    mainT = pd.DataFrame(mainDF3[seriesName]).T
    
    
    return mainT

queryConvertLeadWebService = "SELECT ApplicationName__c, CreatedDate FROM dbo.Log__c l where l.CreatedDate > DATEADD(month, -4, GETDATE()) and ApplicationName__c = 'ConvertLeadWebService'"
queryOpportunityCreated = "SELECT ID, CreatedDate FROM opportunity O WHERE CreatedDate >= DATEADD(month, DATEDIFF(month, 0, dateadd(month, -4, getdate())), 0)"

queryDecisionWebService = "SELECT ApplicationName__c, CreatedDate FROM dbo.Log__c l where l.CreatedDate >= DATEADD(month, DATEDIFF(month, 0, dateadd(month, -4, getdate())), 0) and ApplicationName__c = 'Decision WebService BLN'"
queryScoreCreated = "SELECT CreatedDate FROM score__c WHERE CreatedDate >= DATEADD(month, DATEDIFF(month, 0, dateadd(month, -4, getdate())), 0)"

queryScrutinyService = "SELECT ApplicationName__c, CreatedDate FROM dbo.Log__c l where l.CreatedDate >= DATEADD(month, DATEDIFF(month, 0, dateadd(month, -4, getdate())), 0) and ApplicationName__c = 'Scrutiny WebService'"
querySBSCCreated = "SELECT CreatedDate FROM SBCS__c WHERE CreatedDate >= DATEADD(month, DATEDIFF(month, 0, dateadd(month, -4, getdate())), 0)"

queryCreditPolicyUtility = "SELECT ApplicationName__c, CreatedDate FROM dbo.Log__c l where l.CreatedDate >= DATEADD(month, DATEDIFF(month, 0, dateadd(month, -4, getdate())), 0) and ApplicationName__c = 'creditPolicyExceptionUtility'" 
queryOpportunityMovedtoUnderWriting = "SELECT Moved_to_Underwriting_Date__c as CreatedDate FROM opportunity WHERE Moved_to_Underwriting_Date__c >= DATEADD(month, DATEDIFF(month, 0, dateadd(month, -4, getdate())), 0)"

g1 = generateSingleReport("ConvertLeadWebService", queryConvertLeadWebService, queryOpportunityCreated)
g2 = generateSingleReport("Decision WebService BLN", queryDecisionWebService, queryScoreCreated)

g3 = generateSingleReport("Scrutiny WebService",queryScrutinyService, querySBSCCreated)
g4 = generateSingleReport("creditPolicyExceptionUtility", queryCreditPolicyUtility, queryOpportunityMovedtoUnderWriting)



a = []
for i in [(dt.datetime.now() - relativedelta(months = 4)).month, (dt.datetime.now() - relativedelta(months = 3)).month, (dt.datetime.now() - relativedelta(months = 2)).month, (dt.datetime.now() - relativedelta(months = 1)).month]:
    a.append(dt.datetime.strptime(str(i), "%m").strftime("%B"))
i = 9
while (dt.datetime.now() - dt.timedelta(i)) < dt.datetime.now() - (dt.timedelta(1)):
    i = i - 1
    a.append((dt.datetime.now() - dt.timedelta(i)).day)
a

b = pd.concat([g1, g2, g3, g4])
b[a].fillna(0).style.format('{:,.2%}')
