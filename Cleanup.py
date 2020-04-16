#!/usr/bin/env python
# coding: utf-8

# # 1. Uniform the tables in the format of the table for March 31. 

# In[1]:


# Create a database connection to the database and obtain a cursor object
import sqlite3
conn = sqlite3.connect("CoronaDB.db")
cursor = conn.cursor()

# make sure that the current working directry is CoronaDB.db
import os.path
BASE_DIR = os.path.dirname(os.path.abspath("__file__"))
db_path = os.path.join(BASE_DIR, "CoronaDB.db") 
with sqlite3.connect(db_path) as db:
    os.getcwd()
    
# check if we are in CoronaDB.db
cursor.execute("SELECT name FROM sqlite_master WHERE   type='table';")
print(cursor.fetchall())


# In[2]:


# create a list of dates for renaming the tables (January 22 to February 29)
import datetime
start = datetime.datetime.strptime("01-22-2020", "%m-%d-%Y").date()
end = datetime.datetime.strptime("03-01-2020", "%m-%d-%Y").date()
dates1 = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]
dates1 = ['{}-{}-{}'.format(m,d,y) for y, m, d in map(lambda x: str(x).split('-'), dates1)]

# create a list of tables for renaming (March 1 to March 21)
start = datetime.datetime.strptime("03-01-2020", "%m-%d-%Y").date()
end = datetime.datetime.strptime("03-22-2020", "%m-%d-%Y").date()
dates2 = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]
dates2 = ['{}-{}-{}'.format(m,d,y) for y, m, d in map(lambda x: str(x).split('-'), dates2)]


# In[3]:


# Set the loop factors x and y, define SQL functions, execute them with buffer printing to separate the execution
for x in dates1:

    # define the suffix of temporary table name
    y = x + "_org"

    # rename the table
    renametable = "ALTER TABLE [" + str(x) + "] RENAME TO [" + str(y) + "]"
    cursor.execute(renametable)
    conn.commit()
    
    # create an empty table with the original name
    createtable = ("CREATE TABLE [" + str(x) + "] "
                   "(FIPS TEXT, "
                   "Admin2 TEXT, "
                   "Province_State TEXT, "
                   "Country_Region TEXT, "
                   "Last_Update REAL, "
                   "Lat REAL, "
                   "Long_ REAL, "
                   "Confirmed REAL, "
                   "Deaths REAL, "
                   "Recovered REAL, "
                   "Active REAL, "
                   "Combined_Key TEXT)")
    cursor.execute(createtable)
    conn.commit()
    
    # insert rows of the temporary table back to the new empty table
    insertback = ("INSERT INTO [" + str(x) + "] "
                  "([Province_State], "
                  "[Country_Region], "
                  "[Last_Update], "
                  "Confirmed, "
                  "Deaths, "
                  "Recovered) "
                  "SELECT [Province/State], "
                  "[Country/Region], "
                  "[Last Update], "
                  "Confirmed, "
                  "Deaths, "
                  "Recovered "
                  "FROM [" + str(y) + "]")
    cursor.execute(insertback)
    conn.commit()
    
    # add columns from the table for March 31
    insertFIPS = ("UPDATE [" + str(x) + "] "
                  "SET FIPS = (SELECT FIPS FROM [03-31-2020] "
                  "WHERE [03-31-2020].Province_State = [" + str(x) + "].Province_State)")  
    cursor.execute(insertFIPS)
    conn.commit()
    
    insertAdmin2 = ("UPDATE [" + str(x) + "] "
                    "SET Admin2 = (SELECT Admin2 FROM [03-31-2020] "
                    "WHERE [03-31-2020].Province_State = [" + str(x) + "].Province_State)")
    cursor.execute(insertAdmin2)
    conn.commit()
    
    insertLat = ("UPDATE [" + str(x) + "] "
                 "SET Lat = (SELECT Lat FROM [03-31-2020] "
                 "WHERE [03-31-2020].Province_State = [" + str(x) + "].Province_State)")
    cursor.execute(insertLat)
    conn.commit()
    
    insertLong_ = ("UPDATE [" + str(x) + "] "
                   "SET Long_ = (SELECT Long_ FROM [03-31-2020] "
                   "WHERE [03-31-2020].Province_State = [" + str(x) + "].Province_State)")
    cursor.execute(insertLong_)
    conn.commit()
    
    insertActive = ("UPDATE [" + str(x) + "] "
                    "SET Active = (SELECT Active FROM [03-31-2020] "
                    "WHERE [03-31-2020].Province_State = [" + str(x) + "].Province_State)")
    cursor.execute(insertActive)
    conn.commit()
    
    insertComkey = ("UPDATE [" + str(x) + "] "
                    "SET Combined_Key = (SELECT Combined_Key FROM [03-31-2020] "
                    "WHERE [03-31-2020].Province_State = [" + str(x) + "].Province_State)")
    cursor.execute(insertComkey)
    conn.commit()
    
    # delete the temporary table
    droptable = "DROP TABLE [" + str(y) + "]"
    cursor.execute(droptable)
    conn.commit()


# In[4]:


# Set the loop factors x and y, define SQL functions, execute them with buffer printing to separate the execution
for x in dates2:

    # define the suffix of temporary table name
    y = x + "_org"
    
    # rename the table
    renametable = "ALTER TABLE [" + str(x) + "] RENAME TO [" + str(y) + "]"
    cursor.execute(renametable)
    conn.commit()
    
    # create an empty table with the original name
    createtable = ("CREATE TABLE [" + str(x) + "] "
                   "(FIPS TEXT, "
                   "Admin2 TEXT, "
                   "Province_State TEXT, "
                   "Country_Region TEXT, "
                   "Last_Update REAL, "
                   "Lat REAL, "
                   "Long_ REAL, "
                   "Confirmed REAL, "
                   "Deaths REAL, "
                   "Recovered REAL, "
                   "Active REAL, "
                   "Combined_Key TEXT)")
    cursor.execute(createtable)
    conn.commit()
    
    # insert rows of the temporary table back to the new empty table
    insertback = ("INSERT INTO [" + str(x) + "] "
                  "([Province_State], "
                  "[Country_Region], "
                  "[Last_Update], "
                  "Lat, "
                  "Long_, "
                  "Confirmed, "
                  "Deaths, "
                  "Recovered) "
                  "SELECT [Province/State], "
                  "[Country/Region], "
                  "[Last Update], "
                  "Latitude, "
                  "Longitude, "
                  "Confirmed, "
                  "Deaths, "
                  "Recovered FROM [" + str(y) + "]")
    cursor.execute(insertback)
    conn.commit()
    
    # add columns from the table for March 31
    insertFIPS = ("UPDATE [" + str(x) + "] "
                  "SET FIPS = (SELECT FIPS FROM [03-31-2020] "
                  "WHERE [03-31-2020].Province_State=[" + str(x) + "].Province_State)")
    cursor.execute(insertFIPS)
    conn.commit()
    
    insertAdmin2 = ("UPDATE [" + str(x) + "] "
                    "SET Admin2 = (SELECT Admin2 FROM [03-31-2020] "
                    "WHERE [03-31-2020].Province_State=[" + str(x) + "].Province_State)")
    cursor.execute(insertAdmin2)
    conn.commit()
    
    insertActive = ("UPDATE [" + str(x) + "] "
                    "SET Active = (SELECT Active FROM [03-31-2020] "
                    "WHERE [03-31-2020].Province_State=[" + str(x) + "].Province_State)")
    cursor.execute(insertActive)
    conn.commit()
    
    insertComkey = ("UPDATE [" + str(x) + "] "
                    "SET Combined_Key = (SELECT Combined_Key FROM [03-31-2020] "
                    "WHERE [03-31-2020].Province_State=[" + str(x) + "].Province_State)")
    cursor.execute(insertComkey)
    conn.commit()
    
    # delete the temporary table
    droptable = "DROP TABLE [" + str(y) + "]"
    cursor.execute(droptable)
    conn.commit()


# # 2. Aggregate rows for states in US and Canada

# In[5]:


# create a list of dates for US row aggregation (January 22 to March 9)
start = datetime.datetime.strptime("01-22-2020", "%m-%d-%Y").date()
end = datetime.datetime.strptime("03-10-2020", "%m-%d-%Y").date()
dates3 = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]
dates3 = ['{}-{}-{}'.format(m,d,y) for y, m, d in map(lambda x: str(x).split('-'), dates3)]


# # Before proceeding, open Correspondence.ipynb with R Kernel and run the codes in the section 1.

# In[6]:


# aggregate rows for the same state of all tables in loop
for x in dates3:

    # convert cells that contains (, [Acronym]) to ([State_Name])
    Alabama = ("UPDATE [" + str(x) + "] "
               "SET [Province_State] = 'Alabama' "
               "WHERE [Province_State] LIKE '%, AL%'")
    cursor.execute(Alabama)
    conn.commit()

    Alaska = ("UPDATE [" + str(x) + "] "
              "SET [Province_State] = 'Alaska' "
              "WHERE [Province_State] LIKE '%, AK%'")
    cursor.execute(Alaska)
    conn.commit()

    AmericanSamoa = ("UPDATE [" + str(x) + "] "
                     "SET [Province_State] = 'American Samoa' "
                     "WHERE [Province_State] LIKE '%, AS%'")
    cursor.execute(AmericanSamoa)
    conn.commit()

    Arizona = ("UPDATE [" + str(x) + "] "
               "SET [Province_State] = 'Arizona' "
               "WHERE [Province_State] LIKE '%, AZ%'")
    cursor.execute(Arizona)
    conn.commit()

    Arkansas = ("UPDATE [" + str(x) + "] "
                "SET [Province_State] = 'Arkansas' "
                "WHERE [Province_State] LIKE '%, AR%'")
    cursor.execute(Arkansas)
    conn.commit()

    California = ("UPDATE [" + str(x) + "] "
                  "SET [Province_State] = 'California' "
                  "WHERE [Province_State] LIKE '%, CA%'")
    cursor.execute(California)
    conn.commit()

    Colorado = ("UPDATE [" + str(x) + "] "
                "SET [Province_State] = 'Colorado' "
                "WHERE [Province_State] LIKE '%, CO%'")
    cursor.execute(Colorado)
    conn.commit()

    Connecticut	 = ("UPDATE [" + str(x) + "] "
                    "SET [Province_State] = 'Connecticut' "
                    "WHERE [Province_State] LIKE '%, CT%'")
    cursor.execute(Connecticut)
    conn.commit()

    Delaware = ("UPDATE [" + str(x) + "] "
                "SET [Province_State] = 'Delaware' "
                "WHERE [Province_State] LIKE '%, DE%'")
    cursor.execute(Delaware)
    conn.commit()

    DistrictofColumbia = ("UPDATE [" + str(x) + "] "
                          "SET [Province_State] = 'District of Columbia' "
                          "WHERE [Province_State] LIKE '%, DC%'")
    cursor.execute(DistrictofColumbia)
    conn.commit()

    FederatedStatesofMicronesia = ("UPDATE [" + str(x) + "] "
                                   "SET [Province_State] = 'Federated States of Micronesia' "
                                   "WHERE [Province_State] LIKE '%, FM%'")
    cursor.execute(FederatedStatesofMicronesia)
    conn.commit()
    
    Florida = ("UPDATE [" + str(x) + "] "
               "SET [Province_State] = 'Florida' "
               "WHERE [Province_State] LIKE '%, FL%'")
    cursor.execute(Florida)
    conn.commit()
    
    Georgia = ("UPDATE [" + str(x) + "] "
               "SET [Province_State] = 'Georgia' "
               "WHERE [Province_State] LIKE '%, GA%'")
    cursor.execute(Georgia)
    conn.commit()
    
    Guam = ("UPDATE [" + str(x) + "] "
            "SET [Province_State] = 'Guam' "
            "WHERE [Province_State] LIKE '%, GU%'")
    cursor.execute(Guam)
    conn.commit()
    
    Hawaii = ("UPDATE [" + str(x) + "] "
              "SET [Province_State] = 'Hawaii' "
              "WHERE [Province_State] LIKE '%, HI%'")
    cursor.execute(Hawaii)
    conn.commit()
    
    Idaho = ("UPDATE [" + str(x) + "] "
             "SET [Province_State] = 'Idaho' "
             "WHERE [Province_State] LIKE '%, ID%'")
    cursor.execute(Idaho)
    conn.commit()
    
    Illinois = ("UPDATE [" + str(x) + "] "
                "SET [Province_State] = 'Illinois' "
                "WHERE [Province_State] LIKE '%, IL%'")
    cursor.execute(Illinois)
    conn.commit()
    
    Indiana = ("UPDATE [" + str(x) + "] "
               "SET [Province_State] = 'Indiana' "
               "WHERE [Province_State] LIKE '%, IN%'")
    cursor.execute(Indiana)
    conn.commit()
    
    Iowa = ("UPDATE [" + str(x) + "] "
            "SET [Province_State] = 'Iowa' "
            "WHERE [Province_State] LIKE '%, IA%'")
    cursor.execute(Iowa)
    conn.commit()
    
    Kansas = ("UPDATE [" + str(x) + "] "
              "SET [Province_State] = 'Kansas' "
              "WHERE [Province_State] LIKE '%, KS%'")
    cursor.execute(Kansas)
    conn.commit()
    
    Kentucky = ("UPDATE [" + str(x) + "] "
                "SET [Province_State] = 'Kentucky' "
                "WHERE [Province_State] LIKE '%, KY%'")
    cursor.execute(Kentucky)
    conn.commit()
    
    Louisiana = ("UPDATE [" + str(x) + "] "
                 "SET [Province_State] = 'Louisiana' "
                 "WHERE [Province_State] LIKE '%, LA%'")
    cursor.execute(Louisiana)
    conn.commit()
    
    Maine = ("UPDATE [" + str(x) + "] "
             "SET [Province_State] = 'Maine' "
             "WHERE [Province_State] LIKE '%, ME%'")
    cursor.execute(Maine)
    conn.commit()
    
    MarshallIslands = ("UPDATE [" + str(x) + "] "
                       "SET [Province_State] = 'Marshall Islands' "
                       "WHERE [Province_State] LIKE '%, MH%'")
    cursor.execute(MarshallIslands)
    conn.commit()
    
    Maryland = ("UPDATE [" + str(x) + "] "
                "SET [Province_State] = 'Maryland' "
                "WHERE [Province_State] LIKE '%, MD%'")
    cursor.execute(Maryland)
    conn.commit()
    
    Massachusetts = ("UPDATE [" + str(x) + "] "
                     "SET [Province_State] = 'Massachusetts' "
                     "WHERE [Province_State] LIKE '%, MA%'")
    cursor.execute(Massachusetts)
    conn.commit()
    
    Michigan = ("UPDATE [" + str(x) + "] "
                "SET [Province_State] = 'Michigan' "
                "WHERE [Province_State] LIKE '%, MI%'")
    cursor.execute(Michigan)
    conn.commit()
    
    Minnesota = ("UPDATE [" + str(x) + "] "
                 "SET [Province_State] = 'Minnesota' "
                 "WHERE [Province_State] LIKE '%, MN%'")
    cursor.execute(Minnesota)
    conn.commit()
    
    Mississippi = ("UPDATE [" + str(x) + "] "
                   "SET [Province_State] = 'Mississippi' "
                   "WHERE [Province_State] LIKE '%, MS%'")
    cursor.execute(Mississippi)
    conn.commit()
    
    Missouri = ("UPDATE [" + str(x) + "] "
                "SET [Province_State] = 'Missouri' "
                "WHERE [Province_State] LIKE '%, MO%'")
    cursor.execute(Missouri)
    conn.commit()
    
    Montana = ("UPDATE [" + str(x) + "] "
               "SET [Province_State] = 'Montana' "
               "WHERE [Province_State] LIKE '%, MT%'")
    cursor.execute(Montana)
    conn.commit()
    
    Nebraska = ("UPDATE [" + str(x) + "] "
                "SET [Province_State] = 'Nebraska' "
                "WHERE [Province_State] LIKE '%, NE%'")
    cursor.execute(Nebraska)
    conn.commit()
    
    Nevada = ("UPDATE [" + str(x) + "] "
              "SET [Province_State] = 'Nevada' "
              "WHERE [Province_State] LIKE '%, NV%'")
    cursor.execute(Nevada)
    conn.commit()
    
    NewHampshire = ("UPDATE [" + str(x) + "] "
                    "SET [Province_State] = 'New Hampshire' "
                    "WHERE [Province_State] LIKE '%, NH%'")
    cursor.execute(NewHampshire)
    conn.commit()
    
    NewJersey = ("UPDATE [" + str(x) + "] "
                 "SET [Province_State] = 'New Jersey' "
                 "WHERE [Province_State] LIKE '%, NJ%'")
    cursor.execute(NewJersey)
    conn.commit()
    
    NewMexico = ("UPDATE [" + str(x) + "] "
                 "SET [Province_State] = 'New Mexico' "
                 "WHERE [Province_State] LIKE '%, NM%'")
    cursor.execute(NewMexico)
    conn.commit()
    
    NewYork = ("UPDATE [" + str(x) + "] "
               "SET [Province_State] = 'New York' "
               "WHERE [Province_State] LIKE '%, NY%'")
    cursor.execute(NewYork)
    conn.commit()
    
    NorthCarolina = ("UPDATE [" + str(x) + "] "
                     "SET [Province_State] = 'North Carolina' "
                     "WHERE [Province_State] LIKE '%, NC%'")
    cursor.execute(NorthCarolina)
    conn.commit()
    
    NorthDakota = ("UPDATE [" + str(x) + "] "
                   "SET [Province_State] = 'North Dakota' "
                   "WHERE [Province_State] LIKE '%, ND%'")
    cursor.execute(NorthDakota)
    conn.commit()
    
    NorthernMarianaIslands = ("UPDATE [" + str(x) + "] "
                              "SET [Province_State] = 'Northern Mariana Islands' "
                              "WHERE [Province_State] LIKE '%, NP%'")
    cursor.execute(NorthernMarianaIslands)
    conn.commit()
    
    Ohio = ("UPDATE [" + str(x) + "] "
            "SET [Province_State] = 'Ohio' "
            "WHERE [Province_State] LIKE '%, OH%'")
    cursor.execute(Ohio)
    conn.commit()
    
    Oklahoma = ("UPDATE [" + str(x) + "] "
                "SET [Province_State] = 'Oklahoma' "
                "WHERE [Province_State] LIKE '%, OK%'")
    cursor.execute(Oklahoma)
    conn.commit()
    
    Oregon = ("UPDATE [" + str(x) + "] "
              "SET [Province_State] = 'Oregon' "
              "WHERE [Province_State] LIKE '%, OR%'")
    cursor.execute(Oregon)
    conn.commit()
    
    Palau = ("UPDATE [" + str(x) + "] "
             "SET [Province_State] = 'Palau' "
             "WHERE [Province_State] LIKE '%, PW%'")
    cursor.execute(Palau)
    conn.commit()
    
    Pennsylvania = ("UPDATE [" + str(x) + "] "
                    "SET [Province_State] = 'Pennsylvania' "
                    "WHERE [Province_State] LIKE '%, PA%'")
    cursor.execute(Pennsylvania)
    conn.commit()
    
    PuertoRico = ("UPDATE [" + str(x) + "] "
                  "SET [Province_State] = 'Puerto Rico' "
                  "WHERE [Province_State] LIKE '%, PR%'")
    cursor.execute(PuertoRico)
    conn.commit()
    
    RhodeIsland = ("UPDATE [" + str(x) + "] "
                   "SET [Province_State] = 'Rhode Island' "
                   "WHERE [Province_State] LIKE '%, RI%'")
    cursor.execute(RhodeIsland)
    conn.commit()
    
    SouthCarolina = ("UPDATE [" + str(x) + "] "
                     "SET [Province_State] = 'South Carolina' "
                     "WHERE [Province_State] LIKE '%, SC%'")
    cursor.execute(SouthCarolina)
    conn.commit()
    
    SouthDakota = ("UPDATE [" + str(x) + "] "
                   "SET [Province_State] = 'South Dakota' "
                   "WHERE [Province_State] LIKE '%, SD%'")
    cursor.execute(SouthDakota)
    conn.commit()
    
    Tennessee = ("UPDATE [" + str(x) + "] "
                 "SET [Province_State] = 'Tennessee' "
                 "WHERE [Province_State] LIKE '%, TN%'")
    cursor.execute(Tennessee)
    conn.commit()
    
    Texas = ("UPDATE [" + str(x) + "] "
             "SET [Province_State] = 'Texas' "
             "WHERE [Province_State] LIKE '%, TX%'")
    cursor.execute(Texas)
    conn.commit()
    
    Utah = ("UPDATE [" + str(x) + "] "
            "SET [Province_State] = 'Utah' "
            "WHERE [Province_State] LIKE '%, UT%'")
    cursor.execute(Utah)
    conn.commit()
    
    Vermont = ("UPDATE [" + str(x) + "] "
               "SET [Province_State] = 'Vermont' "
               "WHERE [Province_State] LIKE '%, VT%'")
    cursor.execute(Vermont)
    conn.commit()
    
    VirginIslands = ("UPDATE [" + str(x) + "] "
                     "SET [Province_State] = 'Virgin Islands' "
                     "WHERE [Province_State] LIKE '%, VI%'")
    cursor.execute(VirginIslands)
    conn.commit()
    
    Virginia = ("UPDATE [" + str(x) + "] "
                "SET [Province_State] = 'Virginia' "
                "WHERE [Province_State] LIKE '%, VA%'")
    cursor.execute(Virginia)
    conn.commit()
    
    Washington = ("UPDATE [" + str(x) + "] "
                  "SET [Province_State] = 'Maryland' "
                  "WHERE [Province_State] LIKE '%, WA%'")
    cursor.execute(Washington)
    conn.commit()
    
    WestVirginia = ("UPDATE [" + str(x) + "] "
                    "SET [Province_State] = 'West Virginia' "
                    "WHERE [Province_State] LIKE '%, WV%'")
    cursor.execute(WestVirginia)
    conn.commit()
    
    Wisconsin = ("UPDATE [" + str(x) + "] "
                 "SET [Province_State] = 'Wisconsin' "
                 "WHERE [Province_State] LIKE '%, WI%'")
    cursor.execute(Wisconsin)
    conn.commit()
    
    Wyoming = ("UPDATE [" + str(x) + "] "
               "SET [Province_State] = 'Wyoming' "
               "WHERE [Province_State] LIKE '%, WY%'")
    cursor.execute(Wyoming)
    conn.commit()
  
    # create a temporary table
    temp = ("CREATE TABLE temp "
            "(FIPS TEXT, "
            "Admin2 TEXT, "
            "[Province_State] TEXT, "
            "[Country_Region] TEXT, "
            "[Last_Update] REAL, "
            "Lat REAL, "
            "Long_ REAL, "
            "Confirmed REAL, "
            "Deaths REAL, "
            "Recovered REAL, "
            "Active REAL, "
            "[Combined_Key] TEXT)")
    cursor.execute(temp)
    conn.commit()
    
    # insert all state names into the temporary table 
    StoreState = ("INSERT INTO temp (Province_State) "
                  "SELECT [State_Name] "
                  "FROM statenames")
    cursor.execute(StoreState)
    conn.commit()
    
    # delete state names in the temporary table that do not exist also in the table
    ClearColumn = ("DELETE FROM temp "
                   "WHERE NOT EXISTS "
                   "(SELECT NULL FROM [" + str(x) + "] "
                   "WHERE [" + str(x) + "].[Province_State] = temp.[Province_State])")
    cursor.execute(ClearColumn)
    conn.commit()
    
    # add country value (US) to the temporary table from the table
    StoreCountry = ("UPDATE temp "
                    "SET [Country_Region] = 'US' "
                    "WHERE [Province_State] IS NOT NULL")
    cursor.execute(StoreCountry)
    conn.commit()

    # add country value (US) to the temporary table from the table
    StoreConfirmed = ("UPDATE temp "
                      "SET [Confirmed] = (SELECT DISTINCT SUM(Confirmed) "
                      "FROM [" + str(x) + "] "
                      "WHERE [Province_State] = temp.[Province_State]"
                      "GROUP BY [Province_State])")
    cursor.execute(StoreConfirmed)
    conn.commit()
    
    StoreDeaths = ("UPDATE temp "
                      "SET [Deaths] = (SELECT DISTINCT SUM(Deaths) "
                      "FROM [" + str(x) + "] "
                      "WHERE [Province_State] = temp.[Province_State]"
                      "GROUP BY [Province_State])")
    cursor.execute(StoreDeaths)
    conn.commit()
    
    StoreRecovered = ("UPDATE temp "
                      "SET [Recovered] = (SELECT DISTINCT SUM(Recovered) "
                      "FROM [" + str(x) + "] "
                      "WHERE [Province_State] = temp.[Province_State]"
                      "GROUP BY [Province_State])")
    cursor.execute(StoreRecovered)
    conn.commit()
    
    # delete all rows of the US
    DeleteRows = ("DELETE FROM [" + str(x) + "] "
                  "WHERE [Country_Region] = 'US'")
    cursor.execute(DeleteRows)
    conn.commit()
    
    # import rows from the temporary table to the original table
    AddSums = ("INSERT INTO [" + str(x) + "] "
               "SELECT * FROM temp")
    cursor.execute(AddSums)
    conn.commit()
    
    # update latitude with the state value            
    AddLatitude = ("UPDATE [" + str(x) + "] "
                   "SET Lat = (SELECT Lat "
                   "FROM [03-31-2020] "
                   "WHERE [" + str(x) + "].[Province_State] = [03-31-2020].[Province_State])")
    cursor.execute(AddLatitude)
    conn.commit()
    
    # update longitude with the state value
    AddLongitude = ("UPDATE [" + str(x) + "] "
                   "SET Long_ = (SELECT Long_ "
                   "FROM [03-31-2020] "
                   "WHERE [" + str(x) + "].[Province_State] = [03-31-2020].[Province_State])")
    cursor.execute(AddLongitude)
    conn.commit()
    
    # delete the temporary table
    DeleteTemp = "DROP TABLE temp"
    cursor.execute(DeleteTemp)
    conn.commit()


# In[7]:


# create a list of tables for Canada row aggregation (January 22 to March 9)
start = datetime.datetime.strptime("01-22-2020", "%m-%d-%Y").date()
end = datetime.datetime.strptime("03-09-2020", "%m-%d-%Y").date()
dates4 = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]
dates4 = ['{}-{}-{}'.format(m,d,y) for y, m, d in map(lambda x: str(x).split('-'), dates4)]


# # !!!!!!! Don't forget to refresh the acronyms and statenames in database with R (use Correspondence.ipynb section 2) !!!!!!!

# In[8]:


# aggregate rows for the same state of all tables in loop
for x in dates4:

    # convert cells that contains (, [Acronym]) to ([State_Name])
    Alberta = ("UPDATE [" + str(x) + "] "
               "SET [Province_State] = 'Alberta' "
               "WHERE [Province_State] LIKE '%, AB%'")
    cursor.execute(Alberta)
    conn.commit()

    BritishColumbia = ("UPDATE [" + str(x) + "] "
                       "SET [Province_State] = 'British Columbia' "
                       "WHERE [Province_State] LIKE '%, BC%'")
    cursor.execute(BritishColumbia)
    conn.commit()

    Manitoba = ("UPDATE [" + str(x) + "] "
                "SET [Province_State] = 'Manitoba' "
                "WHERE [Province_State] LIKE '%, MB%'")
    cursor.execute(Manitoba)
    conn.commit()

    NewBrunswick = ("UPDATE [" + str(x) + "] "
                    "SET [Province_State] = 'New Brunswick' "
                    "WHERE [Province_State] LIKE '%, NB%'")
    cursor.execute(NewBrunswick)
    conn.commit()

    NewfoundlandandLabrador = ("UPDATE [" + str(x) + "] "
                               "SET [Province_State] = 'Newfoundland and Labrador' "
                               "WHERE [Province_State] LIKE '%, NL%'")
    cursor.execute(NewfoundlandandLabrador)
    conn.commit()

    NorthwestTerritories = ("UPDATE [" + str(x) + "] "
                            "SET [Province_State] = 'Northwest Territories' "
                            "WHERE [Province_State] LIKE '%, NT%'")
    cursor.execute(NorthwestTerritories)
    conn.commit()

    NovaScotia = ("UPDATE [" + str(x) + "] "
                  "SET [Province_State] = 'Nova Scotia' "
                  "WHERE [Province_State] LIKE '%, NS%'")
    cursor.execute(NovaScotia)
    conn.commit()

    Nunavut = ("UPDATE [" + str(x) + "] "
               "SET [Province_State] = 'Nunavut' "
               "WHERE [Province_State] LIKE '%, NU%'")
    cursor.execute(Nunavut)
    conn.commit()

    Ontario = ("UPDATE [" + str(x) + "] "
               "SET [Province_State] = 'Ontario' "
               "WHERE [Province_State] LIKE '%, ON%'")
    cursor.execute(Ontario)
    conn.commit()

    PrinceEdwardIsland = ("UPDATE [" + str(x) + "] "
                          "SET [Province_State] = 'Prince Edward Island' "
                          "WHERE [Province_State] LIKE '%, PE%'")
    cursor.execute(PrinceEdwardIsland)
    conn.commit()

    Quebec = ("UPDATE [" + str(x) + "] "
              "SET [Province_State] = 'Quebec' "
              "WHERE [Province_State] LIKE '%, QC%'")
    cursor.execute(Quebec)
    conn.commit()

    Saskatchewan = ("UPDATE [" + str(x) + "] "
                    "SET [Province_State] = 'Saskatchewan' "
                    "WHERE [Province_State] LIKE '%, SK%'")
    cursor.execute(Saskatchewan)
    conn.commit()

    Yukon = ("UPDATE [" + str(x) + "] "
             "SET [Province_State] = 'Yukon' "
             "WHERE [Province_State] LIKE '%, YT%'")
    cursor.execute(Yukon)
    conn.commit()

    # create a temporary table
    temp = ("CREATE TABLE temp "
            "(FIPS TEXT, "
            "Admin2 TEXT, "
            "[Province_State] TEXT, "
            "[Country_Region] TEXT, "
            "[Last_Update] REAL, "
            "Lat REAL, "
            "Long_ REAL, "
            "Confirmed REAL, "
            "Deaths REAL, "
            "Recovered REAL, "
            "Active REAL, "
            "[Combined_Key] TEXT)")
    cursor.execute(temp)
    conn.commit()
    
    # insert all state names into the temporary table 
    StoreState = ("INSERT INTO temp (Province_State) "
                  "SELECT [State_Name] "
                  "FROM statenames")
    cursor.execute(StoreState)
    conn.commit()
    
    # delete state names in the temporary table that do not exist also in the table
    ClearColumn = ("DELETE FROM temp "
                   "WHERE NOT EXISTS "
                   "(SELECT NULL FROM [" + str(x) + "] "
                   "WHERE [" + str(x) + "].[Province_State] = temp.[Province_State])")
    cursor.execute(ClearColumn)
    conn.commit()

    # add country value (Canada) to the temporary table from the table
    StoreCountry = ("UPDATE temp "
                    "SET [Country_Region] = 'Canada' "
                    "WHERE [Province_State] IS NOT NULL")
    cursor.execute(StoreCountry)
    conn.commit()


    # add country value (Canada) to the temporary table from the table
    StoreConfirmed = ("UPDATE temp "
                      "SET [Confirmed] = (SELECT DISTINCT SUM(Confirmed) "
                      "FROM [" + str(x) + "] "
                      "WHERE [Province_State] = temp.[Province_State]"
                      "GROUP BY [Province_State])")
    cursor.execute(StoreConfirmed)
    conn.commit()
    
    StoreDeaths = ("UPDATE temp "
                      "SET [Deaths] = (SELECT DISTINCT SUM(Deaths) "
                      "FROM [" + str(x) + "] "
                      "WHERE [Province_State] = temp.[Province_State]"
                      "GROUP BY [Province_State])")
    cursor.execute(StoreDeaths)
    conn.commit()
    
    StoreRecovered = ("UPDATE temp "
                      "SET [Recovered] = (SELECT DISTINCT SUM(Recovered) "
                      "FROM [" + str(x) + "] "
                      "WHERE [Province_State] = temp.[Province_State]"
                      "GROUP BY [Province_State])")
    cursor.execute(StoreRecovered)
    conn.commit()
    
    # delete all rows of the US
    DeleteRows = ("DELETE FROM [" + str(x) + "] "
                  "WHERE [Country_Region] = 'Canada'")
    cursor.execute(DeleteRows)
    conn.commit()
    
    # import rows from the temporary table to the original table
    AddSums = ("INSERT INTO [" + str(x) + "] "
               "SELECT * FROM temp")
    cursor.execute(AddSums)
    conn.commit()
    
    # update latitude with the state value            
    AddLatitude = ("UPDATE [" + str(x) + "] "
                   "SET Lat = (SELECT Lat "
                   "FROM [03-31-2020] "
                   "WHERE [" + str(x) + "].[Province_State] = [03-31-2020].[Province_State])")
    cursor.execute(AddLatitude)
    conn.commit()
    
    # update longitude with the state value
    AddLongitude = ("UPDATE [" + str(x) + "] "
                   "SET Long_ = (SELECT Long_ "
                   "FROM [03-31-2020] "
                   "WHERE [" + str(x) + "].[Province_State] = [03-31-2020].[Province_State])")
    cursor.execute(AddLongitude)
    conn.commit()
    
    # delete the temporary table
    DeleteTemp = "DROP TABLE temp"
    cursor.execute(DeleteTemp)
    conn.commit()


# In[9]:


conn.close()

