### Data Processing in R and Python 2022Z
### Homework Assignment no. 2
###
### IMPORTANT
### This file should contain only solutions to tasks in the form of a functions
### definitions and comments to the code.
###
import pandas as pd
import numpy as np
import os, os.path
import sqlite3
import tempfile
from IPython.display import display

Badges = pd.read_csv("./travel_stackexchange/Badges.csv.gz", compression="gzip")
Comments = pd.read_csv("./travel_stackexchange/Comments.csv.gz", compression="gzip")
Posts = pd.read_csv("./travel_stackexchange/Posts.csv.gz", compression="gzip")
Users = pd.read_csv("./travel_stackexchange/Users.csv.gz", compression="gzip")
Votes = pd.read_csv("./travel_stackexchange/Votes.csv.gz", compression="gzip")
# path to database file
baza = os.path.join(tempfile.mkdtemp(), 'example.db')
if os.path.isfile(baza):  # if this file already exists...
    os.remove(baza)  # ...we will remove it
conn = sqlite3.connect(baza)  # create the connection
Badges.to_sql("Badges", conn)  # import the data frame into the database
Comments.to_sql("Comments", conn)
Posts.to_sql("Posts", conn)
Users.to_sql("Users", conn)
Votes.to_sql("Votes", conn)

#
# -----------------------------------------------------------------------------#
# Task 1
# -----------------------------------------------------------------------------#
##
def sql_1(Posts):
    return pd.read_sql_query("""
    SELECT STRFTIME('%Y', CreationDate) AS Year, COUNT(*) AS TotalNumber
    FROM Posts
    GROUP BY Year""",
                             conn)

def solution_1(Posts):
    Result = Posts.copy()
    Result["CreationDate"] = Result["CreationDate"].str.slice(0, 4)
    Result = Result.groupby("CreationDate", as_index=False).size()
    Result.columns = ['Year', 'TotalNumber']
    return Result

mysol = solution_1(Posts)
print(mysol.equals(sql_1(Posts)))

##
# -----------------------------------------------------------------------------#
# Task 2
# -----------------------------------------------------------------------------#

def sql_2(Posts):
    return pd.read_sql_query("""
    SELECT Id, DisplayName, SUM(ViewCount) AS TotalViews
    FROM Users
    JOIN (
    SELECT OwnerUserId, ViewCount FROM Posts WHERE PostTypeId = 1
    ) AS Questions
    ON Users.Id = Questions.OwnerUserId
    GROUP BY Id
    ORDER BY TotalViews DESC
    LIMIT 10""",
                             conn)

def solution_2(Posts):
    Questions = Posts.copy()
    Questions = Questions.loc[Questions["PostTypeId"] == 1]
    Questions = Questions.drop(['Id', 'CreationDate'], axis=1)
    Result = Users
    Result = pd.merge(Result, Questions, left_on="Id", right_on="OwnerUserId", how="left")
    Result = Result.groupby(['Id', 'DisplayName']).agg({'ViewCount': ['sum']}).reset_index()
    Result.columns = ['Id', 'DisplayName', 'TotalViews']
    Result.sort_values(by='TotalViews', ascending=False, inplace=True, ignore_index=True)
    return Result.head(10)

mysol = solution_2(Posts)
print(mysol.equals(sql_2(Posts)))

# -----------------------------------------------------------------------------#
# Task 3
# -----------------------------------------------------------------------------#
##
def sql_3(Posts):
    return pd.read_sql_query("""
      SELECT Year, Name, MAX((Count * 1.0) / CountTotal) AS MaxPercentage
        FROM (
        SELECT BadgesNames.Year, BadgesNames.Name, BadgesNames.Count, BadgesYearly.CountTotal
        FROM (
        SELECT Name, COUNT(*) AS Count, STRFTIME('%Y', Badges.Date) AS Year
        FROM Badges
        GROUP BY Name, Year
        ) AS BadgesNames
        JOIN (
        SELECT COUNT(*) AS CountTotal, STRFTIME('%Y', Badges.Date) AS Year
        FROM Badges
        GROUP BY YEAR
        ) AS BadgesYearly
        ON BadgesNames.Year = BadgesYearly.Year
        )
        GROUP BY Year
    """,
                             conn)

def solution_3(Posts):
    BadgesNames = Badges.copy()
    BadgesNames["Date"] = BadgesNames["Date"].str.slice(0, 4)
    BadgesNames = BadgesNames.groupby(['Name', 'Date']).agg({'Date': ['size']}).reset_index()
    BadgesNames.columns = ['Name', 'Year', 'Count']
    BadgesNames['Year'] = pd.to_numeric(BadgesNames['Year'])

    BadgesYearly = Badges
    BadgesYearly["Date"] = BadgesYearly["Date"].str.slice(0, 4)
    BadgesYearly = BadgesYearly.groupby(['Date']).agg({'Date': ['size']}).reset_index()
    BadgesYearly.columns = ['Year', 'CountTotal']
    BadgesYearly['Year'] = pd.to_numeric(BadgesYearly['Year'])

    Result = pd.merge(BadgesNames, BadgesYearly, on='Year', how='left')
    Result = Result.loc[:, ['Year', 'Name', 'Count', 'CountTotal']]
    Result_to_merge = Result.groupby(['Year']).agg({'Count': ['max']}).reset_index()
    Result_to_merge.columns = ['Year', 'Count']
    Result = pd.merge(Result_to_merge, Result, on=['Year', 'Count'], how='left')
    Result['MaxPercentage'] = Result.apply(lambda row: row.Count / row.CountTotal, axis=1)
    Result = Result.loc[:, ['Year', 'Name', 'MaxPercentage']]
    Result['Year'] = Result['Year'].map(str)
    return Result

mysol = solution_3(Posts)
print(mysol.equals(sql_3(Posts)))

# -----------------------------------------------------------------------------#
# Task 4
# -----------------------------------------------------------------------------#
#SQL query is modified a bit, since LIMIT function causes this function to not
#compile, it just idles my computer. I tried looking why is that, but didn't find
#any solution.
##
def sql_4(Posts):
    return pd.read_sql_query("""
SELECT Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location
FROM (
SELECT Posts.OwnerUserId, Posts.Title, Posts.CommentCount, Posts.ViewCount,
CmtTotScr.CommentsTotalScore
FROM (
SELECT PostId, SUM(Score) AS CommentsTotalScore
FROM Comments
GROUP BY PostId
) AS CmtTotScr
JOIN Posts ON Posts.Id = CmtTotScr.PostId
WHERE Posts.PostTypeId=1
) AS PostsBestComments
JOIN Users ON PostsBestComments.OwnerUserId = Users.Id
WHERE CommentsTotalScore>=328
ORDER BY CommentsTotalScore DESC
""",
                             conn)


def solution_4(Posts):
    CmtToScr = Comments.copy()
    CmtToScr = CmtToScr.groupby(['PostId']).agg({'Score': ['sum']}).reset_index()
    CmtToScr.columns = ['PostId', 'CommentsTotalScore']

    PostsBestComments = Posts
    PostsBestComments = PostsBestComments.loc[PostsBestComments['PostTypeId'] == 1]
    PostsBestComments = pd.merge(CmtToScr, PostsBestComments, left_on='PostId', right_on='Id', how='inner')
    PostsBestComments = PostsBestComments.loc[:,
                        ['OwnerUserId', 'Title', 'CommentCount', 'ViewCount', 'CommentsTotalScore']]

    Result = Users
    Result = pd.merge(Result, PostsBestComments, left_on='Id', right_on='OwnerUserId', how="inner")
    Result = Result.loc[:,
             ['Title', 'CommentCount', 'ViewCount', 'CommentsTotalScore', 'DisplayName', 'Reputation', 'Location']]
    Result.sort_values(by='CommentsTotalScore', ascending=False, inplace=True, na_position='last', ignore_index=True)
    return Result[0:10]

mysol = solution_4(Posts)
print(mysol.equals(sql_4(Posts)))

##
# -----------------------------------------------------------------------------#
# Task 5
# -----------------------------------------------------------------------------#
##
#Partially python is used, since both LIMIT and ORDER BY don't want to compile, it takes ages and
#doesn't produce any results.
def sql_5(Posts):
    Result = pd.read_sql_query("""
    SELECT Posts.Title, STRFTIME('%Y-%m-%d', Posts.CreationDate) AS Date, VotesByAge.*
    FROM Posts
    JOIN (
    SELECT PostId,
    MAX(CASE WHEN VoteDate = 'before' THEN Total ELSE 0 END) BeforeCOVIDVotes,
    MAX(CASE WHEN VoteDate = 'during' THEN Total ELSE 0 END) DuringCOVIDVotes,
    MAX(CASE WHEN VoteDate = 'after' THEN Total ELSE 0 END) AfterCOVIDVotes,
    SUM(Total) AS Votes
    FROM (
    SELECT PostId,
    CASE STRFTIME('%Y', CreationDate)
    WHEN '2022' THEN 'after'
    WHEN '2021' THEN 'during'
    WHEN '2020' THEN 'during'
    WHEN '2019' THEN 'during'
    ELSE 'before'
    END VoteDate, COUNT(*) AS Total
    FROM Votes
    WHERE VoteTypeId IN (3, 4, 12)
    GROUP BY PostId, VoteDate
    ) AS VotesDates
    GROUP BY VotesDates.PostId
    ) AS VotesByAge ON Posts.Id = VotesByAge.PostId
    WHERE Title NOT IN ('') AND DuringCOVIDVotes > 0
    """,
                                 conn)
    Result.sort_values(by=['DuringCOVIDVotes', 'Votes'], ascending=False, inplace=True, ignore_index=True)
    return Result.head(20)

##
def Date(value):
    if value == 2022:
        return 'after'
    if 2021 >= value >= 2019:
        return 'during'
    else:
        return 'before'


def solution_5(Posts):
    VotesDates = Votes.copy()
    VotesDates = VotesDates.loc[VotesDates['VoteTypeId'].isin([3, 4, 12])].reset_index()
    VotesDates['CreationDate'] = VotesDates['CreationDate'].str.slice(0, 4)
    VotesDates["CreationDate"] = VotesDates["CreationDate"].astype(int)
    VotesDates['VoteDate'] = VotesDates['CreationDate'].map(Date)
    VotesDates['Total'] = 0
    VotesDates = VotesDates.groupby(by=['PostId', 'VoteDate']).agg({'Total': ['size']}).reset_index()
    VotesDates.columns = ['PostId', 'VoteDate', 'Total']

    VotesByAge = VotesDates.copy()
    VotesByAge['BeforeCOVIDVotes'] = np.where(VotesByAge['VoteDate'] == 'before', VotesByAge['Total'], 0)
    VotesByAge['DuringCOVIDVotes'] = np.where(VotesByAge['VoteDate'] == 'during', VotesByAge['Total'], 0)
    VotesByAge['AfterCOVIDVotes'] = np.where(VotesByAge['VoteDate'] == 'after', VotesByAge['Total'], 0)
    VotesByAge = VotesByAge.groupby(by=['PostId']).agg(
        {'BeforeCOVIDVotes': 'max', 'DuringCOVIDVotes': 'max', 'AfterCOVIDVotes': 'max', 'Total': 'sum'}).reset_index()
    VotesByAge.rename(columns={"Total": "Votes"}, inplace=True)

    Result = VotesByAge.copy()
    Result = pd.merge(left=Posts, right=Result, left_on='Id', right_on='PostId', how='inner')
    Result = Result.dropna(subset=["Title"]).reset_index()
    Result = Result.loc[Result['DuringCOVIDVotes'] > 0].reset_index()
    Result['CreationDate'] = Result['CreationDate'].str.slice(0, 10)
    Result.rename(columns={'CreationDate': 'Date'}, inplace=True)
    Result.sort_values(by=['DuringCOVIDVotes', 'Votes'], ascending=False, inplace=True, ignore_index=True)
    return Result.loc[:, ['Title', 'Date', 'PostId', 'BeforeCOVIDVotes', 'DuringCOVIDVotes', 'AfterCOVIDVotes', 'Votes']]\
        .head(20)

mysol = solution_5(Posts)
print(mysol.equals(sql_5(Posts)))