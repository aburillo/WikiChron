#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
   stats.py

   Descp:

   Created on: 14-nov-2017

   Copyright 2017-2018 Abel 'Akronix' Serrano Juste <akronix5@gmail.com>
"""

import pandas as pd
import numpy as np
import math

# CONSTANTS
MINIMAL_USERS_GINI = 20
MINIMAL_USERS_PERCENTIL_MAX_5 = 100
MINIMAL_USERS_PERCENTIL_MAX_10 = 50
MINIMAL_USERS_PERCENTIL_MAX_20 = 25
MINIMAL_USERS_PERCENTIL_5_10 = 100
MINIMAL_USERS_PERCENTIL_10_20 = 50
MINIMAL_USERS_RATIO_10_90 = 10


def calculate_index_all_months(data):
    monthly_data = data.groupby(pd.Grouper(key='timestamp', freq='MS'))
    index = monthly_data.size().index
    return index

# Pages


def pages_new(data, index):
    # We use the fact that data is sorted first by page_title and them by revision_id
    # If we drop publicates we will get the first revision for each page_title, which
    #  corresponds with the date it was created.
    pages = data.drop_duplicates('page_id')
    series = pages.groupby(pd.Grouper(key='timestamp', freq='MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


def pages_accum(data, index):
    return (pages_new(data, index).cumsum())


def pages_main_new(data, index):
    pages = data.drop_duplicates('page_id')
    main_pages = pages[pages['page_ns'] == 0]
    series = main_pages.groupby(pd.Grouper(key='timestamp', freq='MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


def pages_main_accum(data, index):
    return (pages_main_new(data, index).cumsum())


def pages_edited(data, index):
    month_data = data.groupby([pd.Grouper(key='timestamp', freq='MS')])
    series = monthly_data.apply(lambda x: len(x.page_id.unique()))
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


def main_edited(data, index):
    main_pages = data[data['page_ns'] == 0]
    monthly_data = main_pages.groupby([pd.Grouper(key='timestamp', freq='MS')])
    series = monthly_data.apply(lambda x: len(x.page_id.unique()))
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series

########################################################################

# Editions


def edits(data, index):
    monthly_data = data.groupby(pd.Grouper(key='timestamp', freq='MS'))
    series = monthly_data.size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


def edits_accum(data, index):
    return (edits(data, index).cumsum())


def edits_main_content(data, index):
    edits_main_data = data[data['page_ns'] == 0]
    return (edits(edits_main_data, index))


def edits_main_content_accum(data, index):
    return (edits_main_content(data, index).cumsum())


def edits_article_talk(data, index):
    edits_talk_data = data[data['page_ns'] == 1]
    return (edits(edits_talk_data, index))


def edits_user_talk(data, index):
    edits_talk_data = data[data['page_ns'] == 3]
    return (edits(edits_talk_data, index))

########################################################################

# Users

#users_registered_active according to version 2 definition (active -> more than 4 edits)
def users_registered_active_2(data,index):
    users_registered = data[data['contributor_name'] != 'Anonymous']
# get the number of edits each user has done each month
    monthly_edits = users_registered.groupby([pd.Grouper(key='timestamp', freq='MS'), 'contributor_name']).size()
# filter users with number >= requested, and make timestamp and contributor_name to be COLUMNS, instead of part of the index
    monthly_edits_filtered = monthly_edits[monthly_edits > 4].to_frame(name='pages_edited').reset_index()
# get how many users with >=5 editions exist for each month
    series = monthly_edits_filtered.groupby(pd.Grouper(key='timestamp', freq='MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series

#users who make their second edition in the wiki (we want the count for this kind of users per month)
def users_reincident(data, index):
    data['test_duplicated'] = data['contributor_id'].duplicated()
    users_reincident = data[data['test_duplicated'] == True]
#determine in which month each user performed their second edition-> can be the same month as the first one
#1) get number of editions per month for every user
    users_reincident = users_reincident.groupby(['contributor_id', pd.Grouper(key='timestamp', freq='MS')]).size().to_frame('edits_count').reset_index()
#2) get the accum. number of edits per user each month
    users_reincident['accum_edit_count'] = users_reincident.groupby('contributor_id')['edits_count'].transform(lambda x: x.cumsum())
#3) drop rows in which the accum_edit_count is less than 2
    users_reincident = users_reincident[users_reincident['accum_edit_count'] > 1]
#4) now, we just want the first month in which the user became reincident: (drop_duplicates drops all rows but first, so as it is sorted, for sure we will keep the first month)
    users_reincident = users_reincident.drop_duplicates('contributor_id')
#5) group by timestamp and get the count of reincident users per month
    series = users_reincident.groupby(pd.Grouper(key='timestamp', freq='MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series

#metric which gets the monthly count of users (registered or not) who have contributed in more than 4 editions to the wiki, during that month-> same as users_registered_active_2, but counting anonymous users
def users_active_more_than_4(data, index):
    monthly = data.groupby(pd.Grouper(key='timestamp', freq = 'MS'))
    series = monthly.apply(lambda x: len(x.groupby(['contributor_id']).size().where(lambda y: y>4).dropna()))
    return series

#users active according to version one: those who have contributed to the wiki in one edition during a month (anonymous are included).
def users_active(data, index):
    monthly_data = data.groupby(pd.Grouper(key='timestamp', freq='MS'))
# per each month, get rid  of duplicated contributor_ids and count.
    series = monthly_data.apply(lambda x: len(x.contributor_id.unique()))
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series

#this metric is the same as the users_active, but getting rid of anonymous users	
def users_registered_active(data,index):
# get rid of anonymous users and procceed as it was done in the previous metric.
    user_registered=data[data['contributor_name']!='Anonymous']
    monthly_data_registered=user_registered.groupby(pd.Grouper(key='timestamp', freq='MS'))
    series = monthly_data_registered.apply(lambda x: len(x.contributor_id.unique()))
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series

# this metric is the complementary to users_registered_active: now, we get rid of registered users and focus on anonymous users.
def users_anonymous_active(data,index):
    user_registered=data[data['contributor_name']=='Anonymous']
    monthly_data_anonymous=user_registered.groupby(pd.Grouper(key='timestamp', freq='MS'))
    series = monthly_data_anonymous.apply(lambda x: len(x.contributor_id.unique()))
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series

# this metric gets the users whose last edition is less than three months old (for each month)
def users_less_than_three_months(data, index):
    mothly = data.groupby(pd.Grouper(key = 'timestamp', freq = 'MS'))
    mothly_edits_users = mothly.apply(lambda x: x.contributor_id.unique()).to_frame('edits_users')
    i = len(mothly_edits_users)-1
    edits_users_three_months_old = []
    while i > 1:
        length_one_two_three_month = len(list(set(np.concatenate(np.array(mothly_edits_users.iloc[i-2:i+1, 0])))))
        edits_users_three_months_old.append(length_one_two_three_month)
        i = i -1

    length_one_two_month = len(list(set(np.concatenate((np.array(mothly_edits_users.iloc[0:2, 0])), axis=0))))
    edits_users_three_months_old.append(length_one_two_month)
    edits_users_three_months_old.append(len(list(mothly_edits_users.iloc[0].edits_users)))
    #reverse order of the list
    edits_users_three_months_old = list(reversed(edits_users_three_months_old))               
    #create a new column in the dataframe
    mothly_edits_users['edits_users_three_months_old']=edits_users_three_months_old
    mothly_edits_users = pd.Series(mothly_edits_users.edits_users_three_months_old, index = mothly_edits_users.index.values)
    return mothly_edits_users

# this metric gets the users whose last edition is more than three months old (for each month)
def users_more_than_three_months(data,index):
    mothly = data.groupby(pd.Grouper(key = 'timestamp', freq = 'MS'))
    mothly_edits_users = mothly.apply(lambda x: x.contributor_id.unique()).to_frame('edits_users')
    i = len(mothly_edits_users)-1
    edits_users_three_months_old = []
    while i > 1:
        more_than_three_months = np.concatenate(np.array(mothly_edits_users.iloc[0:i-1,0]))
        less_than_three_months = np.concatenate(np.array(mothly_edits_users.iloc[i-1:i+1,0]))
        list_valid=np.setdiff1d(more_than_three_months, less_than_three_months)
        length_list_valid= len(list_valid)
        edits_users_three_months_old.append(length_list_valid)
        i = i -1
    edits_users_three_months_old = edits_users_three_months_old + [0,0]

    #reverse order of the list
    edits_users_three_months_old = list(reversed(edits_users_three_months_old))
    #create a new column in the dataframe
    mothly_edits_users['edits_users_three_months_old']=edits_users_three_months_old
    mothly_edits_users = pd.Series(mothly_edits_users.edits_users_three_months_old, index = mothly_edits_users.index.values)
    return mothly_edits_users

# this metric gets the users whose last edition is more than 6 months old (for each month)
def edits_users_more_than_six_months(data,index):
    mothly = data.groupby(pd.Grouper(key = 'timestamp', freq = 'MS'))
    mothly_edits_users = mothly.apply(lambda x: x.contributor_id.unique()).to_frame('edits_users')
    i = len(mothly_edits_users)-1
    edits_users_six_months_old = []
    while i > 5:
        more_than_six_months = np.concatenate(np.array(mothly_edits_users.iloc[0:i-5,0]))
        less_than_six_months = np.concatenate(np.array(mothly_edits_users.iloc[i-5:i+1,0]))
        list_valid=np.setdiff1d(more_than_six_months, less_than_six_months)
        length_list_valid= len(list_valid)
        edits_users_six_months_old.append(length_list_valid)
        i = i -1
    edits_users_six_months_old = edits_users_six_months_old + [0,0,0,0,0,0]
    #reverse order of the list
    edits_users_six_months_old = list(reversed(edits_users_six_months_old))
    #create a new column in the dataframe
    mothly_edits_users['edits_users_six_months_old']=edits_users_six_months_old
    mothly_edits_users = pd.Series(mothly_edits_users.edits_users_six_months_old, index = mothly_edits_users.index.values)
    if index is not None:
        mothly_edits_users = mothly_edits_users.reindex(index, fill_value=0)
    return mothly_edits_users

#get the number of users, per each month, whose last edit was between < 3 and >= 6 months ago	
def edits_users_between_three_six_months(data, index):
    mothly = data.groupby(pd.Grouper(key = 'timestamp', freq = 'MS'))
    mothly_edits_users = mothly.apply(lambda x: x.contributor_id.unique()).to_frame('edits_users')
    i = len(mothly_edits_users)-1
    edits_users_three_six_months_old = []
    while i > 2: 
        less_than_three = np.concatenate(np.array(mothly_edits_users.iloc[i-2:i+1,0]))
        if i >5:
            four_six = np.concatenate(np.array(mothly_edits_users.iloc[i-5:i-2,0]))
            more_than_six = np.concatenate(np.array(mothly_edits_users.iloc[0:i-5,0]))
            less_than_three_and_more_than_six = np.concatenate((more_than_six, less_than_three), axis=0)    
        else:
            four_six = np.concatenate(np.array(mothly_edits_users.iloc[0:i-2,0]))
            less_than_three_and_more_than_six = less_than_three
        list_valid=np.setdiff1d(four_six, less_than_three_and_more_than_six)
        length_list_valid= len(list_valid)
        edits_users_three_six_months_old.append(length_list_valid)
        i = i -1 
    edits_users_three_six_months_old = edits_users_three_six_months_old + [0,0,0]
    #reverse order of the list
    edits_users_three_six_months_old = list(reversed(edits_users_three_six_months_old))
    #create a new column in the dataframe
    mothly_edits_users['edits_users_three_six_months_old']=edits_users_three_six_months_old
    series = pd.Series(mothly_edits_users.edits_users_three_six_months_old, index = mothly_edits_users.index.values)
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


#this metric is intended to find out how many users, per each month, have edited a main page
def users_main_page(data, index):
    edits_main_page = data[data['page_ns'] == 0]
    series = edits_main_page.groupby(pd.Grouper(key = 'timestamp', freq = 'MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series

#this metric is intended to find out how many users, per each month, have edited a template page
def users_template_page(data, index):
    edits_template_page = data[data['page_ns'] == 10]
    series = edits_template_page.groupby(pd.Grouper(key = 'timestamp', freq = 'MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


def talk_page_users(data,index):
    data_pageNS_E3 =data[data['page_ns']==3]
    series = data_pageNS_E3.groupby(pd.Grouper(key = 'timestamp', freq = 'MS')).size()
    return series
	
# this metric gets the users who have edited the wiki during three months in a row.	
def users_editing_three_months_in_a_row(data, index):
    mothly = data.groupby(pd.Grouper(key = 'timestamp', freq = 'MS'))
    mothly_edits_users = mothly.apply(lambda x: x.contributor_id.unique()).to_frame('edits_users')
    i = len(mothly_edits_users)-1
    edits_users_three_months_old = []
    intersectList=lambda l : len(list(set(l[0]) & set(l[1]) & set(l[2])))
    while i > 1:
        length_one_two_three_month = intersectList(np.array(mothly_edits_users.iloc[i-2:i+1, 0]))
        edits_users_three_months_old.append(length_one_two_three_month)
        i = i -1
    edits_users_three_months_old=edits_users_three_months_old + [0,0]
    #reverse order of the list
    edits_users_three_months_old = list(reversed(edits_users_three_months_old))               
    #create a new column in the dataframe
    mothly_edits_users['edits_users_three_months_old']=edits_users_three_months_old
    mothly_edits_users = pd.Series(mothly_edits_users.edits_users_three_months_old, index = mothly_edits_users.index.values)
    return mothly_edits_users

# this metric gets the users who have edited the wiki during six months in a row.	
def users_editing_six_months_in_a_row(data, index):
    mothly = data.groupby(pd.Grouper(key = 'timestamp', freq = 'MS'))
    mothly_edits_users = mothly.apply(lambda x: x.contributor_id.unique()).to_frame('edits_users')
    i = len(mothly_edits_users)-1
    edits_six_months_old = []
    intersectList=lambda l : len(list(set(l[0]) & set(l[1]) & set(l[2]) & set(l[3]) & set(l[4]) & set(l[5])))
    while i > 4:
        length_six_month = intersectList(np.array(mothly_edits_users.iloc[i-5:i+1, 0]))
        edits_six_months_old.append(length_six_month)
        i = i -1

    edits_six_months_old=edits_six_months_old + [0,0,0,0,0]
    #reverse order of the list
    edits_six_months_old = list(reversed(edits_six_months_old))               
    #create a new column in the dataframe
    mothly_edits_users['edits_six_months_old']=edits_six_months_old
    mothly_edits_users = pd.Series(mothly_edits_users.edits_six_months_old, index = mothly_edits_users.index.values)
    return mothly_edits_users

# this metric categorizes the user according to their activity level: to be included in the category in month X, they must have completed between 1 and 4 editions until month X-1 (included)
def users_number_of_edits_between_1_and_4(data, index):
# 1) Get the index of the dataframe to analyze: it must include all the months recorded in the history of the wiki.
    new_index = data.groupby(pd.Grouper(key='timestamp', freq='MS')).size().to_frame('months').index
# 2) create a dataframe in which we have the cumulative sum of the editions the user has made all along the history of the wiki.
    users_month_edits =data.groupby(['contributor_id']).apply(lambda x: x.groupby(pd.Grouper(key='timestamp', freq='MS'))
                                                        .size().to_frame('nEdits').reindex(new_index, fill_value=0).cumsum()).reset_index()
# 3) add a new column to the dataframe ('included') in which 2 values are possible: 1. if the user has made between >=1 and <=4 editions in month x - 1 (shift function is used to access the previous row), month appears. 2. Otherwise, the value in the 'included' column will be NaT.
    users_month_edits['included'] = users_month_edits.loc[((users_month_edits['nEdits'].shift()<=4) &
                                                       (users_month_edits['nEdits'].shift()>=1)) & 
                                                      (users_month_edits['contributor_id'].shift() == users_month_edits['contributor_id']), 'timestamp']
# 4) count the number of appereances each timestamp has in the 'included' column:
    series = users_month_edits.groupby(['included']).size().reindex(new_index, fill_value=0)
    return series

# this metric categorizes the user according to their activity level: to be included in the category in month X, they must have completed between 5 and 24 editions until month X-1 (included)
def users_number_of_edits_between_5_and_24(data, index):
# 1) Get the index of the dataframe to analyze: it must include all the months recorded in the history of the wiki.
    new_index = data.groupby(pd.Grouper(key='timestamp', freq='MS')).size().to_frame('months').index
# 2) create a dataframe in which we have the cumulative sum of the editions the user has made all along the history of the wiki.
    users_month_edits =data.groupby(['contributor_id']).apply(lambda x: x.groupby(pd.Grouper(key='timestamp', freq='MS'))
                                                        .size().to_frame('nEdits').reindex(new_index, fill_value=0).cumsum()).reset_index()
# 3) add a new column to the dataframe ('included') in which 2 values are possible: 1. if the user has made between >=1 and <=4 editions in month x - 1 (shift function is used to access the previous row), month appears. 2. Otherwise, the value in the 'included' column will be NaT.
    users_month_edits['included'] = users_month_edits.loc[((users_month_edits['nEdits'].shift()<=24) &
                                                       (users_month_edits['nEdits'].shift()>=5)) & 
                                                      (users_month_edits['contributor_id'].shift() == users_month_edits['contributor_id']), 'timestamp']
# 4) count the number of appereances each timestamp has in the 'included' column:
    series = users_month_edits.groupby(['included']).size().reindex(new_index, fill_value=0)
    return series

# this metric categorizes the user according to their activity level: to be included in the category in month X, they must have completed between 25 and 99 editions until month X-1 (included)
def users_number_of_edits_between_25_and_99(data, index):
# 1) Get the index of the dataframe to analyze: it must include all the months recorded in the history of the wiki.
    new_index = data.groupby(pd.Grouper(key='timestamp', freq='MS')).size().to_frame('months').index
# 2) create a dataframe in which we have the cumulative sum of the editions the user has made all along the history of the wiki.
    users_month_edits =data.groupby(['contributor_id']).apply(lambda x: x.groupby(pd.Grouper(key='timestamp', freq='MS'))
                                                        .size().to_frame('nEdits').reindex(new_index, fill_value=0).cumsum()).reset_index()
# 3) add a new column to the dataframe ('included') in which 2 values are possible: 1. if the user has made between >=1 and <=4 editions in month x - 1 (shift function is used to access the previous row), month appears. 2. Otherwise, the value in the 'included' column will be NaT.
    users_month_edits['included'] = users_month_edits.loc[((users_month_edits['nEdits'].shift()<=99) &
                                                       (users_month_edits['nEdits'].shift()>=25)) & 
                                                      (users_month_edits['contributor_id'].shift() == users_month_edits['contributor_id']), 'timestamp']
# 4) count the number of appereances each timestamp has in the 'included' column:
    series = users_month_edits.groupby(['included']).size().reindex(new_index, fill_value=0)
    return series

# this metric categorizes the user according to their activity level: to be included in the category in month X, they must have completed >=100 editions until month X-1 (included)
def users_number_of_edits_highEq_100(data, index):
# 1) Get the index of the dataframe to analyze: it must include all the months recorded in the history of the wiki.
    new_index = data.groupby(pd.Grouper(key='timestamp', freq='MS')).size().to_frame('months').index
# 2) create a dataframe in which we have the cumulative sum of the editions the user has made all along the history of the wiki.
    users_month_edits =data.groupby(['contributor_id']).apply(lambda x: x.groupby(pd.Grouper(key='timestamp', freq='MS'))
                                                        .size().to_frame('nEdits').reindex(new_index, fill_value=0).cumsum()).reset_index()
# 3) add a new column to the dataframe ('included') in which 2 values are possible: 1. if the user has made between >=1 and <=4 editions in month x - 1 (shift function is used to access the previous row), month appears. 2. Otherwise, the value in the 'included' column will be NaT.
    users_month_edits['included'] = users_month_edits.loc[(users_month_edits['nEdits'].shift()>=100) & 
                                                      (users_month_edits['contributor_id'].shift() == users_month_edits['contributor_id']), 'timestamp']
# 4) count the number of appereances each timestamp has in the 'included' column:
    series = users_month_edits.groupby(['included']).size().reindex(new_index, fill_value=0)
    return series


def users_new(data, index):
    users = data.drop_duplicates('contributor_id')
    series = users.groupby(pd.Grouper(key='timestamp', freq='MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


def users_accum(data, index):
    return (users_new(data, index).cumsum())


def users_new_anonymous(data, index):
    users = data.drop_duplicates('contributor_id')
    anonymous_users = users[users['contributor_name'] == 'Anonymous']
    series = anonymous_users.groupby(pd.Grouper(key='timestamp', freq='MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


def users_anonymous_accum(data, index):
    return (users_new_anonymous(data, index).cumsum())


def users_new_registered(data, index):
    users = data.drop_duplicates('contributor_id')
    non_anonymous_users = users[users['contributor_name'] != 'Anonymous']
    series = non_anonymous_users.groupby(pd.Grouper(key='timestamp', freq='MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


def users_registered_accum(data, index):
    return (users_new_registered(data, index).cumsum())

########################################################################

# RATIOS

##### Helper functions #####


def anonymous_edits(data, index):
    series = data[data['contributor_name'] == 'Anonymous']
    series = series.groupby(pd.Grouper(key='timestamp', freq='MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


##### callable ditribution metrics #####


def edits_per_users_accum(data, index):
    return (edits_accum(data, index) / users_accum(data, index))


def edits_per_users_monthly(data, index):
    return (edits(data, index) / users_active(data, index))


def edits_in_articles_per_users_accum(data, index):
    return (edits_main_content_accum(data, index) / users_accum(data, index))


def edits_in_articles_per_users_monthly(data, index):
    return (edits_main_content(data, index) / users_active(data, index))


def edits_per_pages_accum(data, index):
    return (edits_accum(data, index) / pages_accum(data, index))


def edits_per_pages_monthly(data, index):
    return (edits(data, index) / pages_edited(data, index))


def percentage_edits_by_anonymous_monthly(data, index):
    series_anon_edits = anonymous_edits(data, index)
    series_total_edits = edits(data, index)
    series = series_anon_edits / series_total_edits
    series *= 100 # we want it to be displayed in percentage
    return series


def percentage_edits_by_anonymous_accum(data, index):
    series_anon_edits_accum = anonymous_edits(data, index).cumsum()
    series_total_edits_accum = edits_accum(data, index)
    series = series_anon_edits_accum / series_total_edits_accum
    series *= 100 # we want it to be displayed in percentage
    return series


########################################################################

# Distribution Of Work

##### Helper functions #####


def contributions_per_author(data):
    """
    Takes data and outputs data grouped by its author
    """
    return data.groupby('contributor_id').size()


def calc_ratio_percentile_max(data, index, percentile, minimal_users):
    return calc_ratio_percentile(data, index, 1, percentile, minimal_users)


def calc_ratio_percentile(data, index, top_percentile, percentile, minimal_users):

    # Note that contributions is an *unsorted* list of contributions per author
    def ratio_max_percentile_for_period(contributions, percentage):

        position = int(n_users * percentage)

        # get top users until user who corresponds to percentil n
        top_users = contributions.nlargest(position)

        # get top user and percentil n user
        p_max = top_users[top_percentile-1]
        percentile = top_users[-1]

        # calculate ratio between percentiles
        return p_max / percentile

    percentage = percentile * 0.01
    i = 0
    monthly_data = data.groupby(pd.Grouper(key='timestamp', freq='MS'))
    result = pd.Series(index=monthly_data.size().index)
    indices = result.index
    accum_data = pd.DataFrame()
    for name, group in monthly_data:
        # Accumulate data so far
        accum_data = accum_data.append(group)

        # Get contributions per contributor
        contributions = contributions_per_author(accum_data)

        n_users = len(contributions)

        # Skip when the wiki has too few users
        if n_users < minimal_users:
            result[indices[i]] = np.NaN
        else:
            result[indices[i]] = ratio_max_percentile_for_period(contributions, percentage)
        i = i + 1

    return result

##### callable ditribution metrics #####


def gini_accum(data, index):

    def gini_coeff(values):
        """
        Extracted from wikixray/graphics.py:70
        Plots a GINI graph for author contributions

        @type  values: list of ints
        @param values: list of integers summarizing total contributions for each registered author
        """

        n_users = len(values) # n_users => n + 1
        if (n_users) < MINIMAL_USERS_GINI:
            return np.NaN

        sum_numerator=0
        sum_denominator=0
        for i in range(1, n_users):
            sum_numerator += (n_users-i) * values[i]
            sum_denominator += values[i]
        if sum_denominator == 0:
            return np.NaN
        ## Apply math function for the Gini coefficient
        g_coeff = n_users-2*(sum_numerator/sum_denominator)
        ## Now, apply Deltas, 2003 correction for small datasets:
        g_coeff *= (1.0 / (n_users - 2))
        return g_coeff

    #~ data = raw_data.set_index([raw_data['timestamp'].dt.to_period('M'), raw_data.index])
    monthly_data = data.groupby(pd.Grouper(key='timestamp', freq='MS'))
    if index is not None:
        gini_accum_df = pd.Series(index=index)
    else:
        gini_accum_df = pd.Series(index=monthly_data.size().index)
    indices = gini_accum_df.index
    i = 0
    accum_data = pd.DataFrame()
    for name, group in monthly_data:
        # Accumulate data so far
        accum_data = accum_data.append(group)

        # Get contributions per contributor, sort them
        #   and make it a list to call to gini_coeff()
        values = contributions_per_author(accum_data) \
                .sort_values(ascending=True) \
                .tolist()
        gini_accum_df[indices[i]] = gini_coeff(values)
        i = i + 1

    return gini_accum_df


def ratio_percentiles_max_5(data, index):
    return calc_ratio_percentile_max(data,index, 5,
                    MINIMAL_USERS_PERCENTIL_MAX_5)


def ratio_percentiles_max_10(data, index):
    return calc_ratio_percentile_max(data, index, 10,
                    MINIMAL_USERS_PERCENTIL_MAX_10)


def ratio_percentiles_max_20(data, index):
    return calc_ratio_percentile_max(data, index, 20,
                    MINIMAL_USERS_PERCENTIL_MAX_20)


def ratio_percentiles_5_10(data, index):
    return calc_ratio_percentile(data, index, 5, 10,
                    MINIMAL_USERS_PERCENTIL_5_10)


def ratio_percentiles_10_20(data, index):
    return calc_ratio_percentile(data, index, 10, 20,
                    MINIMAL_USERS_PERCENTIL_10_20)


def ratio_10_90(data, index):

    # contributions is a *sorted* list of contributions per author
    #   in an descending order (from most contributions to less contributions)
    def ratio_top_rest_for_period(contributions, percentage_top):
        top_percent_users = math.ceil(n_users * percentage_top);
        #~ rest_users = floor(n_users * (1 - percentage_top));
        edits_top = contributions[:top_percent_users].sum()
        edits_rest = contributions[top_percent_users:].sum()

        return edits_top / edits_rest


    percentage = 10 * 0.01
    i = 0
    monthly_data = data.groupby(pd.Grouper(key='timestamp', freq='MS'))
    result = pd.Series(index=monthly_data.size().index)
    indices = result.index
    accum_data = pd.DataFrame()
    for name, group in monthly_data:
        # Get contributions per contributor, sort them
        #   and make it a Python list
        accum_data = accum_data.append(group)
        contributions = contributions_per_author(accum_data) \
                .sort_values(ascending=False)

        n_users = len(contributions)

        # Skip when the wiki has too few users
        if n_users < MINIMAL_USERS_RATIO_10_90:
            result[indices[i]] = np.NaN
        else:
            result[indices[i]] = ratio_top_rest_for_period(contributions,
                                                            percentage)
        i = i + 1

    return result

