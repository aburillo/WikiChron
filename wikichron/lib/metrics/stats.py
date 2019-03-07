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
from dateutil.relativedelta import relativedelta


def calculate_index_all_months(data):
    monthly_data = data.groupby(pd.Grouper(key='timestamp', freq='MS'))
    index = monthly_data.size().index
    return index



# Users


###### Helper Functions ######

#### Helper metric users active ####

def users_active_more_than_x_editions(data, index, x):
    monthly_edits = data.groupby([pd.Grouper(key='timestamp', freq='MS'), 'contributor_name']).size()
    monthly_edits_filtered = monthly_edits[monthly_edits > x].to_frame(name='pages_edited').reset_index()
    series = monthly_edits_filtered.groupby(pd.Grouper(key='timestamp', freq='MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


#### Helper metric 3 ####

# this function is a helper for the functions in the metric 3 section: it filters the editors according to a condition which depends on the numbers x and y, passed as parameters by the caller functions.
def filter_users_first_edition(data, index, x, y):
# 1) Get the index of the dataframe to analyze: it must include all the months recorded in the history of the wiki.
    new_index = data.groupby(pd.Grouper(key='timestamp', freq='MS')).size().to_frame('months').index
# 2) create a dataframe in which we have the cumulative sum of the editions the user has made all along the history of the wiki.
    users_month_edits =data.groupby(['contributor_id']).apply(lambda x: x.groupby(pd.Grouper(key='timestamp', freq='MS')).size().to_frame('nEdits').reindex(new_index, fill_value=0).cumsum()).reset_index()
# 3) add a new column to the dataframe ('position') in which the number of each row depending on the contributor ID is computed: note that the count isn't restarted until nEdits > 0.
    cond = users_month_edits['nEdits'] == 0
    users_month_edits['position'] = np.where(cond, 0, users_month_edits.groupby([cond, 'contributor_id']).cumcount() + 1)

    if y > 0:
        condition = ((users_month_edits['position'] >= x) & (users_month_edits['position'] <= y)) & (users_month_edits['nEdits'] != users_month_edits['nEdits'].shift())
    else:
        condition = (users_month_edits['position'] > x) & (users_month_edits['nEdits'] != users_month_edits['nEdits'].shift())

    users_month_edits['included'] = np.where(condition, 1,0)
# 4) count the number of appereances each timestamp has in the 'included' column:
    series = pd.Series(users_month_edits.groupby(['timestamp']).sum()['included'], new_index)
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


#### Helper metric 4 ####

# this function is a helper for the functions in the metric 4 section: it filters the editors according to a condition which depends on the number x , passed as parameter by the caller functions.
def filter_users_last_edition(data, index, x):
# 1) Get the index of the dataframe to analyze: it must include all the months recorded in the history of the wiki.
    new_index = data.groupby(pd.Grouper(key='timestamp', freq='MS')).size().to_frame('months').index
# 2) create a dataframe in which we have the cumulative sum of the editions the user has made all along the history of the wiki.
    users_month_edits =data.groupby(['contributor_id']).apply(lambda x: x.groupby(pd.Grouper(key='timestamp', freq='MS')).size().to_frame('nEdits').reindex(new_index, fill_value=0).cumsum()).reset_index()
# 3) add a new column to the dataframe ('position') in which the number of each row depending grouping by contributor ID is computed: note that the count isn't restarted until nEdits > 0.
    cond = users_month_edits['nEdits'] == 0
    users_month_edits['position'] = np.where(cond, 0, users_month_edits.groupby([cond, 'contributor_id', 'nEdits']).cumcount() + 1)
# 4) add a new column, 'included', which will contain two possible values: 0 if the user didn't edit in month X or edited in month X but not in months specified by caller function, and 1 if the user edited in month X and made his last edition in month X-1

    if x != 6:
        cond1 = ((users_month_edits['position'] == 1) & (users_month_edits['position'].shift() == x)) & (users_month_edits['contributor_id'] == users_month_edits['contributor_id'].shift())
    else:
        cond1 = ((users_month_edits['position'] == 1) & (users_month_edits['position'].shift() > x)) & (users_month_edits['contributor_id'] == users_month_edits['contributor_id'].shift())

    users_month_edits['included'] = np.where(cond1, 1, 0)
# 5) create series
    series = pd.Series(users_month_edits.groupby(['timestamp']).sum()['included'], new_index)
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series



#### Helper metric 5 ####

# this helper functions filters the editors according to their number of editions, which can be in a range: [x, y] or >=x, with x and y specified by the caller functions
def filter_users_number_of_edits(data, index, x, y):
# 1) Get the index of the dataframe to analyze: it must include all the months recorded in the history of the wiki.
    new_index = data.groupby(pd.Grouper(key='timestamp', freq='MS')).size().to_frame('months').index
# 2) create a dataframe in which we have the cumulative sum of the editions the user has made all along the history of the wiki.
    users_month_edits =data.groupby(['contributor_id']).apply(lambda x: x.groupby(pd.Grouper(key='timestamp', freq='MS'))
                                                        .size().to_frame('nEdits').reindex(new_index, fill_value=0).cumsum()).reset_index()
# 3) add a new column to the dataframe ('included') in which 2 values are possible: 1. if the user has made between >=x and <=y editions in month x - 1 (shift function is used to access the previous row), a 1 appears. 2. Otherwise, the value in the 'included' column will be 0.

    if y != 0:
        cond1 = (users_month_edits['contributor_id'].shift() == users_month_edits['contributor_id']) & (users_month_edits['nEdits'] != users_month_edits['nEdits'].shift()) & ((users_month_edits['nEdits'].shift()<=x) & (users_month_edits['nEdits'].shift()>=y))
    else:
        cond1 = (users_month_edits['contributor_id'].shift() == users_month_edits['contributor_id']) & (users_month_edits['nEdits'] != users_month_edits['nEdits'].shift()) & (users_month_edits['nEdits'].shift()>=x)

    users_month_edits['included'] = np.where(cond1, 1, 0)
    series = pd.Series(users_month_edits.groupby(['timestamp']).sum()['included'], new_index)
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


#### Helper metrics 9 and 10 ####

# this helper function gets the number of users that have edited a particular kind of page, specified by the parameter page_ns
def filter_users_pageNS(data, index, page_ns):
    edits_page = data[data['page_ns'] == page_ns]
    series = edits_page.groupby(pd.Grouper(key = 'timestamp', freq = 'MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


###### Callable Functions ######

############################ METRIC 1: USERS NEW AND USERS REINCIDENT ###############################################################
#this metric is the same as the users_active, but getting rid of anonymous users	

#users who make their second edition in the wiki (we want the count for this kind of users per month)
def users_reincident(data, index):
    data['test_duplicated'] = data['contributor_id'].duplicated()
    data = data[data['contributor_name'] != 'Anonymous']
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


############################ METRIC 2 #################################################################################################
def add_x_months(data, months):
    return data['timestamp'].apply(lambda x: x + relativedelta(months = +months))

def displace_x_months(data, months):
    return data.shift(months)
	
def current_streak_this_month(data, index):
    mothly = data.groupby(['contributor_id', pd.Grouper(key = 'timestamp', freq = 'MS')]).size().to_frame('prueba').reset_index()
    mothly['add_one_month'] = mothly['timestamp'].apply(lambda x: x + relativedelta(months = +1))
    group_users = mothly[['contributor_id', 'add_one_month']].groupby(['contributor_id'])
    displace_a_month = group_users['add_one_month'].shift()
    mothly['displace']= displace_a_month
    streak_start_this_month = mothly[mothly['displace'] != mothly['timestamp']]
    series = streak_start_this_month.groupby(pd.Grouper(key = 'timestamp', freq = 'MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


def current_streak_2_or_3_months_in_a_row(data, index):
    mothly = data.groupby(['contributor_id',pd.Grouper(key = 'timestamp', freq = 'MS')]).size().to_frame('prueba').reset_index()
    mothly['add_one_month'] = mothly['timestamp'].apply(lambda x: x + relativedelta(months = +1))
    mothly['add_three_month'] = mothly['timestamp'].apply(lambda x: x + relativedelta(months = +3))
    group_users = mothly[['contributor_id', 'add_one_month', 'add_three_month']].groupby(['contributor_id'])
    displace_a_month = group_users['add_one_month'].shift()
    displace_three_month = group_users['add_three_month'].shift(3)
    mothly['displace_one_month']= displace_a_month
    mothly['displace_three_month']= displace_three_month
    streak_start_this_month = mothly[(mothly['displace_one_month'] == mothly['timestamp']) & (mothly['displace_three_month'] != mothly['timestamp'])]
    series = streak_start_this_month.groupby(pd.Grouper(key = 'timestamp', freq = 'MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


def current_streak_4_or_6_months_in_a_row(data, index):
    mothly = data.groupby(['contributor_id',pd.Grouper(key = 'timestamp', freq = 'MS')]).size().to_frame('prueba').reset_index()
    mothly['add_three_month'] = mothly['timestamp'].apply(lambda x: x + relativedelta(months = +3))
    mothly['add_six_month'] = mothly['timestamp'].apply(lambda x: x + relativedelta(months = +6))
    group_users = mothly[['contributor_id', 'add_three_month', 'add_six_month']].groupby(['contributor_id'])
    displace_three_month = group_users['add_three_month'].shift(3)
    displace_six_month = group_users['add_six_month'].shift(6)
    mothly['displace_three_month']= displace_three_month
    mothly['displace_six_month']= displace_six_month
    streak_start_this_month = mothly[(mothly['displace_three_month'] == mothly['timestamp']) & (mothly['displace_six_month'] != mothly['timestamp'])]
    series = streak_start_this_month.groupby(pd.Grouper(key = 'timestamp', freq = 'MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series


def current_streak_more_than_six_months_in_a_row(data, index):
    mothly = data.groupby(['contributor_id',pd.Grouper(key = 'timestamp', freq = 'MS')]).size().to_frame('prueba').reset_index()
    mothly['add_six_month'] = mothly['timestamp'].apply(lambda x: x + relativedelta(months = +6))
    group_users = mothly[['contributor_id', 'add_six_month']].groupby(['contributor_id'])
    displace_six_month = group_users['add_six_month'].shift(6)
    mothly['displace_six_month']= displace_six_month
    streak_start_this_month = mothly[mothly['displace_six_month'] == mothly['timestamp']]
    series = streak_start_this_month.groupby(pd.Grouper(key = 'timestamp', freq = 'MS')).size()
    if index is not None:
        series = series.reindex(index, fill_value=0)
    return series

############################ METRIC 3 #################################################################################################

# this metric counts the users whose first edition was between 1 and 3 months ago:
def users_first_edit_between_1_3_months_ago(data, index):
    return filter_users_first_edition(data, index, 2, 4)

# this metric counts the users whose first edition was between 4 and 6 months ago:
def users_first_edit_between_4_6_months_ago(data, index):
    return filter_users_first_edition(data, index, 5, 7)

# this metric counts the users whose first edition is more than 6 months old:
def users_first_edit_more_than_6_months_ago(data, index):
    return filter_users_first_edition(data, index, 7, 0)

def users_first_edit(data, index):
    one_three = users_first_edit_between_1_3_months_ago(data, index)
    four_six = users_first_edit_between_4_6_months_ago(data, index)
    more_six = users_first_edit_more_than_6_months_ago(data, index)
    return [one_three, four_six, more_six]
############################ METRIC 4 #################################################################################################

# This metric counts, among the users that have edited in that month X, the ones that have edited the last time in month X-1
def users_last_edit_1_month_ago(data, index):
    return filter_users_last_edition(data, index, 1)

# This metric counts, among the users that have edited in month X, which ones have edited the last time in month X-2 or X-3
def users_last_edit_2_or_3_months_ago(data, index):
    return filter_users_last_edition(data, index, 2)

# This metric counts, per each month X, among the users that have edited in that month X, the ones that have edited the last time in month X-4, X-5 or X-6
def users_last_edit_4_or_5_or_6_months_ago(data, index):
    return filter_users_last_edition(data, index, 4)

# This metric counts, per each month X, among the users that have edited in that month X, the ones that have edited the last time in any month > X-6
def users_last_edit_more_than_6_months_ago(data, index):
    return filter_users_last_edition(data, index, 6)


############################ METRIC 5 #################################################################################################

# In this metric, we want to get, among the users that make an edition in month X, which ones have done n editions, with n in [1,4], until month X-1
def users_number_of_edits_between_1_and_4(data, index):
    return filter_users_number_of_edits(data, index, 4, 1)

# In this metric, we want to get, among the users that make an edition in month X, which ones have done n editions, with n in [5,24], until month X-1
def users_number_of_edits_between_5_and_24(data, index):
    return filter_users_number_of_edits(data, index, 24, 5)

# In this metric, we want to get, among the users that make an edition in month X, which ones have done n editions, with n in [25,99], until month X-1
def users_number_of_edits_between_25_and_99(data, index):
    return filter_users_number_of_edits(data, index, 99, 25)

# In this metric, we want to get, among the users that make an edition in month X, which ones have done n editions, with n>=100, until month X-1
def users_number_of_edits_highEq_100(data, index):
    return filter_users_number_of_edits(data, index, 100, 0)


############################ METRICS 9 and 10 #################################################################################################

#this metric filters how many users have edited a main page
def users_main_page(data, index):
  return filter_users_pageNS(data, index, 0)

#this metric filters how many users have edited a template page
def users_template_page(data, index):
   return filter_users_pageNS(data, index, 10)

#this metric filters how many users have edited a talk page
def talk_page_users(data,index):
    return filter_users_pageNS(data, index, 3)


