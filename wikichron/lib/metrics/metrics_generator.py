#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
   metrics_generator.py

   Descp:

   Created on: 14-nov-2017

   Copyright 2017-2018 Abel 'Akronix' Serrano Juste <akronix5@gmail.com>
"""

from .metric import Metric, MetricCategory
from . import stats

def generate_metrics():
    metrics = []
    # Pages
    metrics.append(Metric('pages_new', 'New pages', MetricCategory.PAGES, stats.pages_new, 'Number of new pages created per month'))
    metrics.append(Metric('pages_main_new', 'New articles', MetricCategory.PAGES, stats.pages_main_new, 'Number of new articles (main content) created per month'))
    metrics.append(Metric('pages_edited', 'Pages edited', MetricCategory.PAGES, stats.pages_edited, 'Number of different pages edited per month'))
    metrics.append(Metric('main_edited', 'Articles edited', MetricCategory.PAGES, stats.main_edited, 'Number of different articles edited per month'))
    metrics.append(Metric('pages_accum', 'Total pages', MetricCategory.PAGES, stats.pages_accum, 'Total of pages accumulated at every month'))
    metrics.append(Metric('pages_main_accum', 'Total articles', MetricCategory.PAGES, stats.pages_main_accum, 'Total of articles (main content) at every month'))

    # Editions
    metrics.append(Metric('edits', 'Edits in pages', MetricCategory.EDITIONS, stats.edits, 'Editions to any part of the wiki grouped by month'))
    metrics.append(Metric('edits_main_content', 'Edits in articles', MetricCategory.EDITIONS, stats.edits_main_content, 'Editions to articles (main content) per month'))
    metrics.append(Metric('edits_article_talk', 'Edits in articles talk', MetricCategory.EDITIONS, stats.edits_article_talk, 'Editions to article discussion pages'))
    metrics.append(Metric('edits_user_talk', 'Edits in user talk', MetricCategory.EDITIONS, stats.edits_user_talk, 'Editions to user discussion pages'))
    metrics.append(Metric('edits_accum', 'Total edits in pages', MetricCategory.EDITIONS, stats.edits_accum, 'Total editions to any part of the wiki accumulated at every month'))
    metrics.append(Metric('edits_main_content_accum', 'Total edits in articles', MetricCategory.EDITIONS, stats.edits_main_content_accum, 'Editions to articles accumulated at every month'))

    # Users

    # metric 1 (newcommers and reincidents)
    metrics.append(Metric('users_reincident', 'Reincident users', MetricCategory.USERS, stats.users_reincident, 'users who have made more than one edition, shown in the month in which they reached >1 editions'))
    metrics.append(Metric('users_new', 'New users', MetricCategory.USERS, stats.users_new, 'Users editing the wiki for the first time'))

    # metric 2
    metrics.append(Metric('Activity_streak_1', 'Activity streak 1', MetricCategory.USERS, stats.current_streak_this_month, 'Users editing the wiki for 1 month in a row.'))
    metrics.append(Metric('Activity_streak_2_3', 'Activity streak 2', MetricCategory.USERS, stats.current_streak_2_or_3_months_in_a_row, 'Users editing the wiki for 2 or 3 months in a row'))
    metrics.append(Metric('Activity_streak_4_6', 'Activity streak 3', MetricCategory.USERS, stats.current_streak_4_or_6_months_in_a_row, 'Users editing the wiki for 4 or 6 months in a row'))
    metrics.append(Metric('Activity_streak_6', 'Activity streak 4', MetricCategory.USERS, stats.current_streak_more_than_six_months_in_a_row, 'Users editing the wiki for more than 6 months in a row'))

    # metric 3
    metrics.append(Metric('users_first_edit_between_1_3_months_ago', 'Users first edit 1', MetricCategory.USERS, stats.users_first_edit_between_1_3_months_ago, 'Users whose first edition was between 1 and 3 months ago'))
    metrics.append(Metric('users_first_edit_between_4_6_months_ago', 'Users first edit 2', MetricCategory.USERS, stats.users_first_edit_between_4_6_months_ago, 'Users whose first edition was between 4 and 6 months ago'))
    metrics.append(Metric('users_first_edit_more_than_6_months_ago', 'Users first edit 3', MetricCategory.USERS, stats.users_first_edit_more_than_6_months_ago, 'Users whose first edition was more than 6 months ago'))

    # metric 4
    metrics.append(Metric('users_last_edit_1_month_ago', 'users last edit 1', MetricCategory.USERS, stats.users_last_edit_1_month_ago, 'Users editing in month X whose last edit was in month X-1'))
    metrics.append(Metric('users_last_edit_2_or_3_months_ago', 'users last edit 2', MetricCategory.USERS, stats.users_last_edit_2_or_3_months_ago, 'Users editing in month X whose last edit was in month X-2 or X-3'))
    metrics.append(Metric('users_last_edit_4_or_5_or_6_months_ago', 'Users last edit 3', MetricCategory.USERS, stats.users_last_edit_4_or_5_or_6_months_ago, 'Users editing in month X whose last edit was in month X-4, X-5 or X-6'))
    metrics.append(Metric('users_last_edit_more_than_6_months_ago', 'users last edit 4', MetricCategory.USERS, stats.users_last_edit_more_than_6_months_ago, 'Users editing in month X whose last edit was in any month > X-6'))

    # metric 5
    metrics.append(Metric('users_edits_between_1_4', 'users #edits 1', MetricCategory.USERS, stats.users_number_of_edits_between_1_and_4, 'Users that have completed between 1 and 4 editions until month X-1 (included)'))
    metrics.append(Metric('users_edits_between_5_24', 'users #edits 2', MetricCategory.USERS, stats.users_number_of_edits_between_5_and_24, 'Users that have completed between 5 and 24 editions until month X-1 (included)'))
    metrics.append(Metric('users_edits_between_25_99', 'users #edits 3', MetricCategory.USERS, stats.users_number_of_edits_between_25_and_99, 'Users that have completed between 25 and 99 editions until month X-1 (included)'))
    metrics.append(Metric('users_edits_highEq_100', 'users #edits 4', MetricCategory.USERS, stats.users_number_of_edits_highEq_100, 'Users that have completed >= 100 editions until month X-1 (included)'))

# metric 6: wikimedia
# metric 7
# metric 8

    # metrics 10 and 9: users editing main, template and talk pages
    metrics.append(Metric('users_main_page', 'Users main page', MetricCategory.USERS, stats.users_main_page, 'Users who have edited a main page'))
    metrics.append(Metric('users_template_page', 'Users template page', MetricCategory.USERS, stats.users_template_page, 'Users who have edited a template page'))
    metrics.append(Metric('talk_page_users', 'Users talk page', MetricCategory.USERS, stats.talk_page_users, 'Users that have edited a talk page.'))

    # users new (registered & anonymous)
    metrics.append(Metric('users_new_registered', 'New registered users', MetricCategory.USERS, stats.users_new_registered, 'New users registration per month who have made at least one edition.'))
    metrics.append(Metric('users_new_anonymous', 'New anonymous users', MetricCategory.USERS, stats.users_new_anonymous, 'Anonymous users who made at least one edition grouped by the month they did their first edit. Anonymous are identified by their ip.'))

    #users active 
    metrics.append(Metric('users_active', 'Active users', MetricCategory.USERS, stats.users_active, 'Number of users who have made at least one contribution for each month.'))
<<<<<<< HEAD
    metrics.append(Metric('users_active_registered', 'Active registered users', MetricCategory.USERS, stats.users_registered_active, 'New registered users who have made at least one edition.'))
    metrics.append(Metric('users_active_anonymous', 'Active anonymous users', MetricCategory.USERS, stats.users_anonymous_active, 'New users who have made at least one edition.'))
    metrics.append(Metric('users_registered_active_>4_edits', 'users >4 edits 1', MetricCategory.USERS, stats.users_registered_active_2, 'Registered users distributed by the month(s) in which they have made more than 4 editions'))
    metrics.append(Metric('users_active_more_than_4', 'users >4 edits 2', MetricCategory.USERS, stats.users_active_more_than_4, 'users (anonymous and registered) distributed by the month(s) in which they have made more than 4 editions'))

    #total users (registered & anonymous)
=======
    metrics.append(Metric('users_active_registered', 'Active registered users', MetricCategory.USERS, stats.users_registered_active, 'Number of registered users who have made at least one contribution for each month.'))
    metrics.append(Metric('users_active_anonymous', 'Active anonymous users', MetricCategory.USERS, stats.users_anonymous_active, 'Number of anonymous users who have made at least one contribution for each month.'))
>>>>>>> oficial-master
    metrics.append(Metric('users_accum', 'Total users', MetricCategory.USERS, stats.users_accum, 'Users who have made at least one edition accumulated at every month.'))
    metrics.append(Metric('users_registered_accum', 'Total registered users', MetricCategory.USERS, stats.users_registered_accum, 'Total registered users at every month. Note that users have to have made at least one edition and they have to be logged with their account when they did that edition.'))
    metrics.append(Metric('users_anonymous_accum', 'Total anonymous users', MetricCategory.USERS, stats.users_anonymous_accum, 'Anonymous users who have made at least one edition accumulated at every month. Anonymous are identified by their ip.'))

    # RATIO
    metrics.append(Metric('edits_per_users_monthly', 'Edits per users', MetricCategory.RATIOS, stats.edits_per_users_monthly, 'Number of edits for every month per number of active users that month'))
    metrics.append(Metric('edits_in_articles_per_users_monthly', 'Article edits per user', MetricCategory.RATIOS, stats.edits_in_articles_per_users_monthly, 'Number of edits in articles per number of users for each month'))
    metrics.append(Metric('edits_per_page_monthly', 'Edits per edited pages', MetricCategory.RATIOS, stats.edits_per_pages_monthly, 'Number of edits for every month per number of pages edited that month'))
    metrics.append(Metric('percentage_edits_by_anonymous_monthly', 'Anonymous edits (%)', MetricCategory.RATIOS, stats.percentage_edits_by_anonymous_monthly, 'Percentage of edits made by anonymous users of the total edits.'))
    metrics.append(Metric('edits_in_articles_per_users_accum', 'Total articles edits per user', MetricCategory.RATIOS, stats.edits_in_articles_per_users_accum, 'Number of total edits in articles per number of users until a given month'))
    metrics.append(Metric('edits_per_pages_accum', 'Total edits per page', MetricCategory.RATIOS, stats.edits_per_pages_accum, 'Number of total edits per number of total pages'))
    metrics.append(Metric('percentage_edits_by_anonymous_accum', 'Total anonymous edits (%)', MetricCategory.RATIOS, stats.percentage_edits_by_anonymous_accum, 'Percentage, per month, of edits made by anonymous users of the total edits.'))

    # DISTRIBUTION
    metrics.append(Metric('gini_accum', 'Gini coefficient', MetricCategory.DISTRIBUTION, stats.gini_accum, 'Gini coefficient (accumulated)'))
    metrics.append(Metric('ratio_10_90', '10:90 ratio', MetricCategory.DISTRIBUTION, stats.ratio_10_90, 'Contributions of the top ten percent more active users between the 90% percent less active'))
    metrics.append(Metric('ratio_percentiles_max_5', 'Participants prctl. top / 5', MetricCategory.DISTRIBUTION, stats.ratio_percentiles_max_5, 'Ratio between contributions of the top and the 5th top users'))
    metrics.append(Metric('ratio_percentiles_max_10', 'Participants prctl. top / 10', MetricCategory.DISTRIBUTION, stats.ratio_percentiles_max_10, 'Ratio between contributions of the top user and the 10th top user'))
    metrics.append(Metric('ratio_percentiles_max_20', 'Participants prctl. top / 20', MetricCategory.DISTRIBUTION, stats.ratio_percentiles_max_20, 'Ratio between contributions of the top user and the 20th top user'))
    metrics.append(Metric('ratio_percentiles_5_10', 'Participants prctl. 5 / 10', MetricCategory.DISTRIBUTION, stats.ratio_percentiles_5_10, 'Ratio between contributions of the 5th user and the 10th top user'))
    metrics.append(Metric('ratio_percentiles_10_20', 'Participants prctl. 10 / 20', MetricCategory.DISTRIBUTION, stats.ratio_percentiles_10_20, 'Ratio between contributions of the 10th user and the 20th top user'))

    # keep this order when plotting graphs inserting 'index_' at the beginning
    #  for every metric code.
    # NOTE: Possibly, It'll be changed in the future by an specifc attr: "order"
    #  in the GUI side, in order to be able to reorder the plots.
    for idx in range(len(metrics)):
        metrics[idx].code = "{idx}_{code}".format(idx=idx, code=metrics[idx].code)

    return metrics

def generate_dict_metrics(list_of_metrics):
    metrics = {}

    for metric in list_of_metrics:
        metrics[metric.code] = metric

    return metrics


def main():
    print (generate_metrics())
    return

if __name__ == '__main__':
    main()

