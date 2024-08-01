import socketserver
import json
import struct
import sys
import time
import os
import storage
import model
import train
import baoctl
import math
import reg_blocker
from constants import (PG_OPTIMIZER_INDEX, DEFAULT_MODEL_PATH,
                       OLD_MODEL_PATH, TMP_MODEL_PATH)


def add_buffer_info_to_plans(buffer_info, plans):
    for p in plans:
        p["Buffers"] = buffer_info
    return plans


class BaoModel:
    def __init__(self):
        self.__current_model = None

    def select_plan(self, messages):
        start = time.time()
        # the last message is the buffer state
        *arms, buffers = messages

        print("============ in select_plan")
        print("arms: ", arms)
        print("buffers: ", buffers)
        print("============ in select_plan")

        # if we don't have a model, default to the PG optimizer
        if self.__current_model is None:
            return PG_OPTIMIZER_INDEX

        # if we do have a model, make predictions for each plan.
        arms = add_buffer_info_to_plans(buffers, arms)
        res = self.__current_model.predict(arms)
        print("============ in select_plan result")
        print("res: ", res)
        print("============ in select_plan result")

        idx = res.argmin()
        stop = time.time()
        print("Selected index", idx,
              "after", f"{round((stop - start) * 1000)}ms",
              "Predicted reward / PG:", res[idx][0],
              "/", res[0][0])
        return idx

    def predict_local(self):
        plan = {'Plan': {'Node Type': 'Other', 'Node Type ID': '42', 'Total Cost': 20001818246.16712, 'Plan Rows': 1.0,
                         'Plans': [{'Node Type': 'Hash Join', 'Node Type ID': '38', 'Total Cost': 20001818245.882122,
                                    'Plan Rows': 110.0, 'Plans': [
                                 {'Node Type': 'Seq Scan', 'Node Type ID': '19', 'Relation Name': 'name',
                                  'Total Cost': 107708.55, 'Plan Rows': 952446.0},
                                 {'Node Type': 'Other', 'Node Type ID': '47', 'Total Cost': 20001706958.522125,
                                  'Plan Rows': 483.0, 'Plans': [
                                     {'Node Type': 'Hash Join', 'Node Type ID': '38', 'Total Cost': 20001706958.522125,
                                      'Plan Rows': 483.0, 'Plans': [
                                         {'Node Type': 'Hash Join', 'Node Type ID': '38',
                                          'Total Cost': 20001706936.784874,
                                          'Plan Rows': 5791.0, 'Plans': [
                                             {'Node Type': 'Seq Scan', 'Node Type ID': '19',
                                              'Relation Name': 'cast_info',
                                              'Total Cost': 615130.44, 'Plan Rows': 36244344.0},
                                             {'Node Type': 'Other', 'Node Type ID': '47',
                                              'Total Cost': 20000955827.094875,
                                              'Plan Rows': 404.0,
                                              'Plans': [{'Node Type': 'Hash Join', 'Node Type ID': '38',
                                                         'Total Cost': 20000955827.094875,
                                                         'Plan Rows': 404.0, 'Plans': [
                                                      {'Node Type': 'Nested Loop', 'Node Type ID': '36',
                                                       'Total Cost': 10000418659.5755, 'Plan Rows': 65262.0, 'Plans': [
                                                          {'Node Type': 'Other', 'Node Type ID': '24',
                                                           'Relation Name': 'movie_info', 'Total Cost': 417841.3855,
                                                           'Plan Rows': 65262.0, 'Plans': [
                                                              {'Node Type': 'Bitmap Index Scan', 'Node Type ID': '23',
                                                               'Relation Name': 'movie_info', 'Total Cost': 28163.645,
                                                               'Plan Rows': 1523628.0}]},
                                                          {'Node Type': 'Other', 'Node Type ID': '39',
                                                           'Total Cost': 2.4175,
                                                           'Plan Rows': 1.0, 'Plans': [
                                                              {'Node Type': 'Seq Scan', 'Node Type ID': '19',
                                                               'Relation Name': 'info_type', 'Total Cost': 2.4125,
                                                               'Plan Rows': 1.0}]}]},
                                                      {'Node Type': 'Other', 'Node Type ID': '47',
                                                       'Total Cost': 10000536723.121874, 'Plan Rows': 15650.0,
                                                       'Plans': [
                                                           {'Node Type': 'Hash Join', 'Node Type ID': '38',
                                                            'Total Cost': 10000536723.121874, 'Plan Rows': 15650.0,
                                                            'Plans': [
                                                                {'Node Type': 'Nested Loop', 'Node Type ID': '36',
                                                                 'Total Cost': 10000431791.16575, 'Plan Rows': 39253.0,
                                                                 'Plans': [
                                                                     {'Node Type': 'Other', 'Node Type ID': '24',
                                                                      'Relation Name': 'movie_info',
                                                                      'Total Cost': 431298.08825,
                                                                      'Plan Rows': 39253.0, 'Plans': [
                                                                         {'Node Type': 'Bitmap Index Scan',
                                                                          'Node Type ID': '23',
                                                                          'Relation Name': 'movie_info',
                                                                          'Total Cost': 26756.475,
                                                                          'Plan Rows': 1447472.0}]},
                                                                     {'Node Type': 'Other', 'Node Type ID': '39',
                                                                      'Total Cost': 2.4175, 'Plan Rows': 1.0, 'Plans': [
                                                                         {'Node Type': 'Seq Scan', 'Node Type ID': '19',
                                                                          'Relation Name': 'info_type',
                                                                          'Total Cost': 2.4125,
                                                                          'Plan Rows': 1.0}]}]},
                                                                {'Node Type': 'Other', 'Node Type ID': '47',
                                                                 'Total Cost': 83840.457375, 'Plan Rows': 1008144.0,
                                                                 'Plans': [
                                                                     {'Node Type': 'Hash Join', 'Node Type ID': '38',
                                                                      'Total Cost': 83840.457375,
                                                                      'Plan Rows': 1008144.0,
                                                                      'Plans': [{'Node Type': 'Seq Scan',
                                                                                 'Node Type ID': '19',
                                                                                 'Relation Name': 'title',
                                                                                 'Total Cost': 73925.905,
                                                                                 'Plan Rows': 2352337.0},
                                                                                {'Node Type': 'Other',
                                                                                 'Node Type ID': '47',
                                                                                 'Total Cost': 1.09625,
                                                                                 'Plan Rows': 3.0,
                                                                                 'Plans': [{'Node Type': 'Seq Scan',
                                                                                            'Node Type ID': '19',
                                                                                            'Relation Name': 'kind_type',
                                                                                            'Total Cost': 1.09625,
                                                                                            'Plan Rows': 3.0}]}]}]}]}]}]}]}]},
                                         {'Node Type': 'Other', 'Node Type ID': '47', 'Total Cost': 1.15,
                                          'Plan Rows': 1.0,
                                          'Plans': [
                                              {'Node Type': 'Seq Scan', 'Node Type ID': '19',
                                               'Relation Name': 'role_type',
                                               'Total Cost': 1.15, 'Plan Rows': 1.0}]}]}]}]}]}}
        buffers = {'title_pkey': 6, 'kind_id_title': 1, 'title': 35998, 'kind_type_pkey': 1, 'movie_info_pkey': 1,
                   'info_type_id_movie_info': 1, 'movie_id_movie_info': 6, 'info_type_pkey': 1, 'cast_info_pkey': 1,
                   'movie_id_cast_info': 8, 'person_id_cast_info': 8, 'person_role_id_cast_info': 1,
                   'role_id_cast_info': 1,
                   'role_type_pkey': 1, 'name_pkey': 4, 'movie_info': 98, 'cast_info': 4, 'name': 1, 'kind_type': 1}
        # if we don't have a model, make a prediction of NaN
        if self.__current_model is None:
            return math.nan

        # if we do have a model, make predictions for each plan.
        plans = add_buffer_info_to_plans(buffers, [plan])
        res = self.__current_model.predict(plans)
        return res[0][0]

    def predict(self, messages):
        # the last message is the buffer state
        plan, buffers = messages

        print("============ in predict")
        print(plan)
        print(buffers)
        print("============ in predict")

        # if we don't have a model, make a prediction of NaN
        if self.__current_model is None:
            return math.nan

        # if we do have a model, make predictions for each plan.
        plans = add_buffer_info_to_plans(buffers, [plan])
        res = self.__current_model.predict(plans)
        return res[0][0]

    def predict_local_test(self, messages):
        # the last message is the buffer state
        arms = [{'Plan': {'Node Type': 'Other', 'Node Type ID': '42', 'Total Cost': 626926.521585, 'Plan Rows': 1.0,
                          'Plans': [{'Node Type': 'Nested Loop', 'Node Type ID': '36', 'Total Cost': 626926.236585,
                                     'Plan Rows': 110.0, 'Plans': [
                                  {'Node Type': 'Seq Scan', 'Node Type ID': '19', 'Relation Name': 'info_type',
                                   'Total Cost': 2.4125, 'Plan Rows': 1.0},
                                  {'Node Type': 'Nested Loop', 'Node Type ID': '36', 'Total Cost': 626922.724085,
                                   'Plan Rows': 110.0, 'Plans': [
                                      {'Node Type': 'Seq Scan', 'Node Type ID': '19', 'Relation Name': 'info_type',
                                       'Total Cost': 2.4125, 'Plan Rows': 1.0},
                                      {'Node Type': 'Other', 'Node Type ID': '45', 'Total Cost': 626919.211585,
                                       'Plan Rows': 110.0, 'Plans': [{'Node Type': 'Nested Loop', 'Node Type ID': '36',
                                                                      'Total Cost': 625908.211585, 'Plan Rows': 46.0,
                                                                      'Plans': [{'Node Type': 'Hash Join',
                                                                                 'Node Type ID': '38',
                                                                                 'Total Cost': 625449.418324,
                                                                                 'Plan Rows': 201.0, 'Plans': [
                                                                              {'Node Type': 'Nested Loop',
                                                                               'Node Type ID': '36',
                                                                               'Total Cost': 625439.685574,
                                                                               'Plan Rows': 2413.0, 'Plans': [
                                                                                  {'Node Type': 'Hash Join',
                                                                                   'Node Type ID': '38',
                                                                                   'Total Cost': 617475.303159,
                                                                                   'Plan Rows': 168.0, 'Plans': [
                                                                                      {'Node Type': 'Nested Loop',
                                                                                       'Node Type ID': '36',
                                                                                       'Total Cost': 617472.514784,
                                                                                       'Plan Rows': 393.0, 'Plans': [
                                                                                          {'Node Type': 'Hash Join',
                                                                                           'Node Type ID': '38',
                                                                                           'Total Cost': 606011.475,
                                                                                           'Plan Rows': 3240.0,
                                                                                           'Plans': [
                                                                                               {'Node Type': 'Seq Scan',
                                                                                                'Node Type ID': '19',
                                                                                                'Relation Name': 'movie_info',
                                                                                                'Total Cost': 285615.0,
                                                                                                'Plan Rows': 16355.0},
                                                                                               {'Node Type': 'Other',
                                                                                                'Node Type ID': '47',
                                                                                                'Total Cost': 277888.0625,
                                                                                                'Plan Rows': 27192.0,
                                                                                                'Plans': [{
                                                                                                              'Node Type': 'Seq Scan',
                                                                                                              'Node Type ID': '19',
                                                                                                              'Relation Name': 'movie_info',
                                                                                                              'Total Cost': 277888.0625,
                                                                                                              'Plan Rows': 27192.0}]}]},
                                                                                          {'Node Type': 'Index Scan',
                                                                                           'Node Type ID': '21',
                                                                                           'Relation Name': 'title',
                                                                                           'Total Cost': 3.537358,
                                                                                           'Plan Rows': 1.0}]},
                                                                                      {'Node Type': 'Other',
                                                                                       'Node Type ID': '47',
                                                                                       'Total Cost': 1.09625,
                                                                                       'Plan Rows': 3.0, 'Plans': [
                                                                                          {'Node Type': 'Seq Scan',
                                                                                           'Node Type ID': '19',
                                                                                           'Relation Name': 'kind_type',
                                                                                           'Total Cost': 1.09625,
                                                                                           'Plan Rows': 3.0}]}]},
                                                                                  {'Node Type': 'Index Scan',
                                                                                   'Node Type ID': '21',
                                                                                   'Relation Name': 'cast_info',
                                                                                   'Total Cost': 47.057038,
                                                                                   'Plan Rows': 35.0}]},
                                                                              {'Node Type': 'Other',
                                                                               'Node Type ID': '47', 'Total Cost': 1.15,
                                                                               'Plan Rows': 1.0, 'Plans': [
                                                                                  {'Node Type': 'Seq Scan',
                                                                                   'Node Type ID': '19',
                                                                                   'Relation Name': 'role_type',
                                                                                   'Total Cost': 1.15,
                                                                                   'Plan Rows': 1.0}]}]},
                                                                                {'Node Type': 'Index Scan',
                                                                                 'Node Type ID': '21',
                                                                                 'Relation Name': 'name',
                                                                                 'Total Cost': 2.282554,
                                                                                 'Plan Rows': 1.0}]}]}]}]}]}}, {
                    'Plan': {'Node Type': 'Other', 'Node Type ID': '42', 'Total Cost': 20033121685.009544,
                             'Plan Rows': 1.0, 'Plans': [
                            {'Node Type': 'Nested Loop', 'Node Type ID': '36', 'Total Cost': 20033121684.724545,
                             'Plan Rows': 110.0, 'Plans': [
                                {'Node Type': 'Hash Join', 'Node Type ID': '38', 'Total Cost': 10033121680.934546,
                                 'Plan Rows': 110.0, 'Plans': [
                                    {'Node Type': 'Seq Scan', 'Node Type ID': '19', 'Relation Name': 'name',
                                     'Total Cost': 107708.55, 'Plan Rows': 952446.0},
                                    {'Node Type': 'Other', 'Node Type ID': '47', 'Total Cost': 10033010393.574545,
                                     'Plan Rows': 483.0, 'Plans': [{'Node Type': 'Hash Join', 'Node Type ID': '38',
                                                                    'Total Cost': 10033010393.574545,
                                                                    'Plan Rows': 483.0, 'Plans': [
                                            {'Node Type': 'Hash Join', 'Node Type ID': '38',
                                             'Total Cost': 10033010387.693918, 'Plan Rows': 1126.0, 'Plans': [
                                                {'Node Type': 'Hash Join', 'Node Type ID': '38',
                                                 'Total Cost': 10033010338.522543, 'Plan Rows': 13517.0, 'Plans': [
                                                    {'Node Type': 'Seq Scan', 'Node Type ID': '19',
                                                     'Relation Name': 'cast_info', 'Total Cost': 615130.44,
                                                     'Plan Rows': 36244344.0},
                                                    {'Node Type': 'Other', 'Node Type ID': '47',
                                                     'Total Cost': 10032259144.835043, 'Plan Rows': 943.0, 'Plans': [
                                                        {'Node Type': 'Merge Join', 'Node Type ID': '37',
                                                         'Total Cost': 10032259144.835043, 'Plan Rows': 943.0,
                                                         'Plans': [{'Node Type': 'Merge Join', 'Node Type ID': '37',
                                                                    'Total Cost': 10016209269.090302,
                                                                    'Plan Rows': 36518.0, 'Plans': [
                                                                 {'Node Type': 'Nested Loop', 'Node Type ID': '36',
                                                                  'Total Cost': 10016080190.291567,
                                                                  'Plan Rows': 39253.0, 'Plans': [
                                                                     {'Node Type': 'Index Scan', 'Node Type ID': '21',
                                                                      'Relation Name': 'movie_info',
                                                                      'Total Cost': 16079697.214067,
                                                                      'Plan Rows': 39253.0},
                                                                     {'Node Type': 'Other', 'Node Type ID': '39',
                                                                      'Total Cost': 2.4175, 'Plan Rows': 1.0, 'Plans': [
                                                                         {'Node Type': 'Seq Scan', 'Node Type ID': '19',
                                                                          'Relation Name': 'info_type',
                                                                          'Total Cost': 2.4125, 'Plan Rows': 1.0}]}]},
                                                                 {'Node Type': 'Index Scan', 'Node Type ID': '21',
                                                                  'Relation Name': 'title', 'Total Cost': 122825.414304,
                                                                  'Plan Rows': 2352337.0}]},
                                                                   {'Node Type': 'Index Scan', 'Node Type ID': '21',
                                                                    'Relation Name': 'movie_info',
                                                                    'Total Cost': 16061152.564067,
                                                                    'Plan Rows': 65262.0}]}]}]},
                                                {'Node Type': 'Other', 'Node Type ID': '47', 'Total Cost': 1.15,
                                                 'Plan Rows': 1.0, 'Plans': [
                                                    {'Node Type': 'Seq Scan', 'Node Type ID': '19',
                                                     'Relation Name': 'role_type', 'Total Cost': 1.15,
                                                     'Plan Rows': 1.0}]}]},
                                            {'Node Type': 'Other', 'Node Type ID': '47', 'Total Cost': 1.09625,
                                             'Plan Rows': 3.0, 'Plans': [{'Node Type': 'Seq Scan', 'Node Type ID': '19',
                                                                          'Relation Name': 'kind_type',
                                                                          'Total Cost': 1.09625,
                                                                          'Plan Rows': 3.0}]}]}]}]},
                                {'Node Type': 'Other', 'Node Type ID': '39', 'Total Cost': 2.4175, 'Plan Rows': 1.0,
                                 'Plans': [{'Node Type': 'Seq Scan', 'Node Type ID': '19', 'Relation Name': 'info_type',
                                            'Total Cost': 2.4125, 'Plan Rows': 1.0}]}]}]}}, {
                    'Plan': {'Node Type': 'Other', 'Node Type ID': '42', 'Total Cost': 634649.843133, 'Plan Rows': 1.0,
                             'Plans': [{'Node Type': 'Nested Loop', 'Node Type ID': '36', 'Total Cost': 634649.558133,
                                        'Plan Rows': 110.0, 'Plans': [
                                     {'Node Type': 'Seq Scan', 'Node Type ID': '19', 'Relation Name': 'info_type',
                                      'Total Cost': 2.4125, 'Plan Rows': 1.0},
                                     {'Node Type': 'Nested Loop', 'Node Type ID': '36', 'Total Cost': 634646.045633,
                                      'Plan Rows': 110.0, 'Plans': [
                                         {'Node Type': 'Seq Scan', 'Node Type ID': '19', 'Relation Name': 'info_type',
                                          'Total Cost': 2.4125, 'Plan Rows': 1.0},
                                         {'Node Type': 'Other', 'Node Type ID': '45', 'Total Cost': 634642.533133,
                                          'Plan Rows': 110.0, 'Plans': [
                                             {'Node Type': 'Nested Loop', 'Node Type ID': '36',
                                              'Total Cost': 633631.533133, 'Plan Rows': 46.0, 'Plans': [
                                                 {'Node Type': 'Hash Join', 'Node Type ID': '38',
                                                  'Total Cost': 632552.605984, 'Plan Rows': 201.0, 'Plans': [
                                                     {'Node Type': 'Nested Loop', 'Node Type ID': '36',
                                                      'Total Cost': 632542.873234, 'Plan Rows': 2413.0, 'Plans': [
                                                         {'Node Type': 'Hash Join', 'Node Type ID': '38',
                                                          'Total Cost': 624024.582747, 'Plan Rows': 168.0, 'Plans': [
                                                             {'Node Type': 'Nested Loop', 'Node Type ID': '36',
                                                              'Total Cost': 624021.794372, 'Plan Rows': 393.0,
                                                              'Plans': [{'Node Type': 'Hash Join', 'Node Type ID': '38',
                                                                         'Total Cost': 606011.475, 'Plan Rows': 3240.0,
                                                                         'Plans': [{'Node Type': 'Seq Scan',
                                                                                    'Node Type ID': '19',
                                                                                    'Relation Name': 'movie_info',
                                                                                    'Total Cost': 285615.0,
                                                                                    'Plan Rows': 16355.0},
                                                                                   {'Node Type': 'Other',
                                                                                    'Node Type ID': '47',
                                                                                    'Total Cost': 277888.0625,
                                                                                    'Plan Rows': 27192.0, 'Plans': [
                                                                                       {'Node Type': 'Seq Scan',
                                                                                        'Node Type ID': '19',
                                                                                        'Relation Name': 'movie_info',
                                                                                        'Total Cost': 277888.0625,
                                                                                        'Plan Rows': 27192.0}]}]},
                                                                        {'Node Type': 'Other', 'Node Type ID': '24',
                                                                         'Relation Name': 'title',
                                                                         'Total Cost': 5.558741, 'Plan Rows': 1.0,
                                                                         'Plans': [{'Node Type': 'Bitmap Index Scan',
                                                                                    'Node Type ID': '23',
                                                                                    'Relation Name': 'title',
                                                                                    'Total Cost': 1.540991,
                                                                                    'Plan Rows': 1.0}]}]},
                                                             {'Node Type': 'Other', 'Node Type ID': '47',
                                                              'Total Cost': 1.09625, 'Plan Rows': 3.0, 'Plans': [
                                                                 {'Node Type': 'Seq Scan', 'Node Type ID': '19',
                                                                  'Relation Name': 'kind_type', 'Total Cost': 1.09625,
                                                                  'Plan Rows': 3.0}]}]},
                                                         {'Node Type': 'Other', 'Node Type ID': '24',
                                                          'Relation Name': 'cast_info', 'Total Cost': 50.35411,
                                                          'Plan Rows': 35.0, 'Plans': [
                                                             {'Node Type': 'Bitmap Index Scan', 'Node Type ID': '23',
                                                              'Relation Name': 'cast_info', 'Total Cost': 2.155946,
                                                              'Plan Rows': 35.0}]}]},
                                                     {'Node Type': 'Other', 'Node Type ID': '47', 'Total Cost': 1.15,
                                                      'Plan Rows': 1.0, 'Plans': [
                                                         {'Node Type': 'Seq Scan', 'Node Type ID': '19',
                                                          'Relation Name': 'role_type', 'Total Cost': 1.15,
                                                          'Plan Rows': 1.0}]}]},
                                                 {'Node Type': 'Other', 'Node Type ID': '24', 'Relation Name': 'name',
                                                  'Total Cost': 5.367797, 'Plan Rows': 1.0, 'Plans': [
                                                     {'Node Type': 'Bitmap Index Scan', 'Node Type ID': '23',
                                                      'Relation Name': 'name', 'Total Cost': 1.352547,
                                                      'Plan Rows': 1.0}]}]}]}]}]}]}}, {
                    'Plan': {'Node Type': 'Other', 'Node Type ID': '42', 'Total Cost': 20001818246.16712,
                             'Plan Rows': 1.0, 'Plans': [
                            {'Node Type': 'Hash Join', 'Node Type ID': '38', 'Total Cost': 20001818245.882122,
                             'Plan Rows': 110.0, 'Plans': [
                                {'Node Type': 'Seq Scan', 'Node Type ID': '19', 'Relation Name': 'name',
                                 'Total Cost': 107708.55, 'Plan Rows': 952446.0},
                                {'Node Type': 'Other', 'Node Type ID': '47', 'Total Cost': 20001706958.522125,
                                 'Plan Rows': 483.0, 'Plans': [
                                    {'Node Type': 'Hash Join', 'Node Type ID': '38', 'Total Cost': 20001706958.522125,
                                     'Plan Rows': 483.0, 'Plans': [{'Node Type': 'Hash Join', 'Node Type ID': '38',
                                                                    'Total Cost': 20001706936.784874,
                                                                    'Plan Rows': 5791.0, 'Plans': [
                                            {'Node Type': 'Seq Scan', 'Node Type ID': '19',
                                             'Relation Name': 'cast_info', 'Total Cost': 615130.44,
                                             'Plan Rows': 36244344.0}, {'Node Type': 'Other', 'Node Type ID': '47',
                                                                        'Total Cost': 20000955827.094875,
                                                                        'Plan Rows': 404.0, 'Plans': [
                                                    {'Node Type': 'Hash Join', 'Node Type ID': '38',
                                                     'Total Cost': 20000955827.094875, 'Plan Rows': 404.0, 'Plans': [
                                                        {'Node Type': 'Nested Loop', 'Node Type ID': '36',
                                                         'Total Cost': 10000418659.5755, 'Plan Rows': 65262.0,
                                                         'Plans': [{'Node Type': 'Other', 'Node Type ID': '24',
                                                                    'Relation Name': 'movie_info',
                                                                    'Total Cost': 417841.3855, 'Plan Rows': 65262.0,
                                                                    'Plans': [{'Node Type': 'Bitmap Index Scan',
                                                                               'Node Type ID': '23',
                                                                               'Relation Name': 'movie_info',
                                                                               'Total Cost': 28163.645,
                                                                               'Plan Rows': 1523628.0}]},
                                                                   {'Node Type': 'Other', 'Node Type ID': '39',
                                                                    'Total Cost': 2.4175, 'Plan Rows': 1.0, 'Plans': [
                                                                       {'Node Type': 'Seq Scan', 'Node Type ID': '19',
                                                                        'Relation Name': 'info_type',
                                                                        'Total Cost': 2.4125, 'Plan Rows': 1.0}]}]},
                                                        {'Node Type': 'Other', 'Node Type ID': '47',
                                                         'Total Cost': 10000536723.121874, 'Plan Rows': 15650.0,
                                                         'Plans': [{'Node Type': 'Hash Join', 'Node Type ID': '38',
                                                                    'Total Cost': 10000536723.121874,
                                                                    'Plan Rows': 15650.0, 'Plans': [
                                                                 {'Node Type': 'Nested Loop', 'Node Type ID': '36',
                                                                  'Total Cost': 10000431791.16575, 'Plan Rows': 39253.0,
                                                                  'Plans': [{'Node Type': 'Other', 'Node Type ID': '24',
                                                                             'Relation Name': 'movie_info',
                                                                             'Total Cost': 431298.08825,
                                                                             'Plan Rows': 39253.0, 'Plans': [
                                                                          {'Node Type': 'Bitmap Index Scan',
                                                                           'Node Type ID': '23',
                                                                           'Relation Name': 'movie_info',
                                                                           'Total Cost': 26756.475,
                                                                           'Plan Rows': 1447472.0}]},
                                                                            {'Node Type': 'Other', 'Node Type ID': '39',
                                                                             'Total Cost': 2.4175, 'Plan Rows': 1.0,
                                                                             'Plans': [{'Node Type': 'Seq Scan',
                                                                                        'Node Type ID': '19',
                                                                                        'Relation Name': 'info_type',
                                                                                        'Total Cost': 2.4125,
                                                                                        'Plan Rows': 1.0}]}]},
                                                                 {'Node Type': 'Other', 'Node Type ID': '47',
                                                                  'Total Cost': 83840.457375, 'Plan Rows': 1008144.0,
                                                                  'Plans': [
                                                                      {'Node Type': 'Hash Join', 'Node Type ID': '38',
                                                                       'Total Cost': 83840.457375,
                                                                       'Plan Rows': 1008144.0, 'Plans': [
                                                                          {'Node Type': 'Seq Scan',
                                                                           'Node Type ID': '19',
                                                                           'Relation Name': 'title',
                                                                           'Total Cost': 73925.905,
                                                                           'Plan Rows': 2352337.0},
                                                                          {'Node Type': 'Other', 'Node Type ID': '47',
                                                                           'Total Cost': 1.09625, 'Plan Rows': 3.0,
                                                                           'Plans': [{'Node Type': 'Seq Scan',
                                                                                      'Node Type ID': '19',
                                                                                      'Relation Name': 'kind_type',
                                                                                      'Total Cost': 1.09625,
                                                                                      'Plan Rows': 3.0}]}]}]}]}]}]}]}]},
                                                                   {'Node Type': 'Other', 'Node Type ID': '47',
                                                                    'Total Cost': 1.15, 'Plan Rows': 1.0, 'Plans': [
                                                                       {'Node Type': 'Seq Scan', 'Node Type ID': '19',
                                                                        'Relation Name': 'role_type',
                                                                        'Total Cost': 1.15,
                                                                        'Plan Rows': 1.0}]}]}]}]}]}}, {
                    'Plan': {'Node Type': 'Other', 'Node Type ID': '42', 'Total Cost': 626926.521585, 'Plan Rows': 1.0,
                             'Plans': [{'Node Type': 'Nested Loop', 'Node Type ID': '36', 'Total Cost': 626926.236585,
                                        'Plan Rows': 110.0, 'Plans': [
                                     {'Node Type': 'Seq Scan', 'Node Type ID': '19', 'Relation Name': 'info_type',
                                      'Total Cost': 2.4125, 'Plan Rows': 1.0},
                                     {'Node Type': 'Nested Loop', 'Node Type ID': '36', 'Total Cost': 626922.724085,
                                      'Plan Rows': 110.0, 'Plans': [
                                         {'Node Type': 'Seq Scan', 'Node Type ID': '19', 'Relation Name': 'info_type',
                                          'Total Cost': 2.4125, 'Plan Rows': 1.0},
                                         {'Node Type': 'Other', 'Node Type ID': '45', 'Total Cost': 626919.211585,
                                          'Plan Rows': 110.0, 'Plans': [
                                             {'Node Type': 'Nested Loop', 'Node Type ID': '36',
                                              'Total Cost': 625908.211585, 'Plan Rows': 46.0, 'Plans': [
                                                 {'Node Type': 'Hash Join', 'Node Type ID': '38',
                                                  'Total Cost': 625449.418324, 'Plan Rows': 201.0, 'Plans': [
                                                     {'Node Type': 'Nested Loop', 'Node Type ID': '36',
                                                      'Total Cost': 625439.685574, 'Plan Rows': 2413.0, 'Plans': [
                                                         {'Node Type': 'Hash Join', 'Node Type ID': '38',
                                                          'Total Cost': 617475.303159, 'Plan Rows': 168.0, 'Plans': [
                                                             {'Node Type': 'Nested Loop', 'Node Type ID': '36',
                                                              'Total Cost': 617472.514784, 'Plan Rows': 393.0,
                                                              'Plans': [{'Node Type': 'Hash Join', 'Node Type ID': '38',
                                                                         'Total Cost': 606011.475, 'Plan Rows': 3240.0,
                                                                         'Plans': [{'Node Type': 'Seq Scan',
                                                                                    'Node Type ID': '19',
                                                                                    'Relation Name': 'movie_info',
                                                                                    'Total Cost': 285615.0,
                                                                                    'Plan Rows': 16355.0},
                                                                                   {'Node Type': 'Other',
                                                                                    'Node Type ID': '47',
                                                                                    'Total Cost': 277888.0625,
                                                                                    'Plan Rows': 27192.0, 'Plans': [
                                                                                       {'Node Type': 'Seq Scan',
                                                                                        'Node Type ID': '19',
                                                                                        'Relation Name': 'movie_info',
                                                                                        'Total Cost': 277888.0625,
                                                                                        'Plan Rows': 27192.0}]}]},
                                                                        {'Node Type': 'Index Scan',
                                                                         'Node Type ID': '21', 'Relation Name': 'title',
                                                                         'Total Cost': 3.537358, 'Plan Rows': 1.0}]},
                                                             {'Node Type': 'Other', 'Node Type ID': '47',
                                                              'Total Cost': 1.09625, 'Plan Rows': 3.0, 'Plans': [
                                                                 {'Node Type': 'Seq Scan', 'Node Type ID': '19',
                                                                  'Relation Name': 'kind_type', 'Total Cost': 1.09625,
                                                                  'Plan Rows': 3.0}]}]},
                                                         {'Node Type': 'Index Scan', 'Node Type ID': '21',
                                                          'Relation Name': 'cast_info', 'Total Cost': 47.057038,
                                                          'Plan Rows': 35.0}]},
                                                     {'Node Type': 'Other', 'Node Type ID': '47', 'Total Cost': 1.15,
                                                      'Plan Rows': 1.0, 'Plans': [
                                                         {'Node Type': 'Seq Scan', 'Node Type ID': '19',
                                                          'Relation Name': 'role_type', 'Total Cost': 1.15,
                                                          'Plan Rows': 1.0}]}]},
                                                 {'Node Type': 'Index Scan', 'Node Type ID': '21',
                                                  'Relation Name': 'name', 'Total Cost': 2.282554,
                                                  'Plan Rows': 1.0}]}]}]}]}]}}]
        buffers = {'title_pkey': 6, 'kind_id_title': 1, 'title': 35998, 'kind_type_pkey': 1, 'movie_info_pkey': 1,
                   'info_type_id_movie_info': 1, 'movie_id_movie_info': 6, 'info_type_pkey': 1, 'cast_info_pkey': 1,
                   'movie_id_cast_info': 8, 'person_id_cast_info': 8, 'person_role_id_cast_info': 1,
                   'role_id_cast_info': 1, 'role_type_pkey': 1, 'name_pkey': 4, 'movie_info': 98, 'cast_info': 4,
                   'name': 1, 'kind_type': 1}

        start = time.time()
        print("============ in select_plan")
        print("arms: ", arms)
        print("buffers: ", buffers)
        print("============ in select_plan")

        # if we don't have a model, default to the PG optimizer
        if self.__current_model is None:
            return PG_OPTIMIZER_INDEX

        # if we do have a model, make predictions for each plan.
        arms = add_buffer_info_to_plans(buffers, arms)
        res = self.__current_model.predict(arms)
        idx = res.argmin()
        stop = time.time()
        print("Selected index", idx,
              "after", f"{round((stop - start) * 1000)}ms",
              "Predicted reward / PG:", res[idx][0],
              "/", res[0][0])
        return idx

    def load_model(self, fp):
        try:
            new_model = model.BaoRegression(have_cache_data=True)
            new_model.load(fp)

            if reg_blocker.should_replace_model(
                    self.__current_model,
                    new_model):
                self.__current_model = new_model
                print("Accepted new model.")
            else:
                print("Rejecting load of new model due to regresison profile.")

        except Exception as e:
            print("Failed to load Bao model from", fp,
                  "Exception:", sys.exc_info()[0])
            raise e


class JSONTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        str_buf = ""
        while True:
            str_buf += self.request.recv(1024).decode("UTF-8")
            if not str_buf:
                # no more data, connection is finished.
                return

            if (null_loc := str_buf.find("\n")) != -1:
                json_msg = str_buf[:null_loc].strip()
                str_buf = str_buf[null_loc + 1:]
                if json_msg:
                    try:
                        if self.handle_json(json.loads(json_msg)):
                            break
                    except json.decoder.JSONDecodeError:
                        print("Error decoding JSON:", json_msg)
                        break


class BaoJSONHandler(JSONTCPHandler):
    def setup(self):
        self.__messages = []

    def handle_json(self, data):
        if "final" in data:
            message_type = self.__messages[0]["type"]
            self.__messages = self.__messages[1:]

            if message_type == "query":
                result = self.server.bao_model.select_plan(self.__messages)
                self.request.sendall(struct.pack("I", result))
                self.request.close()
            elif message_type == "predict":
                result = self.server.bao_model.predict(self.__messages)
                self.request.sendall(struct.pack("d", result))
                self.request.close()
            elif message_type == "reward":
                plan, buffers, obs_reward = self.__messages
                plan = add_buffer_info_to_plans(buffers, [plan])[0]
                storage.record_reward(plan, obs_reward["reward"], obs_reward["pid"])
            elif message_type == "load model":
                path = self.__messages[0]["path"]
                self.server.bao_model.load_model(path)
            else:
                print("Unknown message type:", message_type)

            return True

        self.__messages.append(data)
        return False


def start_server(listen_on, port):
    model = BaoModel()

    if os.path.exists(DEFAULT_MODEL_PATH):
        print("Loading existing model")
        model.load_model(DEFAULT_MODEL_PATH)

    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer((listen_on, port), BaoJSONHandler) as server:
        server.bao_model = model
        server.serve_forever()


if __name__ == "__main__":
    modelObj = BaoModel()
    modelObj.load_model(DEFAULT_MODEL_PATH)
    modelObj.predict_local_test("asdf")



    from multiprocessing import Process
    from config import read_config

    config = read_config()
    port = int(config["Port"])
    listen_on = config["ListenOn"]

    print(f"Listening on {listen_on} port {port}")

    server = Process(target=start_server, args=['0.0.0.0', port])

    print("Spawning server process...")
    server.start()
