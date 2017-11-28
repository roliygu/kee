#! usr/bin/python
# coding=utf-8

import sys
import hashlib

slot_map = {}


def hash_str2int(string):
    return int(hashlib.sha1(string.encode()).hexdigest(), 16) % (1000)


def get_slot_index_dis(fea_name, fea_value):
    map_key = fea_name + '\t' + fea_value
    slot_index = hash_str2int(map_key)
    if map_key not in slot_map:
        slot_map[map_key] = slot_index
    return slot_index


def get_slot_index_con(fea_name):
    map_key = fea_name
    slot_index = hash_str2int(map_key)
    if map_key not in slot_map:
        slot_map[map_key] = slot_index
    return slot_index


def process_one_line(uid, age, sex, activeDate, limit,
                     click_num, order_num, order_sum, loan_num, loan_sum,
                     label):
    # if float(label) <= 0:
    #    return
    age_slot_index = get_slot_index_dis('age', age)
    sex_slot_index = get_slot_index_dis('sex', sex)
    # active_date_slot_index = get_slot_index_dis('active', activeDate)
    limit_slot_index = get_slot_index_con('limit')
    click_num_index = get_slot_index_con('click_num')
    order_num_index = get_slot_index_con('order_num')
    order_sum_index = get_slot_index_con('order_sum')
    loan_num_index = get_slot_index_con('loan_num')
    loan_sum_index = get_slot_index_con('loan_sum')
    print("%s %d:1 %d:1 %d:%s %d:%s %d:%s %d:%s %d:%s %d:%s" %
          (label, age_slot_index, sex_slot_index,
           limit_slot_index, limit,
           click_num_index, click_num,
           order_num_index, order_num,
           order_sum_index, order_sum,
           loan_num_index, loan_num,
           loan_sum_index, loan_sum))


def process(fea_arr):
    uid, age, sex, activeDate, limit, _8_click, _9_click, \
    _10_click, _11_click, _8_order_num, _8_order_sum, \
    _9_order_num, _9_order_sum, _10_order_num, _10_order_sum, \
    _11_order_num, _11_order_sum, _8_loan_num, _8_loan_sum, \
    _9_loan_num, _9_loan_sum, _10_loan_num, _10_loan_sum, \
    _11_loan_num, _11_loan_sum, loan_sum = fea_arr

    process_one_line(uid, age, sex, activeDate, limit, _8_click,
                     _8_order_num, _8_order_sum, _8_loan_num, _8_loan_sum, _9_loan_sum)
    process_one_line(uid, age, sex, activeDate, limit, _9_click,
                     _9_order_num, _9_order_sum, _9_loan_num, _9_loan_sum, _10_loan_sum)
    process_one_line(uid, age, sex, activeDate, limit, _10_click,
                     _10_order_num, _10_order_sum, _10_loan_num, _10_loan_sum, _11_loan_sum)


if __name__ == '__main__':
    for line in sys.stdin:
        try:
            arr = line.strip().split(',')
            assert (len(arr) == 26)
            process(arr)
        except Exception as e:
            print(e)
            continue
