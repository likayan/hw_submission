#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自定义分区器模块
实现CustomPartitioner（基于加盐策略），用于处理数据倾斜问题
"""

def add_salt_to_rdd(rdd, hot_keys, salt_count: int):
    """对RDD中的热点Key进行加盐处理"""
    if hot_keys is None:
        hot_keys_set = frozenset()
    elif isinstance(hot_keys, (set, frozenset, list)):
        hot_keys_set = frozenset(hot_keys)
    else:
        hot_keys_set = frozenset([hot_keys])
    
    salt_count_val = int(salt_count)
    
    def add_salt(item):
        key, value = item
        if key in hot_keys_set:
            return [((key, salt), value) for salt in range(salt_count_val)]
        else:
            return [((key, 0), value)]
    
    return rdd.flatMap(add_salt)


def remove_salt_from_rdd(rdd):
    """去除RDD中的盐值，恢复原始Key"""
    def remove_salt(item):
        (key, salt), value = item
        return (key, value)
    
    return rdd.map(remove_salt)


def apply_salting_strategy(rdd, hot_keys, salt_count: int, 
                          num_partitions: int, reduce_func):
    """完整的CustomPartitioner策略应用流程"""
    hot_keys_frozen = frozenset(hot_keys) if hot_keys else frozenset()
    salt_count_int = int(salt_count)
    num_partitions_int = int(num_partitions)
    
    salted_rdd = add_salt_to_rdd(rdd, hot_keys_frozen, salt_count_int)
    aggregated_salted_rdd = salted_rdd.reduceByKey(reduce_func, numPartitions=num_partitions_int)
    desalted_rdd = remove_salt_from_rdd(aggregated_salted_rdd)
    final_rdd = desalted_rdd.reduceByKey(reduce_func)
    
    return final_rdd

