# coding:utf8
import jieba


def context_jieba(data):
    """jieba分词"""
    seg = jieba.cut_for_search(data)
    l = []
    for word in seg:
        l.append(word)

    return l


def filter_word(data):
    return data not in ['谷', '帮', '客']


def append_word(data):
    """修订关键词"""
    if data == "传智播": data = "传智播客"
    if data == "院校": data = "院校帮"
    if data == "博学": data = "博学谷"
    return (data, 1)


def getUserWord(data):
    """"""
    words = context_jieba(data[1])
    return_list = []
    for word in words:
        if filter_word(word):
            return_list.append(
                (data[0] + "_" + append_word(word)[0], 1)
                               )
    return return_list
