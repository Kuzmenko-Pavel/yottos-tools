#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
import sys
reload(sys)
sys.setdefaultencoding('UTF8')

import os
import io
from pymongo import MongoClient
from itertools import izip_longest, count


project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
os.environ['PYTHON_EGG_CACHE'] = '/usr/lib/python2.7/dist-packages'

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'

conn = MongoClient(host=main_db_host)
db = conn.getmyad_db


class Tree(object):
    "Generic tree node."
    seq_id = count().next

    def __init__(self, name='root', level=None):
        self.id = self.seq_id()
        self.name = name
        self.parent = None
        self.path_id = 0
        self.path_ids = []
        self.level = -1
        if level is not None:
            self.level = level
        self.children = []

    def __repr__(self):
        return self.name

    def add_child(self, node):
        assert isinstance(node, Tree)
        node.parent = self
        self.children.append(node)

    def create_id(self):
        c = 0
        for child in self.children:
            c += 1
            child.path_id = c
            child.path_ids = self.path_ids[::]
            child.path_ids.append(str(child.path_id))
            child.create_id()

    @property
    def path(self):
        if self.path_ids:
            return '.'.join(self.path_ids)
        else:
            return '0'


tree = Tree()

with io.open('tematic_cat.txt', mode="r", encoding="utf-8") as file:
    prev = tree
    for line in file.readlines():
        level = line.count('    ')
        if prev.level < level:
            parent = prev
            node = Tree(name=line.strip(), level=level)
            parent.add_child(node)
            prev = node
        elif prev.level == level:
            parent = prev.parent
            node = Tree(name=line.strip(), level=level)
            parent.add_child(node)
            prev = node
        elif prev.level > level:
            step = prev.level - level
            parent = prev
            for x in range(0, step + 1):
                parent = parent.parent

            node = Tree(name=line.strip(), level=level)
            parent.add_child(node)
            prev = node


def print_n(t):
    if t is None:
        return
    s = '%s %s %s %s' % (('\t' * t.level), str(t.id), t.name, t.path)
    print(s)
    doc = {
        'id': t.id,
        'path_id': t.path_id,
        'name': t.name,
        'path': t.path,
        'children_id': [x.id for x in t.children]
    }
    if t.parent:
        doc['parent_id'] = t.parent.id
    db['thematic'].insert_one(doc)
    for child in t.children:
        print_n(child)

tree.create_id()
print_n(tree)


# def cat_to_int_range(cat):
#     max_size = 1000
#     cats = [int(x) for x in cat.split('.')]
#     cats = [i if i else j for i, j in izip_longest(cats, [0, 0, 0, 0, 0], fillvalue=0)]
#     start_cats = cats[::]
#     end_cats = cats[::]
#     if cats[-1] < 1:
#         l = len(cats)
#         for x, y in enumerate(reversed(cats)):
#             if y == 0:
#                 start_cats[l-(x+1)] = 0
#                 end_cats[l - (x + 1)] = max_size
#             else:
#                 break
#     start = sum([y * (max_size ** x) for x, y in enumerate(reversed(start_cats))])
#     end = sum([y * (max_size ** x) for x, y in enumerate(reversed(end_cats))])
#     return start, end
#
#
# print cat_to_int_range('10.0.0.12')
# print cat_to_int_range('10.0.1')
# print cat_to_int_range('10.0.0.12')
# print cat_to_int_range('12')
# print cat_to_int_range('77.88.21.8')