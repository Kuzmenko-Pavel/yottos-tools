#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
import sys
reload(sys)
sys.setdefaultencoding('UTF8')

import os
import io
from pymongo import MongoClient


project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
os.environ['PYTHON_EGG_CACHE'] = '/usr/lib/python2.7/dist-packages'

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'

conn = MongoClient(host=main_db_host)
db = conn.getmyad_db


class Tree(object):
    "Generic tree node."
    def __init__(self, name='root', parent=None, level=None, children=None):
        self.name = name
        self.parent = parent
        self.level = -1
        if level is not None:
            self.level = level
        self.children = []
        if children is not None:
            for child in children:
                self.add_child(child)

    def __repr__(self):
        return self.name

    def add_child(self, node):
        assert isinstance(node, Tree)
        self.children.append(node)


tree = Tree()

with io.open('tematic_cat.txt', mode="r", encoding="utf-8") as file:
    prev = tree
    for line in file.readlines():
        level = line.count('    ')
        if prev.level < level:
            parent = prev
            node = Tree(name=line, parent=parent, level=level)
            parent.add_child(node)
            prev = node
        elif prev.level == level:
            parent = prev.parent
            node = Tree(name=line, parent=parent, level=level)
            parent.add_child(node)
            prev = node
        elif prev.level > level:
            step = prev.level - level
            parent = prev
            for x in range(0, step):
                parent = parent.parent

            node = Tree(name=line, parent=parent, level=level)
            parent.add_child(node)
            prev = node


def print_n(t):
    if t is None:
        return
    print('%s%s' % ('\t' * t.level, t.name))
    for child in t.children:
        print_n(child)

print_n(tree)