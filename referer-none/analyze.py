# This Python file uses the following encoding: utf-8
from peewee import *
import httpagentparser
db = SqliteDatabase('analyze.db')
db.pragma('foreign_keys', 1, permanent=True)


class Adv(Model):
    guid = UUIDField()

    class Meta:
        database = db


class Log(Model):
    agent = TextField()
    os = CharField()
    browser = CharField()

    class Meta:
        database = db


class Stats(Model):
    agent = TextField()

    class Meta:
        database = db


db.create_tables([Log, Log], safe=True)
