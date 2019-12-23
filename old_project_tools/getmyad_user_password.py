# This Python file uses the following encoding: utf-8
import sys
import csv
from pymongo import MongoClient


sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
conn = MongoClient(host=main_db_host)
db = conn.getmyad_db


def makePassword():
    """Возвращает сгенерированный пароль"""
    from random import Random
    rng = Random()

    righthand = '23456qwertasdfgzxcvbQWERTASDFGZXCVB'
    lefthand = '789yuiophjknmYUIPHJKLNM'
    allchars = righthand + lefthand

    passwordLength = 8
    alternate_hands = True
    password = ''

    for i in range(passwordLength):
        if not alternate_hands:
            password += rng.choice(allchars)
        else:
            if i % 2:
                password += rng.choice(lefthand)
            else:
                password += rng.choice(righthand)
    return password


for user in db.users.find({'accountType': 'user'}, {'login': 1, 'email': 1}):
    new_password = makePassword()
    db.users.update_one({'_id': user.get('_id')},{'$set': {'password': new_password}})
    print('%s\t%s\t%s' % (user.get('login', ''), user.get('email', ''), new_password))
