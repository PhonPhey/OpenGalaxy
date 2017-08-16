import random
import string
import time
import zlib

import numpy as np
from cassandra.cluster import Cluster

import db
import gen

CLUSTER = Cluster()
SESSION = CLUSTER.connect("galaxy_00")


def gen_crc32_hash(params):
    id = str(hex(zlib.crc32(str(params).encode("utf-8"))).split('x')[-1])

    return id


def name_generator(size):

    tumbler = np.random.randint(1, 3)
    if tumbler == 1:
        return string.capwords(''.join(random.choice(string.ascii_uppercase) for _ in range(size)))
    else:
        return string.capwords(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(size)))


def gen_planets(count):
    for i in range(0, count, 1):

        x = round(np.random.uniform(-11, 11), 2)
        y = round(np.random.uniform(-11, 11), 2)
        z = round(np.random.uniform(-11, 11), 2)
        poz = (x, y, z)

        query = int(list(list(SESSION.execute("""SELECT count(*) FROM planets WHERE poz_hash='%s' ALLOW FILTERING;""" % gen_crc32_hash(poz)))[0])[0])

        if query == 1:
            
            print("/|\\")
            tumbler = 0

            while tumbler == 0:
                print("/|\\")

                x = np.random.uniform(-11, 11)
                y = np.random.uniform(-11, 11)
                z = np.random.uniform(-11, 11)
                poz = (x, y, z)

                query = int(list(list(SESSION.execute("""SELECT count(*) FROM planets WHERE poz_hash='%s' ALLOW FILTERING; """ % gen_crc32_hash(poz)))[0])[0])
                if query == 0:
                    tumbler = 1
                
        db.add_record('planet', (name_generator(np.random.randint(3, 6)), poz))
