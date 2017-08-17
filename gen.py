import random
import string
import time
import zlib
import os

import numpy as np
from cassandra.cluster import Cluster

import gen
import driver

CLUSTER = Cluster()
SESSION = CLUSTER.connect("galaxy_00")


def gen_crc32_hash(params):
    id = str(hex(zlib.crc32(str(params).encode("utf-8"))).split('x')[-1])

    return id


def gen_name(size):

    tumbler = np.random.randint(1, 3)
    if tumbler == 1:
        return string.capwords(''.join(random.choice(string.ascii_uppercase) for _ in range(size)))
    else:
        return string.capwords(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(size)))


def gen_planets(count):
    records_queris = str()
    queris_in_pack = 0

    for i in range(0, count, 1):

        x = round(np.random.uniform(-11, 11), 2)
        y = round(np.random.uniform(-11, 11), 2)
        z = round(np.random.uniform(-11, 11), 2)
        poz = (x, y, z)

        query = int(list(list(SESSION.execute(
            """SELECT count(*) FROM planets WHERE id='%s';""" % gen_crc32_hash(poz)))[0])[0])

        if query == 1:

            print("/|\\")
            tumbler = 0

            while tumbler == 0:
                print("/|\\")

                x = np.random.uniform(-11, 11)
                y = np.random.uniform(-11, 11)
                z = np.random.uniform(-11, 11)
                poz = (x, y, z)

                query = int(list(list(SESSION.execute(
                    """SELECT count(*) FROM planets WHERE id='%s'; """ % gen_crc32_hash(poz)))[0])[0])
                if query == 0:
                    tumbler = 1

        id = gen.gen_crc32_hash(poz)
        name = gen_name(np.random.randint(3, 6))
        records_queris += "INSERT INTO galaxy_00.planets (id, name, x, y, z) VALUES ('%s', '%s', %s, %s, %s); " % (
            id, name, poz[0], poz[1], poz[2])
        queris_in_pack += 1
        if queris_in_pack >= 1000:
            queris_in_pack = 0
            driver.execute_cqlsh(records_queris)
            records_queris = ""

        os.system("clear")
        print(i+1)
        print("Queris in pack: %d" % queris_in_pack)
    driver.execute_cqlsh(records_queris)
