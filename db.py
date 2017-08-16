from cassandra.cluster import Cluster
import gen

CLUSTER = Cluster()
SESSION = CLUSTER.connect("galaxy_00")

def add_record(obj_type, params):
    if obj_type == "planet":
        name = params[0]
        poz = params[1]
        poz_hash = gen.gen_crc32_hash(poz)
        id = gen.gen_crc32_hash((name, params[1], poz_hash))

        SESSION.execute(
            """
            INSERT INTO planets (id, name, poz_hash, x, y, z)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (id, name, poz_hash, poz[0], poz[1], poz[2])
        )