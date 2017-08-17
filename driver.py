import os

def execute_cqlsh(query):
    if query != "":
        os.system("python2 /usr/local/bin/cqlsh --cqlversion=\"3.4.4\" -e \"%s\"" % query)