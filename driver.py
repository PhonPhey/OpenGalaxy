import os

def execute_cqlsh(query):
    if query != "":
        os.system("cqlsh -e \"%s\"" % query)
