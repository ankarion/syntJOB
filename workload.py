import os
from settings import WORKLOAD_DIR

# Generator that iterates through all workload
def queries():
    size = len(os.listdir(WORKLOAD_DIR))
    listdir = filter(lambda file: file.endswith(".sql"), os.listdir(WORKLOAD_DIR))
    for i, file in enumerate(listdir):
        with open(WORKLOAD_DIR + '/' + file, "r") as f:
            query = f.read()
            percent = i/size
            percent = int(percent * 100)
            #print("loadbar: " + '[' + '#'*percent + '-'+(100-percent) + ']')
            yield(file, query)
