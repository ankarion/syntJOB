import os
import re

WORKLOAD_DIR = "./"

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

# parse sql query, get:
# tables aliases and join conditions
def parseQuery(query):
    aliases = query.split("FROM")[1]
    aliases = aliases.split("WHERE")[0]

    # in case we didn't have where clause
    aliases = aliases.split(";")[0] 
    aliases = aliases.strip()

    # mark aliases
    # XXX there is a better way to do this
    aliases = aliases.replace(" AS ", "->")
    aliases = aliases.replace(" as ", "->")

    # split aliases from each other
    aliases = aliases.replace(",", " ")
    aliases = aliases.replace("\n", " ")
    aliases = aliases.replace("\t", " ")
    aliases = aliases.split(" ")

    # remove empty lines
    aliases = filter(lambda el: True if el else False, aliases)
    aliases = [i.split("->")[::-1] for i in aliases]
    aliases = dict(aliases)

    # parse join conditions
    joinCondReg = "("
    joinCondReg += "|".join(aliases.keys()) 
    joinCondReg += "|"
    joinCondReg += "|".join(aliases.values())
    joinCondReg += ")"
    # joinCondReg = fr"{joinCondReg}\.\w+ = {joinCondReg}\.\w+"
    joinCondReg = fr"({joinCondReg}\.\w+ = {joinCondReg}\.\w+)"
    joinConds = re.findall(joinCondReg, query)
    joinConds = [i[0] for i in joinConds]

    return(aliases, joinConds)

def parseJoinCond(join,aliases):
    joinTbls = join.split()
    joinTbls = sorted([joinTbls[0], joinTbls[-1]])

    joinFields = ", ".join(joinTbls)
    joinAliases = [i.split(".")[0] for i in joinTbls]
    joinTbls = [aliases[i.split(".")[0]] for i in joinTbls]

    columns = joinFields.split(",")
    columns = [i.strip() for i in columns]
    columns = [i+" AS "+i.replace(".","_") for i in columns]
    columns = ", ".join(columns)

    return(joinTbls, joinAliases, columns)

def getTableName(joinTbls, joinAliases):
    return f"{joinTbls[0]}_as_{joinAliases[0]}_{joinTbls[1]}_as_{joinAliases[1]}"

def getSQL(joinTbls, joinAliases, columns, join):
    table_name = getTableName(joinTbls, joinAliases)
    table_name = "_EQ_".join([i.split("AS")[1].strip() for i in columns.split(",")])

    SQLTemplate =f"""
    CREATE TABLE if not EXISTS {table_name}
    AS 
        SELECT DISTINCT {columns} 
        FROM {joinTbls[0]} AS {joinAliases[0]},
            {joinTbls[1]} AS {joinAliases[1]} 
        WHERE {join};
    """
    return SQLTemplate

def createTables():
    for file, query in queries():
        fileName = file.split(".")[0]
        aliases, joinConds = parseQuery(query)
        # for each join condition - we want to create 
        # proxy table from SELECT
        for join in joinConds:
            joinTbls, joinAliases, columns = parseJoinCond(join, aliases)
            table = getSQL(joinTbls, joinAliases, columns, join)
            stream = os.popen(f"{RUNNER} {DATABASE} -c \"{table}\"")
            stream.read()

def updateWorkload():
    for file, query in queries():
        fileName = file.split(".")[0]
        aliases,joinConds = parseQuery(query)

        for join in joinConds:
            joinTbls, joinAliases, columns = parseJoinCond(join, aliases)
            
            joinFieldsList = [i.replace('.','_') for i in join.split(' = ')]

            newJoinCond = join.split(" = ")
            joinTblName = getTableName(joinTbls, joinAliases)
            newJoinCond[0] += f" = {joinTblName}"
            newJoinCond[0] += f".{joinFieldsList[0]}"
            newJoinCond[1] += f" = {joinTblName}"
            newJoinCond[1] += f".{joinFieldsList[1]}"
            newJoinCond = "\n  AND ".join(newJoinCond)
            newJoinCond = newJoinCond.replace(",","")

            query = query.replace(join,newJoinCond)
            query = query.replace("FROM",f"FROM\n  {joinTblName},")

        ext = ".sql"
        fileName += "_mod." + ext
        print(file) # this is needed for sence of progress

        with open(f"mod/{file}","w") as f:
            f.write(query)

if __name__=="__main__":
    # settings
    DATABASE = 'synt'
    RUNNER = 'psql'

    createTables()
    updateWorkload()
