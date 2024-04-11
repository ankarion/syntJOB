import os
import re
from workload import queries
from settings import RUNNER, DATABASE 


def getAliases(query):
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
    return(aliases)

# parse sql query, get:
# tables aliases and join conditions
def parseQuery(query):
    aliases = getAliases(query)
    # parse join conditions
    joinCondReg = "("
    joinCondReg += "|".join(aliases.keys()) 
    joinCondReg += "|"
    joinCondReg += "|".join(aliases.values())
    joinCondReg += ")"
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


def getSQL(joinCond):
    columns = sorted(joinCond.split(" = "))
    table_name = "_EQ_".join(columns)
    table_name = table_name.replace(".","__")
    joinTbls = [i.split(".")[0] for i in columns]
    columns = ", ".join(columns)

    SQLTemplate =f"""
    CREATE TABLE if not EXISTS {table_name}
    AS 
        SELECT DISTINCT {columns} 
        FROM {joinTbls[0]},
            {joinTbls[1]} 
        WHERE {joinCond};
    """
    return SQLTemplate

def createTables():
    globalJoinConds = set()
    for file, query in queries():
        fileName = file.split(".")[0]
        aliases, joinConds = parseQuery(query)
        for joinCond in joinConds:
            condition = sorted(joinCond.split(" = "))
            condition = [aliases[i.split(".")[0]]+"."+i.split(".")[-1] for i in condition]
            joinCond = " = ".join(condition)
            globalJoinConds.add(joinCond)
    # for each join condition - we want to create 
    # proxy table from SELECT
    for join in globalJoinConds:
        #joinTbls, joinAliases, columns = parseJoinCond(join, aliases)
        tableDDL = getSQL(join)
        print(tableDDL)
        exit()
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
    createTables()
    updateWorkload()
