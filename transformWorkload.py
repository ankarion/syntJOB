import os
import re

WORKLOAD_DIR = "./"

def queries():
    for path,directory,files in os.walk(WORKLOAD_DIR):
        for file in files:
            if file.endswith(".sql"):
                with open(file,"r") as f:
                    query = f.read()
                    yield(file,query)

def parseQuery(query):
    aliases = query.split("FROM")[1]
    aliases = aliases.split("WHERE")[0]
    aliases = re.findall(r".+ AS .+", aliases)
    aliases = list(aliases)
    aliases = [i.strip().replace(",","") for i in aliases]
    aliases = [i.split(" AS ")[::-1] for i in aliases]
    aliases = dict(aliases)

    joinConds = re.findall(r"\w+\.\w+ = \w+\.\w+", query)
    joinConds = list(joinConds)

    return(aliases, joinConds)

def parseJoinCond(join):
    joinTbls = join.split()
    joinTbls = [joinTbls[0], joinTbls[-1]]

    joinFields = ", ".join(joinTbls)
    joinAliases = [i.split(".")[0] for i in joinTbls]
    joinTbls = [aliases[i.split(".")[0]] for i in joinTbls]

    columns = joinFields.split(",")
    columns = [i.strip() for i in columns]
    columns = [i+" AS "+i.replace(".","_") for i in columns]
    columns = ",".join(columns)

    return(joinTbls, joinAliases, columns)

def getSQL(fileName,joinTbls,columns,join):
    SQLTemplate =f"""
    CREATE TABLE if not exists {fileName}_{joinTbls[0]}_{joinTbls[1]}
    as 
        select {columns} 
        from {joinTbls[0]} as {joinAliases[0]},
            {joinTbls[1]} as {joinAliases[1]} 
    where {join}

    """
    return SQLTemplate

def createTables():
    for file, query in queries():
        fileName = file.split(".")[0]
        aliases,joinConds = parseQuery(query)

        for join in joinConds:
            joinTbls, joinAliases, columns = parseJoinCond(join)
            #SQLTemplate = SQLTemplate.replace("\n","")
            table = getSQL(fileName,joinTbls,columns,join)
            stream = os.popen(f"psql synt -c \"{table}\"")
            stream.read()

def updateWorkload():
    for file, query in queries():
        fileName = file.split(".")[0]
        aliases,joinConds = parseQuery(query)

        for join in joinConds:
            joinTbls, joinAliases, columns = parseJoinCond(join)
            joinFields = ", ".join(joinTbls)
            joinFieldsList = [i.replace(".","_") for i in joinFields.split()] 
            newJoinCond = join.split(" = ")
            newJoinCond[0] += f" = {joinTbls[0]}_{joinTbls[1]}"
            newJoinCond[0] += f".{joinFieldsList[0]}"
            newJoinCond[1] += f" = {joinTbls[0]}_{joinTbls[1]}"
            newJoinCond[1] += f".{joinFieldsList[1]}"
            newJoinCond = "\n  AND ".join(newJoinCond)
            newJoinCond = newJoinCond.replace(",","")

            query = query.replace(join,newJoinCond)
            query = query.replace("FROM",f"FROM\n   {joinTbls[0]}_{joinTbls[1]},")

        ext = ".sql"
        fileName += "_mod." + ext
        print(file) # this is needed for sence of progress

        with open(f"mod/{file}","w") as f:
            f.write(query)

if __name__=="__main__":
    createTables()
    updateWorkload()
