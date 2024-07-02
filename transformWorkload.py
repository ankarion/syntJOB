"""
In this file we define all transformations
that can be applied to benchmarks.
Currently available transformations:
    - Split One to Many relation
    - Split columns of tables to many small tables
"""

import os
import re
from workload import queries, getGlobJoinConds
from SQLparser import getTableDDL, execSQL, SQLQueryToAliases, SQLQueryToJoinConds

def createTables():
    globalJoinConds = getGlobJoinConds()
    execLogs = []
    # proxy table from SELECT
    for join in globalJoinConds:
        tableDDL = getTableDDL(join)
        execLogs.append(execSQL(tableDDL))
    return(execLogs)

def updateWorkload():
    for file, query in queries():
        fileName = file.split(".")[0]
        
        aliases = SQLQueryToAliases(query)
        joinConds = SQLQueryToJoinConds(query)
        extraTables = set()
        for joinCond in joinConds:
            # join looks like: 
            # a.id = b.a_id
            #
            # we want it to look like: 
            # a.id = a_oid_id__EQ__b_oid_a_id.a_id AND
            # a_oid_id__EQ__b_oid_a_id.a_id = b.a_id
            #
            # In order to do that, we need to find "=" sign and
            # replace it with 
            # " = {syntTableName}.{firstColumn} AND {syntTableName}.{secondColumn} = " 
            #
            # So, plan is following:
            # 1. Get synt table name
            # 2. Get columns names
            # 3. Replace "=" with that long line

            # localJoinCond -> joinCond
            condition = sorted(joinCond.split(" = "))
            condition = [aliases[i.split(".")[0]]+"."+i.split(".")[-1] for i in condition]
            joinCond = " = ".join(condition)
            
            joinFieldsList = [i.replace('.','_') for i in joinCond.split(' = ')]

            newJoinCond = joinCond.split(" = ")

            columns = sorted(joinCond.split(" = "))
            joinTblName = "_EQ_".join(columns)
            joinTblName = joinTblName.replace(".","__")

            newJoinCond[0] += f" = {joinTblName}"
            newJoinCond[0] += f".{joinFieldsList[0]}"
            newJoinCond[1] += f" = {joinTblName}"
            newJoinCond[1] += f".{joinFieldsList[1]}"
            newJoinCond = "\n  AND ".join(newJoinCond)
            newJoinCond = newJoinCond.replace(",","")
            extraTables.add(joinTblName)

            query = query.replace(join,newJoinCond)

        for joinTblName in extraTables:
            query = query.replace("FROM",f"FROM\n  {joinTblName},")

        ext = ".sql"
        fileName += "_mod." + ext
        print(file) # this is needed for sence of progress

        with open(f"mod/{file}","w") as f:
            f.write(query)

if __name__=="__main__":
    createTables()
    updateWorkload()
