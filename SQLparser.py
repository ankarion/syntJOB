"""
In this file we want to define methods to
transform SQL text format to internal representation
"""

import re, os
from settings import RUNNER, DATABASE 

def execSQL(SQLcmd):
    stream = os.popen(f"{RUNNER} {DATABASE} -c \"{SQLcmd}\"")
    result = stream.read()
    headings = result.split("\n")[0].split()
    content = "\n".join(results.split("\n")[1:])
    return result

def getTableOid(tableName):
    SQLTemplate = f"""
    SELECT relname, oid FROM pg_class
    WHERE relname={tableName};
    """
    rawRes = execSQL(SQLTemplate)
    print(rawRes)
    exit()

def SQLQueryToAliases(query):
    """
    Generates table-alias dict from SQL query
    """
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
    aliases = [i.split("->")[::-1] for i in aliases]

    # remove empty lines
    aliases = filter(lambda el: True if el else False, aliases)

    # find elements in aliases that doesnt have aliases and add oids as their aliases
    aliases = [[el,getTableOid(el)] if len(el)==1 else el for el in aliases]
    aliases = dict(aliases)
    return(aliases)

def SQLQueryToJoinConds(query):
    """
    Generates list of join conditions from SQL query
    """
    aliases = SQLQueryToAliases(query)
    # joinTblReg is a regexp that stands for table names or aliasses
    joinTblRegExp = "("
    joinTblRegExp += "|".join(aliases.keys()) 
    joinTblRegExp += "|"
    joinTblRegExp += "|".join(aliases.values())
    joinTblRegExp += ")"
    # joinCondReg is a regexp that stands for join condition
    joinCondReg = fr"({joinTblRegExp}\.\w+ = {joinTblRegExp}\.\w+)"
    
    joinConds = re.findall(joinCondReg, query)
    joinConds = [i[0] for i in joinConds]

    return(joinConds)

def replaceAliasesInJoinConds(join,aliases):
    """
    modify join condition and replace aliases with tableNames
    """
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


def getTableDDL(joinCond):
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
