#! /usr/bin/python

# July 2013 by Daniel W. Bak
# December 2014 by Phillip Steindel
# February 2015 by Phillip Steindel
# June 2015 by G.E. Barkhuff

import sqlite3
import os
import os.path
import regex

"""
Lower level program module used by __CytoBase__.py, __CytoBLAST__.py,
and __CytoExplorer__.py to interface with the database that is used
to store the protein sequence information for a PSN. In the future
this module could probably be cleaned up a bit, as it is very
repetitive. There could probably be fewer overall functions, that
are more general in nature. This module uses the sqlite3 database
tools, which should be available on all systems (definitly on macs).
"""


def tableCreation(database, table):
    '''
    Creates a table with 12 labelled columns.
    '''
    conn = sqlite3.connect(database)
    c = conn.cursor()
    c.execute('''CREATE TABLE {}''' \
              '''(description TEXT,''' \
              '''sequence TEXT,''' \
              '''seq_length INTEGER,''' \
              '''organism TEXT, ''' \
              '''taxonomy TEXT, ''' \
              '''gi TEXT, ''' \
              '''accessions TEXT, ''' \
              '''domains TEXT, '''
              '''source TEXT, '''
              '''codingStart INT, '''
              '''codingEnd INT, '''
              '''contig TEXT)'''.format(table))
    conn.commit()
    c.close()


def domainTableCreation(databaseName, domainTable, seqTable):
    '''
    Creates a table of just domainInfo: 
    gi, domain, start, end, sequence
    '''
    conn = sqlite3.connect(databaseName)
    c = conn.cursor()
    c.execute('''PRAGMA foreign_keys = ON''')
    c.execute('''CREATE TABLE {}''' \
              '''(gi TEXT REFERENCES {}(gi), ''' \
              '''domain TEXT, ''' \
              '''start INT, ''' \
              '''end INT, ''' \
              '''sequence TEXT)'''.format(domainTable, seqTable))
    conn.commit()
    c.close()


def checkForTable(database, table):
    '''
    Returns True if table exists, False if it or the database doesn't
    '''
    if not os.path.isfile(database):
        return False

    conn = sqlite3.connect(database)
    c = conn.cursor()
    flag = 0
    for row in c.execute('''SELECT name FROM sqlite_master ''' \
                         '''WHERE type='table' AND name=?''', (table,)):
        if row != '':
            flag = 1
    c.close()
    if flag == 1:
        return True
    else:
        return False


def copyTable(database, oldTable, newTable):
    '''
    Copies entire contents of oldTable into newTable, after newTable has been created.
    (alternatively, a Create Table sqlite command could be inserted at the top of
    this method to remove that problem of needing an empty table in database.
    '''
    conn = sqlite3.connect(database)
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO {} SELECT * FROM {}'''.format(newTable, oldTable))
    conn.commit()
    c.close()


def databaseInsert(database_values, database, table):
    '''
    Inserts values into a table which has columns:
    (description, sequence, seq_length, organism,
    taxonomy, gi, accessions, domains, source, codingStart, codingEnd, contig)
    '''
    conn = sqlite3.connect(database)
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO {}''' \
              ''' (description, sequence, seq_length, organism, ''' \
              '''taxonomy, gi, accessions, domains, source, codingStart, codingEnd, contig) ''' \
              '''VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'''.format(table), database_values)

    conn.commit()
    c.close()


def domainDatabaseInsert(domainInfo, databaseName, domainTable):
    '''
    Inserts values into a table which has columns:
    gi , domain, start, end, sequence, score
    '''
    # note that domainInfo is a list of things that need to be inserted
    conn = sqlite3.connect(databaseName)
    c = conn.cursor()
    for element in domainInfo:
        # insert values into domain table
        c.execute('''INSERT OR REPLACE INTO {}''' \
                  ''' (gi , domain, start, end, sequence, score) ''' \
                  '''VALUES (?,?,?,?,?,?)'''.format(domainTable), element)
    conn.commit()
    c.close()


def copyRow(database, oldTable, newTable, gi):
    '''
    Copies row with given gi from oldTable to newTable
    '''
    conn = sqlite3.connect(database)
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO {} SELECT * FROM {} '''
              '''WHERE gi=?'''.format(newTable, oldTable), (gi,))
    conn.commit()
    c.close()


def databaseRows(database, table):
    ''' 
    Gets a list of existing gi values from database
    '''
    conn = sqlite3.connect(database)
    c = conn.cursor()
    giList = []
    for row in c.execute('''SELECT DISTINCT gi FROM {}'''.format(table)):
        giList.append(str(row[0]).strip("'"))
    c.close()
    return giList


def concatDomains(database, primaryTable, query, gi):
    '''
    Concatenates (note the sqlite command is II)
    domains + bar + query
    for a given gi
    '''
    conn = sqlite3.connect(database)
    c = conn.cursor()
    c.execute('''UPDATE {} SET domains = (domains || "|" || (?))''' \
    '''WHERE gi = (?)'''.format(primaryTable), (query, gi))
    conn.commit()
    c.close()


def databaseGetDomains(database, table):
    '''
    Get a list of domain values from database
    '''
    conn = sqlite3.connect(database)
    c = conn.cursor()
    domainList = []
    for row in c.execute('''SELECT DISTINCT domains FROM {}'''.format(table)):
        domainList.append(str(row[0]))
    c.close()
    return domainList


def databaseGetSequence(database, table):
    '''
    Gets a dictionary of every gi :[sequence, domains] pair in the given database
    '''
    conn = sqlite3.connect(database)
    c = conn.cursor()
    seq_dict = {}
    for row in c.execute('''SELECT DISTINCT gi, sequence, domains FROM {}'''.format(table)):
        seq_list = [str(row[1]), str(row[2])]
        seq_dict[str(row[0])] = seq_list
    c.close()
    return seq_dict


def databaseGetDescription(database, table):
    '''
    Gets a dictionary of gi:description pairs from database
    '''
    conn = sqlite3.connect(database)
    c = conn.cursor()
    desc_dict = {}
    # select part of description excluding the []
    for row in c.execute('''SELECT gi, substr(description, 0, instr(description, "[")) FROM {}'''.format(table)):
        desc_dict[str(row[0])] = str(row[1])
    c.close()
    return desc_dict

def getStandardDescriptions(fName, database, tableName):
    '''
    Method to get every {gi: description} pair from table in database.
    If standard description exists for given gi, returns that only.
    RType:: dictionary of gi: desc pairs.
    '''
    # open the database to get every gi, description pairs from primary table
    conn = sqlite3.connect(database)
    c = conn.cursor()
    y = c.execute('''SELECT gi, substr(description, 0, instr(description, "[")) FROM {} ''' \
                  .format(tableName))
    giDescPairs = y.fetchall()
    c.close()

    d = {}
    for item in giDescPairs:
        # see if standard desc. exists
        search = regex.search( \
            "(hypothetical\sprotein|"
            "indolepyruvate|"
            "2-\s?oxoglutarate|"
            "2-\s?ketoisovalerate|"
            "2-\s?oxoacid|"
            "oxidoreductase|"
            "pyruvate|"
            "pyruvic)", item[1], regex.IGNORECASE | regex.VERBOSE)
        # if standard desc. exists
        if search:
            # then put into dict as desc.
            d[item[0]] = search.group()
        else:
            # otherwise just put the given desc.
            d[item[0]] = item[1]
    return d

def databaseAll(database, table, value):
    '''
    Gets a dictionary of gi:'value' pairs from
    database, where the value column is specified by user.
    '''
    conn = sqlite3.connect(database)
    c = conn.cursor()
    value_dict = {}
    for row in c.execute('''SELECT DISTINCT gi, {} FROM {}'''.format(value, table)):
        value_dict[str(row[0])] = str(row[1])
    c.close()
    return value_dict


def databaseGetType(database, table, type):
    '''
    Gets a list of values of a given type from database
    '''
    conn = sqlite3.connect(database)
    c = conn.cursor()
    typeList = []
    for row in c.execute('''SELECT DISTINCT {} FROM {}'''.format(type, table)):
        typeList.append(str(row[0]))
    c.close()
    return typeList


def blastDatabaseCreation(database, table, seqTable):
    '''
    Creates a table for BLAST results
    '''
    conn = sqlite3.connect(database)
    c = conn.cursor()
    c.execute('''PRAGMA foreign_keys = ON''')
    c.execute('''CREATE TABLE {} ''' \
              '''(qseqid TEXT REFERENCES {}(gi),''' \
              '''sseqid TEXT REFERENCES {}(gi),''' \
              '''evalue FLOAT,''' \
              '''score FLOAT,''' \
              '''bitscore FLOAT,''' \
              '''length INTEGER,''' \
              '''qstart INTEGER,''' \
              '''qend INTEGER,''' \
              '''sstart INTEGER,''' \
              '''send INTEGER)'''.format(table, seqTable, seqTable))
    conn.commit()
    c.close()


def blastDatabaseInsert(database, table, line):
    '''
    inserts results of a BLAST file into a table
    '''
    values = line.split('\t')
    conn = sqlite3.connect(database)
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO {}''' \
              ''' (qseqid, sseqid, evalue, score, bitscore,'''
              '''length, qstart, qend, sstart, send)''' \
              '''VALUES (?,?,?,?,?,?,?,?,?,?)'''.format(table), values)
    conn.commit()
    c.close()


def getBlastGIs(database, table):
    conn = sqlite3.connect(database)
    c = conn.cursor()
    qseqid_cursor = c.execute('''SELECT DISTINCT qseqid FROM {}'''.format(table))
    qseqids = [str(x[0]) for x in qseqid_cursor.fetchall()]
    sseqid_cursor = c.execute('''SELECT DISTINCT sseqid FROM {}'''.format(table))
    sseqids = [str(x[0]) for x in sseqid_cursor.fetchall()]
    c.close()
    giList = qseqids or sseqids
    return giList


def deleteBlastRows(database, table, giList):
    conn = sqlite3.connect(database)
    c = conn.cursor()
    for gi in giList:
        c.execute('''DELETE FROM {} WHERE qseqid IS ?'''.format(table), (gi,))
        c.execute('''DELETE FROM {} WHERE sseqid IS ?'''.format(table), (gi,))
    conn.commit()
    c.close()

