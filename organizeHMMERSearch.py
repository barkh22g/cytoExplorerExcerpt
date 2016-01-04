#! /usr/bin/python

# 22 Jun 15 by Grace E. Barkhuff
# Method to run HMMERSearch then insert data into tables.


import domainMethods
import databaseMethods
import warnings
import exePaths
import argparse

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from Bio import SearchIO

# ------------ARGS--------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("alignFile", type=str, help="smaller file, alignment, fasta format")
parser.add_argument("dbFile", type=str, help="bigger file, what was aligned, fasta format")
parser.add_argument("profileFileName", type=str, help="name of profile file to be created")
parser.add_argument("outputFileName", type=str, help="name of output file to be created")
parser.add_argument("databaseName", type=str, help="name of database to add elements")
parser.add_argument("primaryTable", type=str, help="name of primary table to append domains")
parser.add_argument("domainTable", type=str, help="name of domain table to add elements")

args = parser.parse_args()

alignFile = str(args.alignFile)
dbFile = str(args.dbFile)
profileFileName = str(args.profileFileName)
outputFileName = str(args.outputFileName)
databaseName = str(args.databaseName)
primaryTable = str(args.primaryTable)
domainTable = str(args.domainTable)


# ------------------------------------------------

def getHSPs():
    '''
    Selects HSPs (high scoring pair) to add to domainTable.
    '''
    print "adding HSP to domain table"
    # loop through all hits
    for hit in queryResult:
        # if more than one HSP
        if len(hit) > 1:
            # flag in case none >= 50
            flag = False
            for HSP in hit:
                # if at least 50 residues
                if HSP.query_span > 49:
                    insertHSP(HSP, hit)
                    flag = True
            # if neither printed
            if flag == False:
                # print the first one
                insertHSP(hit[0], hit)
        # if only one HSP
        else:
            # print it
            insertHSP(hit[0], hit)


def insertHSP(HSP, hit):
    '''
    Adds HSP to domainTable, helper function for getHSPs
    '''
    # collect info needed to insert into domains table
    # the last element of tuple is sequence, after removing gaps ("_")
    # and capitalizing all letters
    domainInfo = [(hit.id, queryResult.id, HSP.hit_start, HSP.hit_end, \
                   (str(HSP.hit.seq).upper()).replace("-", "")), ]
    # insert into domains db
    databaseMethods.domainDatabaseInsert(domainInfo, databaseName, domainTable)


def getHitsForPrimary():
    '''
    Selects appropriate row from primaryTable, and updates domains
    to include the one from HMMERSearch
    '''
    print "adding domains to primary table"
    # get gis from primary table
    giList = databaseMethods.databaseRows(databaseName, primaryTable)
    # check hit id (gi) is in giList
    for hit in queryResult:
        if hit.id in giList:
            # get domains from gi row in primary table
            domainString = databaseMethods.getGIRow(databaseName, primaryTable, hit.id)
            # add the domain
            domainString2 = domainString + "|" + str(queryResult.id)
            # replace into primary table
            databaseMethods.updateDomains(databaseName, primaryTable, domainString2, hit.id)
        else:
            # send warning message
            warnings.warn("Notice: gi %s is not in primary table." % hit.id)


# -------------METHODS--------------------------
# run HMMERSearch out of domainMethods file
queryResult = domainMethods.HMMERSearch(alignFile, dbFile, profileFileName, outputFileName)
# update domainsTable
getHSPs()
# update primaryTable
getHitsForPrimary()
