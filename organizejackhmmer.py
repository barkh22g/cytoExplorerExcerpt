#! /usr/bin/python/

# Created June 2015
# by Grace Barkhuff
# Method used in conjunction with jackhmmerParse
# to insert information into database.

import databaseMethods
import jackhmmerParse
import argparse
import warnings

# ------------COMMAND_LINE---------------------
parser = argparse.ArgumentParser()
parser.add_argument("databaseName", type=str, help="name of database to add elements")
parser.add_argument("primaryTable", type=str, help="name of primary table to append domains")
parser.add_argument("domainTable", type=str, help="name of domain table to add elements")
parser.add_argument("query", type=str, help="name of domain to be added")
parser.add_argument("jackhmmerFile", type=str, help="name of jackhmmer file to parse")

args = parser.parse_args()

databaseName = str(args.databaseName)
primaryTable = str(args.primaryTable)
domainTable = str(args.domainTable)
query = str(args.query)
jackhmmerFile = str(args.jackhmmerFile)


# ------------METHODS---------------------
def loopHits(jackHmmerInfo):
    '''
    This method runs through every hit and calls helper functions to
    insert the corresponding information into the database.
    '''
    i = 0
    # loop over every hit in the dict
    for key, val in jackHmmerInfo.iteritems():
        # give status update
        if i % 10 == 0:
            percent = (i / float(lenJackHmmer)) * 100
            # note: "%.2f" is simply for formatting below
            print "status: " + str(i) + "/" + str(lenJackHmmer) \
                  + ", percentage: " + "%.2f" % percent
        # insert info into domain table
        insertDomain(val)
        # insert into primary table
        insertPrimary(val["domain"], val["gi"])
        # update counter
        i += 1


def insertDomain(hitInfo):
    '''
    A helper function to insert information into the *domain* table.
    '''
    # collect info needed to insert into domains table
    domainInfo = [(hitInfo["gi"], hitInfo["domain"], hitInfo["start"], \
                  hitInfo["end"], hitInfo["sequence"], hitInfo["score"])]
    # insert into domains db
    databaseMethods.domainDatabaseInsert(domainInfo, databaseName, domainTable)


def insertPrimary(domain, giVal):
    '''
    A helper function to insert information into the *primary* table.
    '''
    gi = giVal.split("_")[0]
    if gi in giList:
        # update domains in primary table
        databaseMethods.concatDomains(databaseName, primaryTable, query, gi)
    else:
        # send warning message
        warnings.warn("Notice: gi %s is not in primary table." % gi)

# ----------------CALLS-----------------------------------
# get information out of jackhmmer file
jackHmmerInfo = jackhmmerParse.jackhmmerParse(query, jackhmmerFile)
# length of jackHMMER info
lenJackHmmer = len(jackHmmerInfo)
# get list of gis from primary table
giList = databaseMethods.databaseRows(databaseName, primaryTable)
# run through every hit in jackHmmerInfo
loopHits(jackHmmerInfo)
