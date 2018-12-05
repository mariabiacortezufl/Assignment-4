#!/usr/bin/env python3

#Code for creating SQL tables for Assignment 4

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import exists
from sqlalchemy import sql, select, join, desc
from sqlalchemy import inspect

#Create a sqlite database 
engine = create_engine('sqlite:////ufrc/zoo6927/share/mariacortez/assignment4.sqlite')

metadata=MetaData(engine)

#Try to load Taxon info from database, if not there, create it.
try:
	Taxon=Table('taxon', metadata, autoload=True)
except:
	Taxon=Table('taxon', metadata,
                Column('ID', Integer, primary_key=True),
                Column('TAXON', String),
                Column('ACCEPTED_NAME', String),
                Column('NAME_INFO', String),
                Column('TAX_STATUS', String),
                Column('NOM_STATUS', String),
                Column('BIBLIOGRAPHY', String),
                Column('ONLINE_REF', String),
                )

#Try to load Distribution info from database, if not there, create it.
try:
	Distribution=Table('distribution', metadata, autoload=True)
except:
	Distribution=Table('distribution', metadata,
                Column('TAXON_ID', Integer, primary_key=True),
                Column('STATE', String),
                Column('SITUATION', String),
                Column('REMARKS', String),
                )

#Try to load 'Speciesprofile' info from database, if not there, create it.
try:
	Speciesprofile=Table('species_profile', metadata, autoload=True)
except:
	Speciesprofile=Table('species_profile', metadata,
                Column('TAXON_ID', Integer,ForeignKey("distribution.TAXON_ID"), primary_key=True),
                Column('LIFE_FORM', String),
                Column('HABITAT', String),
                )
                 
metadata.create_all(engine)

inspector=inspect(engine)
print(inspector.get_table_names())

#Making data ready for loading into tables

import csv

#Add the taxon data to taxon table

conn = engine.connect()

taxon=open("/ufrc/zoo6927/share/mariacortez/taxon.csv")
reader=csv.DictReader(taxon)
for Line in reader:

	ins=Taxon.insert().values(ID=Line['id'],
                TAXON=Line['scientificName'],
                ACCEPTED_NAME=Line['acceptedNameUsage'],
                NAME_INFO=Line['namePublishedIn'],
                TAX_STATUS=Line['taxonomicStatus'],
                NOM_STATUS=Line['nomenclaturalStatus'],
                BIBLIOGRAPHY=Line['bibliographicCitation'],
                ONLINE_REF=Line['references'],   
		)
	conn.execute(ins)

taxon.close()

#Add the distribution data to the distribution table

distribution=open("/ufrc/zoo6927/share/mariacortez/distribution_u.csv")
reader=csv.DictReader(distribution)
for Line in reader:

	ins=Distribution.insert().values(TAXON_ID=Line['id'],
                STATE=Line['locationID'],
                SITUATION=Line['establishmentMeans'],
                REMARKS=Line['occurrenceRemarks'],
		)

	conn.execute(ins)

distribution.close()

#Add the Speciesprofile data to the speciesprofile table

speciesprofile=open("/ufrc/zoo6927/share/mariacortez/speciesprofile.csv")
reader=csv.DictReader(speciesprofile)
for Line in reader:

        ins=Speciesprofile.insert().values(TAXON_ID=Line['id'],
                LIFE_FORM=Line['lifeForm'],
                HABITAT=Line['habitat'],
                )

        conn.execute(ins)

speciesprofile.close()
