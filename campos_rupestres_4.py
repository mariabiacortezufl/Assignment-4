#!/usr/bin/env python3
# coding='latin-1'

#Code for creating SQL tables (taxon, distribution and species_profile) with data from REFLORA
#Script created by Maria Beatriz de Souza Cortez and Matthew Gitzendanner

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import exists
from sqlalchemy import sql, select, join, desc
from sqlalchemy import inspect
import json

#Create a sqlite database
engine = create_engine('sqlite:////blue/soltis/mariacortez/campos_rupestres/SQLite_Jan_2022/campos_rupestres_3.sqlite', encoding='latin1')

metadata=MetaData(engine)

#Try to load Taxon info from database, if not there, create it.
try:
	Taxon=Table('taxon', metadata, autoload=True)
except:
	Taxon=Table('taxon', metadata,
		Column('ID', Integer, primary_key=True),
		Column('TAXON_ID', Integer),
		Column('ACCEPTED_ID', Integer),
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
		Column('ENDEMISM', String),
		Column('AMAZONIA', String),
		Column('CERRADO', String),
		Column('CAATINGA', String),
		Column('MATA_ATLANTICA', String),
		Column('PAMPA', String),
		Column('PANTANAL', String),
		)

#Try to load 'Speciesprofile' info from database, if not there, create it.
try:
	Speciesprofile=Table('species_profile', metadata, autoload=True)
except:
	Speciesprofile=Table('species_profile', metadata,
		Column('TAXON_ID', Integer, primary_key=True),
		Column('AQUATICA_BENTOS', String),
		Column('AQUATICA_NEUSTON', String),
		Column('AQUATICA_PLANCTON', String),
		Column('ARBUSTO', String),
		Column('ARVORE', String),
		Column('BAMBU', String),
		Column('COXIM', String),
		Column('DENDROIDE', String),
		Column('DRACENOIDE', String),
		Column('ERVA', String),
		Column('FLABELADO', String),
		Column('FOLHOSA', String),
		Column('LIANA_VOLUVEL_TREPADERIA', String),
		Column('PALMEIRA', String),
		Column('PARASITA', String),
		Column('PENDENTE', String),
		Column('SAPROBIO', String),
		Column('SIMBIONTE', String),
		Column('SUBARBUSTO', String),
		Column('SUCULENTA', String),
		Column('TALOSA', String),
		Column('TAPETE', String),
		Column('TRAMA', String),
		Column('TUFO', String),
		Column('DESCONHECIDA', String),
		Column('AGUA', String),
		Column('AQUATICA', String),
		Column('AREIA', String),
		Column('CORTICICOLA', String),
		Column('EDAFICA', String),
		Column('EPIFILA', String),
		Column('EPIFITA', String),
		Column('EPIXILA', String),
		Column('FOLHEDO', String),
		Column('FOLHEDO_AEREO', String),
		Column('HEMIEPIFITA', String),
		Column('HEMIPARASITA', String),
		Column('PARASITA_H', String),
		Column('PLANTA_VIVA_CORTEX_CAULE', String),
		Column('PLANTA_VIVA_FOLHA', String),
		Column('PLANTA_VIVA_FRUTO', String),
		Column('PLANTA_VIVA_INFLORESCENCIA', String),
		Column('PLANTA_VIVA_RAIZ', String),
		Column('ROCHA', String),
		Column('RUPICOLA', String),
		Column('SAPROFITA', String),
		Column('SAXICOLA', String),
		Column('SIMBIONTE_H', String),
		Column('SOLO', String),
		Column('SUB_AEREA', String),
		Column('TERRICOLA', String),
		Column('TRONCO_DECOMPOSICAO', String),
		Column('DESCONHECIDO', String),
		Column('ANTROPICA', String),
		Column('CAATINGA_ss', String),
		Column('CAMPINARANA', String),
		Column('CAMPO_ALTITUDE', String),
		Column('CAMPO_VARZEA', String),
		Column('CAMPO_LIMPO', String),
		Column('CAMPO_RUPESTRE', String),
		Column('CARRASCO', String),
		Column('CERRADO_ls', String),
		Column('FLORESTA_CILIAR', String),
		Column('FLORESTA_IGAPO', String),
		Column('FLORESTA_TERRA_FIRME', String),
		Column('FLORESTA_VARZEA', String),
		Column('FLORESTA_DECIDUAL', String),
		Column('FLORESTA_PERENIFOLIA', String),
		Column('FLORESTA_SEMIDECIDUAL', String),
		Column('FLORESTA_OMBROFILA', String),
		Column('FLORESTA_OMBROFILA_MISTA', String),
		Column('MANGUEZAL', String),
		Column('PALMEIRAL', String),
		Column('RESTINGA', String),
		Column('SAVANA_AMAZONICA', String),
		Column('VEGETACAO_AQUATICA', String),
		Column('VEGETACAO_AFLO_ROCHOSO', String),
		)

metadata.create_all(engine)

inspector=inspect(engine)
print(inspector.get_table_names())

#Making data ready for loading into tables

import csv

conn = engine.connect()

#Add the taxon data to taxon table

taxon=open("/blue/soltis/mariacortez/campos_rupestres/SQLite_Jan_2022/taxon.csv")
reader=csv.DictReader(taxon)
for Line in reader:

	ins=Taxon.insert().values(ID=Line['id'],
		TAXON_ID=Line['taxonID'],
		ACCEPTED_ID=Line['acceptedNameUsageID'],
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

distribution=open("/blue/soltis/mariacortez/campos_rupestres/SQLite_Jan_2022/distribution_merged_March_2021.csv")
reader=csv.DictReader(distribution)
for Line in reader:
	# Reset dictionary to all 0s for each domain.
	domain_dict={"Amazônia":0, "Cerrado":0, "Caatinga":0, "Mata Atlântica":0, "Pampa":0, "Pantanal":0}

	distribution_details=Line['occurrenceRemarks']# Occurrence Remarks is a json of data
	try:
		parsed_ddetails=json.loads(distribution_details)
	except:
		continue

	try:
		endemics=parsed_ddetails["endemism"]
	except:
		endemics=None
	try:
		phytogeographic_domain=parsed_ddetails["phytogeographicDomain"]
		for domain in phytogeographic_domain:
			domain_dict[domain]=1

	except:
		phytogeographic_domain=None
		for domain in domain_dict:
			domain_dict[domain]=None

	ins=Distribution.insert().values(TAXON_ID=Line['id'],
		STATE=Line['locationID'],
		SITUATION=Line['establishmentMeans'],
		ENDEMISM=endemics,
		AMAZONIA=domain_dict["Amazônia"],
		CERRADO=domain_dict["Cerrado"],
		CAATINGA=domain_dict["Caatinga"],
		MATA_ATLANTICA=domain_dict["Mata Atlântica"],
		PAMPA=domain_dict["Pampa"],
		PANTANAL=domain_dict["Pantanal"],
		)

	conn.execute(ins)

distribution.close()

#Add the Speciesprofile data to the speciesprofile table

speciesprofile=open("/blue/soltis/mariacortez/campos_rupestres/SQLite_Jan_2022/speciesprofile.csv")
reader=csv.DictReader(speciesprofile)
for Line in reader:
	# Reset dictionary to all 0s for each domain.
	lf_type_dict={"Aquática-Bentos":0, "Aquática-Neuston":0, "Aquática-Plâncton�":0,
	"Arbusto":0, "Árvore":0, "Bambu":0, "Coxim":0, "Dendróide":0, "Dracenóide":0, "Erva":0, 
	"Flabelado":0, "Folhosa":0, "Liana/volúvel/trepadeira":0, "Palmeira":0, "Parasita":0, 
	"Pendente":0, "Saprobio":0, "Simbionte":0, "Subarbusto":0, "Suculenta":0, "Talosa":0, 
	"Tapete":0, "Trama":0, "Tufo":0, "Desconhecida":0}

	habitat_type_dict={"Água":0, "Aquática":0, "Areia�":0, "Corticícola":0, "Edáfica":0, 
	"Epífila":0, "Epífita":0, "Epixila":0, "Folhedo":0, "Folhedo aéreo":0, "Hemiepífita":0, 
	"Hemiparasita":0, "Parasita":0, "Planta viva - córtex do caule":0, "Planta viva - folha":0, 
	"Planta viva - fruto":0, "Planta viva - inflorescência":0, "Planta viva - raiz":0, "Rocha":0, 
	"Rupícola":0, "Saprófita":0, "Saxícola":0, "Simbionte (incluindo fungos liquenizados)":0, 
	"Solo":0, "Sub-aérea":0, "Terrícola":0, "Tronco em decomposição":0, "Desconhecido":0 }

	vege_type_dict={"Área Antrópica":0, "Caatinga (stricto sensu)":0, "Campinarana":0,
	"Campo de Altitude":0, "Campo de Várzea":0, "Campo Limpo":0, "Campo rupestre":0, "Carrasco":0,
	"Cerrado (lato sensu)":0, "Floresta Ciliar ou Galeria":0, "Floresta de Igapó":0,
	"Floresta de Terra Firme":0, "Floresta de Várzea":0, "Floresta Estacional Decidual":0,
	"Floresta Estacional Perenifólia":0, "Floresta Estacional Semidecidual":0, "Floresta Ombrófila (= Floresta Pluvial)":0,
	"Floresta Ombrófila Mista":0, "Manguezal":0, "Palmeiral":0, "Restinga":0, "Savana Amazônica":0,
	 "Vegetação Aquática":0, "Vegetação Sobre Afloramentos Rochosos":0}

	taxon_details=Line['lifeForm'] # Life form is a json of data
	try:
		parsed_details=json.loads(taxon_details)
	except:
		continue
	
	try:
		life_form=parsed_details["lifeForm"]
		for lf_type in life_form:
			lf_type_dict[lf_type]=1

	except:
		life_form=None
		for lf_type in lf_type_dict:
			lf_type_dict[lf_type]=None

	try:
		habitat=parsed_details["habitat"]
		for habitat_type in habitat:
			habitat_type_dict[habitat_type]=1

	except:
		habitat=None
		for habitat_type in habitat_type_dict:
			habitat_type_dict[habitat_type]=None

	try:
		vegetation=parsed_details["vegetationType"]
		for vege_type in vegetation:
			vege_type_dict[vege_type]=1

	except:
		vegetation=None
		for vege_type in vege_type_dict:
			vege_type_dict[vege_type]=None

	ins=Speciesprofile.insert().values(TAXON_ID=Line['id'],
		AQUATICA_BENTOS=lf_type_dict["Aquática-Bentos"],
		AQUATICA_NEUSTON=lf_type_dict["Aquática-Neuston"],
		AQUATICA_PLANCTON=lf_type_dict["Aquática-Plâncton�"],
		ARBUSTO=lf_type_dict["Arbusto"],
		ARVORE=lf_type_dict["Árvore"],
		BAMBU=lf_type_dict["Bambu"],
		COXIM=lf_type_dict["Coxim"],
		DENDROIDE=lf_type_dict["Dendróide"],
		DRACENOIDE=lf_type_dict["Dracenóide"],
		ERVA=lf_type_dict["Erva"],
		FLABELADO=lf_type_dict["Flabelado"],
		FOLHOSA=lf_type_dict["Folhosa"],
		LIANA_VOLUVEL_TREPADERIA=lf_type_dict["Liana/volúvel/trepadeira"],
		PALMEIRA=lf_type_dict["Palmeira"],
		PARASITA=lf_type_dict["Parasita"],
		PENDENTE=lf_type_dict["Pendente"],
		SAPROBIO=lf_type_dict["Saprobio"],
		SIMBIONTE=lf_type_dict["Simbionte"],
		SUBARBUSTO=lf_type_dict["Subarbusto"],
		SUCULENTA=lf_type_dict["Suculenta"],
		TALOSA=lf_type_dict["Talosa"],
		TAPETE=lf_type_dict["Tapete"],
		TRAMA=lf_type_dict["Trama"],
		TUFO=lf_type_dict["Tufo"],
		DESCONHECIDA=lf_type_dict["Desconhecida"],
		AGUA=habitat_type_dict["Água"],
		AQUATICA=habitat_type_dict["Aquática"],
		AREIA=habitat_type_dict["Areia�"],
		CORTICICOLA=habitat_type_dict["Corticícola"],
		EDAFICA=habitat_type_dict["Edáfica"],
		EPIFILA=habitat_type_dict["Epífila"],
		EPIFITA=habitat_type_dict["Epífita"],
		EPIXILA=habitat_type_dict["Epixila"],
		FOLHEDO=habitat_type_dict["Folhedo"],
		FOLHEDO_AEREO=habitat_type_dict["Folhedo aéreo"],
		HEMIEPIFITA=habitat_type_dict["Hemiepífita"],
		HEMIPARASITA=habitat_type_dict["Hemiparasita"],
		PARASITA_H=habitat_type_dict["Parasita"],
		PLANTA_VIVA_CORTEX_CAULE=habitat_type_dict["Planta viva - córtex do caule"],
		PLANTA_VIVA_FOLHA=habitat_type_dict["Planta viva - folha"],
		PLANTA_VIVA_FRUTO=habitat_type_dict["Planta viva - fruto"],
		PLANTA_VIVA_INFLORESCENCIA=habitat_type_dict["Planta viva - inflorescência"],
		PLANTA_VIVA_RAIZ=habitat_type_dict["Planta viva - raiz"],
		ROCHA=habitat_type_dict["Rocha"],
		RUPICOLA=habitat_type_dict["Rupícola"],
		SAPROFITA=habitat_type_dict["Saprófita"],
		SAXICOLA=habitat_type_dict["Saxícola"],
		SIMBIONTE_H=habitat_type_dict["Simbionte (incluindo fungos liquenizados)"],
		SOLO=habitat_type_dict["Solo"],
		SUB_AEREA=habitat_type_dict["Sub-aérea"],
		TERRICOLA=habitat_type_dict["Terrícola"],
		TRONCO_DECOMPOSICAO=habitat_type_dict["Tronco em decomposição"],
		DESCONHECIDO=habitat_type_dict["Desconhecido"],
		ANTROPICA=vege_type_dict["Área Antrópica"],
		CAATINGA_ss=vege_type_dict["Caatinga (stricto sensu)"],
		CAMPINARANA=vege_type_dict["Campinarana"],
		CAMPO_ALTITUDE=vege_type_dict["Campo de Altitude"],
		CAMPO_VARZEA=vege_type_dict["Campo de Várzea"],
		CAMPO_LIMPO=vege_type_dict["Campo Limpo"],
		CAMPO_RUPESTRE=vege_type_dict["Campo rupestre"],
		CARRASCO=vege_type_dict["Carrasco"],
		CERRADO_ls=vege_type_dict["Cerrado (lato sensu)"],
		FLORESTA_CILIAR=vege_type_dict["Floresta Ciliar ou Galeria"],
		FLORESTA_IGAPO=vege_type_dict["Floresta de Igapó"],
		FLORESTA_TERRA_FIRME=vege_type_dict["Floresta de Terra Firme"],
		FLORESTA_VARZEA=vege_type_dict["Floresta de Várzea"],
		FLORESTA_DECIDUAL=vege_type_dict["Floresta Estacional Decidual"],
		FLORESTA_PERENIFOLIA=vege_type_dict["Floresta Estacional Perenifólia"],
		FLORESTA_SEMIDECIDUAL=vege_type_dict["Floresta Estacional Semidecidual"],
		FLORESTA_OMBROFILA=vege_type_dict["Floresta Ombrófila (= Floresta Pluvial)"],
		FLORESTA_OMBROFILA_MISTA=vege_type_dict["Floresta Ombrófila Mista"],
		MANGUEZAL=vege_type_dict["Manguezal"],
		PALMEIRAL=vege_type_dict["Palmeiral"],
		RESTINGA=vege_type_dict["Restinga"],
		SAVANA_AMAZONICA=vege_type_dict["Savana Amazônica"],
		VEGETACAO_AQUATICA=vege_type_dict["Vegetação Aquática"],
		VEGETACAO_AFLO_ROCHOSO=vege_type_dict["Vegetação Sobre Afloramentos Rochosos"],
		)

	conn.execute(ins)

speciesprofile.close()
