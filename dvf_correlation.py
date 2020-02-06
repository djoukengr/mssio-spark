# coding: utf-8
#chargement des données.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dvf_correlation").getOrCreate()
dvf=spark.read.option("header","true").option("delimiter",",").csv("/home/ec2-user/dmde_valeur_fonciere.csv",inferSchema=True)
df_etablissement=spark.read.option("header","true").option("delimiter",";").csv("/home/ec2-user/Etablissements_Geoloc.csv",inferSchema=True)

#Aggrégation des données
dvf_agg=dvf.groupBy("code_commune","nom_commune").agg({"valeur_fonciere": "avg","nombre_lots":"sum","surface_terrain":"sum"})
df_etablissement_agg=df_etablissement.groupBy("Code commune","Commune").agg({"Code établissement":"count"})

#jointure sur les deux nouveau dataframe
df = dvf_agg.join(df_etablissement_agg, dvf_agg.code_commune == df_etablissement_agg["Code commune"],how='left')

#Nettoyage
df_final=df.drop('Code commune','Commune').fillna({'count(Code établissement)':0})

#Save to file
df_final.write.csv("/home/ec2-user/dvf_correlation.csv",header=True)
 
#reload de written dataframe
#test=spark.read.option("header","true").option("delimiter",",").csv("/home/ec2-user/dvf_correlation.csv",inferSchema=True)