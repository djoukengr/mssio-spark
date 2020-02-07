# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse

def main():
    """
    Cette methode est le point d'entrée de mon job.
    Elle va essentiellement faire 3 choses:
        - recuperer les arguments passés via la ligne de commande
        - creer une session spark
        - lancer le traitement
    Le traitement ne doit pas se faire dans la main pour des soucis de testabilité.
    """
    parser = argparse.ArgumentParser(
        description='Discover driving sessions into log files.')
    parser.add_argument('-d', "--demande_valeur_fonciere", help='fichier sur les demandes de valeur foncière', required=True)
    parser.add_argument('-e', "--etablissements", help='Données sur les établissements', required=True)
    parser.add_argument('-o', '--output', help='Output file', required=True)

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    process(spark,args.demande_valeur_fonciere,args.etablissements,args.output)


def process(spark, demande_valeur_fonciere, etablissements, output):
    """
    Contient les traitements qui permettent de lire,transformer et sauvegarder mon resultat.
    :param spark: la session spark
    :param demande_valeur_fonciere:  le chemin du dataset sur les valeurs foncière
    :param etablissements: le chemin du dataset des établissements
    :param output: l'emplacement souhaité du resultat
    """
    dvf=spark.read.option("header","true").option("delimiter",",").csv(demande_valeur_fonciere,inferSchema=True)
    df_etablissement=spark.read.option("header","true").option("delimiter",";").csv(etablissements,inferSchema=True)

    #Aggrégation des données
    dvf_agg=dvf.groupBy("code_commune","nom_commune").agg({"valeur_fonciere": "avg","nombre_lots":"sum","surface_terrain":"sum"})
    df_etablissement_agg=df_etablissement.groupBy("Code commune","Commune").agg({"Code établissement":"count"})

    #jointure sur les deux nouveau dataframe
    df = dvf_agg.join(df_etablissement_agg, dvf_agg.code_commune == df_etablissement_agg["Code commune"],how='left')

    #Nettoyage
    df_final=df.drop('Code commune','Commune').fillna({'count(Code établissement)':0})

    #Save to file
    df_final.write.csv(output,header=True)


if __name__ == '__main__':
    main()