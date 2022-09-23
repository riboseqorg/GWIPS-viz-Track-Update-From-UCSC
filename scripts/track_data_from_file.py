'''
This script is a quick way to deal with tracks whos data is stored in big beds rather than available via .txt.gz
Example MANE

Download the bb file "wget ftp://hgdownload.soe.ucsc.edu/gbdb/hg38/mane/mane.1.0.bb"

'''

import argparse
import os 
import subprocess


def get_file_from_url(url, path_to_files):
    '''
    download the data file from the provided url with wget
    '''
    os.chdir(path_to_files)
    subprocess.run(['wget', '--timestamping', url])
    os.chdir("../..")

def get_table_name_from_hgFindSpec(path_to_track_files):
    '''
    check hgFindSpec inserts and the provided path and return the name of the table to be created
    '''

    with open(path_to_track_files + 'hgFindSpec_inserts.sql', 'r') as f:
        line = f.readline()
        insert_values = line.split('(')[1]
        table_name = insert_values.split(',')
        return table_name.replace('"', '')


def create_table(path_to_files, table_name):
    '''
    Write the table creation from the template below
    '''
    with open(f'{path_to_files}/{table_name}.sql') as f:
        f.write(f"""


DROP TABLE IF EXISTS `{table_name}`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `{table_name}` (
  `filename` varchar(255)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
        
        
        """)

def main(args):
    '''
    run functions
    '''
    get_file_from_url(args.u, args.p)
    table_name = get_table_name_from_hgFindSpec(args.p)
    create_table(args.p, table_name)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-u", help="URL to file on gbdb ")
    parser.add_argument("-p", help="path to sql statements for hgTracks and hgFindSpec")
    parser.add_argument("--dbms", help="DBMS - Database management system (mariadb on poitin, mysql on baileys)")


    args = parser.parse_args()
    main(args)