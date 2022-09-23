'''
This script is a quick way to deal with tracks whos data is stored in big beds rather than available via .txt.gz
Example MANE

Download the bb file "wget ftp://hgdownload.soe.ucsc.edu/gbdb/hg38/mane/mane.1.0.bb"

'''

import argparse
import os 
import subprocess


def get_file_from_url(url, path_to_files, path_to_organismDb_in_gbdb):
    '''
    download the data file from the provided url with wget
    '''
    os.chdir(path_to_files)
    subprocess.run(['sudo', 'wget', '--timestamping', url, '-P', path_to_organismDb_in_gbdb])
    os.chdir("../..")


def get_table_name_from_hgFindSpec(path_to_track_files):
    '''
    check hgFindSpec inserts and the provided path and return the name of the table to be created
    '''

    with open(path_to_track_files + 'hgFindSpec_inserts.sql', 'r') as f:
        line = f.readline()
        insert_values = line.split('(')[1]
        table_name = insert_values.split(',')[0]
        return table_name.replace('"', '')


def create_table(path_to_files, table_name):
    '''
    Write the table creation from the template below
    '''
    with open(f'{path_to_files}/{table_name}.sql', 'w') as f:
        f.write(f"""


DROP TABLE IF EXISTS `{table_name}`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `{table_name}` (
  `filename` varchar(255)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
        
        
        """)
    return f'{path_to_files}/{table_name}.sql'


def write_table_inserts(table_name, path_in_gbdb_to_file, outpath_to_files):
    '''
    Insert into the track table the path to the data file 
    '''
    with open(f"{outpath_to_files}/{table_name}_inserts.sql", 'w') as f:
        f.write(f"""
INSERT INTO {table_name} VALUES ({path_in_gbdb_to_file})
        """)
    return f"{outpath_to_files}/{table_name}_inserts.sql"


def write_bash_wrapper(table_name, path_to_track_files, DBMS):
    '''
    write a shell script to execute the sql inserts created by this script
    '''
    db_name = path_to_track_files.split('/')[-2].split('_')[0]

    with open(f"{path_to_track_files}/run.sh", 'w') as f:
        f.write(f"""
#/usr/bin/env bash 
sudo {DBMS} -u root -p {db_name} < {table_name}.sql
sudo {DBMS} -u root -p {db_name} < {table_name}_inserts.sql


#Add respective hgFindSpec and trackDb entries
sudo {DBMS} -u root -p {db_name} < trackDb_inserts.sql
sudo {DBMS} -u root -p {db_name} < hgFindSpec_inserts.sql
        """)


def main(args):
    '''
    run functions
    '''
    get_file_from_url(args.u, args.p, args.o)
    table_name = get_table_name_from_hgFindSpec(args.p)
    table_creation_sql_path = create_table(args.p, table_name)
    path_in_gbdb_to_file = args.o + '/' + args.u.split("/")[-1] 
    inserts_sql_path = write_table_inserts(table_name, path_in_gbdb_to_file, args.p)

    write_bash_wrapper(table_name, args.p, args.dbms)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-u", help="URL to file on gbdb ")
    parser.add_argument("-p", help="path to sql statements for hgTracks and hgFindSpec")
    parser.add_argument("-o", help="path to oragnsim Db in gbdb on host server")
    parser.add_argument("--dbms", help="DBMS - Database management system (mariadb on poitin, mysql on baileys)")


    args = parser.parse_args()
    main(args)