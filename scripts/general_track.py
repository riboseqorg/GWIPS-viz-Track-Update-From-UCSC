"""
This script creates the sql statements required to add an specific table to GWIPS-viz
Unlike gencode.py it makes no assumptions about table name suffixes 
The user can find the name of the table 
@ https://hgdownload.soe.ucsc.edu/goldenPath/hg38/database/ for humans for example
"""

import argparse
import subprocess 
import os
import gzip


def get_track_files_from_UCSC(table_name, organism_db, base_path="ftp://hgdownload.soe.ucsc.edu/goldenPath"):
    '''
    download files from UCSC for the desired table

    table_name - name of table in UCSC database
    Organism_db - string (eg. hg38)
    base_path is left variable in case it changes over time.
    '''
    url = f"{base_path}/{organism_db}/database/{table_name}*"
    outfile_path = f"./UCSC_files/{organism_db}_{table_name}"
    if not os.path.exists(outfile_path):
        os.mkdir(outfile_path)
    os.chdir(outfile_path)
    subprocess.run(['wget', "--timestamping", url])
    os.chdir("../..")
    return outfile_path


def get_organism_files(organism_db, base_path="ftp://hgdownload.soe.ucsc.edu/goldenPath"):
    '''
    hgFindSpec and trackDb are also required to set the desired annotation track up on GWIPS-viz

    Organism_db - string (eg. hg38)
    base_path is left variable in case it changes over time.
    '''
    trackDb_url = f"{base_path}/{organism_db}/database/trackDb*"
    hgFindSpec_url = f"{base_path}/{organism_db}/database/hgFindSpec*"

    outfile_path = f"./UCSC_files/{organism_db}"
    if not os.path.exists(outfile_path):
        os.mkdir(outfile_path)
    os.chdir(outfile_path)

    subprocess.run(['wget', "--timestamping", trackDb_url])
    subprocess.run(['wget', "--timestamping", hgFindSpec_url])
    os.chdir("../..")

    return outfile_path


def get_txt_filenames_as_list(path_to_files):
    '''
    return all txt.gz files names from the provided directory 
    '''
    txt_file_list = [] 

    for file in os.listdir(path_to_files):
        if file.endswith('txt.gz'):
            txt_file_list.append(file)
    return txt_file_list




def split_txt_file_into_entries(file_object):
    '''
    loop through the lines of the trackDb file and return a list of entries. These are separated by empty lines in the file

    '''
    lines = [i.strip('\n').split('\t') for i in file_object.readlines()]

    entries = []
    entry = [] 
    for line in lines:
        entry.append(line)
        if line == ['']:
            entries.append(entry)
            entry = []
    if entry != []: entries.append(entry)
    return entries



def get_trackDb_entries_as_insert_statements(path_to_track_files, path_to_trackDb, table_name):
    '''
    Search trackDb.txt file for entries pertaining to each table for this table 
    Write SQL insert statements for updating the table in GWIPS
    '''
    track_files = get_txt_filenames_as_list(path_to_track_files)
    table_names = [file.strip(".txt.gz") for file in track_files]


    with gzip.open(path_to_trackDb, 'rt') as f:
        entries = split_txt_file_into_entries(f)
        
        outfile = open(f"{path_to_track_files}/trackDb_inserts.sql", 'w')
        for entry in entries:
            for table in table_names:
                print(table)
                if table == entry[0][0]:
                    col_21 = '\n '.join([i[0].strip('\\') for i in entry[1:]]) # merge the data for col21 into the one list element as is is split over mulitple lines

                    # some entries have missing columns. Add these as blank in the seconds last position to make up numbers
                    if len(entry[0]) < 21:
                        for i in range(21 - len(entry[0])):
                            entry[0].insert(-2,'')

                    entry[0][20] = entry[0][20].strip('\\')+ '\n ' + col_21                    
                    tidy_entry = [i.replace('"', "'") for i in entry[0]]

                    trackDb_entry = '","'.join(tidy_entry)
                    outfile.write(f'INSERT INTO trackDb VALUES ("{trackDb_entry}");\n')
        outfile.close()


def get_hgFindSpec_entries_as_insert_statements(path_to_track_files, path_to_hgFindSpec, table_name):
    '''
    get all entries from the hgFindSpec txt.gz file that contain table name and create insert statements 
    '''
    
    with gzip.open(path_to_hgFindSpec, 'rt') as f:

        outfile = open(f"{path_to_track_files}/hgFindSpec_inserts.sql", 'w')
        for line in f.readlines():
            if table_name in line.split('\t')[0]:
                hgFindSpec_entry = '","'.join(line.split('\t')).strip('\n')
                outfile.write(f'INSERT INTO hgFindSpec VALUES ("{hgFindSpec_entry}");\n')
        outfile.close()

    return True


def write_bash_wrapper(path_to_track_files, track_name, DBMS, db_name):
    '''
    Write a bash script to run the sql table creation and inserts 

    DBMS - Database management system (mariadb on poitin, mysql on baileys)
    '''
    with open(f"{path_to_track_files}/run.sh", 'w') as sh:
        sh.write(f"# This BASH Script adds {track_name} to GWIPS-viz\n")
        sh.write(f'''
#/usr/bin/env bash 

for file in {os.getcwd()}/{path_to_track_files}/*{track_name}.sql; do 
    sudo {DBMS} -u root -p {db_name} < $file
    pathArr=(${{file//// }})
    SQL_NAME=${{pathArr[-1]}}
    SQL_NAME_ARR=(${{SQL_NAME//./ }})
    TABLE_NAME=${{SQL_NAME_ARR[0]}}
    
    echo "inserting ${db_name}"

    zcat ${{TABLE_NAME}}.txt.gz | sudo {DBMS} -u root -p {db_name} --local-infile=1 -e 'LOAD DATA LOCAL INFILE '"/dev/stdin"' INTO TABLE ${{TABLE_NAME}};'
    # sudo {DBMS} -u root -p {db_name} < ${{TABLE_NAME}}_inserts.sql
    echo "Done"
done

sudo {DBMS} -u root -p {db_name} < trackDb_inserts.sql
sudo {DBMS} -u root -p {db_name} < hgFindSpec_inserts.sql

        ''')

def main(args):
    '''
    exeute functions
    '''
    if not os.path.exists("./UCSC_files"):
        os.mkdir("./UCSC_files")
    path_to_track_files = get_track_files_from_UCSC(args.t, args.d)
    path_to_organism_files = get_organism_files(args.d)
    get_trackDb_entries_as_insert_statements(path_to_track_files, path_to_organism_files+"/trackDb.txt.gz", args.t)
    get_hgFindSpec_entries_as_insert_statements(path_to_track_files, path_to_organism_files+"/hgFindSpec.txt.gz", args.t)
    write_bash_wrapper(path_to_track_files, args.t, args.dbms, args.d)
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-t", help="Tracks table name on UCSC")
    parser.add_argument("-d", help="UCSC database name eg. hg38")
    parser.add_argument("--dbms", help="DBMS - Database management system (mariadb on poitin, mysql on baileys)")

    args = parser.parse_args()
    main(args)