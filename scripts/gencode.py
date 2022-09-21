'''
This script produces the SQL required to add a new gencode version to GWIPS-viz
'''

import argparse
import subprocess 
import os
import gzip

def get_gencode_files_from_UCSC(gencode_version, organism_db, base_path="ftp://hgdownload.soe.ucsc.edu/goldenPath"):
    '''
    download files from UCSC for the desired gencode version 

    Gencode_version - integer (eg. 40)
    Organism_db - string (eg. hg38)
    base_path is left variable in case it changes over time.
    '''
    url = f"{base_path}/{organism_db}/database/wgEncodeGencode**V{str(gencode_version)}*"
    outfile_path = f"./UCSC_files/{organism_db}_gencodeV{str(gencode_version)}"
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

def get_gencode_txt_filenames_as_list(path_to_gencode_files):
    '''
    return all txt.gz files names from the provided directory 
    '''
    txt_file_list = [] 

    for file in os.listdir(path_to_gencode_files):
        if file.endswith('txt.gz'):
            txt_file_list.append(file)
    return txt_file_list


def gencode_tables_to_sql_statements(path_to_gencode_files):
    '''
    takes in path to dir that has .txt.gz and .sql files describing the data pertaining to each table

    Parse this data and produce an sql file full of insert statements to populate the table on GWIPS
    '''

    for file in get_gencode_txt_filenames_as_list(path_to_gencode_files):
        table_name = file.strip('.txt.gz')

        if os.path.exists(f"{path_to_gencode_files}/{table_name}_inserts.sql"):
            print(f"Insert statements already created: {table_name}")
            continue
        else:
            print(f"Writing statements for: {file}")

        with gzip.open(f"{path_to_gencode_files}/{file}",'rt') as f:
            lines = [i.strip('\n').split('\t') for i in f.readlines()]

            for line in lines:
                if os.path.exists(f"{path_to_gencode_files}/{table_name}_inserts.sql"):
                    outfile = open(f"{path_to_gencode_files}/{table_name}_inserts.sql", 'a')
                else:
                    outfile = open(f"{path_to_gencode_files}/{table_name}_inserts.sql", 'w')

                entries = '","'.join(line)
                outfile.write(f'INSERT INTO {table_name} VALUES ("{entries}");\n')
                outfile.close()


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


def get_trackDb_entries_as_insert_statements(path_to_gencode_files, path_to_trackDb, gencode_version):
    '''
    Search trackDb.txt file for entries pertaining to each table for this gencode version
    Also, obtain specific entries for wgEncodeGencodeV* and wgEncodeGencodeV*ViewGenes
    Write the insert statments to a file in the gencode dir
    '''
    gencode_files = get_gencode_txt_filenames_as_list(path_to_gencode_files)
    table_names = [file.strip(".txt.gz") for file in gencode_files]
    table_names.append(f'wgEncodeGencodeV{gencode_version}')
    table_names.append(f'wgEncodeGencodeV{gencode_version}ViewGenes')

    with gzip.open(path_to_trackDb, 'rt') as f:
        entries = split_txt_file_into_entries(f)
        
        outfile = open(f"{path_to_gencode_files}/trackDb_inserts.sql", 'w')
        for entry in entries:
            for table in table_names:
                if table == entry[0][0]:
                    col_21 = ' \n '.join([i[0].strip('\\') for i in entry[1:]]) # merge the data for col21 into the one list element as is is split over mulitple lines

                    # some entries have missing columns. Add these as blank in the seconds last position to make up numbers
                    if len(entry[0]) < 21:
                        for i in range(21 - len(entry[0])):
                            entry[0].insert(-2,'')

                    entry[0][20] = entry[0][20].strip('\\')+ ' \n ' + col_21                    

                    trackDb_entry = '","'.join(entry[0])
                    outfile.write(f'INSERT INTO trackDb VALUES ("{trackDb_entry}");\n')
        outfile.close()


def get_hgFindSpec_entries_as_insert_statements(path_to_gencode_files, path_to_hgFindSpec, args.g):
    '''
    get all entries from the hgFindSpec txt.gz file that contain wgEncodeGencode and the gencode version and create insert statements 
    '''

    return True

def main(args):
    path_to_gencode_files = get_gencode_files_from_UCSC(args.g, args.d)
    path_to_organism_files = get_organism_files(args.d)
    gencode_tables_to_sql_statements(path_to_gencode_files)
    get_trackDb_entries_as_insert_statements(path_to_gencode_files, path_to_organism_files+"/trackDb.txt.gz", args.g)
    get_hgFindSpec_entries_as_insert_statements(path_to_gencode_files, path_to_organism_files+"/hgFindSpec.txt.gz", args.g)
    return True



if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-g", help="Gencode version to add (as integer)")
    parser.add_argument("-d", help="UCSC database name eg. hg38")

    args = parser.parse_args()
    main(args)