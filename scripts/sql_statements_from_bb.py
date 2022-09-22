'''
This script is a quick way to deal with tracks whos data is stored in big beds rather than available via .txt.gz
Example MANE

Download the bb file "wget ftp://hgdownload.soe.ucsc.edu/gbdb/hg38/mane/mane.1.0.bb"

'''

import gzip 
import argparse
import os 
import subprocess


def bigBed_to_bed(bigBed_path, bed_outpath):
    '''
    run the bigBed to Bed program
    inputs and outputs from given paths. output is bed file
    '''
    try:
        subprocess.run(['/home/jack/Downloads/Tools/kent/./bigBedToBed', f'{bigBed_path}', f'{bed_outpath}'])
    except:
        subprocess.run(['wget', '--timestamping', 'ftp://hgdownload.soe.ucsc.edu/admin/exe/linux.x86_64.v369/bedToBigBed'])
        subprocess.run(['./bigBedToBed', f'{bigBed_path}', f'{bed_outpath}'])


def inserts_from_bb(path_to_bb):
    '''
    convert big bed to bed and make sql inserts from bed
    '''
    path_list = path_to_bb.split('/')
    path_to_track_files = '/'.join(path_list[:-1])
    file = path_list[-1]
    bigBed_to_bed(path_to_bb, path_to_track_files + '/' + file + '.bed')
    with open(f"{path_to_track_files}/{file}.bed",'r') as f:
        lines = [i.strip('\n').split('\t') for i in f.readlines()]
        avg_line_len = round(sum(len(i) for i in lines[:1000])/len(lines[:1000]))
        
        for line in lines:
            if os.path.exists(f"{path_to_track_files}/{file}_inserts.sql"):
                outfile = open(f"{path_to_track_files}/{file}_inserts.sql", 'a')
            else:
                outfile = open(f"{path_to_track_files}/{file}_inserts.sql", 'w')

            line = [i.replace('"', '') for i in line]

            entries = '","'.join(line)
            outfile.write(f'INSERT INTO {file} VALUES ("{entries}");\n')
            outfile.close()


def main(args):
    '''
    
    '''
    inserts_from_bb(args.t)

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-t", help="Path to bigBed")
    parser.add_argument("-d", help="UCSC database name eg. hg38")
    parser.add_argument("--dbms", help="DBMS - Database management system (mariadb on poitin, mysql on baileys)")

    args = parser.parse_args()
    main(args)