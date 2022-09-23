# GWIPS-viz-Track-Update-From-UCSC
Update tracks on GWIPS-viz from their UCSC counterpart.

Many tracks can be downloaded from goldenPath where the table structure is available as .sql and the data in a tab delimitted .txt.gz file

[See example here for Human (hg38)](https://hgdownload.soe.ucsc.edu/goldenPath/hg38/database/)

## Gencode
Genocde versions can be downloaded by and set up using gencode.py 
This is handled by its own script as it requires managing multiple tables 

Example for Gencode V41
```bash
python scripts/gencode.py -g 41 -d hg38 --dbms mariadb
```

Example for Gecode VM25 for mm10
```bash
python scripts/gencode.py -g M25 -d mm10 --dbms mariadb
```

## General Tracks
Not all tracks require multiple tables. These can be handled with general_track.py

Example for orfeomeMrna
```bash
python scripts/general_track.py -t orfeomeMrna -d hg38 --dbms mariadb
```

## Tracks with Data Stored in Files
In some cases when you run general_tracks.py no insert statements will be created for the tracks table itself


### Requirements 

- python 3.10 