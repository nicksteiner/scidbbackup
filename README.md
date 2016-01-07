scidbbackup
===========

Script to backup/manage scidb arrays during upgrades.

Requires Scidb-py (https://github.com/Paradigm4/SciDB-Py)

Backup example:
```#!bash
python.py scidbbackup.py --init
python.py scidbbackup.py --backup  --path /Path/To/RAID -A BigArray0 BigArray1
```

Restore example:
```bash
python.py scidbbackup.py --restore  --path /Path/To/RAID -A BigArray0 BigArray1
```
