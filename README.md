### Cassandra stand-alone keyspace backup / restore

#### Prerequisite

* Install [nodetool](https://cassandra.apache.org/doc/latest/tools/nodetool/nodetool.html)

#### Cassandra Snapshot example
Snapshots are taken per node using the nodetool snapshot command. To take a global snapshot, run the nodetool snapshot command with a parallel ssh utility, such as pssh.

A snapshot first flushes all in-memory writes to disk, then makes a hard link of the SSTable files for each keyspace. You must have enough free disk space on the node to accommodate making snapshots of your data files. A single snapshot requires little disk space. However, snapshots can cause your disk usage to grow more quickly over time because a snapshot prevents old obsolete data files from being deleted. After the snapshot is complete, you can move the backup files to another location if needed, or you can leave them in place.
Note: Restoring from a snapshot requires the table schema.
##### Example Procedure
Run nodetool cleanup to ensure that invalid replicas are removed.
```bash
nodetool cleanup cycling
```
Run the nodetool snapshot command, specifying the hostname, JMX port, and keyspace. For example:
```bash
nodetool snapshot -t cycling_2017-3-9 cycling
```
##### Example results
The name of the snapshot directory appears:
```
Requested creating snapshot(s) for [cycling] with snapshot name [2015.07.17]
Snapshot directory: cycling_2017-3-9
```
The snapshot files are created in data/keyspace_name/table_name-UUID/snapshots/snapshot_name directory.
```
ls -1 data/cycling/cyclist_name-9e516080f30811e689e40725f37c761d/snapshots/cycling_2017-3-9
```
For all installations, the default location of the data directory is /var/lib/cassandra/data.

The data files extension is .db and the full CQL to create the table is in the schema.cql file.
```
manifest.json
mc-1-big-CompressionInfo.db
mc-1-big-Data.db
mc-1-big-Digest.crc32
mc-1-big-Filter.db
mc-1-big-Index.db
mc-1-big-Statistics.db
mc-1-big-Summary.db
mc-1-big-TOC.txt
schema.cql
```
##### Ref
https://docs.datastax.com/en/dse/5.1/dse-admin/datastax_enterprise/operations/opsBackupTakesSnapshot.html

#### Backup script
```bash
root@4292c12b9d2a:/# python backup.py --help
usage: backup.py [-h] [--cassandra_data_path CASSANDRA_DATA_PATH] --keyspace KEYSPACE [--backup_path BACKUP_PATH]

This tool is able to move cassandra keyspace snapshotwith tag

optional arguments:
  -h, --help            show this help message and exit
  --cassandra_data_path CASSANDRA_DATA_PATH
                        Cassandra data path for snapshot, defaults to /var/lib/cassandra/data
  --keyspace KEYSPACE   specify a keyspace to take snapshot
  --backup_path BACKUP_PATH
                        path to store the Cassandra snapshot files, defaults
                        to /cassandra-backup
root@4292c12b9d2a:/# python backup.py --keyspace test
```
Now you can copy the snapshot folder:
```
The output folder layout should be:
cassandra-backup
└── schema.cql
└── 20200910172500
    └── node
	├── table1
	│   ├── manifest.json
	│   ├── md-1-big-CompressionInfo.db
	│   ├── md-1-big-Data.db
	│   ├── md-1-big-Digest.crc32
	│   ├── md-1-big-Filter.db
	│   ├── md-1-big-Index.db
	│   ├── md-1-big-Statistics.db
	│   ├── md-1-big-Summary.db
	│   ├── md-1-big-TOC.txt
	│   ├── ...

```
#### Cassandra restore from snapshot example
Restoring a keyspace from a snapshot requires all snapshot files for the table, and if using incremental backups, any incremental backup files created after the snapshot was taken. Streamed SSTables (from repair, decommission, and so on) are also hard-linked and included.

Note: Restoring from snapshots and incremental backups temporarily causes intensive CPU and I/O activity on the node being restored.
##### Restoring from local nodes example
This method copies the SSTables from the snapshots directory into the correct data directories.

1. Make sure the table schema exists and is the same as when the snapshot was created.
The nodetool snapshot command creates a table schema in the output directory. If the table does not exist, recreate it using the schema.cql file.

2. If necessary, truncate the table.
Note: You may not need to truncate under certain conditions. For example, if a node lost a disk, you might restart before restoring so that the node continues to receive new writes before starting the restore procedure.
Truncating is usually necessary. For example, if there was an accidental deletion of data, the tombstone from that delete has a later write timestamp than the data in the snapshot. If you restore without truncating (removing the tombstone), the database continues to shadow the restored data. This behavior also occurs for other types of overwrites and causes the same problem.

3. Locate the most recent snapshot folder. For example:
```
/var/lib/cassandra/data/keyspace_name/table_name-UUID/snapshots/snapshot_name
```

4.Copy the most recent snapshot SSTable directory to the /var/lib/cassandra/data/keyspace/table_name-UUID directory.
For all installations, the default location of the data directory is /var/lib/cassandra/data.

5. Run nodetool refresh.

##### Ref
https://docs.datastax.com/en/dse/5.1/dse-admin/datastax_enterprise/operations/opsBackupSnapshotRestore.html


#### Restore script
```bash
root@4292c12b9d2a:/# python restore.py --help
usage: restore.py [-h] [--cassandra_data_path CASSANDRA_DATA_PATH] --keyspace KEYSPACE --snapshot_tag SNAPSHOT_TAG [--backup_path BACKUP_PATH]

This tool is able to move cassandra keyspace snapshotwith tag

optional arguments:
  -h, --help            show this help message and exit
  --cassandra_data_path CASSANDRA_DATA_PATH
                        Cassandra keyspace path to restore, defaults to /var/lib/cassandra/data
  --keyspace KEYSPACE   specify a keyspace to restore
  --snapshot_tag SNAPSHOT_TAG
                        Name for the snapshot directory
  --backup_path BACKUP_PATH
                        Cassandra data path for snapshot, defaults to
                        /cassandra-backup

root@4292c12b9d2a:/# python restore.py --keyspace test --snapshot 20200910172500
```
##### Ref
* https://cassandra.apache.org/doc/latest/tools/nodetool/snapshot.html
* https://docs.datastax.com/en/dse/5.1/dse-admin/datastax_enterprise/operations/opsBackupRestoreTOC.html
* https://gist.github.com/sdluxeon/4da47ac57de0d8cc9b63
* https://thelastpickle.com/blog/2018/04/03/cassandra-backup-and-restore-aws-ebs.html
* https://blog.pythian.com/backup-strategies-cassandra/
* https://www.cohesity.com/blog/limitations-of-cassandra-snapshots-for-backup-and-recovery/
