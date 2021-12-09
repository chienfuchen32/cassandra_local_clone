# coding=utf-8
import os
import shutil
import logging
import argparse
import subprocess
from datetime import datetime


NODE_BACKUP_FOLDER = 'node'
EXPORT_SCHEMA_FILE = 'schema.cql'


class Command():
    @staticmethod
    def run(cmd=[]):
        p = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        out, err = p.communicate()
        return out.decode('utf-8'), err.decode('utf-8')


def export_schema(keyspace='', backup_path='', user='', password=''):
    """
    Args:
        keyspace (`string`): specify a keyspace to take snapshot
        backup_path (`string`): path to store the Cassandra snapshot files
        user (`string`): cassandra user
        password (`string`): cassandra password
    """
    with open(os.path.join(backup_path, EXPORT_SCHEMA_FILE), 'wt') as f:
        cmd = ['cqlsh']
        if user != '' and password != '':
            cmd += ['-u', user, '-p', password]
        cmd += ['-e', 'DESCRIBE KEYSPACE {}'.format(keyspace)]
        logging.info(' '.join(cmd))
        p = subprocess.Popen(cmd, stdout=f)
        out, err = p.communicate()
        assert err == None
        logging.info(out)


def snapshot(snapshot_tag='', keyspace='', user='', password=''):
    """
    Args:
        snapshot_tag (`string`): Name for the snapshot directory,
            installation_path/data/keyspace_name/table-UID/snapshots/snapshot_name
        keyspace (`string`): specify a keyspace to take snapshot
        user (`string`): cassandra user
        password (`string`): cassandra password
    """
    cmd = ['nodetool']
    if user != '' and password != '':
        cmd += ['-u', user, '-pw', password]
    cmd += ['snapshot', '-t', snapshot_tag, keyspace]
    logging.info(' '.join(cmd))
    out, err = Command.run(cmd)
    logging.error(err)
    assert err == ''
    logging.info(out)


def clear_snapshot(snapshot_tag='', keyspace='', user='', password=''):
    """
    Args:
        snapshot_tag (`string`): Name for the snapshot directory,
            installation_path/data/keyspace_name/table-UID/snapshots/snapshot_name
        keyspace (`string`): specify a keyspace to take snapshot
        user (`string`): cassandra user
        password (`string`): cassandra password
    """
    cmd = ['nodetool']
    if user != '' and password != '':
        cmd += ['-u', user, '-pw', password]
    cmd += ['clearsnapshot', '-t', snapshot_tag, keyspace]
    logging.info(' '.join(cmd))
    out, err = Command.run(cmd)
    logging.error(err)
    assert err == ''
    logging.info(out)


def run(cassandra_data_path='/var/lib/cassandra/data',
        keyspace='',
        backup_path='/cassandra-backup',
        user='',
        password=''):
    """This is a tool for moving files from Cassandra database snapshot
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
    Warning: please prepare snapshot files for produced by 'nodetool'
    ref:
        https://docs.datastax.com/en/dse/5.1/dse-admin/datastax_enterprise/tools/nodetool/toolsSnapShot.html
    Args:
        cassandra_data_path (`string`): Cassandra data path for snapshot
        keyspace (`string`): specify a keyspace to take snapshot
        backup_path (`string`): path to store the Cassandra snapshot files
        user (`string`): cassandra user
        password (`string`): cassandra password
    """
    # snapshot
    snapshot_tag = datetime.now().strftime('%Y%m%d%H%M%S')
    snapshot(snapshot_tag, keyspace, user, password)

    # reset path
    snapshot_path = os.path.join(backup_path, snapshot_tag)
    if os.path.isdir(snapshot_path):
        shutil.rmtree(snapshot_path)
    node_backup_path = os.path.join(snapshot_path, NODE_BACKUP_FOLDER)
    os.makedirs(node_backup_path)

    export_schema(keyspace, backup_path, user, password)

    list_source = os.listdir(os.path.join(cassandra_data_path, keyspace))
    '''
    table1-436606e0ee6c11eaabb20fbd76594363
    materialized_views1-43efa940ee6c11eaabb20fbd76594363
    table2-453a2aa0ee6c11eaabb20fbd76594363
    materialized_views2-461177d0ee6c11eaabb20fbd76594363
    ...
    '''
    for source in list_source:
        source_path = os.path.join(cassandra_data_path, keyspace, source,
                                   'snapshots', snapshot_tag)
        if os.path.isdir(source_path) is False:
            logging.info(source_path + ',false')
            continue
        files = os.listdir(source_path)
        '''
        manifest.json
        mc-29-big-CompressionInfo.db
        mc-29-big-Data.db
        mc-29-big-Digest.crc32
        mc-29-big-Filter.db
        mc-29-big-Index.db
        mc-29-big-Statistics.db
        mc-29-big-Summary.db
        mc-29-big-TOC.txt
        schema.cql
        ...
        '''
        target_path = os.path.join(node_backup_path, source[0:source.index('-')])
        os.mkdir(target_path)
        logging.info(
            'moving source_path: ' + source_path +
            'to target_path: ' + target_path +
            'including files' + ', '.join(files))
        for f in files:
            shutil.move(os.path.join(source_path, f), target_path)
        shutil.rmtree(source_path)
    logging.info('All table snapshots has replica to {}'.format(backup_path))
    # clear snapshot
    clear_snapshot(snapshot_tag, keyspace, user, password)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S',
                        level=logging.INFO)

    class SmartFormatter(argparse.HelpFormatter):
        def _split_lines(self, text, width):
            if text.startswith('R|'):
                return text[2:].splitlines()
            return argparse.HelpFormatter._split_lines(self, text, width)
    # arg
    DESCRIPTION = ('This tool is able to move cassandra keyspace snapshot'
                   'with tag')
    parser = argparse.ArgumentParser(description=DESCRIPTION,
                                     formatter_class=SmartFormatter)
    default_cassandra_data_path = '/var/lib/cassandra/data'
    parser.add_argument('--cassandra_data_path', type=str,
                        help=('Cassandra data path for snapshot, '
                              'defaults to {}').format(
                              default_cassandra_data_path),
                        default=default_cassandra_data_path)
    parser.add_argument('--keyspace', type=str,
                        help='specify a keyspace to take snapshot',
                        required=True)
    default_backup_path = '/cassandra-backup'
    parser.add_argument('--backup_path', type=str,
                        help=('path to store the Cassandra snapshot files, '
                              'defaults to {}').format(default_backup_path),
                        default=default_backup_path)
    parser.add_argument('--user', type=str,
                        help=('user',
                        default='')
    parser.add_argument('--password', type=str,
                        help=('password',
                        default='')
    args = parser.parse_args()

    # packing
    run(args.cassandra_data_path, args.keyspace, args.backup_path, args.user, args.password)
