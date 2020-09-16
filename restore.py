# coding=utf-8
import os
import shutil
import logging
import argparse
import subprocess


NODE_BACKUP_FOLDER = 'node'
EXPORT_SCHEMA_FILE = 'schema.cql'
CASSANDRA_GROUP = 'cassandra'
CASSANDRA_USER = 'cassandra'


class Command():
    @staticmethod
    def run(cmd=[]):
        p = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        out, err = p.communicate()
        return out.decode('utf-8'), err.decode('utf-8')


def drop_keyspace(keyspace=''):
    """
    Args:
        keyspace (`string`): specify a keyspace to restore
    """
    cmd = ['cqlsh', '-e', 'DROP KEYSPACE IF EXISTS {};'.format(keyspace),
           '--request-timeout=100']
    logging.info(' '.join(cmd))
    out, err = Command.run(cmd)
    logging.error(err)
    assert err == ''
    logging.info(out)


def restore_schema(backup_path=''):
    """
    Args:
        backup_path (`string`): Cassandra data path for snapshot
    """
    cmd = ['cqlsh', '-f', os.path.join(backup_path, EXPORT_SCHEMA_FILE)]
    logging.info(' '.join(cmd))
    out, err = Command.run(cmd)
    logging.error(err)
    assert err == ''
    logging.info(out)


def chown_data(cassandra_data_path, keyspace):
    """
    Args:
        cassandra_data_path (`string`): Cassandra keyspace path to restore
        keyspace (`string`): specify a keyspace to restore
    """
    cmd = ['chown', '-R', '{}:{}'.format(CASSANDRA_USER, CASSANDRA_GROUP),
           os.path.join(cassandra_data_path, keyspace)]
    logging.info(' '.join(cmd))
    out, err = Command.run(cmd)
    logging.error(err)
    assert err == ''
    logging.info(out)


def refresh_tables(keyspace='', tables=[]):
    """
    Args:
        keyspace (`string`): specify a keyspace to restore
        tables (list of `string`): tables to refresh
    """
    for table in tables:
        cmd = ['nodetool', 'refresh', keyspace, table]
        logging.info(' '.join(cmd))
        out, err = Command.run(cmd)
        logging.error(err)
        assert err == ''
        logging.info(out)


def run(backup_path='/cassandra-backup',
        keyspace='',
        snapshot_tag='',
        cassandra_data_path='/var/lib/cassandra/data'):
    """This is a tool for moving files to restore Cassandra database
    The snapshot folder layout should be:
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
        backup_path (`string`): Cassandra data path for snapshot
        keyspace (`string`): specify a keyspace to restore
        snapshot_tag (`string`): Name for the snapshot directory,
            installation_path/data/keyspace_name/table-UID/snapshots/snapshot_name
        cassandra_data_path (`string`): Cassandra keyspace path to restore
    """
    # reset cassandra keyspace data
    drop_keyspace(keyspace)
    keyspace_path = os.path.join(cassandra_data_path, keyspace)
    if os.path.isdir(keyspace_path):
        shutil.rmtree(os.path.join(cassandra_data_path, keyspace))
    restore_schema(backup_path)
    snapshot_path = os.path.join(backup_path, snapshot_tag)
    node_backup_path = os.path.join(snapshot_path, NODE_BACKUP_FOLDER)
    list_source = os.listdir(node_backup_path)
    '''
    table1
    materialized_view1
    table2
    materialized_view2
    ...
    '''
    list_target = os.listdir(os.path.join(cassandra_data_path, keyspace))
    '''
    table1-436606e0ee6c11eaabb20fbd76594363
    materialized_view1-43efa940ee6c11eaabb20fbd76594363
    table2-453a2aa0ee6c11eaabb20fbd76594363
    materialized_view2-461177d0ee6c11eaabb20fbd76594363
    ...
    '''
    # create mapper
    mapper = {}
    for source in list_source:
        found = False
        for target in list_target:
            if target[0:len(source)+1] == source + '-':
                # folder name is like: table-UID
                mapper[source] = target
                found = True
                break
        if not found:
            logging.info('not found')

    logging.info('len(mapper): {}'.format(len(mapper)))
    # move
    for source in list_source:
        source_path = os.path.join(node_backup_path, source)
        target = mapper[source]
        target_path = os.path.join(keyspace_path, target)

        # remove all files in target_path
        for filename in os.listdir(target_path):
            file_path = os.path.join(target_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                logging.info('Failed to delete %s. %s' % (file_path, e))

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
        logging.info(
            'moving source_path: ' + source_path +
            'to target_path: ' + target_path +
            'including files' + ', '.join(files))
        for f in files:
            if f != 'schema.cql' and f != 'manifest.json':
                # prevent to move unnecessary files
                shutil.move(os.path.join(source_path, f), target_path)
        shutil.rmtree(source_path)

    # change file / folder owner to cassandra
    chown_data(cassandra_data_path, keyspace)

    # reload from disk
    refresh_tables(keyspace, list_source)
    logging.info('All snapshots has replaced current table db')


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
                        help=('Cassandra keyspace path to restore, '
                              'defaults to {}').format(
                              default_cassandra_data_path),
                        default=default_cassandra_data_path)
    parser.add_argument('--keyspace', type=str,
                        help='specify a keyspace to restore',
                        required=True)
    parser.add_argument('--snapshot_tag', type=str,
                        help='Name for the snapshot directory',
                        required=True)
    default_backup_path = '/cassandra-backup'
    parser.add_argument('--backup_path', type=str,
                        help=('Cassandra data path for snapshot, '
                              'defaults to {}').format(default_backup_path),
                        default=default_backup_path)

    args = parser.parse_args()

    # start
    run(args.backup_path, args.keyspace,
        args.snapshot_tag, args.cassandra_data_path)
