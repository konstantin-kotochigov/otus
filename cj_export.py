from hdfs import InsecureClient
from uuid import uuid4
import time
import os
import json

from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from avro.schema import Parse

import os

class CJ_Export:
    def __init__(self, dmp_organization_id, source_name, hdfs_host, cp_schema_path, hdfs_user='hdfs'):
        self.cid = dmp_organization_id
        self.source = source_name
        self.host = hdfs_host
        self.user = hdfs_user
        self.raw_schema = open(cp_schema_path).read()  # TODO: omg add some exceptions
    def make_delta(self, df, mapping, send_update=True):
        timestamp = int(time.time() * 1e3)
        #  very bad. TODO: need to pass than into foreachPartition some other way
        self.timestamp = timestamp
        self.mapping = mapping
        df.rdd.foreachPartition(self.process_partition)
        hdfs_client = InsecureClient(self.host, user=self.user)
        hdfs_client.write('/data/{}/.dmpkit/profiles/{}/cdm/ts={}/_SUCCESS'.format(self.cid, self.source, timestamp),
                          data="")
        hdfs_client.set_owner('/data/{}/.dmpkit/profiles/{}/cdm/ts={}'.format(self.cid, self.source, timestamp),
                              owner='dmpkit')
        if send_update:
            update_id = str(uuid4())
            update_path = '/data/{}/.dmpkit/profiles/.updates/{}.json'.format(self.cid, update_id)
            update_content = {"id": update_id, "owner": self.cid, "dataset": "profiles", "source": self.source,
                              "created": timestamp,
                              "path": '/data/{}/.dmpkit/profiles/{}/cdm/ts={}'.format(self.cid, self.source, timestamp),
                              "mergeStrategy": "merge"}
            hdfs_client.write(update_path, data=json.dumps(update_content), encoding='utf-8')
            hdfs_client.set_owner(update_path, owner='dmpkit')
    def process_partition(self, partition):
        
        now = int(time.time() * 1e3)
        hdfs_client = InsecureClient(self.host, user=self.user)
        json_list = []
        for row in partition:
            profile_version = 393320  # TODO: remove magic constant, validate it's value
            system = [{'id': {'primary': 10000, 'secondary': 10000}, 'value': self.cid, 'confidence': 1.0},
                      {'id': {'primary': 10000, 'secondary': 10001}, 'confidence': 1.0, 'value': now}
                      ]
            id = []
            for k in self.mapping['id']:
                if k in row and row[k] is not None:
                    id.append({'id': self.mapping['id'][k], 'confidence': 1.0, 'value': str(row[k])})
            attributes = []
            for k in self.mapping['attributes']:
                if k in row and row[k] is not None:
                    if 'mapping' in self.mapping['attributes'][k]:
                        if row[k] in self.mapping['attributes'][k]['mapping']:
                            attributes.append({'id': {'secondary': self.mapping['attributes'][k]['mapping'][row[k]],
                                                      'primary': self.mapping['attributes'][k]['primary']},
                                               'confidence': 1.0})
                    else:
                        attributes.append(
                            {'id': {'secondary': -1, 'primary': self.mapping['attributes'][k]['primary']},
                             'confidence': 1.0,
                             'value': row[k]})
            json_dict = {
                'id': id,
                'attributes': attributes,
                'system': system
            }
            json_list.append(json_dict)
        filename = 'part_{}.avro'.format(str(uuid4()))
        clever_schema = Parse(self.raw_schema)
        with DataFileWriter(open(filename, "wb"), DatumWriter(), clever_schema) as writer:
            for record in json_list:
                writer.append(record)
        hdfs_client.upload(
            '/data/{}/.dmpkit/profiles/{}/cdm/ts={}/{}'.format(self.cid, self.source, self.timestamp, filename),
            filename)
        os.remove(filename)