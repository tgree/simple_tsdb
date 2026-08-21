# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
import simple_tsdb
from flask import current_app, g


_no_stsdb_msg = '''\
No SimpleTSDB connection is present.

This means that something has overwritten g._stsdb_client.
'''


class SimpleTSDB:
    def init_app(self, app):
        app.config.setdefault('STSDB_HOST', 'localhost')
        app.config.setdefault('STSDB_PORT', '4000')
        app.config.setdefault('STSDB_USERNAME', None)
        app.config.setdefault('STSDB_PASSWORD', None)
        app.teardown_appcontext(self.teardown)

    @staticmethod
    def connect():
        return simple_tsdb.Client(
            host=current_app.config['STSDB_HOST'],
            port=int(current_app.config['STSDB_PORT']),
            credentials=(
                current_app.config['STSDB_USERNAME'],
                current_app.config['STSDB_PASSWORD']))

    @staticmethod
    def teardown(_exc):
        c = g.pop('_stsdb_client', None)
        if c is not None:
            c.close()

    @property
    def client(self):
        if '_stsdb_client' not in g:
            g._stsdb_client = SimpleTSDB.connect()

        if g._stsdb_client is None:
            raise RuntimeError(_no_stsdb_msg)

        return g._stsdb_client
