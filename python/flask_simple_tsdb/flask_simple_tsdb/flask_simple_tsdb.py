# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
import simple_tsdb
from flask import current_app, _app_ctx_stack
from flask.globals import _app_ctx_err_msg


_no_stsdb_msg = '''\
No SimpleTSDB connection is present.

This means that something has overwritten _app_ctx_stack.top.stsdb_client.
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
        db = getattr(_app_ctx_stack.top, 'stsdb_client', None)
        if db is not None:
            db.close()

    @property
    def client(self):
        ctx = _app_ctx_stack.top
        if ctx is None:
            raise RuntimeError(_app_ctx_err_msg)

        if not hasattr(ctx, 'stsdb_client'):
            ctx.stsdb_client = SimpleTSDB.connect()

        if ctx.stsdb_client is None:
            raise RuntimeError(_no_stsdb_msg)

        return ctx.stsdb_client
