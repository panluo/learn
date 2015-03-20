#!/usr/bin/env python
import os
import sqlite3
from flask import Flask, request, session, g, redirect, url_for, abort,\
        render_template, flash
app = Flask(__name__)

def connect_db():
    rv = sqlite3.connect(app.config['DATABASE'])
    rv.row_factory = sqlite3.Row
    return rv

def get_db():
    """Opens a new database connection if there is none yet for the
        current application context.
    """
    if not hasattr(g,'sqlite_db'):
        g.sqlite_db=connect_db()

    return g.sqlite_db

"""teardown_appcontext 装饰器，将在每次应用环境销毁时执行"""
@app.teardown_appcontext
def close_db(error):
     """Closes the database again at the end of the request."""
     if hasattr(g,'sqlite_db'):
         g.sqlite_db.close()

def init_db():
    with app.app_context():
        db = get_db()
        with app.open_resource('schema.sql',mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()

if __name__ == '__main__':
    app.run()
