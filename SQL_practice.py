import sqlite3 as sq
import os

cur_path = os.path.dirname(os.path.realpath(__file__))
con = sq.connect(cur_path + '/kospi.db')
type(con)
sq.Connection