
import numpy as np
import pandas as pd
import os

import json

def get_env_db_config():

    with open("env.json","r") as fr:
        data = json.load(fr)
        env = data["env"]
        server = data["server"]

    db_config = None
    print(os.path.abspath(os.path.dirname(os.getcwd())))

    if os.path.exists("config.json"):
        print("config.json exists.")
    else:
        print("config.json does not eixsts")
        # if os.path.exists("")
        if os.path.exists("PyFile.py"):
            print("found.")
        else:
            print("not found.")
    with open("config.json", "r") as f:
        data = json.load(f)

        # env = data['env']
        # env_dbname = data['env_db_name']
        # print("using env of {}".format(env))
        # print("using db of {}".format(env_dbname))

        env_dbname = "{}_{}_db".format(server,env)

        db_config = data['dbs'].get(env_dbname)
        # print(db_config)

    return db_config


