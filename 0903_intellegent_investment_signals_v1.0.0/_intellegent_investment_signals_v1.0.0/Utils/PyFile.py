

import os
import sys

class PyFile():

    @staticmethod
    def GenPath(folder_path) -> None:

        if folder_path.endswith('/'):
            folder_path += '/'

        folder_subs = folder_path.split('/')
        mypath = ""
        for o in folder_subs:
            if o:
                mypath += o
                if not os.path.exists(mypath):
                    os.mkdir(mypath)
                mypath += '/'
        return mypath

    @staticmethod
    def ChDir(mainfilename):
        # for working path.

        print(mainfilename)
        if os.path.exists(mainfilename):
            print("file exists.")
        else:
            # working_path = os.path.dirname(mainfilename)
            # # print(working_path)
            # print("working_path: {}".format(working_path))
            #
            # os.chdir(working_path)
            # print("current working path: {}".format(os.getcwd()))
            pass

        # mainfilename = os.getcwd()
        # working_path = os.path.dirname(mainfilename)
        # os.chdir(working_path)
