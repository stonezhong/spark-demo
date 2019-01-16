#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from __future__ import print_function
import os
import sys

def execute(cmd, *args):
    cmd_with_args = cmd.format(*args)
    print("Executing: {}".format(cmd_with_args))
    exit_code = os.system(cmd_with_args)
    return exit_code

def main():
    base_dir = os.path.dirname(sys.argv[0])
    execute(
        "spark-submit --py-files {}/dist/libs.zip,{}/dist/app.zip {}/dist/app.py",
        base_dir, base_dir, base_dir
    )





if __name__ == '__main__':
    main()