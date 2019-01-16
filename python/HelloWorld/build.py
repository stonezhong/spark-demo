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
        "rm -rf {}/build",
        base_dir
    )
    execute("mkdir -p {}/build", base_dir)
    execute("mkdir -p {}/build/libs", base_dir)

    execute(
        "rm -rf {}/dist",
        base_dir
    )
    execute("mkdir -p {}/dist", base_dir)

    # 将全部第三方的package安装到build/libs目录下
    execute(
        "pip install -r {}/requirements.txt -t {}/build/libs",
        base_dir, base_dir
    )
    # 产生libs.zip文件
    os.chdir("{}/build/libs".format(base_dir))
    execute("zip -r {}/dist/libs.zip .", base_dir)
    os.chdir(base_dir)

    # 产生app.zip文件
    os.chdir("{}/src/main".format(base_dir))
    execute("zip -x app.py -r {}/dist/app.zip .", base_dir)
    os.chdir(base_dir)

    # copy app.py to dist目录
    execute("cp {}/src/main/app.py {}/dist/app.py", base_dir, base_dir)

if __name__ == '__main__':
    main()