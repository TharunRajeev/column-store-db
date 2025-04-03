import os

IN_EXPERIMENTATION = True


IS_IN_DOCKER = os.path.exists("/.dockerenv")
BASE_DIR = (
    os.path.expanduser("~/workspace/h/classes/cs165/mainproj/code/proj/")
    if not IS_IN_DOCKER
    else "/cs165/"
)

EXPERIMENT_BASE_DATA_DIR = f"{BASE_DIR}experiments/data/"
