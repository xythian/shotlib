"""Common git-based / pip-based deployment."""

from fabric.api import *
import re, glob

VERSION_R = re.compile(r'^VERSION = "(\d+\.\d+\.(\d+))"', re.MULTILINE)

@task
def clean():
    local("rm -rf build dist")

@task
def dist():
    clean()
    local("python ./setup.py bdist_egg")

def isclean():
    return local("git status | tail -n 1", capture=True) == "nothing to commit (working directory clean)"

def read_version():
    with open('setup.py', 'rt') as f:
        return VERSION_R.search(f.read()).group(1)

def package_eggs():
    return glob.glob("dist/*.egg")

@task
def build():
    if not isclean():
        print "Working directory not clean."
        return -1
    with open('setup.py', 'rt') as f:
        data = f.read()
        m = VERSION_R.search(data)
        v = m.group(2)
        result = data[:m.start(2)] + str(int(v) + 1) + data[m.end(2):]
        version = VERSION_R.search(result).group(1)
    with open('setup.py', 'wt') as f:
        f.write(result)
    local('git add setup.py && git commit -m "Bump version to ' + version + '"')
    dist()
