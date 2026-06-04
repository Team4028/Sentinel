import os
import subprocess

def build_doc(version, tag):
    os.environ["current_version"] = version
    subprocess.run('git checkout ' + tag, shell=True)
    subprocess.run('git checkout main -- conf.py', shell=True)
    subprocess.run('./make html', shell=True)

def move_dir(src, dst):
    subprocess.run(['mkdir', '-p', dst])
    subprocess.run("mv " + src + "* " + dst, shell=True)

os.environ["build_all_docs"] = str(True)
os.environ["pages_root"] = "https://team4028.github.io/"

build_doc("latest", "main")
move_dir("./build/html/", f"../pages/")
## more to come