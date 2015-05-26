import subprocess

def before_all(context):
    build_cmd = "mvn -f cmd-line-pom.xml -DbuildDir=build clean package"
    subprocess.check_call(build_cmd, shell=True)
    
