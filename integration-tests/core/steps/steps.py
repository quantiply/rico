import subprocess
import sys
import os.path
import json
from behave import *

def parse_out(out):
    lines = out.splitlines()
    return [json.loads(line) for line in lines]

def run_task(context):
    cmd = "java -jar build/test-core-0.0.1.jar file://%s" % os.path.abspath(context.task_config_file)
    print(cmd)
    proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout_value, stderr_value = proc.communicate(context.stdin)
    exit_code = proc.wait()
    sys.stderr.write(stderr_value)
    return (stdout_value, stderr_value, exit_code)

def run_task_ok(context):
    (stdout_value, stderr_value, exit_code) = run_task(context)
    assert exit_code == 0
    return (stdout_value, stderr_value)

def run_task_fail(context):
    (stdout_value, stderr_value, exit_code) = run_task(context)
    assert exit_code == 1
    return (stdout_value, stderr_value)

@given('the task config file')
def step_impl(context):
    context.task_config_file = context.text

@when('stdin is')
def step_impl(context):
    context.stdin = context.text

@then('stdout should be')
def step_impl(context):
    expected_output = context.text
    (stdout_value, stderr_value) = run_task_ok(context)
    assert parse_out(expected_output) == parse_out(stdout_value)
    
@then('task should fail')
def step_impl(context):
    run_task_fail(context)