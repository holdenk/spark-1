#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function
import os

def run_python_example(test_name, pyspark_python):
    """
    Run a given python example using the provided python. Exits on failure.
    """
    env = dict(os.environ)
    env.update({'PYSPARK_PYTHON': which(pyspark_python)})
    try:
        per_test_output = tempfile.TemporaryFile()
        args = [os.path.join(SPARK_HOME, "bin/pyspark"), test_name]
        if os.path.isfile(test_name + ".params"):
            with open(test_name + ".params") as f:
                args.extend(f.read().splitlines())
        retcode = subprocess.Popen(
            args, stderr=per_test_output, stdout=per_test_output, env=env).wait()
    except:
        print("Got exception while running %s with %s", test_name, pyspark_python)
        # Here, we use os._exit() instead of sys.exit() in order to force Python to exit even if
        # this code is invoked from a thread other than the main thread.
        os._exit(1)

def discover_python_examples():
    """
    Find the python examples to execute.
    """
    dirpath = os.path.join(os.path.dirname(os.path.realpath(__file__)), "src/main/python")
    return [os.path.join(dirpath, f)
            for dirpath, dirnames, files in os.walk(dirpath)
            for f in files if f.endswith('.py')]

def main():
    print(discover_python_examples())

if __name__ == "__main__":
    main()
