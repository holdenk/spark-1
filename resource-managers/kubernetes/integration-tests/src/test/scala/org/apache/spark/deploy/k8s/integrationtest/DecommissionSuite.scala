/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.k8s.integrationtest

import org.apache.spark.deploy.k8s.integrationtest.TestConfig.{getTestImageRepo, getTestImageTag}

private[spark] trait DecommissionSuite { k8sSuite: KubernetesSuite =>

  import DecommissionSuite._
  import KubernetesSuite.k8sTestTag

  private val pySparkDockerImage =
    s"${getTestImageRepo}/spark-py:${getTestImageTag}"

  test("Run SparkPi with env and mount secrets.", k8sTestTag) {
    sparkAppConf
      .set("spark.worker.decommission.enabled", "true")
      .set("spark.kubernetes.container.image", pySparkDockerImage)
      .set("spark.kubernetes.pyspark.pythonVersion", "2")

    runSparkApplicationAndVerifyCompletion(
      appResource = PYSPARK_DECOMISSIONING,
      mainClass = "",
      expectedLogOnCompletion = Seq("Decommissioning worker"),
      appArgs = Array.empty[String],
      driverPodChecker = doBasicDriverPyPodCheck,
      executorPodChecker = doBasicExecutorPyPodCheck,
      appLocator = appLocator,
      isJVM = false,
      decomissioningTest = true)
  }
}

private[spark] object DecommissionSuite {
  val TEST_LOCAL_PYSPARK: String = "local:///opt/spark/tests/"
  val PYSPARK_DECOMISSIONING: String = TEST_LOCAL_PYSPARK + "decomissioning_waiter.py"
}
