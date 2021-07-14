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
package org.apache.spark.scheduler.cluster.k8s

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.util.Clock

private[spark] abstract class AbstractPodsAllocator(
    conf: SparkConf,
    secMgr: SecurityManager,
    executorBuilder: KubernetesExecutorBuilder,
    kubernetesClient: KubernetesClient,
    snapshotsStore: ExecutorPodsSnapshotsStore,
    clock: Clock) {
  /*
   * Set the total expected executors for an application
   */
  def setTotalExpectedExecutors(applicationId: String,
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Unit
  /*
   * Reference to driver pod.
   */
  def driverPod: Option[Pod]
  /*
   * If the pod for a given exec id is deleted.
   */
  def isDeleted(executorId: String): Boolean
  /*
   * Start hook.
   */
  def start(applicationId: String, schedulerBackend: KubernetesClusterSchedulerBackend): Unit
  /*
   * Stop hook
   */
  def stop(applicationId: String): Unit
}
