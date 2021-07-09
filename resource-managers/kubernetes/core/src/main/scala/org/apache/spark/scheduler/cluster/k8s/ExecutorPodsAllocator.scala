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

import java.time.Instant
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import io.fabric8.kubernetes.api.model.{HasMetadata, PersistentVolumeClaim,
  PersistentVolumeClaimBuilder, Pod, PodBuilder, PodSpec, PodSpecBuilder, PodTemplateSpec,
  PodTemplateSpecBuilder}
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesConf
import org.apache.spark.deploy.k8s.KubernetesUtils.addOwnerReference
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.util.{Clock, Utils}

private[spark] class ExecutorPodsAllocator(
    conf: SparkConf,
    secMgr: SecurityManager,
    executorBuilder: KubernetesExecutorBuilder,
    kubernetesClient: KubernetesClient,
    snapshotsStore: ExecutorPodsSnapshotsStore,
    clock: Clock) extends Logging {

  private val EXECUTOR_ID_COUNTER = new AtomicInteger(0)

  // ResourceProfile id -> total expected executors per profile, currently we don't remove
  // any resource profiles - https://issues.apache.org/jira/browse/SPARK-30749
  private val totalExpectedExecutorsPerResourceProfileId = new ConcurrentHashMap[Int, Int]()

  private val rpIdToResourceProfile = new mutable.HashMap[Int, ResourceProfile]

  private val podAllocationSize = conf.get(KUBERNETES_ALLOCATION_BATCH_SIZE)

  private val podAllocationDelay = conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)

  // TODO: Change to statefulSet?
  private val useReplicasets = conf.get(KUBERNETES_ALLOCATION_REPLICASET)

  private val podCreationTimeout = math.max(
    podAllocationDelay * 5,
    conf.get(KUBERNETES_ALLOCATION_EXECUTOR_TIMEOUT))

  private val driverPodReadinessTimeout = conf.get(KUBERNETES_ALLOCATION_DRIVER_READINESS_TIMEOUT)

  private val executorIdleTimeout = conf.get(DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT) * 1000

  private val namespace = conf.get(KUBERNETES_NAMESPACE)

  private val kubernetesDriverPodName = conf
    .get(KUBERNETES_DRIVER_POD_NAME)

  private val shouldDeleteExecutors = conf.get(KUBERNETES_DELETE_EXECUTORS)

  val driverPod = kubernetesDriverPodName
    .map(name => Option(kubernetesClient.pods()
      .withName(name)
      .get())
      .getOrElse(throw new SparkException(
        s"No pod was found named $name in the cluster in the " +
          s"namespace $namespace (this was supposed to be the driver pod.).")))

  // Executor IDs that have been requested from Kubernetes but have not been detected in any
  // snapshot yet. Mapped to the (ResourceProfile id, timestamp) when they were created.
  private val newlyCreatedExecutors = mutable.LinkedHashMap.empty[Long, (Int, Long)]

  // Executor IDs that have been requested from Kubernetes but have not been detected in any POD
  // snapshot yet but already known by the scheduler backend. Mapped to the ResourceProfile id.
  private val schedulerKnownNewlyCreatedExecs = mutable.LinkedHashMap.empty[Long, Int]

  private val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(conf)

  // visible for tests
  private[k8s] val numOutstandingPods = new AtomicInteger()

  private var lastSnapshot = ExecutorPodsSnapshot()

  // Executors that have been deleted by this allocator but not yet detected as deleted in
  // a snapshot from the API server. This is used to deny registration from these executors
  // if they happen to come up before the deletion takes effect.
  @volatile private var deletedExecutorIds = Set.empty[Long]

  def start(applicationId: String, schedulerBackend: KubernetesClusterSchedulerBackend): Unit = {
    driverPod.foreach { pod =>
      // Wait until the driver pod is ready before starting executors, as the headless service won't
      // be resolvable by DNS until the driver pod is ready.
      Utils.tryLogNonFatalError {
        kubernetesClient
          .pods()
          .withName(pod.getMetadata.getName)
          .waitUntilReady(driverPodReadinessTimeout, TimeUnit.SECONDS)
      }
    }
    snapshotsStore.addSubscriber(podAllocationDelay) {
      onNewSnapshots(applicationId, schedulerBackend, _)
    }
  }

  def setTotalExpectedExecutors(applicationId: String,
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Unit = {

    resourceProfileToTotalExecs.foreach { case (rp, numExecs) =>
      rpIdToResourceProfile.getOrElseUpdate(rp.id, rp)
      totalExpectedExecutorsPerResourceProfileId.put(rp.id, numExecs)
      if (useReplicasets) {
        setTargetExecutorsReplicaset(numExecs, applicationId, rp.id)
      }
    }
    logDebug(s"Set total expected execs to $totalExpectedExecutorsPerResourceProfileId")
    if (numOutstandingPods.get() == 0) {
      snapshotsStore.notifySubscribers()
    }
  }

  def isDeleted(executorId: String): Boolean = deletedExecutorIds.contains(executorId.toLong)

  private def onNewSnapshots(
      applicationId: String,
      schedulerBackend: KubernetesClusterSchedulerBackend,
      snapshots: Seq[ExecutorPodsSnapshot]): Unit = {
    val k8sKnownExecIds = snapshots.flatMap(_.executorPods.keys)
    newlyCreatedExecutors --= k8sKnownExecIds
    schedulerKnownNewlyCreatedExecs --= k8sKnownExecIds

    // Although we are going to delete some executors due to timeout in this function,
    // it takes undefined time before the actual deletion. Hence, we should collect all PVCs
    // in use at the beginning. False positive is okay in this context in order to be safe.
    val k8sKnownPVCNames = snapshots.flatMap(_.executorPods.values.map(_.pod)).flatMap { pod =>
      pod.getSpec.getVolumes.asScala
        .flatMap { v => Option(v.getPersistentVolumeClaim).map(_.getClaimName) }
    }

    // transfer the scheduler backend known executor requests from the newlyCreatedExecutors
    // to the schedulerKnownNewlyCreatedExecs
    val schedulerKnownExecs = schedulerBackend.getExecutorIds().map(_.toLong).toSet
    schedulerKnownNewlyCreatedExecs ++=
      newlyCreatedExecutors.filterKeys(schedulerKnownExecs.contains(_)).mapValues(_._1)
    newlyCreatedExecutors --= schedulerKnownNewlyCreatedExecs.keySet

    // For all executors we've created against the API but have not seen in a snapshot
    // yet - check the current time. If the current time has exceeded some threshold,
    // assume that the pod was either never created (the API server never properly
    // handled the creation request), or the API server created the pod but we missed
    // both the creation and deletion events. In either case, delete the missing pod
    // if possible, and mark such a pod to be rescheduled below.
    val currentTime = clock.getTimeMillis()
    val timedOut = newlyCreatedExecutors.flatMap { case (execId, (_, timeCreated)) =>
      if (currentTime - timeCreated > podCreationTimeout) {
        Some(execId)
      } else {
        logDebug(s"Executor with id $execId was not found in the Kubernetes cluster since it" +
          s" was created ${currentTime - timeCreated} milliseconds ago.")
        None
      }
    }

    if (timedOut.nonEmpty) {
      logWarning(s"Executors with ids ${timedOut.mkString(",")} were not detected in the" +
        s" Kubernetes cluster after $podCreationTimeout ms despite the fact that a previous" +
        " allocation attempt tried to create them. The executors may have been deleted but the" +
        " application missed the deletion event.")

      newlyCreatedExecutors --= timedOut
      if (shouldDeleteExecutors) {
        Utils.tryLogNonFatalError {
          kubernetesClient
            .pods()
            .withLabel(SPARK_APP_ID_LABEL, applicationId)
            .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
            .withLabelIn(SPARK_EXECUTOR_ID_LABEL, timedOut.toSeq.map(_.toString): _*)
            .delete()
        }
      }
    }

    if (snapshots.nonEmpty) {
      lastSnapshot = snapshots.last
    }

    // Make a local, non-volatile copy of the reference since it's used multiple times. This
    // is the only method that modifies the list, so this is safe.
    var _deletedExecutorIds = deletedExecutorIds
    if (snapshots.nonEmpty) {
      val existingExecs = lastSnapshot.executorPods.keySet
      _deletedExecutorIds = _deletedExecutorIds.filter(existingExecs.contains)
    }

    val notDeletedPods = lastSnapshot.executorPods.filterKeys(!_deletedExecutorIds.contains(_))
    // Map the pods into per ResourceProfile id so we can check per ResourceProfile,
    // add a fast path if not using other ResourceProfiles.
    val rpIdToExecsAndPodState =
      mutable.HashMap[Int, mutable.HashMap[Long, ExecutorPodState]]()
    if (totalExpectedExecutorsPerResourceProfileId.size <= 1) {
      rpIdToExecsAndPodState(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID) =
        mutable.HashMap.empty ++= notDeletedPods
    } else {
      notDeletedPods.foreach { case (execId, execPodState) =>
        val rpId = execPodState.pod.getMetadata.getLabels.get(SPARK_RESOURCE_PROFILE_ID_LABEL).toInt
        val execPods = rpIdToExecsAndPodState.getOrElseUpdate(rpId,
          mutable.HashMap[Long, ExecutorPodState]())
        execPods(execId) = execPodState
      }
    }

    var totalPendingCount = 0
    // The order we request executors for each ResourceProfile is not guaranteed.
    totalExpectedExecutorsPerResourceProfileId.asScala.foreach { case (rpId, targetNum) =>
      val podsForRpId = rpIdToExecsAndPodState.getOrElse(rpId, mutable.HashMap.empty)

      val currentRunningCount = podsForRpId.values.count {
        case PodRunning(_) => true
        case _ => false
      }

      val (schedulerKnownPendingExecsForRpId, currentPendingExecutorsForRpId) = podsForRpId.filter {
        case (_, PodPending(_)) => true
        case _ => false
      }.partition { case (k, _) =>
        schedulerKnownExecs.contains(k)
      }
      // This variable is used later to print some debug logs. It's updated when cleaning up
      // excess pod requests, since currentPendingExecutorsForRpId is immutable.
      var knownPendingCount = currentPendingExecutorsForRpId.size

      val newlyCreatedExecutorsForRpId =
        newlyCreatedExecutors.filter { case (_, (waitingRpId, _)) =>
          rpId == waitingRpId
        }

      val schedulerKnownNewlyCreatedExecsForRpId =
        schedulerKnownNewlyCreatedExecs.filter { case (_, waitingRpId) =>
          rpId == waitingRpId
        }

      if (podsForRpId.nonEmpty) {
        logDebug(s"ResourceProfile Id: $rpId " +
          s"pod allocation status: $currentRunningCount running, " +
          s"${currentPendingExecutorsForRpId.size} unknown pending, " +
          s"${schedulerKnownPendingExecsForRpId.size} scheduler backend known pending, " +
          s"${newlyCreatedExecutorsForRpId.size} unknown newly created, " +
          s"${schedulerKnownNewlyCreatedExecsForRpId.size} scheduler backend known newly created.")
      }

      // It's possible that we have outstanding pods that are outdated when dynamic allocation
      // decides to downscale the application. So check if we can release any pending pods early
      // instead of waiting for them to time out. Drop them first from the unacknowledged list,
      // then from the pending. However, in order to prevent too frequent fluctuation, newly
      // requested pods are protected during executorIdleTimeout period.
      //
      // TODO: with dynamic allocation off, handle edge cases if we end up with more running
      // executors than expected.
      val knownPodCount = currentRunningCount +
        currentPendingExecutorsForRpId.size + schedulerKnownPendingExecsForRpId.size +
        newlyCreatedExecutorsForRpId.size + schedulerKnownNewlyCreatedExecsForRpId.size

      if (knownPodCount > targetNum) {
        val excess = knownPodCount - targetNum
        val newlyCreatedToDelete = newlyCreatedExecutorsForRpId
          .filter { case (_, (_, createTime)) =>
            currentTime - createTime > executorIdleTimeout
          }.keys.take(excess).toList
        val knownPendingToDelete = currentPendingExecutorsForRpId
          .filter(x => isExecutorIdleTimedOut(x._2, currentTime))
          .take(excess - newlyCreatedToDelete.size)
          .map { case (id, _) => id }
        val toDelete = newlyCreatedToDelete ++ knownPendingToDelete

        if (toDelete.nonEmpty) {
          logInfo(s"Deleting ${toDelete.size} excess pod requests (${toDelete.mkString(",")}).")
          _deletedExecutorIds = _deletedExecutorIds ++ toDelete

          Utils.tryLogNonFatalError {
            kubernetesClient
              .pods()
              .withField("status.phase", "Pending")
              .withLabel(SPARK_APP_ID_LABEL, applicationId)
              .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
              .withLabelIn(SPARK_EXECUTOR_ID_LABEL, toDelete.sorted.map(_.toString): _*)
              .delete()
            newlyCreatedExecutors --= newlyCreatedToDelete
            knownPendingCount -= knownPendingToDelete.size
          }
        }
      }

      if (newlyCreatedExecutorsForRpId.isEmpty
        && knownPodCount < targetNum && !useReplicasets) {
        requestNewExecutorsDirect(targetNum, knownPodCount, applicationId, rpId, k8sKnownPVCNames)
      }

      totalPendingCount += knownPendingCount

      // The code below just prints debug messages, which are only useful when there's a change
      // in the snapshot state. Since the messages are a little spammy, avoid them when we know
      // there are no useful updates.
      if (log.isDebugEnabled && snapshots.nonEmpty) {
        val outstanding = knownPendingCount + newlyCreatedExecutorsForRpId.size
        if (currentRunningCount >= targetNum && !dynamicAllocationEnabled) {
          logDebug(s"Current number of running executors for ResourceProfile Id $rpId is " +
            "equal to the number of requested executors. Not scaling up further.")
        } else {
          if (outstanding > 0) {
            logDebug(s"Still waiting for $outstanding executors for ResourceProfile " +
              s"Id $rpId before requesting more.")
          }
        }
      }
    }
    deletedExecutorIds = _deletedExecutorIds

    // Update the flag that helps the setTotalExpectedExecutors() callback avoid triggering this
    // update method when not needed. PODs known by the scheduler backend are not counted here as
    // they considered running PODs and they should not block upscaling.
    numOutstandingPods.set(totalPendingCount + newlyCreatedExecutors.size)
  }

  private def getReusablePVCs(applicationId: String, pvcsInUse: Seq[String]) = {
    if (conf.get(KUBERNETES_DRIVER_OWN_PVC) && conf.get(KUBERNETES_DRIVER_REUSE_PVC) &&
        driverPod.nonEmpty) {
      val createdPVCs = kubernetesClient
        .persistentVolumeClaims
        .withLabel("spark-app-selector", applicationId)
        .list()
        .getItems
        .asScala

      val reusablePVCs = createdPVCs.filterNot(pvc => pvcsInUse.contains(pvc.getMetadata.getName))
      logInfo(s"Found ${reusablePVCs.size} reusable PVCs from ${createdPVCs.size} PVCs")
      reusablePVCs
    } else {
      mutable.Buffer.empty[PersistentVolumeClaim]
    }
  }

  // For now just track the sets created, in the future maybe track requested value too.
  val setsCreated = new mutable.HashSet[Int]()

  private def setTargetExecutorsReplicaset(
      expected: Int,
      applicationId: String,
      resourceProfileId: Int): Unit = {
    val setName = s"spark-s-${applicationId}-${resourceProfileId}"
    if (setsCreated.contains(resourceProfileId)) {
      // TODO Update the replicaset with our new target.
    } else {
      // We need to make the new replicaset which is going to involve building
      // a pod.
      val executorConf = KubernetesConf.createExecutorConf(
        conf,
        "EXECID",// template exec IDs
        applicationId,
        driverPod,
        resourceProfileId)
      val resolvedExecutorSpec = executorBuilder.buildFromFeatures(executorConf, secMgr,
        kubernetesClient, rpIdToResourceProfile(resourceProfileId))
      val executorPod = resolvedExecutorSpec.pod
      val podWithAttachedContainer: PodSpec = new PodSpecBuilder(executorPod.pod.getSpec())
        .addToContainers(executorPod.container)
        .build()

      val meta = executorPod.pod.getMetadata()

      // Create a pod template spec with the right resource profile id.
      val podTemplateSpec = new PodTemplateSpecBuilder(
        new PodTemplateSpec(meta, podWithAttachedContainer))
        .editMetadata()
          .addToLabels("rpi", resourceProfileId.toString)
        .endMetadata()
        .build()
      // Resources that need to be created but there not associated per-pod
      val resources = resolvedExecutorSpec.executorKubernetesResources
      val miscK8sResources = resources
        .filter(_.getKind != "PersistentVolumeClaim")
      // We'll let PVCs be handled by the statefulset, we need
      val volumes = resources
        .filter(_.getKind == "PersistentVolumeClaim")
        .map { hm =>
          val v = hm.asInstanceOf[PersistentVolumeClaim]
          new PersistentVolumeClaimBuilder(v)
            .editMetadata()
              .withName(v.getMetadata().getName().replace("EXECID", ""))
            .endMetadata()
            .build()
        }.asJava

      val statefulSet = new io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder()
        .withNewMetadata()
          .withName(setName)
          .withNamespace(conf.get(KUBERNETES_NAMESPACE))
        .endMetadata()
        .withNewSpec()
          .withReplicas(expected)
          .withNewSelector()
            .addToMatchLabels(SPARK_APP_ID_LABEL, applicationId)
            .addToMatchLabels(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
            .addToMatchLabels("rpi", resourceProfileId.toString)
          .endSelector()
          .withTemplate(podTemplateSpec)
          .addAllToVolumeClaimTemplates(volumes)
        .endSpec()
        .build()

      // Complicated how we want to references here. I think lets give it to the driver.
      addOwnerReference(driverPod.get, miscK8sResources)
      kubernetesClient.apps().statefulSets().create(statefulSet)
    }
  }

  private def requestNewExecutorsDirect(
      expected: Int,
      running: Int,
      applicationId: String,
      resourceProfileId: Int,
      pvcsInUse: Seq[String]): Unit = {
    val numExecutorsToAllocate = math.min(expected - running, podAllocationSize)
    logInfo(s"Going to request $numExecutorsToAllocate executors from Kubernetes for " +
      s"ResourceProfile Id: $resourceProfileId, target: $expected running: $running.")
    // Check reusable PVCs for this executor allocation batch
    val reusablePVCs = getReusablePVCs(applicationId, pvcsInUse)
    for ( _ <- 0 until numExecutorsToAllocate) {
      val newExecutorId = EXECUTOR_ID_COUNTER.incrementAndGet()
      val executorConf = KubernetesConf.createExecutorConf(
        conf,
        newExecutorId.toString,
        applicationId,
        driverPod,
        resourceProfileId)
      val resolvedExecutorSpec = executorBuilder.buildFromFeatures(executorConf, secMgr,
        kubernetesClient, rpIdToResourceProfile(resourceProfileId))
      val executorPod = resolvedExecutorSpec.pod
      val podWithAttachedContainer = new PodBuilder(executorPod.pod)
        .editOrNewSpec()
        .addToContainers(executorPod.container)
        .endSpec()
        .build()
      val resources = replacePVCsIfNeeded(
        podWithAttachedContainer, resolvedExecutorSpec.executorKubernetesResources, reusablePVCs)
      val createdExecutorPod = kubernetesClient.pods().create(podWithAttachedContainer)
      try {
        addOwnerReference(createdExecutorPod, resources)
        resources
          .filter(_.getKind == "PersistentVolumeClaim")
          .foreach { resource =>
            if (conf.get(KUBERNETES_DRIVER_OWN_PVC) && driverPod.nonEmpty) {
              addOwnerReference(driverPod.get, Seq(resource))
            }
            val pvc = resource.asInstanceOf[PersistentVolumeClaim]
            logInfo(s"Trying to create PersistentVolumeClaim ${pvc.getMetadata.getName} with " +
              s"StorageClass ${pvc.getSpec.getStorageClassName}")
            kubernetesClient.persistentVolumeClaims().create(pvc)
          }
        newlyCreatedExecutors(newExecutorId) = (resourceProfileId, clock.getTimeMillis())
        logDebug(s"Requested executor with id $newExecutorId from Kubernetes.")
      } catch {
        case NonFatal(e) =>
          kubernetesClient.pods().delete(createdExecutorPod)
          throw e
      }
    }
  }

  private def replacePVCsIfNeeded(
      pod: Pod,
      resources: Seq[HasMetadata],
      reusablePVCs: mutable.Buffer[PersistentVolumeClaim]): Seq[HasMetadata] = {
    val replacedResources = mutable.Set[HasMetadata]()
    resources.foreach {
      case pvc: PersistentVolumeClaim =>
        // Find one with the same storage class and size.
        val index = reusablePVCs.indexWhere { p =>
          p.getSpec.getStorageClassName == pvc.getSpec.getStorageClassName &&
            p.getSpec.getResources.getRequests.get("storage") ==
              pvc.getSpec.getResources.getRequests.get("storage")
        }
        if (index >= 0) {
          val volume = pod.getSpec.getVolumes.asScala.find { v =>
            v.getPersistentVolumeClaim != null &&
              v.getPersistentVolumeClaim.getClaimName == pvc.getMetadata.getName
          }
          if (volume.nonEmpty) {
            val matchedPVC = reusablePVCs.remove(index)
            replacedResources.add(pvc)
            logInfo(s"Reuse PersistentVolumeClaim ${matchedPVC.getMetadata.getName}")
            volume.get.getPersistentVolumeClaim.setClaimName(matchedPVC.getMetadata.getName)
          }
        }
      case _ => // no-op
    }
    resources.filterNot(replacedResources.contains)
  }

  private def isExecutorIdleTimedOut(state: ExecutorPodState, currentTime: Long): Boolean = {
    try {
      val startTime = Instant.parse(state.pod.getStatus.getStartTime).toEpochMilli()
      currentTime - startTime > executorIdleTimeout
    } catch {
      case _: Exception =>
        logDebug(s"Cannot get startTime of pod ${state.pod}")
        true
    }
  }
}
