package org.apache.spark.scheduler.cluster.nomad

import java.net.URI
import java.util

import com.hashicorp.nomad.apimodel.Task
import org.apache.spark.deploy.nomad.ApplicationRunCommand
import org.apache.spark.deploy.nomad.NomadClusterModeLauncher.PrimaryJar
import org.apache.spark.{SparkConf, SparkFunSuite}

class DriverTaskTest extends SparkFunSuite {

  test("Jars of 'spark.driver.extraClassPath' precede ApplicationJar in --driver-class-path") {
    def buildDriverTaskParameters = {
      val commonConf = SparkNomadJob.CommonConf(appId = "test-driver-classpath-app-id",
        appName = "test-driver-classpath-app",
        dockerImage = Some("dockerhub.com/nomad-spark/spark-2.4.6-bin-hadoop-2.7-nomad-0.8.6:v1"),
        dockerAuth = None,
        sparkDistribution = Some(new URI("local:///spark")),
        preventOverwrite = true)

      val applicationJarPath = "https://mvn.central.com/spark-examples.jar"
      val sparkConf = new SparkConf()
      sparkConf.set("spark.driver.extraClassPath", "/hadoop-2.8.5/share/hadoop/tools/lib/hadoop-aws-2.8.5.jar")
      // "spark.jars" property always get auto-populated with ApplicationJar at SparkSubmit.prepareSubmitEnvironment
      sparkConf.set("spark.jars", applicationJarPath)

      val parameters = DriverTask.Parameters(ApplicationRunCommand(PrimaryJar(applicationJarPath),
        mainClass = "org.examples.SparkPi",
        arguments = List()),
        None)

      (commonConf, sparkConf, parameters)
    }

    val (commonConf: SparkNomadJob.CommonConf, sparkConf: SparkConf, parameters: DriverTask.Parameters) = buildDriverTaskParameters
    val sparkDriverTask = new Task()

    // Build the Nomad JobSpec for DriverTask
    DriverTask.configure(commonConf, sparkConf, sparkDriverTask, parameters)

    val driverConfigArgs = sparkDriverTask.getConfig.get("args").asInstanceOf[util.List[String]]
    assert(true == driverConfigArgs.contains("--driver-class-path=/hadoop-2.8.5/share/hadoop/tools/lib/hadoop-aws-2.8.5.jar:/local/spark-examples.jar"))
    assert(true == driverConfigArgs.contains("--conf=spark.jars=file:///local/spark-examples.jar"))
  }


}
