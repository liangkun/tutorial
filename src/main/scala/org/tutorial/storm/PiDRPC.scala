/**
 * Copyright (c) 2015 Liang Kun. All Rights Reserved.
 * Authors: Liang Kun <liangkun@data-intelli.com>
 */
package org.tutorial.storm

import scala.io.StdIn

import backtype.storm.{Config, LocalCluster, LocalDRPC}
import backtype.storm.drpc.LinearDRPCTopologyBuilder
import backtype.storm.tuple.Fields
import org.tutorial.storm.bolt.{RandomPiResult, RandomPi}

object PiDRPC {
  def main(args: Array[String]): Unit = {
    var computingTasks = 4
    if (args.nonEmpty) {
      computingTasks = args(0).toInt
    }
    val drpc = new LocalDRPC()
    val cluster = new LocalCluster()
    val config = new Config()
    config.setNumWorkers(2)

    val builder = new LinearDRPCTopologyBuilder("pi")
    builder.addBolt(new RandomPi)
      .setNumTasks(computingTasks)
    builder.addBolt(new RandomPiResult)
      .fieldsGrouping(new Fields("request-id"))
    cluster.submitTopology("pi-drpc", config, builder.createLocalTopology(drpc))

    var exit = false
    while (!exit) {
      try {
        val cmdline = StdIn.readLine("==> ").trim.split("\\W+")
        cmdline(0) match {
          case "" =>
            // ignore this

          case "exit" =>
            exit = true

          case "pi" =>
            val countPerTask = cmdline(1).toInt / computingTasks
            val result = drpc.execute("pi", countPerTask.toString)
            println(s"RESULT: $result")

          case cmd =>
            println(s"Unknown command: $cmd")
        }
      } catch {
        case e: Exception => println("ERROR: " + e.getMessage)
      }
    }

    cluster.shutdown()
    drpc.shutdown()
  }
}
