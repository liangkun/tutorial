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
    var count = 20000
    if (args.length > 0) {
      count = args(0).toInt
    }

    val builder = new LinearDRPCTopologyBuilder("pi")
    builder.addBolt(new RandomPi)
           .setNumTasks(4)
    builder.addBolt(new RandomPiResult)
           .fieldsGrouping(new Fields("id"))

    val drpc = new LocalDRPC()
    val cluster = new LocalCluster()
    val config = new Config()
    config.setNumWorkers(2)

    cluster.submitTopology("pi-drpc", config, builder.createLocalTopology(drpc))

    var exit = false
    while (!exit) {
      try {
        val cmdline = StdIn.readLine("==> ").trim.split("\\W+")
        if (cmdline.nonEmpty) {
          cmdline(0) match {
            case "exit" => exit = true

            case "pi" =>
              val count = cmdline(1).toInt
              val result = drpc.execute("pi", count.toString)
              println(s"RESULT: $result")

            case cmd =>
              println(s"Unknown command: $cmd")
          }
        }
      } catch {
        case e: Exception => println("ERROR: " + e.getMessage)
      }
    }

    cluster.shutdown()
    drpc.shutdown()
  }
}
