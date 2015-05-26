/**
 * Copyright (c) 2015 Liang Kun. All Rights Reserved.
 * Authors: Liang Kun <liangkun@data-intelli.com>
 */
package org.tutorial.storm

import scala.io.StdIn

import backtype.storm.generated.DistributedRPC.Iface
import backtype.storm.generated.StormTopology
import backtype.storm.utils.DRPCClient
import backtype.storm.{ILocalDRPC, StormSubmitter, Config, LocalCluster, LocalDRPC}
import backtype.storm.tuple.{Fields, Values}
import com.typesafe.config.ConfigFactory
import org.tutorial.storm.spout.SocketServerSpout
import storm.trident.TridentTopology
import storm.trident.operation.builtin.{Sum, FilterNull, MapGet, Count}
import storm.trident.operation.{TridentCollector, BaseFunction}
import storm.trident.state.{State, QueryFunction}
import storm.trident.testing.MemoryMapState
import storm.trident.tuple.TridentTuple

object TridentWordCount {
  val appConfig = ConfigFactory.load()

  def main(args: Array[String]): Unit = {
    val config = new Config
    config.setMaxSpoutPending(16)

    var drpc: Iface = null
    if (args.isEmpty) {
      val localDrpc = new LocalDRPC()
      drpc = localDrpc
      val cluster = new LocalCluster()
      cluster.submitTopology("TridentWordCount", config, tridentWordCountTopology(Some(localDrpc)))
    } else {
      config.setNumWorkers(2)
      StormSubmitter.submitTopologyWithProgressBar("TridentWordCount", config, tridentWordCountTopology(None))
      drpc = new DRPCClient("localhost", 3772)
    }

    var exit = false
    while (!exit) {
      try {
        val cmdline = StdIn.readLine("==> ").trim.split("\\W+")
        cmdline(0) match {
          case "" =>
          // ignore this

          case "exit" =>
            exit = true

          case "word" =>
            val result = drpc.execute("word", cmdline.drop(1).mkString(" "))
            println(s"RESULT: $result")

          case cmd =>
            println(s"Unknown command: $cmd")
        }
      } catch {
        case e: Exception => println("ERROR: " + e.getMessage)
      }
    }

    System.exit(0)
  }

  class Splitter extends BaseFunction {
    override def execute(tuple: TridentTuple, collector: TridentCollector): Unit = {
      val document = tuple.getString(0)
      for (word <- document.split("\\W+")) {
        collector.emit(new Values(word))
      }
    }
  }

  /** Build and return the trident word count topology */
  def tridentWordCountTopology(drpc: Option[LocalDRPC]): StormTopology = {
    val topology = new TridentTopology

    val wordCount = topology
      .newStream("socket-server-stream", new SocketServerSpout(appConfig))
      .parallelismHint(4)
      .each(new Fields("document"), new Splitter, new Fields("word"))
      .groupBy(new Fields("word"))
      .persistentAggregate(new MemoryMapState.Factory, new Count, new Fields("count"))
      .parallelismHint(4)

    topology.newDRPCStream("word", drpc.orNull)
      .each(new Fields("args"), new Splitter, new Fields("word"))
      .groupBy(new Fields("word"))
      .stateQuery(
        wordCount,
        new Fields("word"),
        (new MapGet).asInstanceOf[QueryFunction[State, _]],
        new Fields("count"))
      .each(new Fields("count"), new FilterNull)
      .aggregate(new Fields("count"), new Sum, new Fields("sum"))

    topology.build()
  }
}
