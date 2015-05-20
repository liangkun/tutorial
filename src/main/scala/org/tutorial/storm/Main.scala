/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved.
 * Authors: Liang Kun <liangkun@data-intelli.com>
 */
package org.tutorial.storm

import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields
import backtype.storm.{LocalCluster, Config}
import com.typesafe.config.{ConfigFactory => AppConfigFactory}
import org.tutorial.storm.bolt.{Printer, WordCounter, Document2Word}
import org.tutorial.storm.spout.SocketServerSpout

object Main {
  private val appConfig = AppConfigFactory.load()

  def main(args: Array[String]): Unit = {
    var topologyName = "wordcount"
    if (args.length > 0) {
      topologyName = args(0)
    }

    val config = new Config()
    config.setNumWorkers(2)
    val cluster = new LocalCluster()
    cluster.submitTopology("wordcount", config, topology(topologyName))
  }

  /** Get topology by name */
  def topology(name: String): StormTopology = name match {
    case "wordcount" => wordCountTopology
    case _ => throw new IllegalArgumentException(s"unknown topology: $name")
  }

  /** Simple word count topology */
  def wordCountTopology: StormTopology = {
    val builder = new TopologyBuilder
    builder.setSpout("SocketServerSpout", new SocketServerSpout(appConfig))
    builder.setBolt("Document2Word", new Document2Word)
      .shuffleGrouping("SocketServerSpout")
    builder.setBolt("WordCounter", new WordCounter)
      .fieldsGrouping("Document2Word", new Fields("word"))
    builder.setBolt("Printer", new Printer)
      .shuffleGrouping("WordCounter")
      .setNumTasks(1)
    builder.createTopology()
  }
}
