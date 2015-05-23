/**
 * Copyright (c) 2015 Liang Kun. All Rights Reserved.
 * Authors: Liang Kun <liangkun@data-intelli.com>
 */

package org.tutorial.storm.bolt

import java.util.{Map => JMap}
import scala.collection.JavaConverters._

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.{OutputFieldsDeclarer, IRichBolt}
import backtype.storm.tuple.Tuple

class Printer extends IRichBolt  {
  private var collector: OutputCollector = null

  /** @see IBolt */
  override def prepare(config: JMap[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    this.collector = collector
  }

  /** @see IBolt */
  override def execute(tuple: Tuple): Unit = {
    println(tuple.getValues.asScala.mkString("\t"))
    collector.ack(tuple)
  }

  /** @see IBolt */
  override def cleanup(): Unit = {}

  /** @see IComponent */
  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {}

  /** @see IComponent */
  override def getComponentConfiguration: JMap[String, AnyRef] = null
}
