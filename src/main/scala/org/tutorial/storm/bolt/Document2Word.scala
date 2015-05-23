/**
 * Copyright (c) 2015 Liang Kun. All Rights Reserved.
 * Authors: Liang Kun <liangkun@data-intelli.com>
 */

package org.tutorial.storm.bolt

import java.util.{Map => JMap}

import backtype.storm.task.{TopologyContext, OutputCollector}
import backtype.storm.topology.{IRichBolt, OutputFieldsDeclarer}
import backtype.storm.tuple.{Values, Tuple, Fields}

class Document2Word extends IRichBolt {
  private var collector: OutputCollector = null

  /** @see IBolt */
  override def prepare(config: JMap[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    this.collector = collector
  }

  /** @see IBolt */
  override def execute(tuple: Tuple): Unit = {
    val document = tuple.getString(0)
    for (word <- document.split("\\W+")) {
      collector.emit(tuple, new Values(word))
      collector.ack(tuple)
    }
  }

  /** @see IBolt */
  override def cleanup(): Unit = {}

  /** @see IComponent */
  override def getComponentConfiguration: JMap[String, AnyRef] = null

  /** @see IComponent */
  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("word"))
  }
}
