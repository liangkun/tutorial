/**
 * Copyright (c) 2015 Liang Kun. All Rights Reserved.
 * Authors: Liang Kun <liangkun@data-intelli.com>
 */
package org.tutorial.storm.bolt

import scala.util.Random

import backtype.storm.topology.{OutputFieldsDeclarer, BasicOutputCollector}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.{Values, Fields, Tuple}

class RandomPi extends BaseBasicBolt {
  /** @see IBasicBolt */
  override def execute(tuple: Tuple, collector: BasicOutputCollector): Unit = {
    val id = tuple.getValue(0)
    val count = tuple.getString(1).toInt
    var hit = 0
    for (i <- 1 to count) {
      val x = Random.nextDouble()
      val y = Random.nextDouble()
      if (x * x + y * y <= 1) {
        hit = hit + 1
      }
    }

    collector.emit(new Values(id, int2Integer(count), int2Integer(hit)))
  }

  /** @see IComponent */
  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("id", "count", "hit"))
  }
}
