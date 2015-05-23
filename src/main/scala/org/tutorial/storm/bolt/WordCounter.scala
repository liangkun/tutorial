/**
 * Copyright (c) 2015 Liang Kun. All Rights Reserved.
 * Authors: Liang Kun <liangkun@data-intelli.com>
 */

package org.tutorial.storm.bolt

import scala.collection.mutable

import backtype.storm.topology.{OutputFieldsDeclarer, BasicOutputCollector}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.{Values, Fields, Tuple}

class WordCounter extends BaseBasicBolt {
  private val result = mutable.HashMap[String, Long]()

  /** @see IBasicBolt */
  override def execute(tuple: Tuple, collector: BasicOutputCollector): Unit = {
    val word = tuple.getString(0)
    val count = result.getOrElse(word, 0L) + 1
    result += word -> count
    collector.emit(new Values(word, long2Long(count)))
  }

  /** @see IComponent */
  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("word", "count"))
  }
}
