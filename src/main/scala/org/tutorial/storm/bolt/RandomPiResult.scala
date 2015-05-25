/**
 * Copyright (c) 2015 Liang Kun. All Rights Reserved.
 * Authors: Liang Kun <liangkun@data-intelli.com>
 */
package org.tutorial.storm.bolt

import java.util.{Map => JMap}

import backtype.storm.coordination.BatchOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseBatchBolt
import backtype.storm.tuple.{Values, Fields, Tuple}

class RandomPiResult extends BaseBatchBolt[AnyRef] {
  private var collector: BatchOutputCollector = null
  private var id: AnyRef = null
  private var totalCount = 0.0
  private var totalHit = 0.0

  /** @see IBatchBolt */
  override def prepare(conf: JMap[_, _], context: TopologyContext, collector: BatchOutputCollector, id: AnyRef): Unit = {
    this.collector = collector
    this.id = id
  }

  /** @see IBatchBolt */
  override def execute(tuple: Tuple): Unit = {
    totalCount += tuple.getInteger(1)
    totalHit += tuple.getInteger(2)
  }

  /** @see IBatchBolt */
  override def finishBatch(): Unit = {
    collector.emit(new Values(id, double2Double(totalHit / totalCount * 4)))
  }

  /** @see IComponent */
  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("id", "result"))
  }
}
