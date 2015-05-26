/**
 * Copyright (c) 2015 Liang Kun. All Rights Reserved.
 * Authors: Liang Kun <liangkun@data-intelli.com>
 */
package org.tutorial.storm.spout

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.{Map => JMap}

import scala.collection.mutable
import scala.util.Random

import akka.actor.{ActorSystem, Props, PoisonPill, ActorLogging, Actor}
import akka.io.{Tcp, IO}
import akka.io.Tcp.{ConnectionClosed, Bind, Register, Connected, Received, CommandFailed, Bound}
import backtype.storm.Config
import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.{Values, Fields}
import backtype.storm.utils.Utils
import com.typesafe.config.{Config => AppConfig}
import org.slf4j.LoggerFactory

class SocketServerSpout(appConfig: AppConfig) extends BaseRichSpout {
  import SocketServerSpout._

  val logger = LoggerFactory.getLogger(this.getClass)
  val host = appConfig.getString("org.tutorial.storm.spout.socket.host")
  val port = appConfig.getInt("org.tutorial.storm.spout.socket.port")
  private var actorSystem: ActorSystem = null
  private var queue: MessageQueue = null
  private var collector: SpoutOutputCollector = null
  private val messages = mutable.HashMap[String, String]()

  /** @see ISpout */
  override def open(config: JMap[_, _], topologyContext: TopologyContext, collector: SpoutOutputCollector): Unit = {
    actorSystem = ActorSystem("SocketServerSpout")
    queue = new MessageQueue()
    this.collector = collector
    actorSystem.actorOf(Props(classOf[SocketServer], host, port, queue))
  }

  /** @see ISpout */
  override def nextTuple(): Unit = {
    val document = queue.poll()
    if (document == null) {
      Utils.sleep(50)
    } else {
      val id = nextId()
      collector.emit(new Values(document.toLowerCase), id)
      if (messages.put(id, document).nonEmpty) {
        logger.warn("message id collision: " + id)
      }
    }
  }

  /** @see IComponent */
  override def getComponentConfiguration: JMap[String, AnyRef] = {
    val config = new Config()
    config.setMaxTaskParallelism(1)
    config
  }

  /** @see IComponent */
  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("document"))
  }

  /** @see ISpout */
  override def close(): Unit = {
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }

  /** @see ISpout */
  override def ack(id: AnyRef): Unit = {
    if (messages.remove(id.asInstanceOf[String]).isEmpty) {
      logger.warn("acking unexists message id " + id)
    }
  }

  /** @see ISpout */
  override def fail(id: AnyRef): Unit = {
    val document = messages.remove(id.asInstanceOf[String])
    if (document.isEmpty) {
      logger.warn("fail unexists message id " + id)
    } else {
      queue.push(document.get)
    }
  }

  /** Generate next message id */
  private def nextId(): String = Random.nextString(32)
}

object SocketServerSpout {
  type MessageQueue = ConcurrentLinkedDeque[String]
  class SocketServer(host: String, port: Int, queue: MessageQueue) extends Actor with ActorLogging {
    import context.system

    IO(Tcp) ! Bind(self, new InetSocketAddress(host, port))

    /** @see Actor */
    override def receive = {
      case Bound(localAddress) =>
        println(s"SocketServerSpout listening on: $localAddress")

      case CommandFailed(cmd) =>
        log.error(s"Failed to open socket: ${cmd.failureMessage}")
        self ! PoisonPill

      case Connected(_, _) =>
        val handler = context.actorOf(Props(classOf[MessageHandler], queue))
        sender() ! Register(handler)

      case x =>
        log.warning(s"unknown message $x")
    }
  }

  class MessageHandler(queue: MessageQueue) extends Actor with ActorLogging {
    /** @see Actor */
    override def receive = {
      case Received(data) =>
        val document = new String(data.toArray)
        queue.add(document)

      case c: ConnectionClosed =>
        log.info("connection closed")
        self ! PoisonPill

      case x =>
        log.warning(s"unknown message: $x")
    }
  }
}
