package hu.sztaki.ilab.ps.common

import scala.collection.mutable.ArrayBuffer

trait Combinable[W] {
  def sendCondition(): Boolean

  def logic(adder: (ArrayBuffer[W]) => Unit,
            callback: (Array[W] => Unit) => Unit,
            collectAnswerMsg: Array[W] => Unit): Unit

  //  State logic
  private var send: Boolean = false

  def shouldSend(): Boolean = send

  def send(callback: (Array[W] => Unit) => Unit,
           collect: (Array[W] => Unit)): Unit = {
    send = true
    callback(collect)
  }

  def reset() = {
    send = false
  }

}
