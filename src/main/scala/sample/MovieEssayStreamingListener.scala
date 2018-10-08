package sample

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted, StreamingListenerOutputOperationCompleted, StreamingListenerReceiverStopped}

/**
  * feng
  * 18-10-3
  */
class MovieEssayStreamingListener(scc:StreamingContext) extends StreamingListener with Logging {

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {
    logError("---------------onReceiverStopped-----------------")
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    logError("---------------onBatchCompleted-----------------")
  }

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
    logError("---------------onOutputOperationCompleted-----------------")
  }
}
