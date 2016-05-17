package io.mediachain.client


/**
  * Helper to perform blocking IO on a separate thread pool from non-blocking
  * or CPU-bound futures.
  *
  * see https://github.com/alexandru/scala-best-practices/blob/master/sections/4-concurrency-parallelism.md#46-should-use-a-separate-thread-pool-for-blocking-io
  */
object BlockingIO {
  import java.util.concurrent.atomic.AtomicLong
  import java.util.concurrent.{Executors, ThreadFactory}

  import org.slf4j.LoggerFactory

  import scala.concurrent.{Future, Promise, blocking}
  import scala.util.control.NonFatal

  private val logger = LoggerFactory.getLogger(BlockingIO.getClass)

  private val ioThreadPool = Executors.newCachedThreadPool(
    new ThreadFactory {
      private val counter = new AtomicLong(0L)

      def newThread(r: Runnable) = {
        val th = new Thread(r)
        th.setName("io-thread-" +
          counter.getAndIncrement.toString)
        th.setDaemon(true)
        th
      }
    })


  def execute[T](cb: => T): Future[T] = {
    val p = Promise[T]()

    ioThreadPool.execute(new Runnable {
      def run() = try {
        p.success(blocking(cb))
      }
      catch {
        case NonFatal(ex) =>
          logger.error(s"Uncaught I/O exception", ex)
          p.failure(ex)
      }
    })

    p.future
  }

}
