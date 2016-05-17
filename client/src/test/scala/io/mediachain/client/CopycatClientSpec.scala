package io.mediachain.client

import java.nio.file.{Files, Path, Paths}

import io.atomix.copycat.server.CopycatServer
import io.mediachain.BaseSpec
import io.mediachain.protocol.Datastore._
import io.mediachain.protocol.InMemoryDatastore
import io.mediachain.util.cbor.CborAST.CInt
import org.specs2.specification.AfterAll

import scala.concurrent.Await
import scala.concurrent.duration._

object CopycatClientSpec extends BaseSpec
  with AfterAll
{

  def is = sequential ^
    s2"""
        - allows submitting canonicals $submitsCanonicals
        - catches up prior blocks $catchesUpPriorBlocks
        - accepts new blocks after catchup $acceptsNewBlocks
      """

  val blockSize = 10
  val catchupBlockCount = 10
  val timeout = 20.seconds
  lazy val context = setupContext()

  def setupContext(): SpecContext = {
    val context = SpecContext("127.0.0.1:45678", blockSize)
    println("starting copycat server")
    context.server.bootstrap().join()
    context
  }


  def afterAll: Unit = {
    context.cleanup()
  }

  def submitsCanonicals = {
    val client = new MediachainCopycatClient(context.datastore)
    client.connect(context.serverAddress)

    // submit a few blocks worth of canonicals to test catchup protocol
    val results = for (i <- 1 to blockSize * catchupBlockCount) yield {
      val artefact = Artefact(Map("foo" -> CInt(i)))
      Await.result(client.addCanonical(artefact).value, timeout)
    }

    client.close()
    results must contain(allOf(beRightXor))
  }


  def catchesUpPriorBlocks = {
    val client = new MediachainCopycatClient(context.datastore)
    client.connect(context.serverAddress)
    client.catchupJournal()

    val result = client.knownBlocks must eventually(
      haveSize(catchupBlockCount),
      retries = 10,
      sleep = 1.second
    )

    client.close()
    result
  }


  def acceptsNewBlocks = {
    val client = new MediachainCopycatClient(context.datastore)
    client.connect(context.serverAddress)
    client.catchupJournal()

    client.knownBlocks must eventually(
      haveSize(catchupBlockCount),
      retries = 10,
      sleep = 1.second
    )

    for (i <- 1 to blockSize) {
      val artefact = Artefact(Map("bar" -> CInt(i)))
      Await.result(client.addCanonical(artefact).value, timeout)
    }

    val result = client.knownBlocks must eventually(
      haveSize(catchupBlockCount + 1),
      retries = 5,
      sleep = 1.second
    )

    client.close()
    result
  }
}


case class SpecContext(
  server: CopycatServer,
  datastore: Datastore,
  serverAddress: String,
  logDir: Path
) {

  def cleanup(): Unit = {
    server.shutdown().join()

    import sys.process._
    s"rm -rf ${logDir.toString}".!
  }
}

object SpecContext {
  import io.mediachain.copycat
  def apply(serverAddress: String, blockSize: Int): SpecContext = {
    val datastore = new InMemoryDatastore
    val logDir = Files.createTempDirectory(Paths.get("/tmp"), "copycat-client-spec")
    val server = copycat.Server.build(serverAddress, logDir.toString, datastore, blocksize = blockSize)
    SpecContext(server, datastore, serverAddress, logDir)
  }
}

