package io.mediachain.copycat

import io.atomix.copycat.server.CopycatServer
import io.atomix.catalyst.transport.Address
import io.mediachain.protocol.Transactor.JournalClient

case class DummyContext(
  server: CopycatServer, 
  client: JournalClient,
  store: Dummies.DummyStore,
  logdir: String
)

object DummyContext {
  def setup(address: String, blocksize: Int = StateMachine.JournalBlockSize) = {
    println("*** SETUP DUMMY COPYCAT CONTEXT")
    val logdir = setupLogdir()
    val store = new Dummies.DummyStore
    val server = Server.build(address, logdir, store, None, blocksize)
    server.bootstrap().join()
    val client = Client.build()
    client.connect(address)
    DummyContext(server, client, store, logdir)
  }
  
  def setupLogdir() = {
    import sys.process._
    val logdir = "mktemp -d".!!.trim
    (s"mkdir -p $logdir").!
    logdir
  }
  
  def cleanupLogdir(logdir: String) {
    import sys.process._
    (s"rm -rf $logdir").!
  }
  
  def shutdown(context: DummyContext) {
    println("*** SHUT DOWN DUMMY COPYCAT CONTEXT")
    context.client.close()
    context.server.shutdown().join()
    cleanupLogdir(context.logdir)
  }
}

case class DummyClusterContext(
  dummies: Array[DummyContext]
)

object DummyClusterContext {
  def setup(address1: String, address2: String, address3: String,
            blocksize: Int = StateMachine.JournalBlockSize) = {
    println("*** SETUP DUMMY COPYCAT CLUSTER")
    val dummies = Array(address1, address2, address3)
      .map { address =>
        val store = new Dummies.DummyStore
        val logdir = DummyContext.setupLogdir()
        val server = Server.build(address, logdir, store, None, blocksize)
        val client = Client.build()
        DummyContext(server, client, store, logdir)
    }
    
    // start cluster
    dummies(0).server.bootstrap().join()
    dummies(1).server.join(new Address(address1)).join()
    dummies(2).server.join(new Address(address1), new Address(address2)).join()
    // connect clients
    dummies(0).client.connect(address1)
    dummies(1).client.connect(address2)
    dummies(2).client.connect(address3)
    
    DummyClusterContext(dummies)
  }
  
  def shutdown(context: DummyClusterContext) {
    println("*** SHUTDOWN DUMMY COPYCAT CLUSTER")
    // close all clients before shutting down any servers
    context.dummies.foreach(_.client.close())
    context.dummies.foreach(_.server.shutdown().join())
    context.dummies.foreach(dummy => DummyContext.cleanupLogdir(dummy.logdir))
  }
}
