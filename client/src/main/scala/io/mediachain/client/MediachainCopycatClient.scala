package io.mediachain.client


import java.util.concurrent.Executors

import io.mediachain.copycat.Client.ClientStateListener
import io.mediachain.protocol.Datastore.Datastore
import io.mediachain.protocol.Transactor.JournalListener

import scala.concurrent.ExecutionContext


class MediachainCopycatClient(datastore: Datastore)
  (implicit executionContext: ExecutionContext = ExecutionContext.global)
  extends MediachainClient with JournalListener with ClientStateListener
{
  import java.util.concurrent.LinkedBlockingQueue

  import cats.data.{Streaming, XorT}
  import io.mediachain.copycat
  import io.mediachain.copycat.Client.ClientState
  import io.mediachain.protocol.Datastore._
  import io.mediachain.protocol.MediachainError

  import collection.JavaConverters._
  import scala.collection.concurrent.{Map => ConcurrentMap}
  import scala.concurrent.Future


  def allCanonicalReferences =  {
    // FIXME - this should pull from the set of known blocks and
    // return references from their CanonicalEntries.

    Streaming.empty
  }

  def chainForCanonical(ref: Reference): Future[Option[Reference]] = {
    cluster.lookup(ref)
  }

  def addListener(listener: ClientEventListener): Unit = {
    listeners += listener
  }


  /**
    * Add a new CanonicalRecord to the system
    *
    * @param canonicalRecord a new Entity or Artefact to add to the system
    * @return an Xor-wrapped future that will complete with either a ClientError or a
    *         Reference to the new record
    */
  def addCanonical(canonicalRecord: CanonicalRecord) =
    XorT[Future, MediachainError, Reference] {
      cluster.insert(canonicalRecord)
        .map(_.bimap(
          err => MediachainError.Journal(err),
          entry => entry.ref
        ))
    }

  /**
    * Update an existing CanonicalRecord
    *
    * @param canonicalReference a Reference to the canonical to update
    * @param chainCell          a ChainCell containing the new metadata for the record.
    * @return an Xor-wrapped future that will complete with either a ClientError
    *         or a Reference to the new head of the record's chain
    */
  def updateCanonical(canonicalReference: Reference, chainCell: ChainCell) =
    XorT[Future, MediachainError, Reference] {
      cluster.update(canonicalReference, chainCell)
        .map(_.bimap(
          err => MediachainError.Journal(err),
          entry => entry.chain
        ))
    }

  /**
    * Connect to a member of the transactor cluster
    *
    * @param address address to connect to
    */
  def connect(address: String) = {
    cluster.connect(address)
    cluster.listen(this)
    catchupWorkerExecutor.submit(new BlockCatchupWorker(catchupBlockReferenceQueue))
  }

  /**
    * Close the connection to the transactor cluster
    */
  def close() = cluster.close()


  var listeners: Set[ClientEventListener] = Set()
  val knownBlocks: ConcurrentMap[BigInt, Reference] =
    new java.util.concurrent.ConcurrentHashMap[BigInt, Reference]().asScala

  @volatile var clusterClientState: ClientState = ClientState.Disconnected


  // when following the chain backwards, block references are placed on this
  // queue.  A worker thread pulls the blocks from the datastore.  If we've not
  // previously seen this block, it adds the block to the `knownBlocks` map,
  // then checks its `chain` reference.  If the `chain` reference is defined,
  // it adds that reference to the queue, and the process repeats until
  // we hit a block we've already processed, or a block with a nil `chain`
  // reference (the genesis block)
  val catchupBlockReferenceQueue: LinkedBlockingQueue[Reference] = new LinkedBlockingQueue()


  class BlockCatchupWorker(refQueue: LinkedBlockingQueue[Reference]) extends Runnable {
    override def run(): Unit = {
      while (MediachainCopycatClient.this.clusterClientState == ClientState.Connected) {
        val ref = refQueue.take()

        val block = datastore.getAs[JournalBlock](ref)
          .getOrElse(
            throw new RuntimeException(s"Unable to retrieve block with ref $ref")
          )

        knownBlocks.synchronized {
          if (!knownBlocks.contains(block.index)) {

            knownBlocks.put(block.index, ref)
            block.chain.foreach(refQueue.put)
          }
        }
      }
    }
  }


  val catchupWorkerExecutor = Executors.newSingleThreadExecutor()
  val cluster = copycat.Client.build()
  cluster.addStateListener(this)

  /**
    * Respond to state change events from the transactor cluster
    *
    * @param state
    */
  def onStateChange(state: ClientState): Unit = {
    import ClientState._
    clusterClientState = state

    state match {
      case Connected => catchupJournal()
      case _ => ()
    }
  }

  /**
    * Request the current block from the transactor cluster, and fetch
    * any missing blocks from the blockchain.
    */
  def catchupJournal(): Unit = {
    cluster.currentBlock.map { block =>
      block.chain.foreach { prevBlockRef =>
        catchupBlockReferenceQueue.put(prevBlockRef)
      }
    }
  }




  private def handleJournalEntry(entry: JournalEntry): Unit = {
  }


  //
  // JournalListener interface
  //

  override def onJournalCommit(entry: JournalEntry): Unit =
    handleJournalEntry(entry)

  override def onJournalBlock(ref: Reference): Unit = {
    Future {
      catchupBlockReferenceQueue.put(ref)
    }
  }
}
