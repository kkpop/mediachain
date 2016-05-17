package io.mediachain.client

import io.mediachain.copycat.Client.ClientStateListener
import io.mediachain.protocol.Datastore.Datastore
import io.mediachain.protocol.Transactor.JournalListener

import scala.concurrent.{ExecutionContext, Future}


class MediachainCopycatClient(datastore: Datastore)
  (implicit executionContext: ExecutionContext = ExecutionContext.global)
  extends MediachainClient with JournalListener with ClientStateListener
{
  import cats.data.{Streaming, XorT}
  import io.mediachain.copycat
  import io.mediachain.copycat.Client.ClientState
  import io.mediachain.protocol.Datastore._
  import io.mediachain.protocol.MediachainError


  // TODO: lazy streaming implementation
  def allCanonicalReferences = Streaming.fromIterable(canonicalRefs)

  def chainForCanonical(ref: Reference): Future[Option[Reference]] = {
    // TODO: handle disconnected cluster state
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
  }

  /**
    * Close the connection to the transactor cluster
    */
  def close() = cluster.close()


  var listeners: Set[ClientEventListener] = Set()
  var canonicalRefs: Set[Reference] = Set()
  var clusterClientState: ClientState = ClientState.Disconnected

  val cluster = copycat.Client.build()
  cluster.listen(this)
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
    // Note that we don't set the `latestBlockIndex` to the index of the
    // current block, since the current block is still in the process of
    // being constructed by the transactor cluster.
    cluster.currentBlock.map { block =>
      block.entries.foreach(handleJournalEntry)
      block.chain.foreach(retrieveBlock)
    }
  }

  /**
    * Retrieve a block from the datastore and process it
    *
    * @param blockRef reference to a `JournalBlock`
    */
  private def retrieveBlock(blockRef: Reference): Unit = {
    val block = datastore.getAs[JournalBlock](blockRef)
      .getOrElse(
        // TODO: better failure handling
        throw new RuntimeException(s"Unable to fetch block with ref: $blockRef")
      )
    handleBlock(block)
  }

  private var latestBlockIndex: BigInt = -1

  /**
    * Handle each journal entry in the given `block`. If it's newer than
    * the most recently seen block, follow its `chain` backwards until
    * we're caught up.
    *
    * @param block a `JournalBlock` to handle
    */
  private def handleBlock(block: JournalBlock): Unit = {
    block.entries.foreach(handleJournalEntry)

    if (block.index > latestBlockIndex) {
      block.chain.foreach(retrieveBlock)
      latestBlockIndex = block.index
    }
  }

  private def handleJournalEntry(entry: JournalEntry): Unit = {
    canonicalRefs += entry.ref
  }


  //
  // JournalListener interface
  //

  override def onJournalCommit(entry: JournalEntry): Unit =
    handleJournalEntry(entry)

  override def onJournalBlock(ref: Reference): Unit = {
    // TODO: handle failure when fetching block from datastore
    datastore.getAs[JournalBlock](ref).foreach(handleBlock)
  }
}
