package io.mediachain.protocol


sealed trait MediachainError

object MediachainError {
  import io.mediachain.protocol.Transactor.JournalError
  import io.mediachain.protocol.CborSerialization.DeserializationError

  case class Journal(journalError: JournalError) extends MediachainError
  case class Deserialization(deserializationError: DeserializationError) extends MediachainError
  case class Network(cause: Throwable) extends MediachainError
}
