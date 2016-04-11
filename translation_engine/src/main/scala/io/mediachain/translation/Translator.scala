package io.mediachain.translation

import java.io.File

import scala.io.Source
import cats.data.Xor
import io.mediachain.Types._
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph
import org.json4s._
import io.mediachain.core.{Error, TranslationError}
import io.mediachain.Ingress
import io.mediachain.translation.JsonLoader.parseJArray
import org.json4s.jackson.Serialization.write
import com.fasterxml.jackson.core.JsonFactory
import io.mediachain.signatures.{PEMFileUtil, Signatory}
import io.mediachain.util.orient.MigrationHelper

trait Implicit {
  implicit val factory = new JsonFactory
}
object `package` extends Implicit


trait Translator {
  val name: String
  val version: Int
  def translate(source: JObject): Xor[TranslationError, ImageBlob]
}

trait FSLoader[T <: Translator] {
  val translator: T

  val pairI: Iterator[Xor[TranslationError, (JObject, String)]]
  val path: String

  def signedBlobs(
    signatory: Signatory,
    imageBlob: ImageBlob,
    rawBlob: RawMetadataBlob): (ImageBlob, RawMetadataBlob) = {

    val author = imageBlob.author.map(
      _.withSignature(signatory))

    val signedImageBlob = imageBlob.copy(author = author)
      .withSignature(signatory)

    val signedRawBlob = rawBlob.withSignature(signatory)

    (signedImageBlob, signedRawBlob)
  }

  def loadImageBlobs(signatory: Option[Signatory] = None)
  : Iterator[Xor[TranslationError,(ImageBlob, RawMetadataBlob)]] = {
    pairI.map { pairXor =>
      pairXor.flatMap { case (json, raw) =>
        translator.translate(json).map { imageBlob: ImageBlob =>
          val rawBlob = RawMetadataBlob (None, raw)

          signatory
            .map(signedBlobs(_, imageBlob, rawBlob))
            .getOrElse((imageBlob, rawBlob))
        }
      }
    }
  }
}

trait DirectoryWalkerLoader[T <: Translator] extends FSLoader[T] {
  val fileI: Iterator[File] = DirectoryWalker.findWithExtension(new File(path), ".json")

  val (jsonI, rawI) = {
    val (left, right) = fileI.duplicate
    val jsonI = {
      left.map { file =>
        val obj = for {
          parser <- JsonLoader.createParser(file)
          obj <- JsonLoader.parseJOBject(parser)
        } yield obj

        obj.leftMap(err =>
          TranslationError.ParsingFailed(new RuntimeException(err + " at " + file.toString)))
      }
    }
    val rawI = right.map(Source.fromFile(_).mkString)

    (jsonI, rawI)
  }

  val pairI = jsonI.zip(rawI).map {
    case (jsonXor, raw) => jsonXor.map((_,raw))
    case _ => throw new RuntimeException("Should never get here")
  }
}

trait FlatFileLoader[T <: Translator] extends FSLoader[T] {
  val pairI = {
    implicit val formats = org.json4s.DefaultFormats

    JsonLoader.createParser(new File(path)) match {
      case err@Xor.Left(_) => Iterator(err)
      case Xor.Right(parser) => {
        parseJArray(parser).map {
          case Xor.Right(json: JObject) => Xor.right((json, write(json)))
          case err@(Xor.Left(_) | Xor.Right(_)) => Xor.left(TranslationError.ParsingFailed(new RuntimeException(err.toString)))
        }
      }
    }
  }
}

object TranslatorDispatcher {
  // TODO: move + inject me
  def getGraph: OrientGraph =
    MigrationHelper.getMigratedGraph().get

  def dispatch(partner: String, path: String, signingIdentity: String, privateKeyPath: String) = {
    val translator = partner match {
      case "moma" => new moma.MomaLoader(path)
      case "tate" => new tate.TateLoader(path)
    }

    val privateKeyXor = PEMFileUtil.privateKeyFromFile(privateKeyPath)

    privateKeyXor match {
      case Xor.Left(err) =>
        println(s"Unable to load private key for $signingIdentity from $privateKeyPath: " +
          err + "\nStatements will not be signed.")
      case _ => ()
    }

    val signatory: Option[Signatory] = privateKeyXor
      .toOption
      .map(Signatory(signingIdentity, _))

    val blobI: Iterator[Xor[TranslationError, (ImageBlob, RawMetadataBlob)]] =
      translator.loadImageBlobs(signatory)

    val graph = getGraph

    val results: Iterator[Xor[Error, Canonical]] = blobI.map { pairXor =>
      pairXor.flatMap { case (blob: ImageBlob, raw: RawMetadataBlob) =>
        Ingress.addImageBlob(graph, blob, Some(raw))
      }
    }
    val errors: Iterator[Error] = results.collect { case Xor.Left(err) => err }
    val canonicals: Iterator[Canonical] = results.collect { case Xor.Right(c) => c }

    println(s"Import finished: ${canonicals.length} canonicals imported ${errors.length} errors reported (see below)")
    println(errors)
  }
}
