package io.mediachain.signatures

import java.nio.charset.StandardCharsets
import java.security._
import java.security.cert.{CertificateException, X509Certificate}

import cats.data.Xor
import io.mediachain.Types.Signable
import io.mediachain.core.SignatureError
import io.mediachain.core.SignatureError.{InvalidCertificate, InvalidSignature, SignatureNotFound}
import io.mediachain.core.TranslationError.InvalidFormat
import io.mediachain.util.{CborSerializer, JsonUtils}
import org.apache.commons.codec.binary.Hex
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.json4s._

object Signer {
  Security.addProvider(new BouncyCastleProvider)
  val SIGNING_ALGORITHM = "SHA512withRSA"
  val CRYPTO_PROVIDER = BouncyCastleProvider.PROVIDER_NAME

  private def makeSigner(): Signature =
    Signature.getInstance(SIGNING_ALGORITHM, CRYPTO_PROVIDER)


  /// SIGN

  def signBytes(bytes: Array[Byte], signingKey: PrivateKey): String = {
    val signer = makeSigner()
    signer.initSign(signingKey)
    signer.update(bytes)
    Hex.encodeHexString(signer.sign())
  }


  def signText(text: String, signingKey: PrivateKey): String =
    signBytes(text.getBytes(StandardCharsets.UTF_8), signingKey)


  def signatureForSignable[S <: Signable](s: S, signingKey: PrivateKey)
  : String = {
    val bytes = CborSerializer.bytesForSignable(s)
    signBytes(bytes, signingKey)
  }

  // TODO: these names are kind of unwieldy... find better ones?
  def signCborRepresentationOfJsonValue(json: JValue, signingKey: PrivateKey)
  : String = {
    val canonicalCbor = CborSerializer.bytesForJsonValue(json)
    signBytes(canonicalCbor, signingKey)
  }


  def signCborRepresentationOfJsonText(jsonString: String, signingKey: PrivateKey)
  :Xor[InvalidFormat, String] =
    JsonUtils.parseJsonString(jsonString)
      .map(parsed => signCborRepresentationOfJsonValue(parsed, signingKey))


  /// VERIFY

  def verifySignedBytes(bytes: Array[Byte], signature: String, publicKey: PublicKey)
  : Boolean = {
    val signer = makeSigner()
    signer.initVerify(publicKey)
    signer.update(bytes)
    val sigBytes = Hex.decodeHex(signature.toCharArray)
    signer.verify(sigBytes)
  }

  def verifySignedText(text: String, signature: String, publicKey: PublicKey)
  : Boolean =
    verifySignedBytes(
      text.getBytes(StandardCharsets.UTF_8),
      signature,
      publicKey
    )


  def verifySignedJsonText(jsonText: String, signature: String, publicKey: PublicKey)
  : Xor[InvalidFormat, Boolean] = {
    val canonicalBytes = CborSerializer.bytesForJsonText(jsonText)
    canonicalBytes.map(verifySignedBytes(_, signature, publicKey))
  }

  def verifySignedJsonObject(json: JValue, signature: String, publicKey: PublicKey)
  : Boolean = {
    val canonicalBytes = CborSerializer.bytesForJsonValue(json)
    verifySignedBytes(canonicalBytes, signature, publicKey)
  }

  def verifySignedSignable[S <: Signable]
  (signable: S, signature: String, publicKey: PublicKey)
  : Boolean = verifySignedBytes(
    CborSerializer.bytesForSignable(signable),
    signature,
    publicKey
  )

  def validateCertificate(cert: X509Certificate): Xor[SignatureError, Unit] =
    Xor.catchOnly[CertificateException] {
      cert.checkValidity()
    }.leftMap(InvalidCertificate)


  def validateSignableWithCertificate[S <: Signable]
  (signable: S, cert: X509Certificate)
  : Xor[SignatureError, Boolean] = {
    for {
      _ <- validateCertificate(cert)

      name <- CertificateUtil.commonName(cert)

      signature <- Xor.fromOption(signable.signatures.get(name),
        SignatureNotFound(s"No signature by $name exists."))

      valid <- if (verifySignedSignable(signable, signature, cert.getPublicKey)) {
        Xor.right(true)
      } else {
        Xor.left(InvalidSignature(
          s"The signature by $name is not valid for the object $signable"))
      }
    } yield valid
  }


  def validateSignableWithCertificateStore[S <: Signable]
  (signable: S, certificateStore: CertificateStore)
  : Xor[SignatureError, Boolean] = {
    val certs: Iterable[Xor[SignatureError, X509Certificate]] =
      signable.signatures.keys.map(certificateStore.certificateForCommonName)

    val results = for (certXor <- certs) yield for {
      c <- certXor
      valid <- validateSignableWithCertificate(signable, c)
    } yield valid

    results.foldLeft(Xor.right[SignatureError, Boolean](true)) {
      case (_, Xor.Left(err)) => Xor.left(err)
      case (Xor.Left(err), _) => Xor.left(err)
      case (Xor.Right(a), Xor.Right(valid)) => Xor.right(a && valid)
    }
  }
}
