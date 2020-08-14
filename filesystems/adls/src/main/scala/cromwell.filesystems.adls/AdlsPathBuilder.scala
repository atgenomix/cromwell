/**
  * Copyright (C) 2020, Atgenomix Incorporated. All Rights Reserved.
  * This program is an unpublished copyrighted work which is proprietary to
  * Atgenomix Incorporated and contains confidential information that is not to
  * be reproduced or disclosed to any other person or entity without prior
  * written consent from Atgenomix, Inc. in each and every instance.
  *
  * warning Unauthorized reproduction of this program as well as unauthorized
  * preparation of derivative works based upon the program or distribution of
  * copies by sale, rental, lease or lending are violations of federal copyright
  * laws and state trade secret laws, punishable by civil and criminal penalties.
  */
package cromwell.filesystems.adls

import java.io.ByteArrayInputStream
import java.net.URI

import better.files.File.OpenOptions
import com.azure.storage.file.datalake.{DataLakeFileClient, DataLakeServiceClient}
import com.google.common.net.UrlEscapers
import cromwell.cloudsupport.azure.adls.AdlsStorage
import cromwell.cloudsupport.azure.auth.AzureAuthMode
import cromwell.core.WorkflowOptions
import cromwell.core.path.{NioPath, Path, PathBuilder}
import cromwell.filesystems.adls.AdlsPathBuilder.{InvalidAdlsPath, PossiblyValidRelativeAdlsPath, ValidFullAdlsPath, _}

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Codec
import scala.language.postfixOps
import scala.util.{Failure, Try}

object AdlsPathBuilder {

  // Provides some level of validation of bucket names
  // This is meant to alert the user early if they mistyped a path in their workflow / inputs and not to validate
  // exact file system syntax.
  // See https://docs.aws.amazon.com/AmazonAdls/latest/dev/BucketRestrictions.html
  val AbfsFileSystemPattern =
    """
      (?x)                                      # Turn on comments and whitespace insensitivity
      ^abfs://
      (                                         # Begin capturing group for bucket name
        [a-z0-9][a-z0-9-_\.]+[a-z0-9]           # Regex for bucket name - soft validation, see comment above
      )                                         # End capturing group for bucket name
      (?:
        /.*                                     # No validation here
      )?
    """.trim.r

  sealed trait AdlsPathValidation
  case class ValidFullAdlsPath(bucket: String, path: String) extends AdlsPathValidation
  case object PossiblyValidRelativeAdlsPath extends AdlsPathValidation
  sealed trait InvalidAdlsPath extends AdlsPathValidation {
    def pathString: String
    def errorMessage: String
  }
  final case class InvalidScheme(pathString: String) extends InvalidAdlsPath {
    override def errorMessage: String = s"Azure Data Lake Storage Gen2 URIs must have 'abfs' scheme: $pathString"
  }
  final case class InvalidFullAdlsPath(pathString: String) extends InvalidAdlsPath {
    override def errorMessage: String = {
      s"""
         |The path '$pathString' does not seem to be a valid Azure Data Lake Storage Gen2 path.
         |Please check that it starts with abfs:// and that the file system(container) and object follow Adls naming guidelines at
         |https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
      """.stripMargin.replace("\n", " ").trim
    }
  }
  final case class UnparseableAdlsPath(pathString: String, throwable: Throwable) extends InvalidAdlsPath {
    override def errorMessage: String =
      List(s"The specified Azure Data Lake Storage Gen2 path '$pathString' does not parse as a URI.", throwable.getMessage).mkString("\n")
  }

  // Tries to extract a bucket name out of the provided string
  private def softBucketParsing(string: String): Option[String] = string match {
    case AbfsFileSystemPattern(bucket) => Option(bucket)
    case _ => None
  }

  def pathToUri(string: String): URI =
    URI.create(UrlEscapers.urlFragmentEscaper.escape(string))

  def validateAdlsPath(string: String): AdlsPathValidation = {
    Try {
      val uri = pathToUri(string)
      if (uri.getScheme == null) { PossiblyValidRelativeAdlsPath }
      else if (uri.getScheme.equalsIgnoreCase("abfs")) {
        if (uri.getHost == null) {
          softBucketParsing(string) map { ValidFullAdlsPath(_, uri.getPath) } getOrElse InvalidFullAdlsPath(string)
        } else { ValidFullAdlsPath(uri.getHost, uri.getPath) }
      } else { InvalidScheme(string) }
    } recover { case t => UnparseableAdlsPath(string, t) } get
  }

  def fromAuthMode(authMode: AzureAuthMode,
                   options: WorkflowOptions,
                   storageAccount: String)(implicit ec: ExecutionContext): Future[AdlsPathBuilder] = {
    val client = AdlsStorage.adlsClient(authMode.credential(), storageAccount)
    Future(new AdlsPathBuilder(client, storageAccount))
  }
}

class AdlsPathBuilder(client: DataLakeServiceClient, storageAccount: String) extends PathBuilder {
  // Tries to create a new AdlsPath from a String representing an absolute adls path: abfs://<file system>/<account name>.dfs.core.windows.net[/<path>].
  def build(string: String): Try[AdlsPath] = {
    validateAdlsPath(string) match {
      case ValidFullAdlsPath(fileSystem, uri) =>
        Try {
          val path = uri.stripPrefix(s"@$storageAccount.dfs.core.windows.net/")
          val fileClient = client.createFileSystem(fileSystem)
            .createFile(path, true)
          AdlsPath(java.nio.file.Paths.get(path), storageAccount, fileSystem, fileClient)
        }
      case PossiblyValidRelativeAdlsPath => Failure(new IllegalArgumentException(s"$string does not have a Azure Data Lake Storage Gen2 scheme"))
      case invalid: InvalidAdlsPath => Failure(new IllegalArgumentException(invalid.errorMessage))
    }
  }

  override def name: String = "Azure Data Lake Storage Gen2"
}

case class AdlsPath private[adls](nioPath: NioPath,
                                  storageAccount: String,
                                  fs: String,
                                  client: DataLakeFileClient
                                 ) extends Path {
  override protected def newPath(nioPath: NioPath): AdlsPath = AdlsPath(nioPath, storageAccount, fs, client)

  override def pathAsString: String = s"abfs://$pathWithoutScheme"

  override def pathWithoutScheme: String = s"$fs@$storageAccount.dfs.core.windows.net/$nioPath"

  override def writeContent(content: String)(openOptions: OpenOptions, codec: Codec, compressPayload: Boolean)(implicit ec: ExecutionContext) = {
    val inputStream = new ByteArrayInputStream(content.getBytes)
    val fileSize = content.length
    client.append(inputStream, 0, fileSize)
    client.flush(fileSize)
    this
  }
}
