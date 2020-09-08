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

import java.net.URI

import com.google.common.net.UrlEscapers
import com.typesafe.config.Config
import cromwell.cloudsupport.azure.auth.{AzureAuthMode, ClientSecretCredentialMode, SharedKeyCredentialMode}
import cromwell.core.WorkflowOptions
import cromwell.core.path.{NioPath, Path, PathBuilder}
import cromwell.filesystems.adls.AdlsPathBuilder.{InvalidAdlsPath, PossiblyValidRelativeAdlsPath, ValidFullAdlsPath, _}
import nio.{AzureGen2FileSystem, AzureGen2FileSystemProvider, AzureGen2Path}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object AdlsPathBuilder {
  val AbfsFileSystemPattern = "abfs[s]?://([0-9a-zA-Z-_]+)@([0-9a-zA-Z-_]+).dfs.core.windows.net/.+".r

  sealed trait AdlsPathValidation
  case class ValidFullAdlsPath(account: String, fileSystem: String, path: String) extends AdlsPathValidation
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

  // Tries to extract a file system name out of the provided string
  private def getAccountAndFileSystem(string: String): Option[(String, String)] = string match {
    case AbfsFileSystemPattern(fileSystem, account) => Option(account -> fileSystem)
    case _ => None
  }

  def pathToUri(string: String): URI =
    URI.create(UrlEscapers.urlFragmentEscaper.escape(string))

  def validateAdlsPath(string: String): AdlsPathValidation = {
    Try {
      val uri = pathToUri(string)
      val scheme = uri.getScheme
      if (uri.getScheme == null) {
        PossiblyValidRelativeAdlsPath
      }
      else if (scheme.equalsIgnoreCase("abfs") || scheme.equalsIgnoreCase("abfss")) {
        if (uri.getHost != null) {
          getAccountAndFileSystem(string) map {
            case (account, fs) => ValidFullAdlsPath(account, fs, uri.getPath)
          } getOrElse InvalidFullAdlsPath(string)
        } else {
          InvalidScheme(string)
        }
      } else {
        InvalidScheme(string)
      }
    } recover { case t => UnparseableAdlsPath(string, t) } get
  }

  def fromAuthMode(authMode: AzureAuthMode,
                   options: WorkflowOptions,
                   backendConfig: Config
                  )(implicit ec: ExecutionContext): Future[AdlsPathBuilder] = {
    Future(new AdlsPathBuilder(authMode, backendConfig: Config))
  }
}

class AdlsPathBuilder(authMode: AzureAuthMode, backendConfig: Config) extends PathBuilder {
  // Tries to create a new AdlsPath from a String representing an absolute adls path: abfs://<file system>/<account name>.dfs.core.windows.net[/<path>].
  def build(string: String): Try[AdlsPath] = {
    val storageAccount = authMode.accountName

    validateAdlsPath(string) match {
      case ValidFullAdlsPath(accountName, fileStoreName, path) =>
        if (accountName != storageAccount) {
          Failure(new IllegalArgumentException(s"Invalid storage account: $accountName"))
        } else {
          val provider = authMode match {
            case _: SharedKeyCredentialMode => AzureGen2FileSystemProvider(accountName, authMode.sharedKeyCredential().get)
            case _: ClientSecretCredentialMode => AzureGen2FileSystemProvider(accountName, authMode.credential().get)
          }
          val configMap = backendConfig.root().keySet().asScala.map(k => k -> backendConfig.getString(k)).toMap.asJava
          val fileSystem: AzureGen2FileSystem = provider.getOrCreateFileSystem(accountName, configMap).asInstanceOf[AzureGen2FileSystem]
          val azureGen2Path = AzureGen2Path(fileSystem, fileStoreName, path)
          Success(AdlsPath(azureGen2Path, accountName, fileStoreName))
        }
      case PossiblyValidRelativeAdlsPath => Failure(new IllegalArgumentException(s"$string does not have a Azure Data Lake Storage Gen2 scheme"))
      case invalid: InvalidAdlsPath => Failure(new IllegalArgumentException(invalid.errorMessage))
    }
  }

  override def name: String = "Azure Data Lake Storage Gen2"
}

case class AdlsPath private[adls](nioPath: NioPath,
                                  storageAccount: String,
                                  fileSystemName: String
                                 ) extends Path {
  override protected def newPath(nioPath: NioPath): AdlsPath = AdlsPath(nioPath, storageAccount, fileSystemName)

  override def pathAsString: String = s"abfs://$pathWithoutScheme"

  override def pathWithoutScheme: String = s"$fileSystemName@$storageAccount.dfs.core.windows.net${azureGen2Path.pathString}"

  def azureGen2Path: AzureGen2Path = nioPath match {
    case azureGen2Path: AzureGen2Path => azureGen2Path
    case _ => throw new RuntimeException(s"Internal path was not a cloud storage path: $nioPath")
  }
}
