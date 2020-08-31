package nio

import java.io.IOException
import java.net.URI
import java.nio.channels.{FileChannel, SeekableByteChannel}
import java.nio.file._
import java.nio.file.attribute.{BasicFileAttributes, FileAttribute, FileAttributeView}
import java.nio.file.spi.FileSystemProvider
import java.util

import com.azure.core.util.logging.ClientLogger

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.util.matching.Regex

object AzureGen2FileSystemProvider {
  /**
    * A helper for setting the HTTP properties when creating a directory.
    */
  val CONTENT_TYPE = "Content-Type"

  val CONTENT_DISPOSITION = "Content-Disposition"

  val CONTENT_LANGUAGE = "Content-Language"

  val CONTENT_ENCODING = "Content-Encoding"

  val CONTENT_MD5 = "Content-MD5"

  val CACHE_CONTROL = "Cache-Control"

  private val ACCOUNT_QUERY_KEY = "account"
  private val COPY_TIMEOUT_SECONDS = 30

  def apply(accountName: String, accountKey: String): AzureGen2FileSystemProvider = {
    val provider = new AzureGen2FileSystemProvider {
      @throws(classOf[FileSystemAlreadyExistsException])
      @throws(classOf[IOException])
      @throws(classOf[IllegalArgumentException])
      override def newFileSystem(uri: URI, env: util.Map[String, _]): FileSystem = {
        extractAccountName(uri) match {
          case Right(accountName) =>
            this.openFileSystems.get(accountName) match {
              case Some(_) =>
                throw LoggingUtility.logError(this.logger, new FileSystemAlreadyExistsException("Name: " + accountName))
              case None =>
                val afs = AzureGen2FileSystem(this, accountName, accountKey, env.asScala.toMap)
                this.openFileSystems.put(accountName, afs).get
            }
          case Left(ex) => throw ex
        }
      }
    }

    provider
  }

  def apply(clientId: String, clientSecret: String, tenantId: String): AzureGen2FileSystemProvider = {
    val provider = new AzureGen2FileSystemProvider {
      @throws(classOf[FileSystemAlreadyExistsException])
      @throws(classOf[IOException])
      @throws(classOf[IllegalArgumentException])
      override def newFileSystem(uri: URI, env: util.Map[String, _]): FileSystem = {
        extractAccountName(uri) match {
          case Right(accountName) =>
            this.openFileSystems.get(accountName) match {
              case Some(_) =>
                throw LoggingUtility.logError(this.logger, new FileSystemAlreadyExistsException("Name: " + accountName))
              case None =>
                val afs = AzureGen2FileSystem(this, accountName, clientId, clientSecret, tenantId, env.asScala.toMap)
                this.openFileSystems.put(accountName, afs).get
            }
          case Left(ex) => throw ex
        }
      }
    }

    provider
  }
}

abstract class AzureGen2FileSystemProvider extends FileSystemProvider {
  private val logger = new ClientLogger(classOf[AzureGen2FileSystemProvider])
  private val openFileSystems = TrieMap[String, FileSystem]()

  override def getScheme = "abfs"
  def getTlsScheme = "abfss"

  def pattern: Regex = "(abfs[s]?)://([0-9a-zA-Z-_]+)@([0-9a-zA-Z-_]+).dfs.core.windows.net(.+)".r

  @throws(classOf[FileSystemNotFoundException])
  @throws(classOf[IllegalArgumentException])
  override def getFileSystem(uri: URI): FileSystem = {
    extractAccountName(uri) match {
      case Left(ex) => throw ex
      case Right(fs) => this.openFileSystems(fs)
    }
  }

  override def getPath(uri: URI): Path = { getFileSystem(uri).getPath(uri.getPath) }

//  override def newInputStream(path: Path, options: OpenOption*): InputStream = ???
//  override def newOutputStream(path: Path, options: OpenOption*): OutputStream = ???

  override def newFileChannel(path: Path, options: util.Set[_ <: OpenOption], attrs: FileAttribute[_]*): FileChannel = {
    val azurePath = AzureGen2Path(getFileSystem(path.toUri).asInstanceOf[AzureGen2FileSystem], path.toUri.toString)
    AzureGen2FileChannel(azurePath, options.asScala.toSet)
  }

  override def newByteChannel(path: Path, options: util.Set[_ <: OpenOption], attrs: FileAttribute[_]*): SeekableByteChannel = {
    val azurePath = AzureGen2Path(getFileSystem(path.toUri).asInstanceOf[AzureGen2FileSystem], path.toUri.toString)
    AzureGen2SeekableByteChannel(azurePath, options.asScala.toSet)
  }

  override def newDirectoryStream(dir: Path, filter: DirectoryStream.Filter[_ >: Path]): DirectoryStream[Path] = {
    val azureDir = AzureGen2Path(getFileSystem(dir.toUri).asInstanceOf[AzureGen2FileSystem], dir.toUri.toString)
    new DirectoryStream[Path]() {
      @throws[IOException]
      override def close(): Unit = {
        // nothing to do here
      }

      override def iterator: util.Iterator[Path] = AzureGen2Iterator(azureDir)
    }
  }

  override def createDirectory(dir: Path, attrs: FileAttribute[_]*): Unit = ???
  override def delete(path: Path): Unit = ???
  override def deleteIfExists(path: Path): Boolean = ???
  override def copy(source: Path, target: Path, options: CopyOption*): Unit = ???
  override def move(source: Path, target: Path, options: CopyOption*): Unit = ???
  override def isSameFile(path: Path, path2: Path): Boolean = ???
  override def isHidden(path: Path): Boolean = ???
  override def getFileStore(path: Path): FileStore = ???
  override def checkAccess(path: Path, modes: AccessMode*): Unit = ???
  override def getFileAttributeView[V <: FileAttributeView](path: Path, `type`: Class[V], options: LinkOption*): V = ???
  override def readAttributes(path: Path, attributes: String, options: LinkOption*): util.Map[String, AnyRef] = ???
  override def readAttributes[A <: BasicFileAttributes](path: Path, `type`: Class[A], options: LinkOption*): A = ???
  override def setAttribute(path: Path, attribute: String, value: scala.Any, options: LinkOption*): Unit = ???

  def closeFileSystem(fileSystemName: String): Unit = {
    this.openFileSystems.remove(fileSystemName)
  }

  private def extractFileSystemName(uri: URI): Either[Throwable, String] = {
    extract(uri, 2)
  }

  private def extractAccountName(uri: URI): Either[Throwable, String] = {
    extract(uri, 3)
  }

  private def extractPathName(uri: URI): Either[Throwable, String] = {
    extract(uri, 4).map { i =>
      val idx = i.lastIndexOf("/")
      i.take(idx)
    }
  }

  private def extractFileName(uri: URI): Either[Throwable, String] = {
    extract(uri, 4).map(_.split("/").last)
  }

  private def extract(uri: URI, idx: Int): Either[Throwable, String] = {
    if (uri.getScheme != this.getScheme && uri.getScheme != this.getTlsScheme) {
      Left(LoggingUtility.logError(this.logger, new IllegalArgumentException("URI scheme does not match this provider")))
    }

    this.pattern
      .findFirstMatchIn(uri.toString)
      .map(_.group(idx)) match {
      case Some(i) => Right(i)
      case None => Left(LoggingUtility.logError(this.logger, new IllegalArgumentException("URI invalid format")))
    }
  }
}
