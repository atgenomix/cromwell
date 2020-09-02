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
import scala.util.{Failure, Success, Try}
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
        val accountName = extractAccountName(uri)
        this.openFileSystems.get(accountName) match {
          case Some(_) =>
            throw LoggingUtility.logError(this.logger, new FileSystemAlreadyExistsException("Name: " + accountName))
          case None =>
            val afs = AzureGen2FileSystem(this, accountName, accountKey, env.asScala.toMap)
            this.openFileSystems.put(accountName, afs).get
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
        val accountName = extractAccountName(uri)
        this.openFileSystems.get(accountName) match {
          case Some(_) =>
            throw LoggingUtility.logError(this.logger, new FileSystemAlreadyExistsException("Name: " + accountName))
          case None =>
            val afs = AzureGen2FileSystem(this, accountName, clientId, clientSecret, tenantId, env.asScala.toMap)
            this.openFileSystems.put(accountName, afs).get
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

  def pattern: Regex = "(abfs[s]?)://([0-9a-zA-Z-_]+)@([0-9a-zA-Z-_]+).dfs.core.windows.net/(.+)".r

  @throws(classOf[FileSystemNotFoundException])
  @throws(classOf[IllegalArgumentException])
  override def getFileSystem(uri: URI): FileSystem = {
    val accountName = extractAccountName(uri)
    this.openFileSystems(accountName)
  }

  override def getPath(uri: URI): Path = { getFileSystem(uri).getPath(uri.getPath) }

//  override def newInputStream(path: Path, options: OpenOption*): InputStream = ???
//  override def newOutputStream(path: Path, options: OpenOption*): OutputStream = ???

  override def newFileChannel(path: Path, options: util.Set[_ <: OpenOption], attrs: FileAttribute[_]*): FileChannel = {
    val azureGen2Path = toAzureGen2Path(path)
    AzureGen2FileChannel(azureGen2Path, options.asScala.toSet)
  }

  override def newByteChannel(path: Path, options: util.Set[_ <: OpenOption], attrs: FileAttribute[_]*): SeekableByteChannel = {
    val azureGen2Path = toAzureGen2Path(path)
    AzureGen2SeekableByteChannel(azureGen2Path, options.asScala.toSet)
  }

  override def newDirectoryStream(dir: Path, filter: DirectoryStream.Filter[_ >: Path]): DirectoryStream[Path] = {
    val azureGen2Dir = toAzureGen2Path(dir)
    new DirectoryStream[Path]() {
      @throws[IOException]
      override def close(): Unit = {
        // nothing to do here
      }

      override def iterator: util.Iterator[Path] = AzureGen2Iterator(azureGen2Dir)
    }
  }

  override def createDirectory(dir: Path, attrs: FileAttribute[_]*): Unit = {
    val azureGen2Dir = toAzureGen2Path(dir)
    val fullPath = extractFullPathName(dir.toUri)
    val parentDir = azureGen2Dir.getParent.toUri.toString
    val parentDirClient = azureGen2Dir.toFileSystemClient.getDirectoryClient(parentDir)

    if (parentDirClient.exists()) {
      val subDirClient = parentDirClient.getSubdirectoryClient(fullPath)
      subDirClient.setMetadata(attrs.map(i => i.name() -> i.value().toString).toMap.asJava)
      subDirClient.create()
    } else {
      throw LoggingUtility.logError(logger, new IOException("Parent directory does not exist for path: " + parentDir))
    }
  }

  override def delete(path: Path): Unit = {
    val azureGen2Path = toAzureGen2Path(path)
    val fullPath = extractFullPathName(path.toUri)
    val dirClient = azureGen2Path.toFileSystemClient.getDirectoryClient(fullPath)

    if (dirClient.exists()) {
      Try(dirClient.delete()) match {
        case Success(_) => ()
        case Failure(ex) => throw LoggingUtility.logError(logger, ex)
      }
    } else {
      throw LoggingUtility.logError(logger, new NoSuchFileException(fullPath))
    }
  }

  override def deleteIfExists(path: Path): Boolean = ???
  override def copy(source: Path, target: Path, options: CopyOption*): Unit = ???
  override def move(source: Path, target: Path, options: CopyOption*): Unit = ???
  override def isSameFile(path: Path, path2: Path): Boolean = ???
  override def getFileStore(path: Path): FileStore = ???

  override def isHidden(path: Path): Boolean = false


  override def checkAccess(path: Path, modes: AccessMode*): Unit = ???
  override def getFileAttributeView[V <: FileAttributeView](path: Path, `type`: Class[V], options: LinkOption*): V = ???
  override def readAttributes(path: Path, attributes: String, options: LinkOption*): util.Map[String, AnyRef] = ???
  override def readAttributes[A <: BasicFileAttributes](path: Path, `type`: Class[A], options: LinkOption*): A = ???
  override def setAttribute(path: Path, attribute: String, value: scala.Any, options: LinkOption*): Unit = ???

  def closeFileSystem(fileSystemName: String): Unit = {
    this.openFileSystems.remove(fileSystemName)
  }

  private def extractFileSystemName(uri: URI): String = {
    extract(uri, 2)
  }

  private def extractAccountName(uri: URI): String = {
    extract(uri, 3)
  }

  private def extractPathName(uri: URI): String = {
    val fullName = extract(uri, 4)
    val idx = fullName.lastIndexOf("/")
    fullName.take(idx)
  }

  private def extractFileName(uri: URI): String = {
    val fullName = extract(uri, 4)
    fullName.split("/").last
  }

  private def extractFullPathName(uri: URI): String = {
    extract(uri, 4)
  }

  private def extract(uri: URI, idx: Int): String = {
    if (uri.getScheme != this.getScheme && uri.getScheme != this.getTlsScheme) {
      Left(LoggingUtility.logError(this.logger, new IllegalArgumentException("URI scheme does not match this provider")))
    }

    this.pattern
      .findFirstMatchIn(uri.toString)
      .map(_.group(idx)) match {
      case Some(i) => i
      case None => throw LoggingUtility.logError(this.logger, new IllegalArgumentException("URI invalid format"))
    }
  }

  def toAzureGen2Path(path: Path): AzureGen2Path = {
    AzureGen2Path(getFileSystem(path.toUri).asInstanceOf[AzureGen2FileSystem], path.toUri.toString)
  }
}
