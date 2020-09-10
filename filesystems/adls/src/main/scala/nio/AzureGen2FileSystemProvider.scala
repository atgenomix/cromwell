package nio

import java.io.IOException
import java.net.URI
import java.nio.channels.{FileChannel, SeekableByteChannel}
import java.nio.file._
import java.nio.file.attribute.{BasicFileAttributes, FileAttribute, FileAttributeView}
import java.nio.file.spi.FileSystemProvider
import java.util

import com.azure.core.util.logging.ClientLogger
import com.azure.identity.ClientSecretCredential
import com.azure.storage.blob.nio.{AzureBasicFileAttributeView, AzureBasicFileAttributes, AzureBlobFileAttributeView, AzureBlobFileAttributes}
import com.azure.storage.common.StorageSharedKeyCredential
import com.azure.storage.file.datalake.models.DataLakeStorageException
import com.azure.storage.file.datalake.{DataLakeDirectoryClient, DataLakeFileClient}
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

trait AzureGen2Client {
  def delete(): Unit
  def exists(): Boolean
  def rename(destinationFileSystem: String, destinationPath: String): Unit
}

case class AzureGen2DirClient(client: DataLakeDirectoryClient) extends AzureGen2Client {
  override def delete(): Unit = client.delete()

  override def exists(): Boolean = client.exists()

  override def rename(destinationFileSystem: String, destinationPath: String): Unit = {
    client.rename(destinationFileSystem, destinationPath)
    ()
  }
}

case class AzureGen2FileClient(client: DataLakeFileClient) extends AzureGen2Client {
  override def delete(): Unit = client.delete()

  override def exists(): Boolean = client.exists()

  override def rename(destinationFileSystem: String, destinationPath: String): Unit = {
    client.rename(destinationFileSystem, destinationPath)
    ()
  }
}

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

  private val openFileSystems: TrieMap[String, FileSystem] = TrieMap[String, FileSystem]()

  def apply(accountName: String, sharedKeyCredential: StorageSharedKeyCredential): AzureGen2FileSystemProvider = {
    val provider = new AzureGen2FileSystemProvider {
      @throws(classOf[FileSystemAlreadyExistsException])
      @throws(classOf[IOException])
      @throws(classOf[IllegalArgumentException])
      override def newFileSystem(uri: URI, env: util.Map[String, _]): FileSystem = {
        val accountName = extractAccountName(uri)
        openFileSystems.get(accountName) match {
          case Some(_) =>
            throw LoggingUtility.logError(this.logger, new FileSystemAlreadyExistsException("Name: " + accountName))
          case None =>
            val afs = AzureGen2FileSystem(this, accountName, sharedKeyCredential, env.asScala.toMap)
            openFileSystems.put(accountName, afs).get
        }
      }

      override def newFileSystem(accountName: String, env: Map[String, Any]): FileSystem = {
        val afs = AzureGen2FileSystem(this, accountName, sharedKeyCredential, env)
        openFileSystems.put(accountName, afs).get
      }
    }

    provider
  }

  def apply(accountName: String, credential: ClientSecretCredential): AzureGen2FileSystemProvider = {
    val provider = new AzureGen2FileSystemProvider {
      @throws(classOf[FileSystemAlreadyExistsException])
      @throws(classOf[IOException])
      @throws(classOf[IllegalArgumentException])
      override def newFileSystem(uri: URI, env: util.Map[String, _]): FileSystem = {
        val accountName = extractAccountName(uri)
        openFileSystems.get(accountName) match {
          case Some(_) =>
            throw LoggingUtility.logError(this.logger, new FileSystemAlreadyExistsException("Name: " + accountName))
          case None =>
            val afs = AzureGen2FileSystem(this, accountName, credential, env.asScala.toMap)
            openFileSystems.put(accountName, afs).get
        }

      }

      override def newFileSystem(accountName: String, env: Map[String, Any]): FileSystem = {
        val afs = AzureGen2FileSystem(this, accountName, credential, env)
        openFileSystems.put(accountName, afs).get
      }
    }

    provider
  }
}

trait AzureGen2FileSystemProvider extends FileSystemProvider {
  protected val logger = new ClientLogger(classOf[AzureGen2FileSystemProvider])

  override def getScheme = "abfs"
  def getTlsScheme = "abfss"

  def pattern: Regex = "(abfs[s]?)://([0-9a-zA-Z-_]+)@([0-9a-zA-Z-_]+).dfs.core.windows.net/(.+)".r

  def getOrCreateFileSystem(accountName: String, storageConfig: Config): FileSystem = {
    Try(getFileSystem(accountName)) match {
      case Success(fs) => fs
      case Failure(_: NoSuchElementException) =>
        val env = storageConfig.root().keySet().asScala.map(k => k -> storageConfig.getAnyRef(k)).toMap
        newFileSystem(accountName, env)
      case Failure(ex) => throw ex
    }
  }

  def newFileSystem(accountName: String, env: Map[String, Any]): FileSystem

  def getFileSystem(accountName: String): FileSystem = {
    AzureGen2FileSystemProvider.openFileSystems(accountName)
  }

  @throws(classOf[FileSystemNotFoundException])
  @throws(classOf[IllegalArgumentException])
  override def getFileSystem(uri: URI): FileSystem = {
    val accountName = extractAccountName(uri)
    AzureGen2FileSystemProvider.openFileSystems(accountName)
  }

  override def getPath(uri: URI): Path = {
    val fileSystemName = extractFileSystemName(uri)
    getFileSystem(uri)
      .asInstanceOf[AzureGen2FileSystem]
      .getPathByFileStore(fileSystemName, uri.getPath)
  }

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
    val parentDir: AzureGen2Path = azureGen2Dir.getParent.asInstanceOf[AzureGen2Path]
    val parentDirClient = parentDir.toDirectoryClient

    if (parentDirClient.exists()) {
      val subDirClient = parentDirClient.getSubdirectoryClient(fullPath)
      subDirClient.setMetadata(attrs.map(i => i.name() -> i.value().toString).toMap.asJava)
      subDirClient.create()
      ()
    } else {
      throw LoggingUtility.logError(logger, new IOException("Parent directory does not exist for path: " + parentDir))
    }
  }

  override def delete(path: Path): Unit = {
    val fullPath = extractFullPathName(path.toUri)
    val client = getClient(path)

    Try(client.delete()) match {
      case Success(_) => ()
      case Failure(ex: DataLakeStorageException) =>
        if (ex.getStatusCode == 404)
          throw LoggingUtility.logError(logger, new NoSuchFileException(fullPath))
        else
          throw LoggingUtility.logError(logger, ex)
      case Failure(ex) =>
        logger.error(ex.getMessage)
        throw ex
    }
  }

  override def copy(source: Path, target: Path, options: CopyOption*): Unit = {
    val tmpFileName = source.getFileName.toString + ".tmp"
    val tmpPath: Path = source.getParent.resolve(tmpFileName)

    move(source, tmpPath, options: _*)
    move(tmpPath, target, options: _*)
    delete(tmpPath)
  }

  override def move(source: Path, target: Path, options: CopyOption*): Unit = {
    val dstFileSystem = extractFileSystemName(target.toUri)
    val dst = extractFullPathName(target.toUri)

    getClient(source).rename(dstFileSystem, dst)
  }

  override def isSameFile(path: Path, path2: Path): Boolean = {
    if (isDir(path) || isDir(path2))
      throw LoggingUtility.logError(logger, new IOException("path or path2 should be file"))

    val azureGen2Path = toAzureGen2Path(path)
    val azureGen2Path2 = toAzureGen2Path(path2)
    val pathMd5 = azureGen2Path.toFileClient.getProperties.getContentMd5
    val path2Md5 = azureGen2Path2.toFileClient.getProperties.getContentMd5

    pathMd5.sameElements(path2Md5)
  }

  override def isHidden(path: Path): Boolean = false

  override def getFileStore(path: Path): FileStore = {
    val fileStore = extractFileSystemName(path.toUri)
    val azureGen2Path = toAzureGen2Path(path)

    azureGen2Path.parentFileSystem.getFileStore(fileStore)
  }

  // if file exists, then it's acccessable
  override def checkAccess(path: Path, modes: AccessMode*): Unit = {
    getClient(path).exists()
    ()
  }

  override def getFileAttributeView[V <: FileAttributeView](path: Path, `type`: Class[V], options: LinkOption*): V = {
    if ((`type` eq classOf[BasicFileAttributeView]) || (`type` eq classOf[AzureBasicFileAttributeView]))
      new AzureBasicFileAttributeView(path).asInstanceOf[V]
    else if (`type` eq classOf[AzureBlobFileAttributeView])
      new AzureBlobFileAttributeView(path).asInstanceOf[V]
    else
      throw new UnsupportedOperationException()
  }

  @throws[IOException]
  override def readAttributes[A <: BasicFileAttributes](path: Path, `type`: Class[A], options: LinkOption*): A = {
    if ((`type` eq classOf[BasicFileAttributes]) || (`type` eq classOf[AzureBasicFileAttributes]))
      getFileAttributeView(path, classOf[AzureBasicFileAttributeView], options: _*).readAttributes.asInstanceOf[A]
    else if (`type` eq classOf[AzureBlobFileAttributes])
      getFileAttributeView(path, classOf[AzureBlobFileAttributeView], options: _*).readAttributes.asInstanceOf[A]
    else
      throw new UnsupportedOperationException
  }

  override def readAttributes(path: Path, attributes: String, options: LinkOption*): util.Map[String, AnyRef] = ???
  override def setAttribute(path: Path, attribute: String, value: scala.Any, options: LinkOption*): Unit = ???

  def closeFileSystem(fileSystemName: String): Unit = {
    AzureGen2FileSystemProvider.openFileSystems.remove(fileSystemName)
    ()
  }

  private def extractFileSystemName(uri: URI): String = {
    extract(uri, 2)
  }

  protected def extractAccountName(uri: URI): String = {
    extract(uri, 3)
  }

//  private def extractPathName(uri: URI): String = {
//    val fullName = extract(uri, 4)
//    val idx = fullName.lastIndexOf("/")
//    fullName.take(idx)
//  }

//  private def extractFileName(uri: URI): String = {
//    val fullName = extract(uri, 4)
//    fullName.split("/").last
//  }

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

  private def toAzureGen2Path(path: Path): AzureGen2Path = {
    if (!path.isInstanceOf[AzureGen2Path]) {
      throw new ProviderMismatchException(s"Not a Azure Data Lake Gen2 storage path $path")
    }

    path.asInstanceOf[AzureGen2Path]
  }

  private def isDir(path: Path): Boolean = path.toString.endsWith("/")

  private  def getClient(path: Path): AzureGen2Client = {
    val azureGen2Path = toAzureGen2Path(path)

    if (isDir(path)) {
      AzureGen2DirClient(azureGen2Path.toDirectoryClient)
    } else {
      AzureGen2FileClient(azureGen2Path.toFileClient)
    }
  }
}
