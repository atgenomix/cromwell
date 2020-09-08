package nio

import java.io.IOException
import java.nio.file.attribute.{BasicFileAttributeView, FileAttributeView}
import java.nio.file.spi.FileSystemProvider
import java.nio.file.{FileStore, FileSystem, Path}
import java.util

import com.azure.core.http.policy.HttpLogDetailLevel
import com.azure.core.util.logging.ClientLogger
import com.azure.identity.ClientSecretCredential
import com.azure.storage.blob.nio.{AzureBasicFileAttributeView, AzureBlobFileAttributeView}
import com.azure.storage.common.StorageSharedKeyCredential
import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}
import com.azure.storage.file.datalake.{DataLakeServiceClient, DataLakeServiceClientBuilder}

import scala.collection.JavaConverters._

object AzureGen2FileSystem {
  // Configuration constants for blob clients.
  /**
    * Expected type: com.azure.core.http.policy.HttpLogLevelDetail
    */
  val AZURE_STORAGE_HTTP_LOG_DETAIL_LEVEL = "AzureStorageHttpLogDetailLevel"

  /**
    * Expected type: Integer
    */
  val AZURE_STORAGE_MAX_TRIES = "AzureStorageMaxTries"

  val AZURE_STORAGE_TRY_TIMEOUT = "AzureStorageTryTimeout"

  /**
    * Expected type: Long
    */
  val AZURE_STORAGE_RETRY_DELAY_IN_MS = "AzureStorageRetryDelayInMs"

  val AZURE_STORAGE_MAX_RETRY_DELAY_IN_MS = "AzureStorageMaxRetryDelayInMs"

  /**
    * Expected type: com.azure.storage.common.policy.RetryPolicyType
    */
  val AZURE_STORAGE_RETRY_POLICY_TYPE = "AzureStorageRetryPolicyType"

  val AZURE_STORAGE_SECONDARY_HOST = "AzureStorageSecondaryHost"

  val AZURE_STORAGE_UPLOAD_BLOCK_SIZE = "AzureStorageUploadBlockSize"

  val AZURE_STORAGE_MAX_CONCURRENCY_PER_REQUEST = "AzureStorageMaxConcurrencyPerRequest"

  val AZURE_STORAGE_PUT_BLOB_THRESHOLD = "AzureStoragePutBlobThreshold"

  val AZURE_STORAGE_DOWNLOAD_RESUME_RETRIES = "AzureStorageDownloadResumeRetries"

  /**
    * Expected type: Boolean
    */
  val AZURE_STORAGE_USE_HTTPS = "AzureStorageUseHttps"

  /**
    * Expected type: String
    */
  private val AZURE_STORAGE_FILE_STORES = "AzureStorageFileStores"

  val SUPPORTED_ATTRIBUTE_VIEWS = Map[Class[_ <: FileAttributeView], String](
    (classOf[BasicFileAttributeView], "basic"),
    (classOf[AzureBasicFileAttributeView], "azureBasic"),
    (classOf[AzureBlobFileAttributeView], "azureBlob"))


  private[nio] val PATH_SEPARATOR = "/"

  private[nio] val ROOT_DIR_SUFFIX = ":"

  private def getRetryOptions(config: Map[String, Any]) = {
    new RequestRetryOptions(
      config.getOrElse(AzureGen2FileSystem.AZURE_STORAGE_RETRY_POLICY_TYPE, RetryPolicyType.EXPONENTIAL).asInstanceOf[RetryPolicyType],
      config.getOrElse(AzureGen2FileSystem.AZURE_STORAGE_MAX_TRIES, 4).asInstanceOf[Integer],
      config.getOrElse(AzureGen2FileSystem.AZURE_STORAGE_TRY_TIMEOUT, 60).asInstanceOf[Integer],
      config.getOrElse(AzureGen2FileSystem.AZURE_STORAGE_RETRY_DELAY_IN_MS, 4L).asInstanceOf[Long],
      config.getOrElse(AzureGen2FileSystem.AZURE_STORAGE_MAX_RETRY_DELAY_IN_MS, 120L).asInstanceOf[Long],
      config.getOrElse(AzureGen2FileSystem.AZURE_STORAGE_SECONDARY_HOST, "").asInstanceOf[String])
  }

  def apply(parentFileSystemProvider: AzureGen2FileSystemProvider,
            accountName: String,
            sharedKeyCredential: StorageSharedKeyCredential,
            config: Map[String, Any]): AzureGen2FileSystem = {
    val endpoint = "https://" + accountName + ".dfs.core.windows.net"
    val retryOptions = getRetryOptions(config)
    val logLevel: HttpLogDetailLevel = config
      .getOrElse(AzureGen2FileSystem.AZURE_STORAGE_HTTP_LOG_DETAIL_LEVEL, HttpLogDetailLevel.BASIC)
      .asInstanceOf[HttpLogDetailLevel]

    val client = new DataLakeServiceClientBuilder()
      .credential(sharedKeyCredential)
      .endpoint(endpoint)
      .retryOptions(retryOptions)
      .httpLogOptions(DataLakeServiceClientBuilder.getDefaultHttpLogOptions.setLogLevel(logLevel))
      .buildClient()

    AzureGen2FileSystem(parentFileSystemProvider, client, config)
  }

  def apply(parentFileSystemProvider: AzureGen2FileSystemProvider,
            accountName: String,
            credential: ClientSecretCredential,
            config: Map[String, Any]): AzureGen2FileSystem = {
    val endpoint = "https://" + accountName + ".dfs.core.windows.net"
    val retryOptions = getRetryOptions(config)
    val logLevel: HttpLogDetailLevel = config
      .getOrElse(AzureGen2FileSystem.AZURE_STORAGE_HTTP_LOG_DETAIL_LEVEL, HttpLogDetailLevel.BASIC)
      .asInstanceOf[HttpLogDetailLevel]

    val client = new DataLakeServiceClientBuilder()
      .credential(credential)
      .endpoint(endpoint)
      .retryOptions(retryOptions)
      .httpLogOptions(DataLakeServiceClientBuilder.getDefaultHttpLogOptions.setLogLevel(logLevel))
      .buildClient()

    AzureGen2FileSystem(parentFileSystemProvider, client, config)
  }
}

// Note: NIO file system -> Gen2 storage account
//       NIO file store -> Gen2 file system
case class AzureGen2FileSystem(
                                parentFileSystemProvider: AzureGen2FileSystemProvider,
                                dataLakeServiceClient: DataLakeServiceClient,
                                config: Map[String, _]) extends FileSystem {
  private val logger = new ClientLogger(classOf[AzureGen2FileSystem])

  val requiredCfg = Set(
    AzureGen2FileSystem.AZURE_STORAGE_UPLOAD_BLOCK_SIZE,
    AzureGen2FileSystem.AZURE_STORAGE_PUT_BLOB_THRESHOLD,
    AzureGen2FileSystem.AZURE_STORAGE_MAX_CONCURRENCY_PER_REQUEST,
    AzureGen2FileSystem.AZURE_STORAGE_DOWNLOAD_RESUME_RETRIES)
  if (!config.keySet.subsetOf(requiredCfg))
    LoggingUtility.logError(
      logger,
      new IllegalArgumentException("There was an error parsing the " + "configurations map. Please ensure all fields are set to a legal value of the correct type."))

  private val blockSize = config.get(AzureGen2FileSystem.AZURE_STORAGE_UPLOAD_BLOCK_SIZE).asInstanceOf[Long]
  private val putBlobThreshold = config.get(AzureGen2FileSystem.AZURE_STORAGE_PUT_BLOB_THRESHOLD).asInstanceOf[Long]
  private val maxConcurrencyPerRequest = config.get(AzureGen2FileSystem.AZURE_STORAGE_MAX_CONCURRENCY_PER_REQUEST).asInstanceOf[Integer]
  private var defaultFileStore: FileStore = _
  private val fileStores: Map[String, FileStore] = initializeFileStores(config)
  private var closed = false

  /**
    * Returns the provider that created this file system.
    *
    * @return the provider that created this file system.
    */
  override def provider: FileSystemProvider = this.parentFileSystemProvider

  /**
    * Closes this file system.
    * <p>
    * After a file system is closed then all subsequent access to the file system, either by methods defined by this
    * class or on objects associated with this file system, throw ClosedFileSystemException. If the file system is
    * already closed then invoking this method has no effect.
    * <p>
    * Closing the file system will not block on outstanding operations. Any operations in progress will be allowed to
    * terminate naturally after the file system is closed, though no further operations may be started after the
    * parent file system is closed.
    * <p>
    * Once closed, a file system with the same identifier as the one closed may be re-opened.
    *
    * @throws IOException If an I/O error occurs.
    */
  @throws[IOException]
  override def close(): Unit = {
    this.closed = true
    this.parentFileSystemProvider.closeFileSystem(this.getFileSystemName)
  }

  /**
    * Tells whether or not this file system is open.
    *
    * @return whether or not this file system is open.
    */
  override def isOpen: Boolean = !this.closed

  /**
    * Tells whether or not this file system allows only read-only access to its file stores.
    * <p>
    * Always returns false. It may be the case that the authentication method provided to this file system only
    * supports read operations and hence the file system is implicitly read only in this view, but that does not
    * imply the underlying account/file system is inherently read only. Creating/specifying read only file
    * systems is not supported.
    *
    * @return false
    */
  override def isReadOnly = false

  /**
    * Returns the name separator, represented as a string.
    * <p>
    * The separator used in this file system is {@code "/"}.
    *
    * @return "/"
    */
  override def getSeparator: String = AzureGen2FileSystem.PATH_SEPARATOR

  /**
    * Returns an object to iterate over the paths of the root directories.
    * <p>
    * The list of root directories corresponds to the list of available file stores and therefore containers specified
    * upon initialization. A root directory always takes the form {@code "<file-store-name>:"}. This list will
    * respect the parameters provided during initialization.
    * <p>
    * If a finite list of containers was provided on start up, this list will not change during the lifetime of this
    * object. If containers are added to the account after initialization, they will be ignored. If a container is
    * deleted or otherwise becomes unavailable, its root directory will still be returned but operations to it will
    * fail.
    *
    * @return an object to iterate over the paths of the root directories
    */
  override def getRootDirectories: Iterable[Path] = {
    this.fileStores.values.map { store =>
      new AzureGen2Path(this, store.name, "")
    }
  }

  /**
    * Returns an object to iterate over the underlying file stores
    * <p>
    * This list will respect the parameters provided during initialization.
    * <p>
    * If a finite list of containers was provided on start up, this list will not change during the lifetime of this
    * object. If containers are added to the account after initialization, they will be ignored. If a container is
    * deleted or otherwise becomes unavailable, its root directory will still be returned but operations to it will
    * fail.
    */
  override def getFileStores: Iterable[FileStore] = {
    this.fileStores.values
  }

  /**
    * Returns the set of the names of the file attribute views supported by this FileSystem.
    * <p>
    * This file system supports the following views:
    * <ul>
    * <li>{@link java.nio.file.attribute.BasicFileAttributeView}</li>
    * <li>{@link AzureBasicFileAttributeView}</li>
    * </ul>
    */
  override def supportedFileAttributeViews: util.Set[String] = AzureGen2FileSystem.SUPPORTED_ATTRIBUTE_VIEWS.values.toSet.asJava

  /**
    * Converts a path string, or a sequence of more that when joined form a path string, to a Path.
    * <p>
    * If more does not specify any elements then the value of the first parameter is the path string to convert. If
    * more specifies one or more elements then each non-empty string, including first, is considered to be a sequence
    * of name elements (see Path) and is joined to form a path string. The more will be joined using the name
    * separator.
    * <p>
    * Each name element will be {@code String}-joined to the other elements by this file system'first path separator.
    * Naming conventions and allowed characters are as
    * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/Naming-and-Referencing-Containers--Blobs--and-Metadata">defined</a>
    * by the Azure Blob Storage service. The root component is interpreted as the container name and all name elements
    * are interpreted as a part of the blob name. The character {@code ':'} is only allowed in the root component and
    * must be the last character of the root component.
    *
    * @param first the path string or initial part of the path string
    * @param more  additional strings to be joined to form the path string
    * @throws InvalidPathException if the path string cannot be converted.
    */
  override def getPath(first: String, more: String*) = {
    new AzureGen2Path(this, defaultFileStore.name(), more.foldLeft(first)(_ ++ _))
  }

  def getPathByFileStore(fileStoreName: String, first: String, more: String*) = {
    new AzureGen2Path(this, fileStoreName, more.foldLeft(first)(_ ++ _))
  }
  /**
    * Unsupported.
    *
    * @param s the matcher
    * @throws UnsupportedOperationException unsupported.
    * @throws IllegalArgumentException      never
    */
  @throws[IllegalArgumentException]
  override def getPathMatcher(s: String) = throw LoggingUtility.logError(logger, new UnsupportedOperationException)

  /**
    * Unsupported.
    *
    * @throws UnsupportedOperationException unsupported.
    */
  override def getUserPrincipalLookupService = throw LoggingUtility.logError(logger, new UnsupportedOperationException)

  /**
    * Unsupported.
    *
    * @throws UnsupportedOperationException unsupported.
    * @throws IOException                   never.
    */
  @throws[IOException]
  override def newWatchService = throw LoggingUtility.logError(logger, new UnsupportedOperationException)

  def getFileSystemName: String = this.dataLakeServiceClient.getAccountName

  def getFileSystemClient: DataLakeServiceClient = this.dataLakeServiceClient

  @throws[IOException]
  private def initializeFileStores(config: Map[String, _]): Map[String, FileStore] = {
    var fileStores = Map[String, FileStore]()
    val fileStoreNames = config.get(AzureGen2FileSystem.AZURE_STORAGE_FILE_STORES).asInstanceOf[Option[String]]

    fileStoreNames match {
      case Some(names) => names.split(",").foreach { n =>
        val fs: FileStore = AzureGen2FileStore(this, n)
        if (this.defaultFileStore == null)
          this.defaultFileStore = fs
        fileStores += (n -> fs)
      }
      case None => throw LoggingUtility.logError(logger, new IllegalArgumentException("The list of FileStores cannot be " + "null."))
    }

    fileStores
  }

  override def equals(o: Any): Boolean = {
    if (this == o)
      return true

    if (o == null || (getClass ne o.getClass))
      return false

    val that = o.asInstanceOf[AzureGen2FileSystem]
    this.getFileSystemName == that.getFileSystemName
  }

  override def hashCode: Int = this.getFileSystemName.hashCode

  @throws[IOException]
  def getFileStore(name: String) = {
    this.fileStores.get(name) match {
      case Some(store) => store
      case None => throw LoggingUtility.logError(logger, new IOException("Invalid file store: " + name))
    }
  }

  private[nio] def getBlockSize = this.blockSize

  private[nio] def getPutBlobThreshold = this.putBlobThreshold

  private[nio] def getMaxConcurrencyPerRequest = this.maxConcurrencyPerRequest
}
