package nio

import java.io.IOException
import java.nio.file.FileStore
import java.nio.file.attribute.{FileAttributeView, FileStoreAttributeView}

import com.azure.core.util.logging.ClientLogger

import scala.collection.JavaConverters._

object AzureGen2FileStore {
  private val AZURE_FILE_STORE_TYPE = "AzureBlobContainer"
}

case class AzureGen2FileStore(parentFileSystem: AzureGen2FileSystem, fileSystemName: String) extends FileStore {
  private val logger = new ClientLogger(classOf[AzureGen2FileStore])

  private val fileSystemClient = parentFileSystem.getDataLakeServiceClient.getFileSystemClient(fileSystemName)
  if (!parentFileSystem.getDataLakeServiceClient.listFileSystems().asScala.exists(_.getName == fileSystemName))
    fileSystemClient.create()

  /**
    * Returns the name of the container that underlies this file store.
    *
    * @return the name of the container that underlies this file store.
    */
  override def name: String = this.fileSystemClient.getFileSystemName

  /**
    * Returns the {@code String "AzureBlobContainer"} to indicate that the file store is backed by a remote blob
    * container in Azure Storage.
    *
    * @return { @code "AzureBlobContainer"}
    */
  override def `type`: String = AzureGen2FileStore.AZURE_FILE_STORE_TYPE

  /**
    * Always returns false.
    * <p>
    * It may be the case that the authentication method provided to this file system only
    * supports read operations and hence the file store is implicitly read only in this view, but that does not
    * imply the underlying container/file store is inherently read only. Creating/specifying read only file stores
    * is not currently supported.
    *
    * @return false.
    */
  override def isReadOnly = false

  /**
    * Returns the size, in bytes, of the file store.
    * <p>
    * Containers do not limit the amount of data stored. This method will always return max long.
    *
    * @return the size of the file store.
    * @throws IOException If an I/O error occurs.
    */
  @throws[IOException]
  override def getTotalSpace: Long = Long.MaxValue

  /**
    * Returns the number of bytes available to this Java virtual machine on the file store.
    * <p>
    * Containers do not limit the amount of data stored. This method will always return max long.
    *
    * @return the number of bytes available on the file store.
    * @throws IOException If an I/O error occurs.
    */
  @throws[IOException]
  override def getUsableSpace: Long = Long.MaxValue

  /**
    * Returns the number of unallocated bytes in the file store.
    * <p>
    * Containers do not limit the amount of data stored. This method will always return max long.
    *
    * @return the number of unallocated bytes in the file store.
    * @throws IOException If an I/O error occurs.
    */
  @throws[IOException]
  override def getUnallocatedSpace: Long = Long.MaxValue

  /**
    * Tells whether or not this file store supports the file attributes identified by the given file attribute view.
    * <p>
    * All file stores in this file system support the following views:
    * <ul>
    * <li>{@link java.nio.file.attribute.BasicFileAttributeView}</li>
    * <li>{@link AzureBasicFileAttributeView}</li>
    * <li>{@link AzureBlobFileAttributeView}</li>
    * </ul>
    *
    * @param type the file attribute view type
    * @return Whether the file attribute view is supported.
    */
  override def supportsFileAttributeView(`type`: Class[_ <: FileAttributeView]): Boolean = AzureGen2FileSystem.SUPPORTED_ATTRIBUTE_VIEWS.get(`type`).isDefined

  /**
    * Tells whether or not this file store supports the file attributes identified by the given file attribute view.
    * <p>
    * All file stores in this file system support the following views:
    * <ul>
    * <li>{@link java.nio.file.attribute.BasicFileAttributeView}</li>
    * <li>{@link AzureBasicFileAttributeView}</li>
    * <li>{@link AzureBlobFileAttributeView}</li>
    * </ul>
    *
    * @param name the name of the file attribute view
    * @return whether the file attribute view is supported.
    */
  override def supportsFileAttributeView(name: String): Boolean = AzureGen2FileSystem.SUPPORTED_ATTRIBUTE_VIEWS.values.exists(_ == name)

  /**
    * Returns a FileStoreAttributeView of the given type.
    * <p>
    * This method always returns null as no {@link FileStoreAttributeView} is currently supported.
    *
    * @param aClass a class
    * @return null
    */
  override def getFileStoreAttributeView[V <: FileStoreAttributeView](aClass: Class[V]): V = {
    AzureGen2FileStoreAttributeView(name).asInstanceOf[V]
  }

  /**
    * Unsupported.
    * <p>
    * This method always throws an {@code UnsupportedOperationException} as no {@link FileStoreAttributeView} is
    * currently supported.
    *
    * @param s a string
    * @return The attribute value.
    * @throws UnsupportedOperationException unsupported
    * @throws IOException                   never
    */
  @throws[IOException]
  override def getAttribute(s: String) = getFileStoreAttributeView(classOf[AzureGen2FileStoreAttributeView]).getAttribute(s)
}
