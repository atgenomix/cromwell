package nio

import java.io.IOException
import java.lang.String.format
import java.nio.channels.{FileChannel, FileLock, ReadableByteChannel, WritableByteChannel}
import java.nio.file._
import java.nio.{ByteBuffer, MappedByteBuffer}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object AzureGen2FileChannel {

  private def isExists(path: AzureGen2Path, options: Set[_ <: OpenOption]): Unit = {
    val fsClient = path.toFileClient
    val exists = fsClient.exists()

    if (exists && options.exists(_ == StandardOpenOption.CREATE_NEW))
      throw new FileAlreadyExistsException(format("target already exists: %s", path))
    else if (!exists && !options.exists(_ == StandardOpenOption.CREATE_NEW) && !options.exists(_ == StandardOpenOption.CREATE))
      throw new NoSuchFileException(format("target not exists: %s", path))
  }

  def apply(path: AzureGen2Path, options: Set[_ <: OpenOption]): AzureGen2FileChannel = {
    isExists(path, options)

    val tempFile = Files.createTempFile(path.getFileName.toString, "")
    AzureGen2FileChannel(path, options, tempFile)
  }
}

case class AzureGen2FileChannel(path: AzureGen2Path, options: Set[_ <: OpenOption], tempFile: Path) extends FileChannel {
  private val fsClient = path.toFileClient
  private val filechannel: FileChannel = this.createChannel(options)

  private def createChannel(options: Set[_ <: OpenOption]): FileChannel = {
    Try(fsClient.readToFile(tempFile.toAbsolutePath.toString, true)) match {
      case Success(_) => ()
      case Failure(_) => Files.deleteIfExists(tempFile)
    }

    FileChannel.open(tempFile, options.filterNot(_ == StandardOpenOption.CREATE_NEW).asJava)
  }


  @throws[IOException]
  override def read(dst: ByteBuffer): Int = filechannel.read(dst)

  @throws[IOException]
  override def read(dsts: Array[ByteBuffer], offset: Int, length: Int): Long = filechannel.read(dsts, offset, length)

  @throws[IOException]
  override def write(src: ByteBuffer): Int = filechannel.write(src)

  @throws[IOException]
  override def write(srcs: Array[ByteBuffer], offset: Int, length: Int): Long = filechannel.write(srcs, offset, length)

  @throws[IOException]
  override def position: Long = filechannel.position

  @throws[IOException]
  override def position(newPosition: Long): FileChannel = filechannel.position(newPosition)

  @throws[IOException]
  override def size: Long = filechannel.size

  @throws[IOException]
  override def truncate(size: Long): FileChannel = filechannel.truncate(size)

  @throws[IOException]
  override def force(metaData: Boolean): Unit = {
    filechannel.force(metaData)
  }

  @throws[IOException]
  override def transferTo(position: Long, count: Long, target: WritableByteChannel): Long = filechannel.transferTo(position, count, target)

  @throws[IOException]
  override def transferFrom(src: ReadableByteChannel, position: Long, count: Long): Long = filechannel.transferFrom(src, position, count)

  @throws[IOException]
  override def read(dst: ByteBuffer, position: Long): Int = filechannel.read(dst, position)

  @throws[IOException]
  override def write(src: ByteBuffer, position: Long): Int = filechannel.write(src, position)

  @throws[IOException]
  override def map(mode: FileChannel.MapMode, position: Long, size: Long): MappedByteBuffer = filechannel.map(mode, position, size)

  @throws[IOException]
  override def lock(position: Long, size: Long, shared: Boolean): FileLock = filechannel.lock(position, size, shared)

  @throws[IOException]
  override def tryLock(position: Long, size: Long, shared: Boolean): FileLock = filechannel.tryLock(position, size, shared)

  @throws[IOException]
  override protected def implCloseChannel(): Unit = {
    super.close()
    filechannel.close()
    if (!options.exists(_ == StandardOpenOption.READ))
      fsClient.uploadFromFile(tempFile.toAbsolutePath.toString, true)

    Files.deleteIfExists(tempFile)
    ()
  }
}
