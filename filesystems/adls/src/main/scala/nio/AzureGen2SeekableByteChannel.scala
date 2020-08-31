package nio

import java.io.IOException
import java.lang.String.format
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class AzureGen2SeekableByteChannel(path: AzureGen2Path, options: Set[_ <: OpenOption]) extends SeekableByteChannel {

  private val fsClient = path.toFileClient
  private val seekable: SeekableByteChannel = this.createChannel(path, options)
  private var tempFile: Path = null

  private def createChannel(path: AzureGen2Path, options: Set[_ <: OpenOption]): SeekableByteChannel = {
    val exists = fsClient.exists()

    if (exists && options.exists(_ == StandardOpenOption.CREATE_NEW))
      throw new FileAlreadyExistsException(format("target already exists: %s", path))
    else if (!exists && !options.exists(_ == StandardOpenOption.CREATE_NEW) && !options.exists(_ == StandardOpenOption.CREATE))
      throw new NoSuchFileException(format("target not exists: %s", path))

    this.tempFile = Files.createTempFile(path.getFileName.toString, "")
    if (exists) {
      Try(fsClient.readToFile(tempFile.toUri.getPath, true)) match {
        case Success(_) => ()
        case Failure(_) => Files.deleteIfExists(tempFile)
      }
    }

    Files.newByteChannel(tempFile, options.filterNot(_ == StandardOpenOption.CREATE_NEW).asJava)
  }


  override def isOpen: Boolean = seekable.isOpen

  @throws[IOException]
  override def close(): Unit = {
    Try {
      if (!seekable.isOpen)
        return

      seekable.close()

      if (options.exists(_ == StandardOpenOption.DELETE_ON_CLOSE)) {
        fsClient.delete()
        return
      }

      if (options.exists(_ == StandardOpenOption.READ) && options.size == 1) return
    } match {
      case Success(_) => fsClient.uploadFromFile(tempFile.toUri.getPath, true)
      case Failure(_) => ()
    }

    Files.deleteIfExists(tempFile)
  }


  @throws[IOException]
  override def write(src: ByteBuffer): Int = seekable.write(src)

  @throws[IOException]
  override def truncate(size: Long): SeekableByteChannel = seekable.truncate(size)

  @throws[IOException]
  override def size: Long = seekable.size

  @throws[IOException]
  override def read(dst: ByteBuffer): Int = seekable.read(dst)

  @throws[IOException]
  override def position(newPosition: Long): SeekableByteChannel = seekable.position(newPosition)

  @throws[IOException]
  override def position: Long = seekable.position
}
