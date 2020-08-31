package nio

import java.nio.file.Path
import java.util

case class AzureGen2Iterator(path: AzureGen2Path) extends util.Iterator[Path] {
  val iterator = path.toFileSystemClient.listPaths().iterator()

  override def hasNext: Boolean = iterator.hasNext

  override def next(): AzureGen2Path = {
    val item = iterator.next()
    AzureGen2Path(path.parentFileSystem, item.getName)
  }
}
