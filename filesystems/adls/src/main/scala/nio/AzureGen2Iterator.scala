package nio

import java.nio.file.Path
import java.util

import com.azure.storage.file.datalake.models.ListPathsOptions

case class AzureGen2Iterator(path: AzureGen2Path) extends util.Iterator[Path] {
  val options = new ListPathsOptions().setPath(path.withoutRoot)
  val iterator = path.toFileSystemClient.listPaths(options, null).iterator()

  override def hasNext: Boolean = iterator.hasNext

  override def next(): AzureGen2Path = {
    val item = iterator.next()
    AzureGen2Path(path.parentFileSystem, path.fileStoreName, item.getName)
  }
}
