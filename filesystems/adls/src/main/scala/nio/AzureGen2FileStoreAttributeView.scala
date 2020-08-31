package nio

import java.nio.file.attribute.FileStoreAttributeView

object  AzureGen2FileStoreAttributeView {
  val ATTRIBUTE_VIEW_NAME = "S3FileStoreAttributeView"
}

case class AzureGen2FileStoreAttributeView(fileStoreName: String) extends FileStoreAttributeView {
  override def name: String = AzureGen2FileStoreAttributeView.ATTRIBUTE_VIEW_NAME

  def getAttribute(attribute: String): String = fileStoreName
}
