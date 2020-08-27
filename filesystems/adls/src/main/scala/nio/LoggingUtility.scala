package nio

import com.azure.core.util.logging.ClientLogger

object LoggingUtility {
  def logError[T <: Exception](logger: ClientLogger, e: T): T = {
    logger.error(e.getMessage)
    e
  }
}
