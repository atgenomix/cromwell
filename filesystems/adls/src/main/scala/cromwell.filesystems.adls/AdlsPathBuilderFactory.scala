/**
  * Copyright (C) 2020, Atgenomix Incorporated. All Rights Reserved.
  * This program is an unpublished copyrighted work which is proprietary to
  * Atgenomix Incorporated and contains confidential information that is not to
  * be reproduced or disclosed to any other person or entity without prior
  * written consent from Atgenomix, Inc. in each and every instance.
  *
  * warning Unauthorized reproduction of this program as well as unauthorized
  * preparation of derivative works based upon the program or distribution of
  * copies by sale, rental, lease or lending are violations of federal copyright
  * laws and state trade secret laws, punishable by civil and criminal penalties.
  */
package cromwell.filesystems.adls

import akka.actor.ActorSystem
import com.typesafe.config.Config
import common.validation.ErrorOr.ErrorOr
import common.validation.Validation._
import cromwell.cloudsupport.azure.AzureConfiguration
import cromwell.cloudsupport.azure.auth.AzureAuthMode
import cromwell.core.WorkflowOptions
import cromwell.core.path.PathBuilderFactory
import net.ceedubs.ficus.Ficus._

import scala.concurrent.{ExecutionContext, Future}

// The constructor of this class is required to be Config, Config by cromwell
// So, we need to take this config and get the AuthMode out of it
final case class AdlsPathBuilderFactory private(globalConfig: Config, instanceConfig: Config, accountName: String)
  extends PathBuilderFactory {

  // Grab the authMode out of configuration
  val conf: AzureConfiguration = AzureConfiguration(globalConfig)
  val authModeAsString: String = instanceConfig.as[String]("auth")
  val authModeValidation: ErrorOr[AzureAuthMode] = conf.auth(authModeAsString)
  val authMode = authModeValidation.unsafe(s"Failed to get authentication mode for $authModeAsString")
  val storageAccount: String = accountName

  def withOptions(options: WorkflowOptions)(implicit as: ActorSystem, ec: ExecutionContext): Future[AdlsPathBuilder] = {
    AdlsPathBuilder.fromAuthMode(authMode, options, storageAccount)
  }
}

object AdlsPathBuilderFactory {
  def apply(globalConfig: Config, instanceConfig: Config, accountName: String): AdlsPathBuilderFactory = {
    new AdlsPathBuilderFactory(globalConfig, instanceConfig, accountName: String)
  }
}
