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
  * lazure and state trade secret lazure, punishable by civil and criminal penalties.
  */
package cromwell.cloudsupport.azure

import cats.data.Validated._
import cats.instances.list._
import cats.syntax.traverse._
import cats.syntax.validated._
import com.typesafe.config.Config
import common.exception.MessageAggregation
import common.validation.ErrorOr._
import common.validation.Validation._
import cromwell.cloudsupport.azure.auth.{AzureAuthMode, CredentialMode}
import net.ceedubs.ficus.Ficus._
import org.slf4j.LoggerFactory

final case class AzureConfiguration private(applicationName: String,
                                            authsByName: Map[String, AzureAuthMode]) {

  def auth(name: String): ErrorOr[AzureAuthMode] = {
    authsByName.get(name) match {
      case None =>
        val knownAuthNames = authsByName.keys.mkString(", ")
        s"`azure` configuration stanza does not contain an auth named '$name'.  Known auth names: $knownAuthNames".invalidNel
      case Some(a) => a.validNel
    }
  }
}

object AzureConfiguration {
  import scala.concurrent.duration._
  import scala.language.postfixOps

  lazy val DefaultConnectionTimeout = 3 minutes
  lazy val DefaultReadTimeout = 3 minutes

  private val log = LoggerFactory.getLogger("AzureConfiguration")

  final case class AzureConfigurationException(errorMessages: List[String]) extends MessageAggregation {
    override val exceptionContext = "ADLS configuration"
  }

  def apply(config: Config): AzureConfiguration = {

    val azureConfig = config.getConfig("azure")

    val appName = validate { azureConfig.as[String]("application-name") }

    def buildAuth(authConfig: Config): ErrorOr[AzureAuthMode] = {

      def credentialAuth(name: String, tenant: String, clientId: String, secret: String): ErrorOr[AzureAuthMode] = validate {
        CredentialMode(name, tenant, clientId, secret)
      }

      val tenant = authConfig.getString("tenant")
      val clientId = authConfig.getString("client_id")
      val secret = authConfig.getString("secret")

      credentialAuth("credential", tenant, clientId, secret)
    }

    val listOfErrorOrAuths: List[ErrorOr[AzureAuthMode]] = azureConfig.as[List[Config]]("auths") map buildAuth
    val errorOrAuthList: ErrorOr[List[AzureAuthMode]] = listOfErrorOrAuths.sequence[ErrorOr, AzureAuthMode]

    def uniqueAuthNames(list: List[AzureAuthMode]): ErrorOr[Unit] = {
      val duplicateAuthNames = list.groupBy(_.name) collect { case (n, as) if as.size > 1 => n }
      if (duplicateAuthNames.nonEmpty) {
        ("Duplicate auth names: " + duplicateAuthNames.mkString(", ")).invalidNel
      } else {
        ().validNel
      }
    }

    (appName, errorOrAuthList).flatMapN { (name, list) =>
      uniqueAuthNames(list) map { _ =>
        AzureConfiguration(name, list map { a => a.name -> a } toMap)
      }
    } match {
      case Valid(r) => r
      case Invalid(f) =>
        val errorMessages = f.toList.mkString(", ")
        log.error(errorMessages)
        throw AzureConfigurationException(f.toList)
    }
  }
}
