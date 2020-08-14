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
package cromwell.cloudsupport.azure.auth

import com.azure.identity.{ClientSecretCredential, ClientSecretCredentialBuilder}
import com.google.api.client.json.jackson2.JacksonFactory
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object AzureAuthMode {
  type OptionLookup = String => String
  lazy val jsonFactory = JacksonFactory.getDefaultInstance
}

sealed trait AzureAuthMode {
  protected lazy val log = LoggerFactory.getLogger(getClass.getSimpleName)

  /**
   * The name of the auth mode
   */
  def name: String

  def credential(): ClientSecretCredential

  /**
    * Enables swapping out credential validation for various testing purposes ONLY.
    *
    * All traits in this file are sealed, all classes final, meaning things
    * like Mockito or other java/scala overrides cannot work.
    */
  private[auth] var credentialsValidation: (ClientSecretCredential) => Unit =
    (credential: ClientSecretCredential) => {
      // TODO: how to do credential validation on Azure?
      // val scope = "https://storage.azure.com/.default"
      // val request = new TokenRequestContext()
      // request.addScopes(scope)
      // credential.getToken(request)
      //   .block()
      ()
    }

  protected def validateCredentials(credential: ClientSecretCredential): ClientSecretCredential = {
    Try(credentialsValidation(credential)) match {
      case Failure(ex) => throw new RuntimeException(s"Azure credentials are invalid: ${ex.getMessage}", ex)
      case Success(_) => credential
    }
  }
}

/**
 * A mock AzureAuthMode using the anonymous credentials provider.
 */
case object MockAuthMode extends AzureAuthMode {
  override val name = "no_auth"

  override def credential(): ClientSecretCredential = ???
}

object CustomKeyMode

/**
 * The AzureAuthMode constructed from credential.
 *
 * @param name
 * @param tenant tenant ID
 * @param clientId client ID
 * @param secret client secret
 */
final case class CredentialMode(override val name: String,
                                tenant: String,
                                clientId: String,
                                secret: String
                                ) extends AzureAuthMode {
  override def credential(): ClientSecretCredential = {
    val credential = new ClientSecretCredentialBuilder()
      .clientId(clientId)
      .clientSecret(secret)
      .tenantId(tenant)
      .build()
    validateCredentials(credential)
  }
}

class OptionLookupException(val key: String, cause: Throwable) extends RuntimeException(key, cause)
