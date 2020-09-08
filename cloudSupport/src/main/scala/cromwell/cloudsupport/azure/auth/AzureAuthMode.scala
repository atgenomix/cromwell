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
import com.azure.storage.common.StorageSharedKeyCredential
import com.google.api.client.json.jackson2.JacksonFactory
import org.slf4j.LoggerFactory

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

  def accountName: String

  def credential(): Option[ClientSecretCredential]

  def sharedKeyCredential(): Option[StorageSharedKeyCredential]
}

/**
  * The AzureAuthMode constructed from shared key credential.
  *
  * @param name
  * @param accountName account name
  * @param accountKey account key
  */
final case class SharedKeyCredentialMode(override val name: String,
                                         override val accountName: String,
                                         accountKey: String
                                        ) extends AzureAuthMode {
  override def credential(): Option[ClientSecretCredential] = None

  override def sharedKeyCredential(): Option[StorageSharedKeyCredential] = {
    val credential = new StorageSharedKeyCredential(accountName, accountKey)
    Some(credential)
  }
}

/**
 * The AzureAuthMode constructed from client secret credential.
 *
 * @param name
 * @param tenant tenant ID
 * @param clientId client ID
 * @param secret client secret
 */
final case class ClientSecretCredentialMode(override val name: String,
                                            override val accountName: String,
                                            tenant: String,
                                            clientId: String,
                                            secret: String
                                           ) extends AzureAuthMode {
  override def credential(): Option[ClientSecretCredential] = {
    val credential = new ClientSecretCredentialBuilder()
      .clientId(clientId)
      .clientSecret(secret)
      .tenantId(tenant)
      .build()

    Some(credential)
  }

  override def sharedKeyCredential(): Option[StorageSharedKeyCredential] = None
}

class OptionLookupException(val key: String, cause: Throwable) extends RuntimeException(key, cause)
