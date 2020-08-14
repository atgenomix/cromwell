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
package cromwell.cloudsupport.azure.adls

import com.azure.identity.ClientSecretCredential
import com.azure.storage.file.datalake.DataLakeServiceClient
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder

object AdlsStorage {
  // val DefaultConfiguration = {}

  def adlsClient(credential: ClientSecretCredential, account: String): DataLakeServiceClient = {
    val endpoint = "https://" + account + ".dfs.core.windows.net"
    new DataLakeServiceClientBuilder().credential(credential).endpoint(endpoint).buildClient
  }
}
