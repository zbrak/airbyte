/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.protocol

import io.airbyte.protocol.models.ConfiguredAirbyteCatalog

interface ProtocolSerializer {
    fun serialize(configuredAirbyteCatalog: ConfiguredAirbyteCatalog): String?
}
