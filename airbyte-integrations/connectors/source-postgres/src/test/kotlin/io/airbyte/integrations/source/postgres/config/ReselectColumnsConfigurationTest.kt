/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.config

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.CliRunner
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ReselectColumnsConfigurationTest {

    private fun cdcSpec(
        block: CdcReplicationMethodConfigurationSpecification.() -> Unit = {}
    ): PostgresSourceConfigurationSpecification =
        PostgresSourceConfigurationSpecification().apply {
            host = "localhost"
            port = 5432
            username = "user"
            password = "secret"
            database = "db"
            schemas = listOf("public")
            encryptionJson = EncryptionDisable
            setIncrementalConfigurationSpecificationValue(
                CdcReplicationMethodConfigurationSpecification().apply {
                    replicationSlot = "airbyte_slot"
                    publication = "airbyte_publication"
                    block()
                },
            )
        }

    @Test
    fun `reselect_columns is passed through to the CDC configuration`() {
        val config: PostgresSourceConfiguration =
            PostgresSourceConfigurationFactory()
                .make(cdcSpec { reselectColumns = "public.check_ins:data" })
        assertEquals("public.check_ins:data", config.cdc?.reselectColumns)
    }

    @Test
    fun `reselect_columns is disabled by default`() {
        val config: PostgresSourceConfiguration =
            PostgresSourceConfigurationFactory().make(cdcSpec())
        val cdc: CdcIncrementalConfiguration =
            requireNotNull(config.cdc) {
                "expected a CDC configuration, got ${config.incrementalConfiguration}"
            }
        // The spec default is the empty string; the Debezium properties builder treats null and
        // blank alike, so either is acceptable here as long as it is blank.
        assertTrue(
            cdc.reselectColumns.isNullOrBlank(),
            "expected blank, got '${cdc.reselectColumns}'"
        )
    }

    @Test
    fun `reselect_columns is absent for non-CDC replication methods`() {
        val spec =
            cdcSpec().apply {
                setIncrementalConfigurationSpecificationValue(
                    StandardReplicationMethodConfigurationSpecification
                )
            }
        val config: PostgresSourceConfiguration = PostgresSourceConfigurationFactory().make(spec)
        assertNull(config.cdc)
    }

    @Test
    fun `connector spec exposes reselect_columns on the CDC replication method`() {
        val spec: JsonNode = CliRunner.source("spec").run().specs().single().connectionSpecification
        val replicationMethods: JsonNode = spec["properties"]["replication_method"]["oneOf"]
        val cdc: JsonNode = replicationMethods.single { it.isReplicationMethod("CDC") }
        val field: JsonNode =
            requireNotNull(cdc["properties"]["reselect_columns"]) {
                "reselect_columns is missing from the CDC branch of the spec: $cdc"
            }
        assertEquals("string", field["type"].asText())
        assertEquals("", field["default"].asText())
        assertEquals(8, field["order"].asInt())
        assertTrue(field["description"].asText().contains("schema.table:column"))
        // The option is advanced and optional: it must never be required.
        val required: List<String> = cdc["required"]?.map { it.asText() } ?: emptyList()
        assertTrue("reselect_columns" !in required)
        // Other replication methods are untouched.
        replicationMethods
            .filterNot { it.isReplicationMethod("CDC") }
            .forEach { assertNull(it["properties"]["reselect_columns"]) }
    }

    /** The spec encodes each `oneOf` branch's discriminator as `method: {"enum": ["<name>"]}`. */
    private fun JsonNode.isReplicationMethod(name: String): Boolean =
        this["properties"]["method"]["enum"]?.any { it.asText() == name } ?: false
}
