/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.cdc

import io.airbyte.cdk.read.cdc.DebeziumPropertiesBuilder
import io.debezium.processors.reselect.ReselectColumnsPostProcessor
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.NullAndEmptySource
import org.junit.jupiter.params.provider.ValueSource

class ReselectColumnsPropertiesTest {

    private fun reselectProperties(reselectColumns: String?): Map<String, String> =
        DebeziumPropertiesBuilder()
            .with("plugin.name", "pgoutput")
            .withReselectColumns(reselectColumns)
            .buildMap()
            .filterKeys {
                it == "post.processors" || it.startsWith("$RESELECT_POST_PROCESSOR_NAME.")
            }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = ["   ", "\t\n"])
    fun `no reselect post processor is configured when the option is unset or blank`(
        reselectColumns: String?
    ) {
        val properties: Map<String, String> =
            DebeziumPropertiesBuilder()
                .with("plugin.name", "pgoutput")
                .withReselectColumns(reselectColumns)
                .buildMap()
        assertFalse(properties.containsKey("post.processors"), properties.toString())
        assertFalse(
            properties.keys.any { it.startsWith(RESELECT_POST_PROCESSOR_NAME) },
            properties.toString(),
        )
        // Unrelated properties are untouched.
        assertEquals("pgoutput", properties["plugin.name"])
    }

    @Test
    fun `reselect post processor is configured for the given include list`() {
        val properties =
            reselectProperties("public.check_ins:data,public.benefit_checks:result_data")
        assertEquals(
            mapOf(
                "post.processors" to "reselector",
                "reselector.type" to ReselectColumnsPostProcessor::class.java.name,
                "reselector.reselect.columns.include.list" to
                    "public.check_ins:data,public.benefit_checks:result_data",
                // Only values Postgres omitted from the WAL (unchanged TOAST) trigger a lookup ...
                "reselector.reselect.unavailable.values" to "true",
                // ... never a column that is legitimately NULL.
                "reselector.reselect.null.values" to "false",
                // Match rows by the table's primary key rather than the event key.
                "reselector.reselect.use.event.key" to "false",
                // A row deleted between the event and the lookup must not fail the sync.
                "reselector.reselect.error.handling.mode" to "warn",
            ),
            properties,
        )
    }

    @Test
    fun `include list is trimmed`() {
        val properties = reselectProperties("  public.t:c \n")
        assertEquals("public.t:c", properties["reselector.reselect.columns.include.list"])
    }

    @Test
    fun `post processor class name is the one shipped with the Debezium version on the classpath`() {
        // Guards against a silent Debezium upgrade moving or renaming the processor: the property
        // is a class name string, so a stale value would only fail at runtime.
        assertEquals(
            "io.debezium.processors.reselect.ReselectColumnsPostProcessor",
            ReselectColumnsPostProcessor::class.java.name,
        )
    }
}
