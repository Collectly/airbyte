/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.integrations.source.postgres.cdc

import io.debezium.connector.postgresql.UnchangedToastedReplicationMessageColumn
import io.debezium.spi.converter.CustomConverter
import io.debezium.spi.converter.RelationalColumn
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import kotlin.test.assertEquals
import kotlin.test.assertNull
import org.apache.kafka.connect.data.SchemaBuilder
import org.junit.jupiter.api.Test
import org.postgresql.util.PGInterval

class PostgresCustomConverterTest {

    @Test
    fun `converts interval default value represented as microseconds`() {
        val converter = converterFor(defaultValue = 90_061_000_000L)

        assertEquals("1 days 01:01:01", converter.convert(null))
    }

    @Test
    fun `converts interval PGInterval value`() {
        val converter = converterFor(defaultValue = 0L)

        assertEquals("1 days 02:03:04", converter.convert(PGInterval(0, 0, 1, 2, 3, 4.0)))
    }

    @Test
    fun `converts negative interval default value represented as microseconds`() {
        val converter = converterFor(defaultValue = -90_061_000_000L)

        assertEquals("-1 days -01:01:01", converter.convert(null))
    }

    @Test
    fun `microsecondsToPgInterval decomposes microseconds into days hours minutes seconds`() {
        val result = PostgresCustomConverter().microsecondsToPgInterval(90_061_000_000L)

        assertEquals(PGInterval(0, 0, 1, 1, 1, 1.0), result)
    }

    @Test
    fun `microsecondsToPgInterval decomposes negative microseconds`() {
        val result = PostgresCustomConverter().microsecondsToPgInterval(-90_061_000_000L)

        assertEquals(PGInterval(0, 0, -1, -1, -1, -1.0), result)
    }

    @Test
    fun `unchanged TOAST sentinel on a text column becomes the Debezium placeholder`() {
        val converter = converterFor(typeName = "text")
        assertEquals(
            UNAVAILABLE_VALUE_PLACEHOLDER,
            converter.convert(UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE),
        )
        // Sanity: an actual value still converts as before.
        assertEquals("hello", converter.convert("hello"))
    }

    @Test
    fun `unchanged TOAST sentinel on a varchar column becomes the Debezium placeholder`() {
        val converter = converterFor(typeName = "varchar")
        assertEquals(
            UNAVAILABLE_VALUE_PLACEHOLDER,
            converter.convert(UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE),
        )
    }

    @Test
    fun `unchanged TOAST sentinel on a bytea column becomes the Debezium placeholder`() {
        val converter = converterFor(typeName = "bytea")
        assertEquals(
            UNAVAILABLE_VALUE_PLACEHOLDER,
            converter.convert(UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE),
        )
        assertEquals("\\x0102", converter.convert(byteArrayOf(1, 2)))
    }

    @Test
    fun `unchanged TOAST sentinel on a string-element array column becomes a placeholder list`() {
        val converter = converterFor(typeName = "_date")
        assertEquals(
            listOf(UNAVAILABLE_VALUE_PLACEHOLDER),
            converter.convert(
                UnchangedToastedReplicationMessageColumn.UNCHANGED_TEXT_ARRAY_TOAST_VALUE
            ),
        )
    }

    @Test
    fun `unchanged TOAST sentinel on a numeric-element array column becomes null instead of failing`() {
        val converter = converterFor(typeName = "_money")
        assertNull(
            converter.convert(UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE)
        )
    }

    @Test
    fun `placeholder constant matches Debezium's default`() {
        assertEquals("__debezium_unavailable_value", UNAVAILABLE_VALUE_PLACEHOLDER)
    }

    private fun converterFor(typeName: String): CustomConverter.Converter {
        val field = mockk<RelationalColumn>()
        every { field.name() } returns "col"
        every { field.typeName() } returns typeName
        every { field.isOptional() } returns true
        every { field.hasDefaultValue() } returns false
        val registration = mockk<CustomConverter.ConverterRegistration<SchemaBuilder?>>()
        val converter = slot<CustomConverter.Converter>()
        every { registration.register(any(), capture(converter)) } returns Unit
        PostgresCustomConverter().converterFor(field, registration)
        verify { registration.register(any(), any()) }
        return converter.captured
    }

    private fun converterFor(defaultValue: Long): CustomConverter.Converter {
        val field = mockk<RelationalColumn>()
        every { field.typeName() } returns "interval"
        every { field.isOptional() } returns false
        every { field.hasDefaultValue() } returns true
        every { field.defaultValue() } returns defaultValue

        val registration = mockk<CustomConverter.ConverterRegistration<SchemaBuilder?>>()
        val converter = slot<CustomConverter.Converter>()
        every { registration.register(any(), capture(converter)) } returns Unit

        PostgresCustomConverter().converterFor(field, registration)

        verify { registration.register(any(), any()) }
        return converter.captured
    }
}
