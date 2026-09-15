/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres

import io.airbyte.cdk.jdbc.JdbcConnectionFactory
import io.airbyte.cdk.output.BufferingOutputConsumer
import io.airbyte.cdk.test.fixtures.connector.IntegrationTestOperations
import io.airbyte.cdk.util.Jsons
import io.airbyte.integrations.source.postgres.config.CdcReplicationMethodConfigurationSpecification
import io.airbyte.integrations.source.postgres.config.EncryptionDisable
import io.airbyte.integrations.source.postgres.config.PostgresSourceConfigurationFactory
import io.airbyte.integrations.source.postgres.config.PostgresSourceConfigurationSpecification
import io.airbyte.integrations.source.postgres.legacy.PostgresTestDatabase
import io.airbyte.integrations.source.postgres.legacy.PostgresTestDatabase.BaseImage
import io.airbyte.integrations.source.postgres.legacy.PostgresTestDatabase.ContainerModifier
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.SyncMode
import java.sql.Connection
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.PostgreSQLContainer

/**
 * End-to-end CDC test for the `reselect_columns` option.
 *
 * Postgres stores large column values out of line (TOAST) and omits them from the WAL when an
 * UPDATE leaves them unchanged, unless the table's replica identity is FULL. Debezium then emits
 * `__debezium_unavailable_value` for such columns. With `reselect_columns` set, the connector
 * re-reads those columns from the source instead.
 *
 * The table carries two TOAST-able columns on purpose: a `jsonb` one, which Debezium converts
 * itself, and a `text` one, which goes through this connector's [cdc.PostgresCustomConverter]. Both
 * must behave identically.
 *
 * Each scenario provisions its own table, replication slot and publication so the two cannot see
 * each other's WAL.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(value = 600)
class PostgresSourceCdcToastReselectTest {

    private lateinit var testdb: PostgresTestDatabase
    private val container: PostgreSQLContainer<*>
        get() = testdb.container

    @BeforeAll
    fun startContainer() {
        // CONF applies the packaged postgresql.conf, which sets wal_level=logical.
        testdb = PostgresTestDatabase.`in`(BaseImage.POSTGRES_17, ContainerModifier.CONF)
    }

    @AfterAll
    fun stopContainer() {
        testdb.close()
    }

    @Test
    fun `unchanged TOAST values arrive as the Debezium placeholder without reselect_columns`() {
        val fixture = Fixture(suffix = "off")
        val update: AirbyteRecordMessage = fixture.runScenario(reselectColumns = null)
        assertEquals(fixture.expectedSmallAfterUpdate, update.data["small"].asInt())
        // The update did not touch the TOAST-ed columns, so Postgres omits them from the WAL and
        // Debezium emits its placeholder: this is the behavior the option exists to fix.
        assertEquals(DEBEZIUM_UNAVAILABLE_VALUE_PLACEHOLDER, update.data["big_json"].asText())
        assertEquals(DEBEZIUM_UNAVAILABLE_VALUE_PLACEHOLDER, update.data["big_text"].asText())
    }

    @Test
    fun `unchanged TOAST values are re-read from the source with reselect_columns`() {
        val fixture = Fixture(suffix = "on")
        val update: AirbyteRecordMessage =
            fixture.runScenario(
                reselectColumns =
                    "public.${fixture.table}:big_json,public.${fixture.table}:big_text"
            )
        assertEquals(fixture.expectedSmallAfterUpdate, update.data["small"].asInt())
        assertNotEquals(DEBEZIUM_UNAVAILABLE_VALUE_PLACEHOLDER, update.data["big_json"].asText())
        assertNotEquals(DEBEZIUM_UNAVAILABLE_VALUE_PLACEHOLDER, update.data["big_text"].asText())
        assertJsonEquals(fixture.bigJson, update.data["big_json"].asText())
        assertEquals(fixture.bigText, update.data["big_text"].asText())
    }

    private inner class Fixture(suffix: String) {
        val table = "toast_reselect_$suffix"
        private val slot = "toast_reselect_slot_$suffix"
        private val publication = "toast_reselect_pub_$suffix"

        /** Long enough to be stored out of line; see STORAGE EXTERNAL below. */
        val bigText: String = (1..16_384).joinToString("") { ('a' + (it % 26)).toString() }
        /** A jsonb document of the same size; jsonb is the type this option exists for. */
        val bigJson: String = """{"payload":"$bigText"}"""
        val expectedSmallAfterUpdate = 2

        /**
         * Provisions the table with one row, snapshots it, updates a non-TOAST column, and returns
         * the single CDC record emitted for that update.
         */
        fun runScenario(reselectColumns: String?): AirbyteRecordMessage {
            val configSpec: PostgresSourceConfigurationSpecification = configSpec(reselectColumns)
            withConnection(configSpec) { c ->
                c.createStatement().use { stmt ->
                    stmt.execute(
                        "CREATE TABLE public.$table " +
                            "(id INT PRIMARY KEY, small INT, big_json JSONB, big_text TEXT)"
                    )
                    // EXTERNAL disables compression, so the values go to the TOAST table as soon as
                    // they exceed the ~2 KB tuple target, however compressible they are.
                    stmt.execute(
                        "ALTER TABLE public.$table ALTER COLUMN big_json SET STORAGE EXTERNAL"
                    )
                    stmt.execute(
                        "ALTER TABLE public.$table ALTER COLUMN big_text SET STORAGE EXTERNAL"
                    )
                    stmt.execute("ALTER TABLE public.$table REPLICA IDENTITY DEFAULT")
                    stmt.execute("CREATE PUBLICATION $publication FOR TABLE public.$table")
                    stmt.execute("SELECT pg_create_logical_replication_slot('$slot', 'pgoutput')")
                }
                c.prepareStatement(
                        "INSERT INTO public.$table (id, small, big_json, big_text) " +
                            "VALUES (1, 1, ?::jsonb, ?)"
                    )
                    .use { ps ->
                        ps.setString(1, bigJson)
                        ps.setString(2, bigText)
                        ps.executeUpdate()
                    }
            }

            val ops = IntegrationTestOperations(configSpec)
            val stream: AirbyteStream =
                ops.discover()[table] ?: error("discover did not return $table")
            val catalog =
                ConfiguredAirbyteCatalog()
                    .withStreams(
                        listOf(
                            CatalogHelpers.toDefaultConfiguredStream(stream)
                                .withSyncMode(SyncMode.INCREMENTAL)
                                .withPrimaryKey(listOf(listOf("id"))),
                        ),
                    )

            // Sync 1: initial snapshot. The TOAST-ed values are read directly from the table, so
            // they are complete regardless of the option. (Depending on where the cold-start
            // offset lands, the INSERT may additionally surface as a CDC event; either way every
            // record must carry the full values.)
            val snapshot: BufferingOutputConsumer = ops.read(catalog)
            val snapshotRecords: List<AirbyteRecordMessage> =
                snapshot.records().filter { it.stream == table }
            assertTrue(snapshotRecords.isNotEmpty(), "snapshot emitted no records for $table")
            snapshotRecords.forEach {
                assertJsonEquals(bigJson, it.data["big_json"].asText())
                assertEquals(bigText, it.data["big_text"].asText())
            }
            val state: List<AirbyteStateMessage> =
                listOf(snapshot.states().lastOrNull() ?: error("snapshot emitted no state"))

            // Touch a small column only; the TOAST-ed columns are unchanged and therefore absent
            // from the WAL record.
            withConnection(configSpec) { c ->
                c.createStatement().use {
                    it.execute(
                        "UPDATE public.$table SET small = $expectedSmallAfterUpdate WHERE id = 1"
                    )
                }
            }

            // Sync 2: CDC from the saved state. Exactly one change event is expected.
            val cdc: BufferingOutputConsumer = ops.read(catalog, state)
            return cdc.records().singleOrNull { it.stream == table }
                ?: error("expected exactly one CDC record, got ${cdc.records()}")
        }

        private fun configSpec(reselectColumns: String?): PostgresSourceConfigurationSpecification =
            PostgresSourceConfigurationSpecification().apply {
                host = container.host
                port = container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)
                username = container.username
                password = container.password
                database = container.databaseName
                schemas = listOf("public")
                jdbcUrlParams = ""
                encryptionJson = EncryptionDisable
                checkpointTargetIntervalSeconds = 60
                maxDbConnections = 1
                setIncrementalConfigurationSpecificationValue(
                    CdcReplicationMethodConfigurationSpecification().apply {
                        replicationSlot = slot
                        this.publication = this@Fixture.publication
                        // Advance the slot as we read so the two scenarios stay independent.
                        lsnCommitBehavior = "While reading Data"
                        initialWaitingSeconds = 60
                        this.reselectColumns = reselectColumns
                    },
                )
            }

        private fun <T> withConnection(
            configSpec: PostgresSourceConfigurationSpecification,
            block: (Connection) -> T,
        ): T =
            JdbcConnectionFactory(PostgresSourceConfigurationFactory().make(configSpec)).get().use {
                it.isReadOnly = false
                it.autoCommit = true
                block(it)
            }
    }

    companion object {
        /** Debezium's default `unavailable.value.placeholder`. */
        const val DEBEZIUM_UNAVAILABLE_VALUE_PLACEHOLDER = "__debezium_unavailable_value"

        /** jsonb round-trips through Postgres normalized (whitespace, key order): compare trees. */
        fun assertJsonEquals(expected: String, actual: String) {
            assertEquals(Jsons.readTree(expected), Jsons.readTree(actual), "json mismatch: $actual")
        }
    }
}
