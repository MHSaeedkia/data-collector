package io.tibobit.normalizer.avro;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/**
 * Loads Avro {@link Schema}s. {@link #loadLatest} is what production (de)serializers use — it
 * fetches the schema from the Schema Registry, which is the single source of truth for wire
 * validation. {@link #load} reads a classpath resource and exists only for unit test fixtures,
 * which must not depend on a live Schema Registry.
 * Ported verbatim from flink/orderbook-consolidator.
 */
public final class AvroSchemaLoader {

    private AvroSchemaLoader() {
    }

    public static Schema loadLatest(String schemaRegistryUrl, String subject) {
        try (SchemaRegistryClient client = new CachedSchemaRegistryClient(schemaRegistryUrl, 10)) {
            String rawSchema = client.getLatestSchemaMetadata(subject).getSchema();
            return new Schema.Parser().parse(rawSchema);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to fetch latest Avro schema for subject '" + subject + "' from " + schemaRegistryUrl, e);
        }
    }

    public static Schema load(String classpathResource) {
        try (InputStream in = AvroSchemaLoader.class.getResourceAsStream(classpathResource)) {
            if (in == null) {
                throw new IllegalStateException("Avro schema resource not found on classpath: " + classpathResource);
            }
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load Avro schema resource: " + classpathResource, e);
        }
    }
}
