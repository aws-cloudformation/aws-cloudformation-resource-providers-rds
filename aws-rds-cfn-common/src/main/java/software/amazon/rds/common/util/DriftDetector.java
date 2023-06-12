package software.amazon.rds.common.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.BooleanUtils;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;

import com.api.jsonata4java.expressions.EvaluateException;
import com.api.jsonata4java.expressions.Expressions;
import com.api.jsonata4java.expressions.ParseException;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import software.amazon.cloudformation.resource.ResourceTypeSchema;

public class DriftDetector {

    private static final String INSERTION_ORDER = "insertionOrder";
    private static final String PROPERTIES_ROOT = "/properties";
    private static final String PROPERTY_OR_SPLIT_REGEX = "\\s+\\$OR\\s+";
    private static final String PROPERTY_PATH_SEPARATOR = "/";

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Map<String, Mutation> NO_DRIFT = Collections.emptyMap();

    private final ResourceTypeSchema schema;
    private final Set<String> writeOnlyLookup;

    public DriftDetector(final ResourceTypeSchema schema) {
        this.schema = schema;
        this.writeOnlyLookup = new HashSet<>(
                Optional.ofNullable(schema.getWriteOnlyPropertiesAsStrings())
                        .orElse(Collections.emptyList())
        );
    }

    private static boolean isTransformableProperty(
            final ResourceTypeSchema schema,
            final String propertyName
    ) throws DriftDetectorRuntimeException {
        return schema.getPropertyTransform().containsKey(propertyName);
    }

    private static Object castToPrimitive(final JsonNode node, final Class<?> klass) {
        if (Integer.class.equals(klass)) {
            return node.asInt();
        } else if (Boolean.class.equals(klass)) {
            return node.asBoolean();
        } else if (String.class.equals(klass)) {
            return node.asText();
        }

        throw new DriftDetectorRuntimeException("Unexpected cast input class: " + klass.getName());
    }

    private List<Object> getTransformations(
            final Object in,
            final String path,
            final JsonNode rootNode
    ) {
        final List<Object> result = new ArrayList<>();
        final String propTxs = schema.getPropertyTransform().get(path);
        if (propTxs != null) {
            for (final String tx : propTxs.split(PROPERTY_OR_SPLIT_REGEX)) {
                try {
                    final Expressions expr = Expressions.parse(tx);
                    final JsonNode txNode = expr.evaluate(rootNode);
                    result.add(castToPrimitive(txNode, in.getClass()));
                } catch (ParseException | IOException | EvaluateException e) {
                    throw new DriftDetectorRuntimeException("Failed to parse jsonata expression: " + tx, e);
                }
            }
        }
        return result;
    }

    private boolean isWriteOnly(final String path) {
        return writeOnlyLookup.contains(path);
    }

    public <T> Map<String, Mutation> detectDrift(final T prev, final T upd) {
        try {
            final JsonNode rootNode = objectMapper.readTree(objectMapper.writeValueAsString(prev));
            return detectDrift(prev, upd, PROPERTIES_ROOT, rootNode);
        } catch (JsonProcessingException e) {
            throw new DriftDetectorRuntimeException("Failed to parse the root node", e);
        }
    }

    private <T> Map<String, Mutation> detectDriftTransformable(
            final T prev,
            final T upd,
            final String path,
            final JsonNode rootNode
    ) {
        final List<Object> transformations = getTransformations(prev, path, rootNode);

        if (transformations.stream().anyMatch(tx -> {
            if (Objects.equals(tx, upd)) {
                return true;
            }
            if (prev instanceof String) {
                final String transformed = tx.toString().replaceAll("^\"|\"$", "");
                // Add regexp anchors to avoid loose comparisons.
                final Pattern pattern = Pattern.compile("^" + transformed + "$");
                return transformed.equals(upd) || pattern.matcher((String) upd).matches();
            }
            return false;
        })) {
            return NO_DRIFT;
        }

        return ImmutableMap.of(path, new Mutation(prev, upd));
    }

    private boolean isPrimitive(final Object obj) {
        return obj instanceof Integer ||
                obj instanceof String ||
                obj instanceof Boolean;
    }

    private <T> Map<String, Mutation> detectDriftCmp(
            final T prev,
            final T upd,
            final String path
    ) {
        if (Objects.equals(prev, upd)) {
            return NO_DRIFT;
        }
        return ImmutableMap.of(path, new Mutation(prev, upd));
    }

    protected <T> Map<String, Mutation> detectDriftObj(
            final T prev,
            final T upd,
            final String path,
            final JsonNode rootNode
    ) {
        final Map<String, Mutation> mutations = new HashMap<>();
        final Field[] fields = prev.getClass().getDeclaredFields();
        for (final Field field : fields) {
            final JsonProperty jsonProperty = field.getAnnotation(JsonProperty.class);
            if (jsonProperty == null) {
                continue;
            }
            final String fieldName = jsonProperty.value();
            field.setAccessible(true);
            Object prevFieldVal = null;
            Object updFieldVal = null;
            try {
                prevFieldVal = field.get(prev);
                if (upd != null) {
                    updFieldVal = field.get(upd);
                }
            } catch (IllegalAccessException e) {
                throw new DriftDetectorRuntimeException(e);
            }

            final String propertyName = path + PROPERTY_PATH_SEPARATOR + fieldName;

            if (isEqual(prevFieldVal, updFieldVal, propertyName)) {
                continue;
            }

            mutations.putAll(detectDrift(prevFieldVal, updFieldVal, propertyName, rootNode));
        }

        return mutations;
    }

    private <T extends List<?>> Map<String, Mutation> detectDriftList(
            final T prev,
            final T upd,
            final String path
    ) {
        final Schema propertySchema = getPropertySchema(path);
        if (!(propertySchema instanceof ArraySchema)) {
            throw new DriftDetectorRuntimeException(String.format("Malformed schema for path: %s", path));
        }
        final ArraySchema arraySchema = (ArraySchema) propertySchema;
        // InsertionOrder is not defined as a part of the common ArraySchema and would be located in the unprocessed properties
        final Boolean insertionOrder = (Boolean) arraySchema.getUnprocessedProperties().get(INSERTION_ORDER);
        if (BooleanUtils.isFalse(insertionOrder)) {
            // compare unordered lists
            return detectDriftCmp(freqAll(prev), freqAll(upd == null ? Collections.emptyList() : upd), path);
        }
        return detectDriftCmp(prev, upd == null ? Collections.emptyList() : upd, path);
    }

    private <T extends List<?>> Map<?, Long> freqAll(final T list) {
        return list.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));
    }

    private <T extends Set<?>> Map<String, Mutation> detectDriftSet(
            final T prev,
            final T upd,
            final String path
    ) {
        return detectDriftCmp(prev, upd == null ? Collections.emptySet() : upd, path);
    }

    private Schema getPropertySchema(String pathOrName) {
        if (pathOrName.startsWith(PROPERTIES_ROOT)) {
            pathOrName = Iterables.getLast(Arrays.asList(pathOrName.split("/")));
        }
        return ((ObjectSchema) schema.getSchema()).getPropertySchemas().get(pathOrName);
    }

    private <T> boolean isEqual(final T prev, final T upd, final String path) {
        return prev == null || Objects.equals(prev, upd) || isWriteOnly(path);
    }

    protected <T> Map<String, Mutation> detectDrift(
            final T prev,
            final T upd,
            final String path,
            final JsonNode rootNode
    ) throws DriftDetectorRuntimeException {
        if (isEqual(prev, upd, path)) {
            return NO_DRIFT;
        }

        if (upd != null && isTransformableProperty(schema, path)) {
            return detectDriftTransformable(prev, upd, path, rootNode);
        } else if (isPrimitive(prev)) {
            return detectDriftCmp(prev, upd, path);
        } else if (prev instanceof List<?>) {
            return detectDriftList((List<?>) prev, (List<?>) upd, path);
        } else if (prev instanceof Set) {
            return detectDriftSet((Set<?>) prev, (Set<?>) upd, path);
        }
        return detectDriftObj(prev, upd, path, rootNode);
    }
}
