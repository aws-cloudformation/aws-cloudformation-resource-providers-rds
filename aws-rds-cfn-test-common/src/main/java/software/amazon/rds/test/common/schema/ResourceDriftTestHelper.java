package software.amazon.rds.test.common.schema;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import org.assertj.core.api.Assertions;
import org.json.JSONObject;

import com.api.jsonata4java.expressions.EvaluateException;
import com.api.jsonata4java.expressions.Expressions;
import com.api.jsonata4java.expressions.ParseException;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

public class ResourceDriftTestHelper {

    private final static Set<Class<?>> primitiveTypes = ImmutableSet.of(
            Integer.class,
            String.class,
            Boolean.class
    );

    private static final String PROPERTIES = "/properties";
    private static final String PROPERTY_OR_SPLIT_REGEX = "\\s+\\$OR\\s+";
    private static final String PROPERTY_PATH_SEPARATOR = "/";

    public static void assertResourceNotDrifted(
            final Object input,
            final Object output,
            final JSONObject resourceSchema
    ) {
        assertResourceNotDrifted(input, output, resourceSchema, PROPERTIES);
    }

    public static void assertResourceNotDrifted(
            final Object input,
            final Object output,
            final JSONObject resourceSchema,
            final String basePath
    ) {
        if (input == null || output == null) {
            Assertions.assertThat(input).isEqualTo(output);
            return;
        }
        Assertions.assertThat(input).hasSameClassAs(output);

        final JSONObject propertyTransformMap = resourceSchema.getJSONObject("propertyTransform");
        final ObjectMapper objectMapper = new ObjectMapper();

        JsonNode inputJsonNode = null;
        try {
            // JSONata accepts JsonNode as an input. The easiest way to get a JsonNode from an object
            // is to convert it to a string and parse immediately.
            inputJsonNode = objectMapper.readTree(objectMapper.writeValueAsString(input));
        } catch (JsonProcessingException e) {
            Assertions.fail(e.getMessage());
        }

        final Field[] fields = input.getClass().getDeclaredFields();

        NextField:
        for (final Field field : fields) {
            // This code assumes that all data fields will have a @JsonProperty annotation.
            final JsonProperty annotation = field.getAnnotation(JsonProperty.class);
            if (annotation == null) {
                continue;
            }
            final String fieldName = annotation.value();
            field.setAccessible(true);
            Object inputFieldVal = null;
            Object outputFieldVal = null;
            try {
                inputFieldVal = field.get(input);
                outputFieldVal = field.get(output);
            } catch (IllegalAccessException e) {
                Assertions.fail(e.getMessage());
            }
            if (Objects.equals(inputFieldVal, outputFieldVal)) {
                continue;
            }
            final String propertyTransformPath = basePath + PROPERTY_PATH_SEPARATOR + fieldName;
            if (!primitiveTypes.contains(field.getType())) {
                assertResourceNotDrifted(inputFieldVal, outputFieldVal, resourceSchema, propertyTransformPath);
                continue;
            }
            if (propertyTransformMap.has(propertyTransformPath)) {
                final String propertyTransform = propertyTransformMap.getString(propertyTransformPath);
                // $OR is a CFN-specific feature. We need to split the expression into or-expressions and test
                // the results independently. The test passes if any of these alternative variants match.
                final String[] orExprs = propertyTransform.split(PROPERTY_OR_SPLIT_REGEX);
                for (final String expr : orExprs) {
                    JsonNode altInputFieldVal = null;
                    try {
                        final Expressions compiledExpr = Expressions.parse(expr);
                        altInputFieldVal = compiledExpr.evaluate(inputJsonNode);
                    } catch (ParseException | IOException | EvaluateException e) {
                        Assertions.fail(e.getMessage());
                    }

                    // Boolean and Integer are going to be tested as is: no regexp.
                    if (outputFieldVal instanceof Boolean) {
                        if (Objects.equals(altInputFieldVal.asBoolean(), outputFieldVal)) {
                            continue NextField;
                        }
                        break;
                    }

                    if (outputFieldVal instanceof Integer) {
                        if (Objects.equals(altInputFieldVal.asInt(), outputFieldVal)) {
                            continue NextField;
                        }
                        break;
                    }

                    // Please review the contents of primitiveClasses if this assertion fails. The failing field type
                    // might require a separate comparator.
                    if (!(outputFieldVal instanceof String)) {
                        Assertions.fail("Unhandled field type: %s", field.getClass().getName());
                    }

                    // JSONata would return a string evaluation result in quotes, e.g "\"result\"", we need to strip it.
                    final String transformedVal = altInputFieldVal.toString().replaceAll("^\"|\"$", "");

                    // Add regexp anchors to avoid loose comparisons.
                    final Pattern pattern = Pattern.compile("^" + transformedVal + "$");
                    if (transformedVal.equals(outputFieldVal) || pattern.matcher((String) outputFieldVal).matches()) {
                        continue NextField;
                    }
                }
            }
            Assertions.fail("Field %s drifted: input: %s, output: %s", fieldName, inputFieldVal, outputFieldVal);
        }
    }
}
