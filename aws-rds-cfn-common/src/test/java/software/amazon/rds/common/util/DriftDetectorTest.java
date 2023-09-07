package software.amazon.rds.common.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import lombok.Builder;
import software.amazon.cloudformation.resource.ResourceTypeSchema;

class DriftDetectorTest {

    private final static ResourceTypeSchema RESOURCE_SCHEMA = ResourceTypeSchema.load(new JSONObject("{" +
            "\"typeName\":\"AWS::Test::Type\"," +
            "\"properties\": {" +
            "\"BoolProperty\": {\"type\":\"boolean\"}," +
            "\"IntegerProperty\": {\"type\":\"integer\"}," +
            "\"StringProperty\": {\"type\":\"string\"}," +
            "\"ReadOnlyStringProperty\": {\"type\":\"string\"}," +
            "\"WriteOnlyStringProperty\": {\"type\":\"string\"}," +
            "\"StringPropertyList\":{\"type\":\"array\",\"uniqueItems\":false,\"items\":{\"type\":\"string\"}}," +
            "\"UnorderedStringPropertyList\":{\"type\":\"array\",\"uniqueItems\":false,\"insertionOrder\":false,\"items\":{\"type\":\"string\"}}" +
            "}," +
            "\"description\": \"Test schema\"," +
            "\"primaryIdentifier\": [\"/properties/StringProperty\"]," +
            "\"additionalProperties\": false," +
            "\"propertyTransform\": {" +
            "\"/properties/BoolProperty\": \"BoolProperty or true\"," +
            "\"/properties/IntegerProperty\": \"IntegerProperty * IntegerProperty\"," +
            "\"/properties/StringProperty\": \"$lowercase(StringProperty)\"," +
            "\"/properties/NestedObject/StringProperty\": \"$join([NestedObject.StringProperty, '-pass'])\"" +
            "}," +
            "\"writeOnlyProperties\": [\"/properties/WriteOnlyStringProperty\"]," +
            "\"readOnlyProperties\": [\"/properties/ReadOnlyStringProperty\"]," +
            "}"));

    private DriftDetector driftDetector;

    @BeforeEach
    public void setUp() {
        driftDetector = new DriftDetector(RESOURCE_SCHEMA);
    }

    private <T> void assertResourceNotDrifted(final T input, final T output, final ResourceTypeSchema schema) {
        final Map<String, Mutation> drift = driftDetector.detectDrift(input, output);
        assertThat(drift).isEmpty();
    }

    @Test
    void test_detectDrift_CompareBoolProperty_TrueVsTrue() {
        final TestDataClass input = TestDataClass.builder()
                .boolProperty(true)
                .build();
        final TestDataClass output = TestDataClass.builder()
                .boolProperty(true)
                .build();
        assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    void test_detectDrift_CompareBoolProperty_FalseVsFalse() {
        final TestDataClass input = TestDataClass.builder()
                .boolProperty(false)
                .build();
        final TestDataClass output = TestDataClass.builder()
                .boolProperty(false)
                .build();
        assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    // This test uses the following propertyTransform from RESOURCE_SCHEMA: BoolProperty or true
    // The output would be compared to the input directly: true Vs false and to the transformed value: true Vs true.
    @Test
    void test_detectDrift_CompareBoolProperty_FalseVsTrue_EvalPropertyTransform() {
        final TestDataClass input = TestDataClass.builder()
                .boolProperty(false)
                .build();
        final TestDataClass output = TestDataClass.builder()
                .boolProperty(true)
                .build();
        assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    // In this test input value true would mismatch the output value false.
    // PropertyTransform would evaluate: BoolProperty(true) or true -> true.
    // This value still mismatches with the output, so the assertion fails.
    @Test
    void test_detectDrift_CompareBoolProperty_TrueVsFalse_EvalPropertyTransform_Drifted() {
        final TestDataClass input = TestDataClass.builder()
                .boolProperty(true)
                .build();
        final TestDataClass output = TestDataClass.builder()
                .boolProperty(false)
                .build();
        assertThatThrownBy(() -> {
            assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    void test_detectDrift_CompareIntegerProperty_SameValue() {
        final TestDataClass input = TestDataClass.builder()
                .integerProperty(42)
                .build();
        final TestDataClass output = TestDataClass.builder()
                .integerProperty(42)
                .build();
        assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    void test_detectDrift_CompareIntegerProperty_EvalPropertyTransform() {
        final TestDataClass input = TestDataClass.builder()
                .integerProperty(16)
                .build();
        final TestDataClass output = TestDataClass.builder()
                .integerProperty(256)
                .build();
        assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    void test_detectDrift_CompareIntegerProperty_EvalPropertyTransform_Drifted() {
        final TestDataClass input = TestDataClass.builder()
                .integerProperty(16)
                .build();
        final TestDataClass output = TestDataClass.builder()
                .integerProperty(17)
                .build();
        assertThatThrownBy(() -> {
            assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    void test_detectDrift_CompareStringProperty_SameValue() {
        final TestDataClass input = TestDataClass.builder()
                .stringProperty("TestString")
                .build();
        final TestDataClass output = TestDataClass.builder()
                .stringProperty("TestString")
                .build();
        assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    void test_detectDrift_CompareStringProperty_EvalPropertyTransform() {
        final TestDataClass input = TestDataClass.builder()
                .stringProperty("TestString")
                .build();
        final TestDataClass output = TestDataClass.builder()
                .stringProperty("teststring")
                .build();
        assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    void test_detectDrift_CompareStringProperty_EvalPropertyTransform_Drifted() {
        final TestDataClass input = TestDataClass.builder()
                .stringProperty("TestString")
                .build();
        final TestDataClass output = TestDataClass.builder()
                .stringProperty("teststring123")
                .build();
        assertThatThrownBy(() -> {
            assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    void test_detectDrift_CompareNestedObject_SameValue() {
        final TestDataClass input = TestDataClass.builder()
                .nestedObject(TestDataClass.builder().stringProperty("test-string").build())
                .build();
        final TestDataClass output = TestDataClass.builder()
                .nestedObject(TestDataClass.builder().stringProperty("test-string").build())
                .build();
        assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    void test_detectDrift_CompareNestedObject_EvalPropertyTransform() {
        final TestDataClass input = TestDataClass.builder()
                .nestedObject(TestDataClass.builder().stringProperty("test-string").build())
                .build();
        final TestDataClass output = TestDataClass.builder()
                .nestedObject(TestDataClass.builder().stringProperty("test-string-pass").build())
                .build();
        assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    void test_detectDrift_CompareNestedObject_EvalPropertyTransform_Drifted() {
        final TestDataClass input = TestDataClass.builder()
                .nestedObject(TestDataClass.builder().stringProperty("test-string").build())
                .build();
        final TestDataClass output = TestDataClass.builder()
                .nestedObject(TestDataClass.builder().stringProperty("test-string-no-pass").build())
                .build();
        assertThatThrownBy(() -> {
            assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    void test_detectDrift_nullArgs() {
        assertThat(driftDetector.detectDrift(null, null)).isEqualTo(Collections.emptyMap());
    }

    @Test
    void test_detectDrift_readOnlyPropertyMissingFromInputShouldNotDrift() {
        final TestDataClass input = TestDataClass.builder()
                .readOnlyStringProperty(null)
                .build();
        final TestDataClass output = TestDataClass.builder()
                .readOnlyStringProperty("TestString")
                .build();
        assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    void test_detectDrift_readOnlyPropertyMissingFromOutputShouldDrift() {
        final TestDataClass input = TestDataClass.builder()
                .readOnlyStringProperty("TestString")
                .build();
        final TestDataClass output = TestDataClass.builder()
                .readOnlyStringProperty(null)
                .build();
        assertThatThrownBy(() -> {
            assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    void test_detectDrift_readOnlyPropertyMismatchingValuesShouldDrift() {
        final TestDataClass input = TestDataClass.builder()
                .readOnlyStringProperty("TestString-1")
                .build();
        final TestDataClass output = TestDataClass.builder()
                .readOnlyStringProperty("TestString-2")
                .build();
        assertThatThrownBy(() -> {
            assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    void test_detectDrift_readOnlyPropertyEmptyInputShouldNotDrift() {
        final TestDataClass output = TestDataClass.builder()
                .readOnlyStringProperty("TestString")
                .build();
        assertResourceNotDrifted(null, output, RESOURCE_SCHEMA);
    }

    @Test
    void test_detectDrift_writeOnlyPropertyMissingFromOutputShouldNotDrift() {
        final TestDataClass input = TestDataClass.builder()
                .writeOnlyStringProperty("TestString")
                .build();
        final TestDataClass output = TestDataClass.builder()
                .writeOnlyStringProperty(null)
                .build();
        assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    void test_detectDrift_writeOnlyPropertyEmptyOutputShouldNotDrift() {
        final TestDataClass input = TestDataClass.builder()
                .writeOnlyStringProperty("TestString")
                .build();
        assertResourceNotDrifted(input, null, RESOURCE_SCHEMA);
    }

    @Test
    void test_detectDrift_equalListPropertiesShouldNotDrift() {
        final TestDataClass input = TestDataClass.builder()
                .stringPropertyList(ImmutableList.of("element1", "element2", "element3"))
                .build();
        final TestDataClass output = TestDataClass.builder()
                .stringPropertyList(ImmutableList.of("element1", "element2", "element3"))
                .build();
        assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    void test_detectDrift_inequalListPropertiesShouldDrift() {
        final TestDataClass input = TestDataClass.builder()
                .stringPropertyList(ImmutableList.of("element1", "element2", "element3"))
                .build();
        final TestDataClass output = TestDataClass.builder()
                .stringPropertyList(ImmutableList.of("element4", "element5", "element6"))
                .build();
        assertThatThrownBy(() -> {
            assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    void test_detectDrift_outOfOrderListPropertiesShouldNotDrift() {
        final TestDataClass input = TestDataClass.builder()
                .unorderedStringPropertyList(ImmutableList.of("element1", "element2", "element3"))
                .build();
        final TestDataClass output = TestDataClass.builder()
                .unorderedStringPropertyList(ImmutableList.of("element3", "element2", "element1"))
                .build();
        assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Builder
    static class TestDataClass {
        @JsonProperty(value = "BoolProperty")
        private final Boolean boolProperty;

        @JsonProperty(value = "IntegerProperty")
        private final Integer integerProperty;

        @JsonProperty(value = "StringProperty")
        private final String stringProperty;

        @JsonProperty(value = "NestedObject")
        private final TestDataClass nestedObject;

        @JsonProperty(value = "ReadOnlyStringProperty")
        private final String readOnlyStringProperty;

        @JsonProperty(value = "WriteOnlyStringProperty")
        private final String writeOnlyStringProperty;

        @JsonProperty(value = "StringPropertyList")
        private final List<String> stringPropertyList;

        @JsonProperty(value = "UnorderedStringPropertyList")
        private final List<String> unorderedStringPropertyList;
    }
}
