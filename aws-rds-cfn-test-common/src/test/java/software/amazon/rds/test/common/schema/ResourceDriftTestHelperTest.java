package software.amazon.rds.test.common.schema;

import org.assertj.core.api.Assertions;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

class ResourceDriftTestHelperTest {

    private final static JSONObject RESOURCE_SCHEMA = new JSONObject("{" +
            "\"propertyTransform\": {" +
            "\"/properties/BoolProperty\": \"BoolProperty or true\"," +
            "\"/properties/IntegerProperty\": \"IntegerProperty * IntegerProperty\"," +
            "\"/properties/StringProperty\": \"$lowercase(StringProperty)\"," +
            "\"/properties/NestedObject/StringProperty\": \"$join([StringProperty, '-pass'])\"" +
            "}" +
            "}");

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
    }

    @Test
    public void test_assertResourceNotDrifted_NullVsNull() {
        ResourceDriftTestHelper.assertResourceNotDrifted(null, null, RESOURCE_SCHEMA);
    }

    @Test
    public void test_assertResourceNotDrifted_NullVsNonNull_Drifted() {
        Assertions.assertThatThrownBy(() -> {
            ResourceDriftTestHelper.assertResourceNotDrifted(null, 42, RESOURCE_SCHEMA);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    public void test_assertResourceNotDrifted_NonNullVsNull_Drifted() {
        Assertions.assertThatThrownBy(() -> {
            ResourceDriftTestHelper.assertResourceNotDrifted(42, null, RESOURCE_SCHEMA);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    public void test_assertResourceNotDrifted_BooleanVsBoolean() {
        ResourceDriftTestHelper.assertResourceNotDrifted(true, true, RESOURCE_SCHEMA);
    }

    @Test
    public void test_assertResourceNotDrifted_IntegerVsInteger() {
        ResourceDriftTestHelper.assertResourceNotDrifted(42, 42, RESOURCE_SCHEMA);
    }

    @Test
    public void test_assertResourceNotDrifted_StringVsString() {
        ResourceDriftTestHelper.assertResourceNotDrifted("test-string", "test-string", RESOURCE_SCHEMA);
    }

    @Test
    public void test_assertResourceNotDrifted_CompareBoolProperty_TrueVsTrue() {
        final TestDataClass input = TestDataClass.builder()
                .boolProperty(true)
                .build();
        final TestDataClass output = TestDataClass.builder()
                .boolProperty(true)
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    public void test_assertResourceNotDrifted_CompareBoolProperty_FalseVsFalse() {
        final TestDataClass input = TestDataClass.builder()
                .boolProperty(false)
                .build();
        final TestDataClass output = TestDataClass.builder()
                .boolProperty(false)
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    // This test uses the following propertyTransform from RESOURCE_SCHEMA: BoolProperty or true
    // The output would be compared to the input directly: true Vs false and to the transformed value: true Vs true.
    @Test
    public void test_assertResourceNotDrifted_CompareBoolProperty_FalseVsTrue_EvalPropertyTransform() {
        final TestDataClass input = TestDataClass.builder()
                .boolProperty(false)
                .build();
        final TestDataClass output = TestDataClass.builder()
                .boolProperty(true)
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    // In this test input value true would mismatch the output value false.
    // PropertyTransform would evaluate: BoolProperty(true) or true -> true.
    // This value still mismatches with the output, so the assertion fails.
    @Test
    public void test_assertResourceNotDrifted_CompareBoolProperty_TrueVsFalse_EvalPropertyTransform_Drifted() {
        final TestDataClass input = TestDataClass.builder()
                .boolProperty(true)
                .build();
        final TestDataClass output = TestDataClass.builder()
                .boolProperty(false)
                .build();
        Assertions.assertThatThrownBy(() -> {
            ResourceDriftTestHelper.assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    public void test_assertResourceNotDrifted_CompareIntegerProperty_SameValue() {
        final TestDataClass input = TestDataClass.builder()
                .integerProperty(42)
                .build();
        final TestDataClass output = TestDataClass.builder()
                .integerProperty(42)
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    public void test_assertResourceNotDrifted_CompareIntegerProperty_EvalPropertyTransform() {
        final TestDataClass input = TestDataClass.builder()
                .integerProperty(16)
                .build();
        final TestDataClass output = TestDataClass.builder()
                .integerProperty(256)
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    public void test_assertResourceNotDrifted_CompareIntegerProperty_EvalPropertyTransform_Drifted() {
        final TestDataClass input = TestDataClass.builder()
                .integerProperty(16)
                .build();
        final TestDataClass output = TestDataClass.builder()
                .integerProperty(17)
                .build();
        Assertions.assertThatThrownBy(() -> {
            ResourceDriftTestHelper.assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    public void test_assertResourceNotDrifted_CompareStringProperty_SameValue() {
        final TestDataClass input = TestDataClass.builder()
                .stringProperty("TestString")
                .build();
        final TestDataClass output = TestDataClass.builder()
                .stringProperty("TestString")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    public void test_assertResourceNotDrifted_CompareStringProperty_EvalPropertyTransform() {
        final TestDataClass input = TestDataClass.builder()
                .stringProperty("TestString")
                .build();
        final TestDataClass output = TestDataClass.builder()
                .stringProperty("teststring")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    public void test_assertResourceNotDrifted_CompareStringProperty_EvalPropertyTransform_Drifted() {
        final TestDataClass input = TestDataClass.builder()
                .stringProperty("TestString")
                .build();
        final TestDataClass output = TestDataClass.builder()
                .stringProperty("teststring123")
                .build();
        Assertions.assertThatThrownBy(() -> {
            ResourceDriftTestHelper.assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    public void test_assertResourceNotDrifted_CompareNestedObject_SameValue() {
        final TestDataClass input = TestDataClass.builder()
                .nestedObject(TestDataClass.builder().stringProperty("test-string").build())
                .build();
        final TestDataClass output = TestDataClass.builder()
                .nestedObject(TestDataClass.builder().stringProperty("test-string").build())
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    public void test_assertResourceNotDrifted_CompareNestedObject_EvalPropertyTransform() {
        final TestDataClass input = TestDataClass.builder()
                .nestedObject(TestDataClass.builder().stringProperty("test-string").build())
                .build();
        final TestDataClass output = TestDataClass.builder()
                .nestedObject(TestDataClass.builder().stringProperty("test-string-pass").build())
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
    }

    @Test
    public void test_assertResourceNotDrifted_CompareNestedObject_EvalPropertyTransform_Drifted() {
        final TestDataClass input = TestDataClass.builder()
                .nestedObject(TestDataClass.builder().stringProperty("test-string").build())
                .build();
        final TestDataClass output = TestDataClass.builder()
                .nestedObject(TestDataClass.builder().stringProperty("test-string-no-pass").build())
                .build();
        Assertions.assertThatThrownBy(() -> {
            ResourceDriftTestHelper.assertResourceNotDrifted(input, output, RESOURCE_SCHEMA);
        }).isInstanceOf(AssertionError.class);
    }
}
