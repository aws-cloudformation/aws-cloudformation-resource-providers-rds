package software.amazon.rds.common.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructMap;
import software.amazon.cloudformation.resource.ResourceTypeSchema;

class DifferenceUtilsTest {

    @Test
    void listDifference_differs() {
        final List<String> previous = ImmutableList.of("role1", "role3");
        final List<String> desired = ImmutableList.of("role2", "role3");

        final List<String> diff = DifferenceUtils.diff(previous, desired);
        assertThat(diff).isEqualTo(ImmutableList.of("role2", "role3"));
    }

    @Test
    void listDifference_equals() {
        final List<String> previous = ImmutableList.of("role1", "role3");
        final List<String> desired = ImmutableList.of("role1", "role3");

        final List<String> diff = DifferenceUtils.diff(previous, desired);
        assertThat(diff).isEmpty();
        assertThat(diff.getClass()).isEqualTo(DefaultSdkAutoConstructList.class);
    }

    @Test
    void listDifference_bothNull() {
        final List<String> previous = null;
        final List<String> diff = DifferenceUtils.diff(previous, null);

        assertThat(diff).isEmpty();
        assertThat(diff.getClass()).isEqualTo(DefaultSdkAutoConstructList.class);
    }

    @ParameterizedTest
    @ArgumentsSource(EmptyListArgumentsProvider.class)
    void listDifference_empty(final List<String> prev, final List<String> upd) {
        final List<String> diff = DifferenceUtils.diff(prev, upd);

        assertThat(diff).isEmpty();
        assertThat(diff.getClass()).isEqualTo(DefaultSdkAutoConstructList.class);
    }

    @Test
    void mapDifference_equals() {
        final Map<String, String> previous = ImmutableMap.of("key1", "value1", "key2", "value2");
        final Map<String, String> desired = ImmutableMap.of("key1", "value1", "key2", "value2");

        final Map<String, String> diff = DifferenceUtils.diff(previous, desired);
        assertThat(diff).isEmpty();
        assertThat(diff.getClass()).isEqualTo(DefaultSdkAutoConstructMap.class);
    }

    @Test
    void mapDifference_differs() {
        final Map<String, String> previous = ImmutableMap.of("key1", "value1", "key2", "value2");
        final Map<String, String> desired = ImmutableMap.of("key1", "value3", "key3", "value2");

        final Map<String, String> diff = DifferenceUtils.diff(previous, desired);
        assertThat(diff).isEqualTo(ImmutableMap.of("key1", "value3", "key3", "value2"));
    }

    @ParameterizedTest
    @ArgumentsSource(EmptyMapArgumentsProvider.class)
    void mapDifference_empty(final Map<String, String> prev, final Map<String, String> upd) {
        final Map<String, String> diff = DifferenceUtils.diff(prev, upd);

        assertThat(diff).isEmpty();
        assertThat(diff.getClass()).isEqualTo(DefaultSdkAutoConstructMap.class);
    }

    @Test
    void stringDifference_differs() {
        final String previous = "previous";
        final String desired = "desired";

        final String diff = DifferenceUtils.diff(previous, desired);
        assertThat(diff).isEqualTo("desired");
    }

    @Test
    void stringDifference_equals() {
        final String previous = "previous";
        final String desired = "previous";

        final String diff = DifferenceUtils.diff(previous, desired);
        assertThat(diff).isNull();
    }

    static class EmptyListArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
            return Stream.of(
                    Arguments.of(null, null),
                    Arguments.of(null, Collections.emptyList()),
                    Arguments.of(Collections.emptyList(), Collections.emptyList()),
                    Arguments.of(Collections.emptyList(), null)
            );
        }
    }

    static class EmptyMapArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
            return Stream.of(
                    Arguments.of(null, null),
                    Arguments.of(null, Collections.emptyMap()),
                    Arguments.of(Collections.emptyMap(), Collections.emptyMap()),
                    Arguments.of(Collections.emptyMap(), null)
            );
        }
    }
}
