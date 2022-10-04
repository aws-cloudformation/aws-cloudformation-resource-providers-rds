package software.amazon.rds.common.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructMap;

import java.util.List;
import java.util.Map;

public class TranslatorUtilsTest {

    @Test
    public void listDifference_differs() {
        final List<String> previous = ImmutableList.of("role1", "role3");
        final List<String> desired = ImmutableList.of("role2", "role3");

        final List<String> diff = TranslatorUtils.difference(previous, desired);
        Assertions.assertThat(diff).isEqualTo(ImmutableList.of("role2", "role3"));
    }

    @Test
    public void listDifference_equals() {
        final List<String> previous = ImmutableList.of("role1", "role3");
        final List<String> desired = ImmutableList.of("role1", "role3");

        final List<String> diff = TranslatorUtils.difference(previous, desired);
        Assertions.assertThat(diff).isEmpty();
        Assertions.assertThat(diff.getClass()).isEqualTo(DefaultSdkAutoConstructList.class);
    }

    @Test
    public void listDifference_bothNull() {
        final List<String> previous = null;
        final List<String> diff = TranslatorUtils.difference(previous, null);

        Assertions.assertThat(diff).isEmpty();
        Assertions.assertThat(diff.getClass()).isEqualTo(DefaultSdkAutoConstructList.class);
    }

    @Test void mapDifference_equals() {
        final Map<String, String> previous = ImmutableMap.of("key1", "value1", "key2", "value2");
        final Map<String, String> desired = ImmutableMap.of("key1", "value1", "key2", "value2");

        final Map<String, String> diff = TranslatorUtils.difference(previous, desired);
        Assertions.assertThat(diff).isEmpty();
        Assertions.assertThat(diff.getClass()).isEqualTo(DefaultSdkAutoConstructMap.class);
    }

    @Test void mapDifference_differs() {
        final Map<String, String> previous = ImmutableMap.of("key1", "value1", "key2", "value2");
        final Map<String, String> desired = ImmutableMap.of("key1", "value3", "key3", "value2");

        final Map<String, String> diff = TranslatorUtils.difference(previous, desired);
        Assertions.assertThat(diff).isEqualTo(ImmutableMap.of("key1", "value3", "key3", "value2"));
    }

    @Test void mapDifference_prevNull() {
        final Map<String, String> desired = ImmutableMap.of("key1", "value3", "key3", "value2");

        final Map<String, String> diff = TranslatorUtils.difference(null, desired);
        Assertions.assertThat(diff).isEqualTo(ImmutableMap.of("key1", "value3", "key3", "value2"));
    }

    @Test void stringDifference_differs() {
        final String previous = "previous";
        final String desired = "desired";

        final String diff = TranslatorUtils.difference(previous, desired);
        Assertions.assertThat(diff).isEqualTo("desired");
    }

    @Test void stringDifference_equals() {
        final String previous = "previous";
        final String desired = "previous";

        final String diff = TranslatorUtils.difference(previous, desired);
        Assertions.assertThat(diff).isNull();
    }
}
