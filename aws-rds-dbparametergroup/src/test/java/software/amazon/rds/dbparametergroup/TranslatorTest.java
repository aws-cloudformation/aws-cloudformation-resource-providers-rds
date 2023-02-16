package software.amazon.rds.dbparametergroup;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.services.rds.model.Parameter;

class TranslatorTest {

    @Test
    public void test_translateParametersFromSdk() {
        final Parameter p1 = Parameter.builder()
                .parameterName("param name 1")
                .parameterValue("param value 1")
                .build();
        final Parameter p2 = Parameter.builder()
                .parameterName("param name 2")
                .parameterValue("param value 2")
                .build();

        final Map<String, Parameter> parameters = ImmutableMap.of(
                p1.parameterName(), p1,
                p2.parameterName(), p2
        );

        Assertions.assertThat(Translator.translateParametersFromSdk(parameters))
                .isEqualTo(ImmutableMap.of(
                        p1.parameterName(), p1.parameterValue(),
                        p2.parameterName(), p2.parameterValue()
                ));
    }
}
