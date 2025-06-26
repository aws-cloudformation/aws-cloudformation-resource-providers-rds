package software.amazon.rds.dbparametergroup;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.services.rds.model.Parameter;

class BaseHandlerStdTest {

    @Test
    public void test_computeModifiedDBParameters() {
        final Map<String, Parameter> currentDBParameters = ImmutableMap.of(
                "param1", Parameter.builder().parameterName("param1").parameterValue("value1").source("engine-default").build(),
                "param2", Parameter.builder().parameterName("param2").parameterValue("value1-2").source("user").build(),
                "param4", Parameter.builder().parameterName("param4").source("system").build(),
                "param5", Parameter.builder().parameterName("param5").parameterValue("value5").source("user").build()
        );

        final Map<String, Parameter> modifiedParameters = BaseHandlerStd.computeModifiedDBParameters(
                currentDBParameters
        );

        Assertions.assertThat(modifiedParameters)
                .isEqualTo(ImmutableMap.of(
                        "param2", Parameter.builder().parameterName("param2").parameterValue("value1-2").source("user").build(),
                        "param5", Parameter.builder().parameterName("param5").parameterValue("value5").source("user").build()
                ));
    }
}
