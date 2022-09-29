package software.amazon.rds.common.util;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class TranslatorUtilsTest {

    private static ResourceModel.ResourceModelBuilder RESOURCE_MODEL_BUILDER() {
        return ResourceModel.builder()
                .allocatedStorage("allocated storage")
                .allowMajorVersionUpgrade(true)
                .associatedRoles(ImmutableList.of("role1", "role2"))
                .timezone("timezone")
                .licenseModel("license model");
    }

    @Test
    public void reflectionDifference() {
        final ResourceModel result = TranslatorUtils.difference(RESOURCE_MODEL_BUILDER().build(), RESOURCE_MODEL_BUILDER()
                .associatedRoles(ImmutableList.of("role2", "role3"))
                .timezone("new timezone")
                .build(), ResourceModel::new);

        assertThat(result.getAssociatedRoles()).isEqualTo(ImmutableList.of("role2", "role3"));
        assertThat(result.getTimezone()).isEqualTo("new timezone");

        for(final Method getMethod : Arrays.stream(result.getClass().getDeclaredMethods())
                .filter(method -> method.getName().startsWith("get"))
                .filter(method -> method.isAnnotationPresent(JsonProperty.class))
                .filter(method -> !method.getName().equals("getAssociatedRoles"))
                .filter(method -> !method.getName().equals("getTimezone"))
                .collect(Collectors.toList())) {
            try {
                final Object fieldValue = getMethod.invoke(result);
                assertThat(fieldValue).isNull();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class ResourceModel {
        @JsonIgnore
        public static final String TYPE_NAME = "AWS::RDS::DBInstance";

        @JsonIgnore
        public static final String IDENTIFIER_KEY_DBINSTANCEIDENTIFIER = "/properties/DBInstanceIdentifier";

        @JsonProperty("AllocatedStorage")
        private String allocatedStorage;

        @JsonProperty("AllowMajorVersionUpgrade")
        private Boolean allowMajorVersionUpgrade;

        @JsonProperty("AssociatedRoles")
        private List<String> associatedRoles;

        @JsonProperty("LicenseModel")
        private String licenseModel;

        @JsonProperty("Timezone")
        private String timezone;
    }
}
