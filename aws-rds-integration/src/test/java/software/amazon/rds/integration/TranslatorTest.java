package software.amazon.rds.integration;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.rds.model.DescribeIntegrationsRequest;
import software.amazon.awssdk.services.rds.model.Filter;

import java.util.List;

@ExtendWith(MockitoExtension.class)
public class TranslatorTest {

    @Test
    public void translateTags_withNullTags() {
        Assertions.assertNull(Translator.translateTags(null));
    }

    @Test
    public void translateTags_withNonNullTags() {
        Assertions.assertEquals(
                ImmutableSet.of(
                        Tag.builder().key("k1").value("v1").build(),
                        Tag.builder().key("k2").value("v2").build()
                ),
                Translator.translateTags(
                    ImmutableSet.of(
                            software.amazon.awssdk.services.rds.model.Tag.builder().key("k1").value("v1").build(),
                            software.amazon.awssdk.services.rds.model.Tag.builder().key("k2").value("v2").build()
                    )
            )
        );
    }

    @Test
    public void translateDescribeWithoutArn_shouldUseName() {
        DescribeIntegrationsRequest request = Translator.describeIntegrationsRequest(
                ResourceModel.builder()
                        .integrationName("integname12345").build()
        );
        Assertions.assertNull(request.integrationIdentifier());
        List<Filter> filters = request.filters();
        Assertions.assertEquals(filters.size(), 1);
        Assertions.assertEquals("integname12345", filters.get(0).name());
    }

    @Test
    public void translateDescribeWithoutArnOrName_shouldThrow() {
        Assertions.assertThrows(RuntimeException.class, () -> {
            Translator.describeIntegrationsRequest(
                    ResourceModel.builder().build()
            );
        });
    }
}
