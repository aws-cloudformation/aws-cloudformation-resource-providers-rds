package software.amazon.rds.dbsubnetgroup;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.rds.model.CreateDbSubnetGroupRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbSubnetGroupRequest;
import software.amazon.rds.common.handler.Tagging;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
public class TranslatorTest {

    @Test
    public void createDBSubnetGroupTrimsWhitespaces() {
        final CreateDbSubnetGroupRequest request = Translator.createDbSubnetGroupRequest(ResourceModel.builder()
                .subnetIds(ImmutableList.of(
                        " \t\n\r\nsubnet1",
                        "subnet2 \t\n\r\n",
                        " \t\n\r\nsubnet3 \t\n\r\n")).build(),
                Tagging.TagSet.builder().build());

        assertThat(request.subnetIds()).containsExactly("subnet1", "subnet2", "subnet3");
    }

    @Test
    public void modifyDBSubnetGroupTrimsWhitespaces() {
        final ModifyDbSubnetGroupRequest request = Translator.modifyDbSubnetGroupRequest(ResourceModel.builder()
                .subnetIds(ImmutableList.of(
                        " \t\n\r\nsubnet1",
                        "subnet2 \t\n\r\n",
                        " \t\n\r\nsubnet3 \t\n\r\n")).build());

        assertThat(request.subnetIds()).containsExactly("subnet1", "subnet2", "subnet3");
    }
}
