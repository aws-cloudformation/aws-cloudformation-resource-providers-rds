package software.amazon.rds.dbcluster;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.Ec2ClientBuilder;
import software.amazon.rds.common.client.BaseSdkClientProvider;
import software.amazon.rds.common.annotations.ExcludeFromJacocoGeneratedReport;

public class Ec2ClientProvider extends BaseSdkClientProvider<Ec2ClientBuilder, Ec2Client> {

    @ExcludeFromJacocoGeneratedReport
    @Override
    public Ec2Client getClient() {
        return setHttpClient(setUserAgent(Ec2Client.builder())).build();
    }
}
