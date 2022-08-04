package software.amazon.rds.dbinstance.client;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.Ec2ClientBuilder;
import software.amazon.rds.common.client.BaseSdkClientProvider;

public class Ec2ClientProvider extends BaseSdkClientProvider<Ec2ClientBuilder, Ec2Client> {

    @Override
    public Ec2Client getClient() {
        return setHttpClient(setUserAgent(Ec2Client.builder())).build();
    }
}
