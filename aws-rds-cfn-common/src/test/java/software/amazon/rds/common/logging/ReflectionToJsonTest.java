package software.amazon.rds.common.logging;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.commons.lang3.EnumUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

class ReflectionToJsonTest {

    @Test
    void TestBuildJson() throws IllegalAccessException {
        ResourceHandlerRequest<Void> request = new ResourceHandlerRequest<>();
        request.setAwsAccountId("AWS_ACCOUNT_ID");
        request.setClientRequestToken("TOKEN");
        request.setStackId("STACK_ID");
        Map<String, Object> features = Maps.newHashMap();
        features.put("key1", "value1");
        features.put("key2", 5);
        features.put("key3", Lists.newArrayList(1, "saf", true, new ResourceHandlerRequest<>()));
        request.setFeatures(features);
        ReflectionToJson reflectionToJson = new ReflectionToJson();
        String result = reflectionToJson.buildJson(request,0).toString();
        System.out.println(result);
    }
    
    @Test
    void test(){
//        Map<String, Object> features = Maps.newHashMap();
//        System.out.println(ConcreteTypes.Collection.isInstanceOf(features));
//        System.out.println(features instanceof Collection);

        System.out.println(Arrays.stream(SkippedParameters.values()).anyMatch(p -> p.equals("masterUserPassword")));
    }
}