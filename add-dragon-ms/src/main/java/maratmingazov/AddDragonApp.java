package maratmingazov;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;
import software.amazon.awssdk.services.ssm.model.GetParameterResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Hello world!
 *
 */
public class AddDragonApp implements RequestHandler<Dragon, String> {

    private final SsmClient ssmClient = SsmClient.builder().build();
    private final S3Client s3Client = S3Client.builder().build();
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    @Override
    public String handleRequest(Dragon dragon, Context context) {
        addDragon(dragon);
        return "Dragon added";
    }

    private void addDragon(Dragon event) {
        // get object from S3
        ResponseInputStream<GetObjectResponse> object = s3Client.getObject(
                GetObjectRequest.builder()
                                .bucket(getBucketName())
                                .key(getKey())
                                .build());

        // convert input stream to string
        String dragonDataString = convertTextInputStreamToString(object);
        // convert string to List<Dragon> to work with more easily
        // This is because I am trying to avoid doing raw string manipulation
        List<Dragon> dragonDataList = convertStringtoList(dragonDataString);
        // // add dragon to List
        addNewDragonToList(event, dragonDataList);
        uploadObjectToS3(getBucketName(), getKey(), dragonDataList);
    }

    private List<Dragon> convertStringtoList(String dragonData) {
        return gson.fromJson(dragonData, new TypeToken<List<Dragon>>() {
        }.getType());
    }

    private void uploadObjectToS3(String bucketName, String key, List<Dragon> dragons) {
        // uploads the object to S3
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                                                            .bucket(bucketName)
                                                            .key(key)
                                                            .build();

        // converts List<Dragon> to a JSON String before writing
        s3Client.putObject(putObjectRequest, RequestBody.fromString(gson.toJson(dragons)));
    }

    private static void addNewDragonToList(Dragon newDragon, List<Dragon> dragons) {
        dragons.add(newDragon);
    }


    private String convertTextInputStreamToString(InputStream object) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(object));
        String line = null;
        String objectContent = "";
        try {
            while ((line = reader.readLine()) != null) {
                objectContent += line;
            }
        } catch (IOException e) {
            // in the real world please do something with errors
            // do not just log them
            // do as I say not as I do
            System.out.println(e.getMessage());
        }
        return objectContent;
    }

    private String getBucketName() {
        GetParameterRequest getParameterRequest = GetParameterRequest
                .builder()
                .name("dragon_data_bucket_name")
                .withDecryption(false).build();
        GetParameterResponse getParameterResponse = ssmClient.getParameter(getParameterRequest);
        return getParameterResponse.parameter().value();
    }

    private String getKey() {
        GetParameterRequest getParameterRequest = GetParameterRequest
                .builder()
                .name("dragon_data_file_name")
                .withDecryption(false).build();
        GetParameterResponse getParameterResponse = ssmClient.getParameter(getParameterRequest);
        return getParameterResponse.parameter().value();
    }
}
