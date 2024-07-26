package infore.SDE.storage;

import infore.SDE.synopses.AMSsynopsis;
import infore.SDE.synopses.Synopsis;
import org.codehaus.jettison.json.JSONString;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;
import java.util.List;


public class StorageManager {

    private static final String BUCKET_NAME = "sde-e";
    private static final Region REGION = Region.EU_NORTH_1;
    private static final String AWS_ACCESS_KEY_ID = "AKIAS2XH2Y6R6OHOWKO4";
    private static final String AWS_SECRET_ACCESS_KEY = "";

    private static final S3Client s3 =  S3Client.builder()
            .region(REGION)
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)))
            .build();

    /**
     * This method serializes a Synopsis object irrelevant of type (AMS, LSH, CM etc)
     * and saves the serialization output to an S3 bucket.
     *
     * @param synopsis Any synopsis object implementing the Serializable interface and
     *                 both of the writeObject() and readObject() methods.
     *
     * @param storageKeyName The name/key of the file in the S3 bucket to store the synopsis
     *                       state under
     */
    public static void serializeSynopsisToS3(Synopsis synopsis, String storageKeyName){
        File tempFile = new File(storageKeyName);
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tempFile))) {
            oos.writeObject(synopsis);
            s3.putObject(PutObjectRequest.builder().bucket(BUCKET_NAME).key(storageKeyName).build(),
                    RequestBody.fromFile(tempFile));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            tempFile.delete();
        }
    }


    /**
     * This method looks for an entry inside the S3 bucket based on the name/key given as argument. It returns
     * a deserialized synopsis object of the specified synopsis class.
     * @param storageKeyName The key to look for inside the S3 bucket
     * @param synopsisClass The class of the synopsis object to return
     * @return Synopsis object of specific synopsis class not generic T
     */
    public static <T extends Synopsis> T deserializeSynopsisFromS3(String storageKeyName, Class<T> synopsisClass) {
        File tempFile = new File(storageKeyName);
        try {
            s3.getObject(GetObjectRequest.builder().bucket(BUCKET_NAME).key(storageKeyName).build(),
                    ResponseTransformer.toFile(tempFile.toPath()));

            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(tempFile))) {
                Object obj = ois.readObject();
                if (synopsisClass.isInstance(obj)) {
                    return synopsisClass.cast(obj);
                } else {
                    throw new ClassNotFoundException("Deserialized object is not of type " + synopsisClass.getName());
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                return null;
            }
        } finally {
            tempFile.delete();
        }
    }


    /**
     * This method serializes a Synopsis object irrelevant of type (AMS, LSH, CM etc)
     * and saves the serialization output to an S3 bucket.
     *
     * @param synopsis Any synopsis object implementing the Serializable interface and
     *                 both of the writeObject() and readObject() methods.
     *
     * @param storageKeyName The name/key of the file in the S3 bucket to store the synopsis
     *                       state under
     */
    public static void storeSnapshotOfFormatInS3(Synopsis synopsis, String storageKeyName){
        File tempFile = new File(storageKeyName);
        AMSsynopsis amsSynopsis = (AMSsynopsis) synopsis;
        try {
            String json = amsSynopsis.toJson();

            // Write JSON to a temporary file
            Files.write(Paths.get(tempFile.getAbsolutePath()), json.getBytes());

            // Upload the file to S3
            s3.putObject(PutObjectRequest.builder().bucket(BUCKET_NAME).key(storageKeyName).build(),
                    RequestBody.fromFile(tempFile));

            // Delete the temporary file
            Files.delete(tempFile.toPath());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Method to put (or overwrite) object with key=storageKeyName using content for
     * its value, in an S3 bucket.
     * @param storageKeyName The key of the object
     * @param content The value is String format
     */
    public static void putObjectToS3(String storageKeyName, String content) {

        try {
            // Upload the file to S3
            s3.putObject(PutObjectRequest.builder().bucket(BUCKET_NAME).key(storageKeyName).build(),
                    RequestBody.fromBytes(content.getBytes()));
            System.out.println("Put object under " + storageKeyName+ " in bucket " + BUCKET_NAME);
        } catch (S3Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to get the content of a specific object using its key from an
     * S3 bucket.
     * @param storageKeyName The key of the object
     * @return The content of the object in String format
     */
    public static String getObjectFromS3(String storageKeyName) {
        StringBuilder content = new StringBuilder();

        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(storageKeyName)
                    .build();

            ResponseInputStream<?> response = s3.getObject(getObjectRequest);

            BufferedReader reader = new BufferedReader(new InputStreamReader(response, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line+"\n");
            }

        } catch (S3Exception | IOException e) {
            System.err.println(e.getMessage());
        }

        return content.toString();
    }


    public static String getSynopsisMetadata(){

        return null;
    }

    public static String getSynopsisLatestVersion(){

        return null;
    }

    public static List<String> getSynopsisVersions(){

        return null;
    }

    public static void buildOrUpdateSynopsisMetadata(){

    }

    /**
     * Returns in a List of String with details about the versions of the file corresponding
     * to the given key from a versioned S3 bucket
     * @param key The key/name of the S3 entry for which the versions are requested
     * @return List of Strings containing entries for all the version with ID, timestamp and size
     */
    public static List<String> getFileVersions(String key) {
        Region region = Region.US_WEST_2; // Replace with your region

        try {
            ListObjectVersionsRequest request = ListObjectVersionsRequest.builder()
                    .bucket(BUCKET_NAME)
                    .prefix(key)
                    .build();

            ListObjectVersionsResponse response = s3.listObjectVersions(request);

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss O");

            return response.versions().stream()
                    .map(version -> {
                        String formattedDate = ZonedDateTime.ofInstant(version.lastModified(), ZoneId.systemDefault()).format(formatter);
                        return String.format("Version ID: %s, Snapshot at: %s, Size: %d bytes",
                                version.versionId(),
                                formattedDate,
                                version.size());
                    })
                    .collect(Collectors.toList());

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            return null;
        } finally {
            s3.close();
        }
    }
}
