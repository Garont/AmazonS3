package biz.clickky;

import java.io.*;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3 {

    public static void main(String[] args) throws IOException {

        String fileName = "";
        String bucketName = "";
        String operationType = "";
        String regionType = "";

        if (args.length < 1) {
            usage();
        }
        else {
            try {
                regionType = args[0];
                operationType = args[1];
                System.out.println(operationType);
                if (args.length >= 3) {
                    bucketName = args[2];
                }
                if (args.length >= 4) {
                    fileName = args[3];
                }
            } catch (Exception e) {
                usage();
            }
        }

        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        AmazonS3 s3 = new AmazonS3Client(credentials);
        Region currentRegion = Region.getRegion(Regions.fromName(regionType));
        s3.setRegion(currentRegion);
        String key = fileName;

        try {
            if (operationType.equals("-upload")) {
                System.out.println("Uploading a new object " + fileName + " to S3 bucket " + bucketName + " from a file\n");
                File file = new File(fileName);
                s3.putObject(new PutObjectRequest(bucketName, key, file));
            }

            else if(operationType.equals("-download")) {

                System.out.println("Downloading an object");
                s3.getObject(
                   new GetObjectRequest(bucketName, fileName),
                   new File(fileName)
                );
            }

            else if(operationType.equals("-list")) {
                System.out.println("Hardcoded Region: "+currentRegion.getName());
                System.out.println("Listing all buckets:");
                for (Bucket bucket : s3.listBuckets()) {
                    System.out.println(" - " + bucket.getName());
                }

                if (bucketName.equals("")){
                    System.out.println("No default bucket selected. Trying to use first founded");
                    bucketName = s3.listBuckets().get(0).getName();
                }
                System.out.println("Listing objects");
                ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                        .withBucketName(bucketName)
                        .withPrefix(""));
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    System.out.println(" - " + objectSummary.getKey() + "  " +
                            "(size = " + objectSummary.getSize() + ")");
                }
                System.out.println();
            }

            else if(operationType.equals("-delete")) {

                System.out.println("Deleting an object\n");
                s3.deleteObject(bucketName, fileName);

            }
            else{
                usage();
            }

            //System.out.println("Deleting bucket " + bucketName + "\n");
            //s3.deleteBucket(bucketName);
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    static private void usage(){
        System.out.println("usage: java -jar S3.jar <-upload|-list|-download|-delete> Region BucketName archive.tar.gz(optional)\nExample java -jar S3.jar eu-central-1 -list");
        System.exit(1);
    }
}
