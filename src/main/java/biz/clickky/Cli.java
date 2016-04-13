package biz.clickky;

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
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.File;
import java.io.PrintStream;
import java.util.List;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Cli {
    private String[] args = null;
    private Options options = new Options();

    private AWSCredentials SetCredentials() {
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        }
        catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. Please make sure that your credentials file is at the correct location (~/.aws/credentials), and is in valid format.", e);
        }
        return credentials;
    }

    private AmazonS3 setRegion(String regionType) {
        AmazonS3Client s3 = new AmazonS3Client(this.SetCredentials());
        Region currentRegion = Region.getRegion(Regions.fromName(regionType));
        s3.setRegion(currentRegion);
        return s3;
    }

    public Cli(String[] args) {
        this.args = args;
        this.options.addOption("h", "help", false, "show help.");
        this.options.addOption("r", "region", true, "set region");
        this.options.addOption("b", "bucket", true, "set bucket");
        this.options.addOption("f", "file", true, "file name");
        this.options.addOption("p", "destination-path", true, "destination file path");
        this.options.addOption("u", "upload", false, "upload file");
        this.options.addOption("l", "list", false, "list files");
        this.options.addOption("g", "get", false, "download file");
        this.options.addOption("d", "delete", false, "delete file");
    }

    public void parse() {
        BasicParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(this.options, this.args);
            if (cmd.hasOption("h")) {
                this.help();
            }
            if (cmd.hasOption("u") && cmd.hasOption("r") && cmd.hasOption("b") && cmd.hasOption("f") && cmd.hasOption("p")) {
                AmazonS3 s3 = this.setRegion(cmd.getOptionValue("r"));
                String bucketName = cmd.getOptionValue("b");
                String fileName = cmd.getOptionValue("f");
                String key = cmd.getOptionValue("p");
                try {
                    System.out.println("Uploading a new object " + fileName + " to S3 bucket " + bucketName + " from a file\n");
                    File file = new File(fileName);
                    s3.putObject(new PutObjectRequest(bucketName, key, file));
                }
                catch (AmazonServiceException ase) {
                    this.aseCatcher(ase);
                }
                catch (AmazonClientException ace) {
                    this.aceCatcher(ace);
                }
            } else if (cmd.hasOption("g") && cmd.hasOption("r") && cmd.hasOption("b") && cmd.hasOption("f")) {
                AmazonS3 s3 = this.setRegion(cmd.getOptionValue("r"));
                String bucketName = cmd.getOptionValue("b");
                String fileName = cmd.getOptionValue("f");
                System.out.println("Downloading an object " + fileName);
                try {
                    s3.getObject(new GetObjectRequest(bucketName, fileName), new File(fileName));
                }
                catch (AmazonServiceException ase) {
                    this.aseCatcher(ase);
                }
                catch (AmazonClientException ace) {
                    this.aceCatcher(ace);
                }
            } else if (cmd.hasOption("d") && cmd.hasOption("r") && cmd.hasOption("b") && cmd.hasOption("f")) {
                AmazonS3 s3 = this.setRegion(cmd.getOptionValue("r"));
                String bucketName = cmd.getOptionValue("b");
                String fileName = cmd.getOptionValue("f");
                System.out.println("Deleting an object " + fileName);
                try {
                    s3.deleteObject(bucketName, fileName);
                }
                catch (AmazonServiceException ase) {
                    this.aseCatcher(ase);
                }
                catch (AmazonClientException ace) {
                    this.aceCatcher(ace);
                }
            } else if (cmd.hasOption("l") && cmd.hasOption("r") && cmd.hasOption("b")) {
                AmazonS3 s3 = this.setRegion(cmd.getOptionValue("r"));
                String bucketName = cmd.getOptionValue("b");
                System.out.println("Region: " + cmd.getOptionValue("r"));
                System.out.println("Listing all buckets:");
                try {
                    for (Bucket bucket : s3.listBuckets()) {
                        System.out.println(" - " + bucket.getName());
                    }
                    System.out.println("Listing objects");
                    ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(""));
                    for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                        System.out.println(objectSummary.getKey() + ";" + "(size=" + objectSummary.getSize() + ")");
                    }
                    System.out.println();
                }
                catch (AmazonServiceException ase) {
                    this.aseCatcher(ase);
                }
                catch (AmazonClientException ace) {
                    this.aceCatcher(ace);
                }
            } else {
                this.help();
            }
        }
        catch (ParseException e) {
            this.help();
        }
    }

    private void aseCatcher(AmazonServiceException ase) {
        System.out.println("Caught an AmazonServiceException, which means your request made it to Amazon S3, but was rejected with an error response for some reason.");
        System.out.println("Error Message:    " + ase.getMessage());
        System.out.println("HTTP Status Code: " + ase.getStatusCode());
        System.out.println("AWS Error Code:   " + ase.getErrorCode());
        System.out.println("Error Type:       " + ase.getErrorType());
        System.out.println("Request ID:       " + ase.getRequestId());
    }

    private void aceCatcher(AmazonClientException ace) {
        System.out.println("Caught an AmazonClientException, which means the client encountered a serious internal problem while trying to communicate with S3, such as not being able to access the network.");
        System.out.println("Error Message: " + ace.getMessage());
    }

    private void help() {
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("Main", this.options);
        System.exit(0);
    }
}
