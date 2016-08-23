package com.ran;

import java.io.File;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class S3OperationIAMRole {

	private String bucketName = "";
	private String fileName = "test.xml";
	private String keyName = "input/"+fileName;
	
	public static void main(String[] args) {
		S3OperationIAMRole s3put = new S3OperationIAMRole();
		s3put.process();
	}

	private void process() {
		AmazonS3 s3Client = getClient();
		uploadFile(s3Client);
	}

	private AmazonS3 getClient() {
		return new AmazonS3Client();
	}

	private void uploadFile(AmazonS3 s3Client){
		try {
            System.out.println("Uploading a file to S3 bucket\n");
            File file = new File(fileName);
            s3Client.putObject(new PutObjectRequest(
            		                 bucketName, keyName, file));
            System.out.println("file got uploaded-----------");
         } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
            		"means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
            		"means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
	}
}
