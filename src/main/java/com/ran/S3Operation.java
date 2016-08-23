package com.ran;

import java.io.File;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class S3Operation {

	//please provide valid credentials and bucketname
	String accessKey = "";
	String secretKey = "";
	String bucketName = "";
	
	String fileName = "test.txt";
	String keyName = "input/"+fileName;
	
	public static void main(String[] args) {
		S3Operation s3put = new S3Operation();
		s3put.process();
	}

	private void process() {
		AmazonS3 s3Client = getClient();
		uploadFile(s3Client);
		
		copyFile(s3Client);
		try {
			Thread.currentThread().sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		deleteFile(s3Client);
		
	}

	private void deleteFile(AmazonS3 s3Client) {
		System.out.println("deleting the file---");
		s3Client.deleteObject(bucketName, keyName);
		System.out.println("file got deleted-----");
	}

	private void copyFile(AmazonS3 s3Client) {
		String outputKey = "output/processed-"+keyName;
		try {
            // Copying object
            CopyObjectRequest copyObjRequest = new CopyObjectRequest(
            		bucketName, keyName, bucketName, outputKey);
            System.out.println("Copying object.");
            s3Client.copyObject(copyObjRequest);
            System.out.println("file got copied-----------");
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, " +
            		"which means your request made it " + 
            		"to Amazon S3, but was rejected with an error " +
                    "response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, " +
            		"which means the client encountered " +
                    "an internal error while trying to " +
                    " communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
		
	}

	private AmazonS3 getClient() {
		BasicAWSCredentials credential = new BasicAWSCredentials(accessKey, secretKey);
		return new AmazonS3Client(credential);
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
