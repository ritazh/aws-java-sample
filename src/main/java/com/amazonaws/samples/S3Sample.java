/*
 * Copyright 2010-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.samples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.util.IOUtils;

/**
 * This sample demonstrates how to make basic requests to Amazon S3 using
 * the AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon S3. For more information on
 * Amazon S3, see http://aws.amazon.com/s3.
 * <p>
 * <b>Important:</b> Be sure to provide your access credentials in
 * [TODO: Add file path for configs] before you try to run this sample.
 */
public class S3Sample {

    public static void main(String[] args) throws IOException {

        String s3proxydemoCredentialKey = null;
        s3proxydemoCredentialKey = System.getProperty("key");
        String s3proxydemoCredentialSecret = null;
        s3proxydemoCredentialSecret = System.getProperty("secret");
        String s3proxydemoBucketname = null;
        s3proxydemoBucketname = System.getProperty("bucketname");
        String s3proxydemoEndpint = null;
        s3proxydemoEndpint = System.getProperty("s3endpoint");
        String s3proxydemoFilepath = null;
        s3proxydemoFilepath = System.getProperty("filepath");


        if (s3proxydemoCredentialKey == null || s3proxydemoCredentialKey.length() == 0) {
            System.err.println("-Dkey cannot be empty");
            System.exit(1);
        }
        if (s3proxydemoCredentialSecret == null || s3proxydemoCredentialSecret.length() == 0) {
            System.err.println("-Dsecret cannot be empty");
            System.exit(1);
        }
        if (s3proxydemoBucketname == null || s3proxydemoBucketname.length() == 0) {
            System.err.println("-Dbucketname cannot be empty");
            System.exit(1);
        }
        if (s3proxydemoEndpint == null || s3proxydemoEndpint.length() == 0) {
            System.err.println("-Ds3endpoint cannot be empty");
            System.exit(1);
        }
        if (s3proxydemoFilepath == null || s3proxydemoFilepath.length() == 0) {
            System.err.println("-Dfilepath cannot be empty");
            System.exit(1);
        }

    	// To overcome etag issue
    	System.setProperty("com.amazonaws.services.s3.disablePutObjectMD5Validation", "1"); 
    	
    	AWSCredentials credentials = new BasicAWSCredentials(s3proxydemoCredentialKey, s3proxydemoCredentialSecret);
    	ClientConfiguration clientConfig = new ClientConfiguration().withSignerOverride("S3SignerType");
    	clientConfig.setProtocol(Protocol.HTTP);
    	AmazonS3 s3 = new AmazonS3Client(credentials, clientConfig);

        String bucketName = s3proxydemoBucketname;
        String key = "MyObjectKey";
        String keylarge = "MyObjectKeyUploadPartRequest.zip";
        String keylargeTM = "MyObjectKeyTransferManager.zip";

        System.out.println("===========================================");
        System.out.println("Getting Started with S3 Java Test Client");
        System.out.println("===========================================\n");
    	
        s3.setEndpoint(s3proxydemoEndpint);
    	 
        try {
            /*
             * Create a new S3 bucket - Amazon S3 bucket names are globally unique,
             * so once a bucket name has been taken by any user, you can't create
             * another bucket with that same name.
             *
             * You can optionally specify a location for your bucket if you want to
             * keep your data closer to your applications or users.
             */
            try{ 
                System.out.println("Creating bucket " + bucketName + "\n");
                s3.createBucket(bucketName);
            }catch(AmazonServiceException ase) {
                System.out.println("Error Message:    " + ase.getMessage());
                System.out.println("HTTP Status Code: " + ase.getStatusCode());
                System.out.println("AWS Error Code:   " + ase.getErrorCode());
                if(ase.getErrorCode().compareTo("BucketAlreadyOwnedByYou") == 0){
                    System.out.println("BucketAlreadyOwnedByYou Skipping this...");
                }
            }
            /*
             * List the buckets in your account
             */
            System.out.println("Listing buckets");
            for (Bucket bucket : s3.listBuckets()) {
                System.out.println(" - " + bucket.getName());
            }
            System.out.println();

            /*
             * Upload an object to your bucket - You can easily upload a file to
             * S3, or upload directly an InputStream if you know the length of
             * the data in the stream. You can also specify your own metadata
             * when uploading to S3, which allows you set a variety of options
             * like content-type and content-encoding, plus additional metadata
             * specific to your applications.
             */
            System.out.println("Uploading a new object to S3 from a file\n");
            s3.putObject(new PutObjectRequest(bucketName, key, createSampleFile()));

            /*
             * Download an object - When you download an object, you get all of
             * the object's metadata and a stream from which to read the contents.
             * It's important to read the contents of the stream as quickly as
             * possibly since the data is streamed directly from Amazon S3 and your
             * network connection will remain open until you read all the data or
             * close the input stream.
             *
             * GetObjectRequest also supports several other options, including
             * conditional downloading of objects based on modification times,
             * ETags, and selectively downloading a range of an object.
             */
            System.out.println("Downloading an object");
            S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
            System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
            displayTextInputStream(object.getObjectContent());
            
            /*
             * List objects in your bucket by prefix - There are many options for
             * listing the objects in your bucket.  Keep in mind that buckets with
             * many objects might truncate their results when listing their objects,
             * so be sure to check if the returned object listing is truncated, and
             * use the AmazonS3.listNextBatchOfObjects(...) operation to retrieve
             * additional results.
             */
            System.out.println("Listing objects");
            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withPrefix("My"));
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + "  " +
                        "(size = " + objectSummary.getSize() + ")");
            }
            System.out.println();
            
            /*
             * Multipart-upload an object to your bucket using UploadPartRequest
             * 
             */
            System.out.println("Uploading a large file to storage using multipart upload\n");
        	String filePath = s3proxydemoFilepath; 
        	
        	 //Using UploadPartRequest
            int size = 10000000;
            
            InitiateMultipartUploadRequest initRequest =
                    new InitiateMultipartUploadRequest(bucketName, keylarge);
            InitiateMultipartUploadResult initResponse =
                    s3.initiateMultipartUpload(initRequest);
            String uploadId = initResponse.getUploadId();
            InputStream is = new FileInputStream(filePath);
            File file = new File (filePath);
           
            ByteSource byteSource = Files.asByteSource(file);
            long fileSize = byteSource.size();
            System.out.println("File size: " + fileSize);
            
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("application/unknown");
            metadata.setContentLength(size);
            UploadPartRequest uploadRequest1 = new UploadPartRequest()
                    .withBucketName(bucketName).withKey(keylarge)
                    .withUploadId(uploadId).withPartNumber(1)
                    .withInputStream(byteSource.openStream())
                    .withObjectMetadata(metadata)
                    .withPartSize(size);

            UploadPartResult uploadPartResult1 = s3.uploadPart(uploadRequest1);
            PartETag partETag1 = uploadPartResult1.getPartETag();
            System.out.println(partETag1.getETag());

            metadata.setContentLength(fileSize-size);
            UploadPartRequest uploadRequest2 = new UploadPartRequest()
                    .withBucketName(bucketName).withKey(keylarge)
                    .withUploadId(uploadId).withPartNumber(2)
                    .withInputStream(byteSource
                            .slice(size, fileSize - size).openStream())
                    .withObjectMetadata(metadata)
                    .withPartSize(fileSize-size);

            UploadPartResult uploadPartResult2 = s3.uploadPart(uploadRequest2);
            PartETag partETag2 = uploadPartResult2.getPartETag();
            System.out.println(partETag2.getETag());
            
            List<PartETag> partETagList = new ArrayList<PartETag>();
            partETagList.add(new PartETag(1, partETag1.getETag()));
            partETagList.add(new PartETag(2, partETag2.getETag()));

            CompleteMultipartUploadRequest completeRequest = new
                    CompleteMultipartUploadRequest(
                    bucketName,
                    keylarge,
                    uploadId,
                    partETagList);
            s3.completeMultipartUpload(completeRequest);
            
            /*
             * List objects in your bucket by prefix - There are many options for
             * listing the objects in your bucket.  Keep in mind that buckets with
             * many objects might truncate their results when listing their objects,
             * so be sure to check if the returned object listing is truncated, and
             * use the AmazonS3.listNextBatchOfObjects(...) operation to retrieve
             * additional results.
             */
            System.out.println("Listing objects");
            objectListing = s3.listObjects(new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withPrefix("My"));
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + "  " +
                        "(size = " + objectSummary.getSize() + ")");
            }
            System.out.println();
           
            /*
             * Multipart-upload an object to your bucket using TransferManager
             * 
             */
        	TransferManager tm = new TransferManager(s3);      
        	System.out.println("Using TransferManager...");
          
        	ObjectMetadata metadataTM = new ObjectMetadata();
        	metadataTM.setContentType("application/unknown");
	          File fileTM = new File(filePath);
	          InputStream isTM = new FileInputStream(filePath);
	          metadataTM.setContentLength(fileTM.length());
          
	        final Upload upload = tm.upload(bucketName, keylargeTM, isTM, metadataTM);
        	
            System.out.println("uploading...");
            if (upload.isDone() == false) {
                System.out.println("Transfer: " + upload.getDescription());
                System.out.println("  - State: " + upload.getState());
            }
            
            upload.addProgressListener(new ProgressListener() {
                // This method is called periodically as your transfer progresses
                public void progressChanged(ProgressEvent progressEvent) {
                    System.out.print(".");
                }
            });
            
            try {
            	// Or you can block and wait for the upload to finish
            	upload.waitForCompletion();
            	System.out.println("Upload complete.");
            	
            } catch (AmazonClientException amazonClientException) {
            	System.out.println("Unable to upload file, upload was aborted.");
            	amazonClientException.printStackTrace();
            } catch (InterruptedException interruptException) {
            	System.out.println("Unable to upload file, upload was interrupted.");
            	interruptException.printStackTrace();
            }


            /*
             * List objects in your bucket by prefix - There are many options for
             * listing the objects in your bucket.  Keep in mind that buckets with
             * many objects might truncate their results when listing their objects,
             * so be sure to check if the returned object listing is truncated, and
             * use the AmazonS3.listNextBatchOfObjects(...) operation to retrieve
             * additional results.
             */
            System.out.println("Listing objects");
            objectListing = s3.listObjects(new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withPrefix("My"));
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + "  " +
                        "(size = " + objectSummary.getSize() + ")");
            }
            System.out.println();

            /*
             * Delete an object - Unless versioning has been turned on for your bucket,
             * there is no way to undelete an object, so use caution when deleting objects.
             */
            System.out.println("Deleting objects...\n");
            s3.deleteObject(bucketName, key);
            s3.deleteObject(bucketName, keylarge);
            s3.deleteObject(bucketName, keylargeTM);

            /*
             * Delete a bucket - A bucket must be completely empty before it can be
             * deleted, so remember to delete any objects from your buckets before
             * you try to delete them.
             */
            System.out.println("Deleting bucket " + bucketName + "\n");
            s3.deleteBucket(bucketName);
            System.out.println("Sucessfully done!");
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

    /**
     * Creates a temporary file with text data to demonstrate uploading a file
     * to Amazon S3
     *
     * @return A newly created temporary file with text data.
     *
     * @throws IOException
     */
    private static File createSampleFile() throws IOException {
        File file = File.createTempFile("aws-java-sdk-", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("01234567890112345678901234\n");
        writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
        writer.write("01234567890112345678901234\n");
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.close();

        return file;
    }

    /**
     * Displays the contents of the specified input stream as text.
     *
     * @param input
     *            The input stream to display as text.
     *
     * @throws IOException
     */
    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

}
