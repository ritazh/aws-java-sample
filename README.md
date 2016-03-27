# AWS SDK for Java Sample Project

A simple Java application illustrating usage of the AWS S3 SDK for Java.

## Requirements

The only requirement of this application is Maven. All other dependencies can
be installed by building the maven package:
    
    mvn package

## Running the S3 sample

### Prerequisites
#### Azure
If you are using Microsoft Azure, you will need to go to the Microsoft Azure portal, Follow this [guide](https://azure.microsoft.com/en-us/documentation/articles/storage-create-storage-account/#create-a-storage-account) to create a storage account and to get the key and secret for your storage account. You can also use the [azure cli](https://azure.microsoft.com/en-us/documentation/articles/storage-azure-cli/) to create and retrieve storage account information.

#### AWS
If you are using AWS S3, follow this [guide](http://docs.aws.amazon.com/AmazonS3/latest/gsg/SigningUpforS3.html) to signup for S3 account and to retrieve access key and secret. The S3 documentation provides a good overview
of the [restrictions for bucket names](http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html).

This sample application connects to an S3 API compatible storage backend. 
- It can create a container (on Microsoft Azure) or a bucket (on AWS S3).
- It can list all containers or all buckets in your storage account.
- It can create an example file to upload to a container/bucket.
- It can upload a large file using multipart upload UploadPartRequest.
- It can upload a large file using multipart upload TransferManager.
- It can list all objects in a container/bucket.
- It can delete all objects in a container/bucket.
- It can delete a container/bucket.


All you need to do is run it with:

```
mvn clean compile exec:java -Dkey=<STORAGE ACCOUNT KEY> -Dsecret=<STORAGE ACCOUNT SECRET> -Dbucketname=<BUCKETNAME OR CONTAINER NAME TO CREATE> -Ds3endpoint=<S3PROXY URL> -Dfilepath=<FULLPATH LOCATION OF DEMO.ZIP FILE>/demo.zip
```

For `<S3PROXY URL>`, here are some options depending on where your S3Proxy instance is hosted. For more information of how to host S3Proxy, checkout the [s3proxydocker project](https://github.com/ritazh/s3proxydocker).

- S3Proxy running as a container app on Cloud Foundry: `-Ds3endpoint=<PUBLIC-IP-OF-CLOUD-FOUNDRY-INSTANCE>`
- S3Proxy running as a container app on Dokku: `-Ds3endpoint=s3proxy.<PUBLIC-IP-OF-DOKKU-INSTANCE>`
- S3Proxy running as a container app locally using Docker machine: `-Ds3endpoint=<docker-machine IP:8080>`
- S3Proxy running as a jetty app locally on port 8000: `-Ds3endpoint=:http://localhost:8000`

If you are hosting S3Proxy on a local machine, ensure you have completed the [Updating Hosts File](https://github.com/ritazh/s3proxydocker#updating-hosts-file) step so you can access subdomains over that ip.

## License

This sample application is distributed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

