package com.tcrb.edgeapi.batchprocessor.job;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.utils.IoUtils;

import java.security.MessageDigest;
import java.util.Base64;

class UploadFileToS3Test {

    @Test
    void uploadFileToS3_shouldSuccess() throws Exception {

        // Bucket name the file will be uploaded to.
        String bucketName = "test-manage-folder-s3";
        Resource inputFile = new ClassPathResource("/s3/edge_test_upload.txt");
        // Specify profile name to be used with the request.
        ProfileCredentialsProvider provider = ProfileCredentialsProvider.create("default");
        // File object key that will be created on s3 including directory path.
        // Example: src/customer-list.txt will create customer-list.txt in src directory from bucket root.
        String objectKey = "revolving-loan/source/edge-test-upload-file.txt";
        // Create MD5 hash for s3 to verify uploaded file integrity.
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.update(IoUtils.toByteArray(inputFile.getInputStream()));
        byte[] digest = md5.digest();
        String hash = new String(Base64.getEncoder().encode(digest));
        System.out.println("MD5: " + hash);

        S3Client s3 = S3Client.builder()
//                .region(Region.of("ap-southeast-1")) // Set region. No need to set this if already set in profile.
                .credentialsProvider(provider) // Config with specific profile name.
                .build();

        PutObjectRequest request = PutObjectRequest.builder()
                .contentMD5(hash)
                .bucket(bucketName)
                .key(objectKey).build();

        PutObjectResponse response = s3.putObject(request, inputFile.getFile().toPath());

        System.out.println("Put object response: ");
        System.out.println(response.toString());

    }
}