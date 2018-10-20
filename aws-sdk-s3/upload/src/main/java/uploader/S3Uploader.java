package uploader;



import java.util.Properties;
import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;

import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonClientException;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;

import com.amazonaws.internal.StaticCredentialsProvider;

import java.time.LocalDateTime;
import java.time.temporal.ChronoField;



public class S3Uploader {
    //Constants
    public static final String CONFIGPARAMETERNAME_AWS_REGION = "aws_region";
    public static final String CONFIGPARAMETERNAME_AWS_ACCESS_KEY_ID = "aws_access_key_id";
    public static final String CONFIGPARAMETERNAME_AWS_SECRET_ACCESS_KEY = "aws_secret_access_key";
    public static final String CONFIGPARAMETERNAME_AWS_S3_BUCKET = "aws_s3_bucket";
    public static final String CONFIGPARAMETERNAME_AWS_S3_BUCKET_FOLDER = "aws_s3_bucket_folder";
    public static final String CONFIGPARAMETERNAME_UPLOAD_FILE_NAME = "upload_file_name";

    public static final String FORWARD_SLASH_SEPARATOR = "/";
    public static final String EQUALS_SEPARATOR = "=";

    public static final String YEAR = "Year";
    public static final String MONTH = "Month";
    public static final String DAY = "Day";
    public static final String HOUR = "Hour";
    public static final String MINUTE = "Minute";


    public static String awsRegion = null;
    public static String awsAccessKeyID = null;
    public static String awsSecretAccessKey = null;
    public static String awsS3BucketName = null;
    public static String folder = null;
    public static String uploadFileName = null;


    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("Invalid CONFIG_PROPERTIES_FILE supplied.  Please supply valid CONFIG_PROPERTIES_FILE.");
        }

        String configPropertiesFile = args[0];

        init(configPropertiesFile);

        displayInputParameters();

        validateInputParameters();

        File uploadFile = new File(uploadFileName);
        System.out.println("\n uploadFile: " + uploadFile);
        System.out.println(" uploadFile.getCanonicalPath(): " + uploadFile.getCanonicalPath());
        System.out.println(" uploadFile.length(): " + uploadFile.length());


        AWSCredentials awsCredentials = new BasicAWSCredentials(awsAccessKeyID, awsSecretAccessKey);
        System.out.println("\n awsCredentials: " + awsCredentials);

        AmazonS3 amazonS3Client = constructS3client(awsRegion, awsCredentials);
        System.out.println("\n amazonS3Client: " + amazonS3Client);

        TransferManager tm = constructTransferManager(amazonS3Client);
        System.out.println("\n tm: " + tm);

        String baseS3Key = getBaseS3Key(folder);
        System.out.println("\n baseS3Key: " + baseS3Key);

        String fileS3Key = baseS3Key + uploadFile.getName();
        System.out.println("\n fileS3Key: " + fileS3Key);

        uploadMultipartFile(uploadFile, fileS3Key, awsS3BucketName, tm);
    }

    public static void init(String canonicalFilePathName) {
        System.out.println("\n init(canonicalFilePathName)::canonicalFilePathName: " + canonicalFilePathName);
        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream(canonicalFilePathName);

            //Load config.properties file
            prop.load(input);

            awsRegion = prop.getProperty(CONFIGPARAMETERNAME_AWS_REGION);
            awsAccessKeyID = prop.getProperty(CONFIGPARAMETERNAME_AWS_ACCESS_KEY_ID);
            awsSecretAccessKey = prop.getProperty(CONFIGPARAMETERNAME_AWS_SECRET_ACCESS_KEY);
            awsS3BucketName = prop.getProperty(CONFIGPARAMETERNAME_AWS_S3_BUCKET);
            folder = prop.getProperty(CONFIGPARAMETERNAME_AWS_S3_BUCKET_FOLDER);
            uploadFileName = prop.getProperty(CONFIGPARAMETERNAME_UPLOAD_FILE_NAME);
        } catch(IOException ioEx) {
            ioEx.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException ioEx) {
                    ioEx.printStackTrace();
                }
            }
        }
    }

    private static void displayInputParameters() {
        System.out.println("\n awsRegion: " + awsRegion);
        System.out.println(" awsAccessKeyID: " + awsAccessKeyID);
        System.out.println(" awsSecretAccessKey: " + awsSecretAccessKey);
        System.out.println(" awsS3BucketName: " + awsS3BucketName);
        System.out.println(" folder: " + folder);
        System.out.println(" uploadFileName: " + uploadFileName);
    }

    private static void validateInputParameters() throws Exception {
        if (awsRegion == null || awsRegion.length() <= 0) {
            throw new IllegalArgumentException("Invalid awsRegion supplied.  Please provide valid input");
        }

        if (awsAccessKeyID == null || awsAccessKeyID.length() <= 0) {
            throw new IllegalArgumentException("Invalid awsAccessKeyID supplied.  Please provide valid input");
        }

        if (awsSecretAccessKey == null || awsSecretAccessKey .length() <= 0) {
            throw new IllegalArgumentException("Invalid awsSecretAccessKey supplied.  Please provide valid input");
        }

        if (awsS3BucketName == null || awsS3BucketName .length() <= 0) {
            throw new IllegalArgumentException("Invalid awsS3BucketName supplied.  Please provide valid input");
        }

        if (folder == null || folder.length() <= 0) {
            throw new IllegalArgumentException("Invalid folder supplied.  Please provide valid input");
        }

        if (uploadFileName == null || uploadFileName.length() <= 0) {
            throw new IllegalArgumentException("Invalid uploadFileName supplied.  Please provide valid input");
        }

        File file = new File(uploadFileName);
        validate(file);
    }

    public static AmazonS3 constructS3client(String awsRegion, AWSCredentials awsCredentials) {
        if (awsRegion == null) {
            throw new IllegalArgumentException("Invalid AWS region supplied.  Please supply valid AWS region.");
        }

        if (awsCredentials == null) {
            throw new IllegalArgumentException("Invalid awsCredentials supplied.  Please supply valid awsCredentials.");
        }

        Region region = RegionUtils.getRegion(awsRegion);
        if (region == null) {
            throw new IllegalArgumentException("The supplied AWS Region: " + awsRegion + " is invalid.  Please supply valid AWS Region.");
        }
        System.out.println(" region: " + region);

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
                   .withRegion(region.getName())
                   .withCredentials(new StaticCredentialsProvider(awsCredentials))
                   .build();

        return amazonS3;
    }

    public static TransferManager constructTransferManager(AmazonS3 amazonS3Client) {
        TransferManager transferManager = null;

        if (amazonS3Client == null) {
            throw new IllegalArgumentException("Invalid amazonS3Client");
        }

        transferManager = TransferManagerBuilder.standard()
                                                          .withS3Client(amazonS3Client)
                                                          .build();

        return transferManager;
    }

    public static void uploadMultipartFile(File file, String s3Key, String bucketName, TransferManager tm) {
        System.out.println("\n\n ==>> void uploadMultipartFile(File file, String s3Key)");
        System.out.println(" file: " + file);
        System.out.println(" s3Key: " + s3Key);

        final PutObjectRequest request = new PutObjectRequest(bucketName, s3Key, file);
        System.out.println("\n uploadFile::request: " + request);

        request.setGeneralProgressListener(new ProgressListener() {
            @Override
            public void progressChanged(ProgressEvent progressEvent) {
                ProgressEventType progressEventType = progressEvent.getEventType();
                System.out.println("\n progressEventType: " + progressEventType);
                System.out.println(" progressEventType.isTransferEvent(): " + progressEventType.isTransferEvent());
                System.out.println(" progressEventType.isRequestCycleEvent(): " + progressEventType.isRequestCycleEvent());
                System.out.println(" progressEventType.isByteCountEvent(): " + progressEventType.isByteCountEvent());

                if (progressEventType.name().equals(ProgressEventType.TRANSFER_STARTED_EVENT.name())) {
                    System.out.println("\n File transfer started");
                } else if (progressEventType.name().equals(ProgressEventType.TRANSFER_COMPLETED_EVENT.name())) {
                    System.out.println("\n File transfer completed");
                } else if (progressEventType.name().equals(ProgressEventType.TRANSFER_FAILED_EVENT.name())) {
                    System.out.println("\n File transfer failed");
                }

                System.out.println("\n progressEvent.getBytesTransferred(): " + progressEvent.getBytesTransferred());
            }
        });


        Upload upload = tm.upload(request);
        System.out.println("\n upload: " + upload);

        try {
            upload.waitForCompletion();
        } catch(AmazonServiceException e) {
            e.printStackTrace();
        } catch(AmazonClientException e) {
            e.printStackTrace();
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

   public static String getBaseS3Key(String awsS3BucketFolder) {
        StringBuilder builder = new StringBuilder();

        LocalDateTime now = LocalDateTime.now();
        int year = now.getYear();
        int month = now.getMonthValue();
        int day = now.getDayOfMonth();
        int hour = now.getHour();
        int minute = now.getMinute();
        int second = now.getSecond();
        int millis = now.get(ChronoField.MILLI_OF_SECOND); // Note: no direct getter available.

        String monthString = String.format("%02d", month);
        String dayString = String.format("%02d", day);
        String hourString = String.format("%02d", hour);
        String minuteString = String.format("%02d", minute);

        builder.append(awsS3BucketFolder).
                append(FORWARD_SLASH_SEPARATOR).
                append(YEAR).
                append(EQUALS_SEPARATOR).
                append(year).
                append(FORWARD_SLASH_SEPARATOR).
                append(MONTH).
                append(EQUALS_SEPARATOR).
                append(monthString).
                append(FORWARD_SLASH_SEPARATOR).
                append(DAY).
                append(EQUALS_SEPARATOR).
                append(dayString).
                append(FORWARD_SLASH_SEPARATOR).
                append(HOUR).
                append(EQUALS_SEPARATOR).
                append(hourString).
                append(FORWARD_SLASH_SEPARATOR).
                append(MINUTE).
                append(EQUALS_SEPARATOR).
                append(minuteString).
                append(FORWARD_SLASH_SEPARATOR);

        return builder.toString();
    }

    /**
     * Validate a file
     *
     * @param file
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public static void validate(File file) throws IllegalArgumentException, IOException {
        if (file == null) {
            throw new IllegalArgumentException(" Invalid: file is null.");
        }

        String filePathName = file.getCanonicalPath();
        if (filePathName == null) {
            throw new IllegalArgumentException(" Invalid: filePathName is null.");
        }

        if (!file.exists()) {
            throw new IllegalArgumentException(" Invalid because " + filePathName + " does not exist.");
        }

        if (file.isDirectory()) {
            throw new IllegalArgumentException(" Invalid because " + filePathName + " is a directory.");
        }

        if (!file.canRead()) {
            throw new IllegalArgumentException(" Invalid because " + filePathName + " is not read-able.");
        }
    }
}
