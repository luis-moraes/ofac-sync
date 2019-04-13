package com.yellow.sync.loader.ftp;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FTPOfac {

    public static final String OFAC_TEMP = System.getenv("OFAC_TEMP");
    public static final String OFAC_S3_REGION = System.getenv("OFAC_S3_REGION");
    public static final String OFAC_S3_BUCKET_NAME = System.getenv("OFAC_S3_BUCKET_NAME");


    public static final String REMOTE_FILE_2 = "/fac_sdn/sdn_xml.zip";
    public static final String OFAC_SDN_JAVA_TRANSFER_ZIP = OFAC_TEMP+ "/sdn_java_transfer.zip";
    public static final String OFAC_SDN_JAVA_TRANSFER = OFAC_TEMP+"/sdn_java_transfer_unzip";


    private static final String fileObjKeyName = "ofac-sdn.xml";
    private static final String fileName = OFAC_SDN_JAVA_TRANSFER + "/sdn.xml";

    public static void main(String[] args) {
        loadFile();
        uploadToS3();
    }








    public static void uploadToS3() {



        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(OFAC_S3_REGION)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();

            // Upload a text string as a new object.
           // s3Client.putObject(bucketName, stringObjKeyName, "Uploaded String Object");

            // Upload a file as a new object with ContentType and title specified.
            PutObjectRequest request = new PutObjectRequest(OFAC_S3_BUCKET_NAME, fileObjKeyName, new File(fileName));
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("plain/text");
            metadata.addUserMetadata("x-amz-meta-title", "ofac-sdn.xml");
            request.setMetadata(metadata);
            s3Client.putObject(request);
        }
        catch(AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        }
        catch(SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
    }

    public static void loadFile(){
        FTPClient client = new FTPClient();

        try {
            client.connect("ofacftp.treas.gov");
            client.enterLocalPassiveMode();
            client.login("anonymous", "guest");
            client.setFileType(FTP.BINARY_FILE_TYPE);

            int reply = client.getReplyCode();

            if(!FTPReply.isPositiveCompletion(reply)) {
                client.disconnect();
                System.err.println("FTP server refused connection.");
            }
            if (client.isConnected()) {

                boolean success = false;

                File downloadFile2 = new File(OFAC_SDN_JAVA_TRANSFER_ZIP);
                OutputStream outputStream2 = new BufferedOutputStream(new FileOutputStream(downloadFile2));
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                InputStream inputStream = client.retrieveFileStream(REMOTE_FILE_2);
                byte[] bytesArray = new byte[4096];
                int bytesRead = -1;
                while ((bytesRead = inputStream.read(bytesArray)) != -1) {
                    outputStream2.write(bytesArray, 0, bytesRead);
                    //os.write(bytesArray, 0, bytesRead);
                }

                success = client.completePendingCommand();
                if (success) {
                    System.out.println("File #2 has been downloaded successfully.");
                }
                os.close();

                //os.toByteArray();
                outputStream2.close();
                inputStream.close();

            }
            client.logout();
            unZipIt(OFAC_SDN_JAVA_TRANSFER_ZIP,OFAC_SDN_JAVA_TRANSFER);

        } catch (IOException e) {
            e.printStackTrace();
        }  finally {
            try {
                client.disconnect();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Unzip it
     * @param zipFile input zip file
     * @param outputFolder zip file output folder
     */
    public static void unZipIt(String zipFile, String outputFolder){

        byte[] buffer = new byte[1024];

        try{

            //create output directory is not exists
            File folder = new File(outputFolder);
            if(!folder.exists()){
                folder.mkdir();
            }

            //get the zip file content
            ZipInputStream zis =
                    new ZipInputStream(new FileInputStream(zipFile));
            //get the zipped file list entry
            ZipEntry ze = zis.getNextEntry();

            while(ze!=null){

                String fileName = ze.getName();
                File newFile = new File(outputFolder + File.separator + fileName);

                System.out.println("file unzip : "+ newFile.getAbsoluteFile());

                //create all non exists folders
                //else you will hit FileNotFoundException for compressed folder
                new File(newFile.getParent()).mkdirs();

                FileOutputStream fos = new FileOutputStream(newFile);

                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }

                fos.close();
                ze = zis.getNextEntry();
            }

            zis.closeEntry();
            zis.close();


            System.out.println("Done");

        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
}
