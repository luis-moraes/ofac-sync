package com.yellow.sync.loader.ftp;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.yellow.sync")
@EnableAutoConfiguration
public class Main
implements CommandLineRunner {

        public static void main(String[] args) throws Exception {

            SpringApplication.run(Main.class, args);

        }

        //access command line arguments
        @Override
        public void run(String... args) throws Exception {

            System.out.println("Sync Init");
            FTPOfac.loadFile();
            FTPOfac.uploadToS3();
            System.out.println("Sync Finish");
            System.exit(0);
        }



}


