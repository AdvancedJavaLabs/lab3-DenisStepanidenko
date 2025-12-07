package org.example;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws IOException {

        HDFSUploader uploader = new HDFSUploader();
        uploader.uploadFilesToHDFS();


    }
}