package org.itmo;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {

        HDFSUploader uploader = new HDFSUploader();
        uploader.uploadFilesToHDFS();


    }
}