package com.ytfs.service;

public class Test {

    public static void main(String[] args) throws Exception {
        String newfilepath = "ewwew.jar";
        int index = newfilepath.lastIndexOf(".");
        if (index > 0) {
            newfilepath= newfilepath.substring(0,index)+".bak"+newfilepath.substring(index);
            System.out.println(newfilepath);
        }
    }

}
