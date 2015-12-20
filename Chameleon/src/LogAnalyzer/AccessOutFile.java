package com.xhahn.gmm.LogAnalyzer;

import java.util.regex.Pattern;

/**
 * User: xhahn
 * Data: 2015/8/31/0031
 * Time: 19:03
 */
public class AccessOutFile {

    public static final String space = " ";
    private int[] coordinates = new int[12];
    public AccessOutFile(String[] coordinates){
        for(int i=0;i<12;i++){
            this.coordinates[i] = Integer.parseInt(coordinates[i]);
        }
    }

    public int[] getCoordinates(){
        return coordinates;
    }

    public static AccessOutFile parseAccessOutFile(String s) {
        final Pattern pattern = Pattern.compile(space);
        String[] coordinates = pattern.split(s);
        return new AccessOutFile(coordinates);
    }
}
