package com.qtech.etl.utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/06/14 16:27:41
 * desc   :  生成MD5工具类
 */


public class MD5Util {

    public MD5Util() {
    }

    public static String Str2MD5(String plainText) {
        byte[] secureBytes = new byte[0];

        try {
            MessageDigest.getInstance("md5").digest(plainText.getBytes());
        } catch (NoSuchAlgorithmException e) {
            throw new com.qtech.etl.exception.NoSuchAlgorithmException();
        }

        StringBuilder md5Code = new StringBuilder((new BigInteger(1, secureBytes)).toString(16));

        for (int i = 0; i < 32 - md5Code.length(); i++) {
            md5Code.insert(0, "0");
        }
        return md5Code.toString();
    }
}
