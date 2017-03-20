package com.pingan.pbear.util;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLDecoder;


/**
 * http请求类
 * 目前只简单封装了get接口
 *
 * Created by ligang on 17/2/10.
 */
public class HttpRequestUtil {
    private static Logger logger = LoggerFactory.getLogger(HttpRequestUtil.class);    //日志记录

    /**
     * 发送get请求
     * @param url    路径
     * @return 返回结果
     */
    public static String httpGet(String url){
        //get请求返回结果
        String strResult = "";
        try {
            //发送get请求
            CloseableHttpClient client = HttpClients.createDefault();
            HttpGet request = new HttpGet(url);
            HttpResponse response = client.execute(request);

            /*请求发送成功，并得到响应*/
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                /*读取服务器返回过来的json字符串数据*/
                strResult = EntityUtils.toString(response.getEntity());
                url = URLDecoder.decode(url, "UTF-8");
            } else {
                logger.error("get请求提交失败:" + url);
            }
        } catch (IOException e) {
            logger.error("get请求提交失败:" + url, e);
        }
        return strResult;
    }

    public static void main(String[] args) {
        String url = "http://172.18.241.7/pbear_etl_api/dbstr/get/DBSTR_ABC_DBSTR";
        System.out.println(httpGet(url));
    }
}
