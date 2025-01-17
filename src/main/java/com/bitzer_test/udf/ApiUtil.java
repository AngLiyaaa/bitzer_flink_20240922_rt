package com.bitzer_test.udf;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import java.security.cert.X509Certificate;
import java.util.Base64;

public class ApiUtil {

    /**
     * 创建一个忽略证书验证的CloseableHttpClient
     *
     * @return CloseableHttpClient
     * @throws Exception 如果创建过程中出现异常
     */
    public static CloseableHttpClient getIgnoeSSLClient() throws Exception {
        SSLContext sslContext = SSLContextBuilder.create().loadTrustMaterial(null, new TrustStrategy() {
            @Override
            public boolean isTrusted(X509Certificate[] x509Certificates, String s) throws java.security.cert.CertificateException {
                return true;
            }
        }).build();
        // 创建httpClient
        CloseableHttpClient client = HttpClients.custom().setSSLContext(sslContext)
                .setSSLHostnameVerifier(new NoopHostnameVerifier()).build();
        return client;
    }

    /**
     * 获取API接口返回的数据
     *
     * @param url API接口的URL
     * @param username 用户名
     * @param password 密码
     * @return API接口返回的数据
     */
    public static String getApiData(String url, String username, String password) {
        try (CloseableHttpClient httpClient = getIgnoeSSLClient()) {
            // 创建HttpGet请求对象
            HttpGet request = new HttpGet(url);

            // 设置基础身份验证的请求头
            String authString = username + ":" + password;
            String authStringEnc = Base64.getEncoder().encodeToString(authString.getBytes());
            request.setHeader("Authorization", "Basic " + authStringEnc);

            // 发送请求并获取响应
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                // 获取响应实体
                HttpEntity entity = response.getEntity();

                // 判断响应实体是否为空
                if (entity != null) {
                    // 将响应实体转换为字符串并返回
                    return EntityUtils.toString(entity);
                } else {
                    // 返回提示信息
                    return "No response entity";
                }
            }
        } catch (Exception e) {
            // 返回异常信息
            return "Error occurred: " + e.getMessage();
        }
    }


//    public static void main(String[] args) {
//        // 明确指定类名和方法名，确保符合 Java 8 规范
//        String url = "https://vhbizpfclb.rot.hec.bitzer.biz/sap/opu/odata/sap/ZCDS_MES_VIEW1_CDS/ZCDS_MES_VIEW1(p_sernr='000000001908103089')/Set?$top=50&$format=json";
//        String username = "PP_ODATA_CN";
//        String password = "b0ZHT?f=6863lATjkHL3";
//        String data = ApiUtil.getApiData(url, username, password);
//        System.out.println(data);
//    }

}