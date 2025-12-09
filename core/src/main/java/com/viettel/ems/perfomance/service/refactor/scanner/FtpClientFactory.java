package com.viettel.ems.perfomance.service.refactor.scanner;

import com.viettel.ems.perfomance.object.FtpServerObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.io.IOException;

@Slf4j
public class FtpClientFactory extends BasePooledObjectFactory<FTPClient> {

    private final FtpServerObject config;

    public FtpClientFactory(FtpServerObject config) {
        this.config = config;
    }

    @Override
    public FTPClient create() throws Exception {
        FTPClient ftpClient = new FTPClient();
        
        // Tối ưu TCP
        ftpClient.setControlEncoding("UTF-8");
        ftpClient.setConnectTimeout(10000); // 10s connect timeout
        ftpClient.setDefaultTimeout(60000); // 60s default timeout
        ftpClient.setDataTimeout(60000);    // 60s data transfer timeout
        
        // Tăng buffer size để download nhanh hơn
        ftpClient.setBufferSize(1024 * 1024 * 2); // 2MB Buffer
        
        try {
            ftpClient.connect(config.getHost(), config.getPort());
            int reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftpClient.disconnect();
                throw new IOException("FTP Server refused connection. Reply code: " + reply);
            }

            if (!ftpClient.login(config.getUsername(), config.getPassword())) {
                ftpClient.disconnect();
                throw new IOException("FTP Login failed.");
            }

            // Cấu hình quan trọng cho hiệu năng và độ ổn định
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            ftpClient.enterLocalPassiveMode(); // Bắt buộc trong môi trường Docker/K8s/NAT
            ftpClient.setControlKeepAliveTimeout(300); // Giữ kết nối sống
            ftpClient.setRemoteVerificationEnabled(false);

        } catch (Exception e) {
            if (ftpClient.isConnected()) {
                try { ftpClient.disconnect(); } catch (Exception ignored) {}
            }
            log.error("Failed to create FTP connection to {}: {}", config.getHost(), e.getMessage());
            throw e;
        }
        return ftpClient;
    }

    @Override
    public PooledObject<FTPClient> wrap(FTPClient ftpClient) {
        return new DefaultPooledObject<>(ftpClient);
    }

    @Override
    public void destroyObject(PooledObject<FTPClient> p) {
        FTPClient client = p.getObject();
        if (client != null && client.isConnected()) {
            try {
                client.logout();
                client.disconnect();
            } catch (Exception ignored) {}
        }
    }

    @Override
    public boolean validateObject(PooledObject<FTPClient> p) {
        FTPClient client = p.getObject();
        try {
            // Gửi lệnh NOOP để check kết nối còn sống không.
            // Nhanh và nhẹ hơn việc đợi timeout khi thực sự truyền dữ liệu.
            return client.isConnected() && client.sendNoOp();
        } catch (Exception e) {
            return false;
        }
    }
}