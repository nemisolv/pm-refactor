package com.viettel.ems.perfomance.service.refactor.scanner;

import com.viettel.ems.perfomance.object.CounterDataObject;
import com.viettel.ems.perfomance.object.FtpServerObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class FtpService {

    private final GenericObjectPool<FTPClient> clientPool;
    private final FtpServerObject serverConfig;

    public FtpService(FtpServerObject config, int maxPoolSize) {
        this.serverConfig = config;

        // Cấu hình Pool
        GenericObjectPoolConfig<FTPClient> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(maxPoolSize); // Số kết nối tối đa (tương ứng số worker threads)
        poolConfig.setMaxIdle(maxPoolSize / 2);
        poolConfig.setMinIdle(2);

        // Kiểm tra kết nối khi lấy ra dùng (tránh lỗi Pipe closed)
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofMinutes(1));
        poolConfig.setBlockWhenExhausted(true); // Chờ nếu hết kết nối
        poolConfig.setMaxWait(Duration.ofSeconds(30)); // Chờ tối đa 30s

        this.clientPool = new GenericObjectPool<>(new FtpClientFactory(config), poolConfig);
    }

    /**
     * Tối ưu 1: Dùng listNames thay vì listFiles.
     * listFiles phải lấy cả metadata (size, date, permissions) -> Rất chậm.
     * listNames chỉ lấy tên -> Nhanh gấp 5-10 lần.
     */
    public List<String> getListFileNames(String path) {
        FTPClient client = null;
        try {
            client = clientPool.borrowObject();
            // Không cần changeWorkingDirectory, dùng đường dẫn tuyệt đối an toàn hơn
            String[] names = client.listNames(path);

            if (names == null || names.length == 0) {
                return Collections.emptyList();
            }

            List<String> result = new ArrayList<>();
            for (String name : names) {
                // Lọc cơ bản . và ..
                if (!name.equals(".") && !name.equals("..") && name.contains(".")) {
                    // listNames trả về full path hoặc filename tùy server,
                    // ta chỉ lấy filename
                    String cleanName = name;
                    if (name.contains("/")) {
                        cleanName = name.substring(name.lastIndexOf("/") + 1);
                    }
                    result.add(cleanName);
                }
            }
            return result;

        } catch (Exception e) {
            log.error("Error listing files in {}: {}", path, e.getMessage());
            invalidate(client); // Đánh dấu client lỗi để pool hủy bỏ
            client = null;
            return Collections.emptyList();
        } finally {
            returnToPool(client);
        }
    }

    /**
     * Download file về RAM (ByteBuffer) để xử lý
     */
    public CounterDataObject downloadFile(String path, String fileName) {
        FTPClient client = null;
        InputStream is = null;
        try {
            client = clientPool.borrowObject();
            String fullPath = path + "/" + fileName;

            // Gửi lệnh RETR
            is = client.retrieveFileStream(fullPath);

            if (is == null) {
                // In ra lý do tại sao Server từ chối
                // Ví dụ: 425 Use PORT or PASV first, 550 Failed to open file...
                String reply = client.getReplyString();
                log.warn("❌ FTP Fail: Could not retrieve stream for {}. Reply: {}", fullPath, reply.trim());

                // Nếu kết nối bị lỗi nặng, invalidate nó khỏi pool
                invalidate(client);
                client = null;
                return null;
            }

            byte[] data = is.readAllBytes();
            is.close();

            if (!client.completePendingCommand()) {
                log.warn("Failed to complete FTP transaction for {}", fileName);
                // Transaction chưa xong thì kết nối này không dùng lại được -> invalidate
                invalidate(client);
                client = null;
                return null;
            }

            ByteBuffer buffer = ByteBuffer.wrap(data);
            return new CounterDataObject(serverConfig, path, fileName, buffer, null);

        } catch (Exception e) {
            log.error("Error downloading {}: {}", fileName, e.getMessage());
            invalidate(client);
            client = null;
            return null;
        } finally {
            if (is != null) { try { is.close(); } catch (Exception ignored) {} }
            returnToPool(client);
        }
    }

    /**
     * Di chuyển file sang thư mục Done
     */
    public boolean moveFileToDone(String path, String fileName, String doneFolder) {
        FTPClient client = null;
        try {
            client = clientPool.borrowObject();
            String from = path + "/" + fileName;
            String to = path + "/" + doneFolder + "/" + fileName;

            // Thử rename
            boolean success = client.rename(from, to);
//            if (!success) {
//                // Nếu thất bại, có thể do chưa có folder Done, thử tạo
//                // (Lưu ý: check này làm chậm, nên cache lại việc đã tạo folder hay chưa ở tầng ngoài)
//                // client.makeDirectory(path + "/" + doneFolder);
//                // success = client.rename(from, to);
//            }
            return success;

        } catch (Exception e) {
            log.error("Error moving file {}: {}", fileName, e.getMessage());
            invalidate(client);
            client = null;
            return false;
        } finally {
            returnToPool(client);
        }
    }

    public void shutdown() {
        clientPool.close();
    }

    // --- Helper methods ---
    private void returnToPool(FTPClient client) {
        if (client != null) {
            clientPool.returnObject(client);
        }
    }

    private void invalidate(FTPClient client) {
        if (client != null) {
            try {
                clientPool.invalidateObject(client);
            } catch (Exception ignored) {}
        }
    }
}