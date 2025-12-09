package com.viettel.ems.perfomance.encryption;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

@Slf4j
public class ZipDeserializer implements Deserializer<String> {

    private static final int POOL_SIZE = 3;
    private static ZipDeserializer instance;
    private final BlockingQueue<Inflater> pool;

    public ZipDeserializer() {
        pool = new ArrayBlockingQueue<>(POOL_SIZE);
        for (int i = 0; i < POOL_SIZE; i++) {
            pool.add(new Inflater());
        }
    }

    public static synchronized ZipDeserializer getInstance() {
        if (instance == null) {
            instance = new ZipDeserializer();
        }
        return instance;
    }

    @Override
    public String deserialize(String topic, byte[] data) {
        byte[] result = decompress(data);
        if (result == null) return null;
        try {
            return new String(result, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return new String(result);
        }
    }

    @Override
    public String deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }

    private byte[] deserialize(byte[] data) {
        AtomicReference<List<byte[]>> result = new AtomicReference<>(new ArrayList<>());
//        ZippingFile.processUnzipFile(data, result);
        return result.get().isEmpty() ? null : result.get().get(0);
    }

    public byte[] decompress(byte[] compressedData) {
        return decompress(compressedData, 1, null);
    }

    public byte[] decompress(byte[] compressedData, int time, InterruptedException e) {
        Inflater inflater = null;
        if (time > 3) {
            log.error("Decompress Error after 3 time (s) due InterruptedException. {}, {}", e.getMessage(), e.getStackTrace());
            return null;
        }
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(compressedData.length)) {
            inflater = pool.take();
            inflater.setInput(compressedData);
            byte[] buffer = new byte[1024];
            while(!inflater.finished()) {
                int counter  = inflater.inflate(buffer);
                outputStream.write(buffer, 0 , counter);
            }
            return outputStream.toByteArray();
        }catch(DataFormatException | IOException ex1) {
            return compressedData;
        }catch(InterruptedException ex) {
            return decompress(compressedData, time + 1, e);
        }finally {
            if(inflater != null) {
                inflater.reset();
                pool.offer(inflater);
            }
        }
    }
}