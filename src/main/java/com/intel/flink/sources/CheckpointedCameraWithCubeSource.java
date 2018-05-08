package com.intel.flink.sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.intel.flink.datatypes.CameraWithCube;
import com.intel.flink.datatypes.InputMetadata;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.Random;

/**
 *
 */
public class CheckpointedCameraWithCubeSource implements SourceFunction<CameraWithCube> {
    private static final Logger logger = LoggerFactory.getLogger(CheckpointedCameraWithCubeSource.class);

    private final int maxDelayMsecs;
    private final int watermarkDelayMSecs;

    private final String dataFilePath;
    private final int servingSpeed;

    //state
    //number of emitted events
    private long eventCnt = 0;

    private static final transient DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-ddHH:mm:ss").withLocale(Locale.US).withZoneUTC();


    public CheckpointedCameraWithCubeSource(String dataFilePath) {
        this(dataFilePath, 0, 1);
    }

    public CheckpointedCameraWithCubeSource(String dataFilePath, int servingSpeedFactor) {
        this(dataFilePath, 0, servingSpeedFactor);
    }

    public CheckpointedCameraWithCubeSource(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
        if (maxEventDelaySecs < 0) {
            throw new IllegalArgumentException("Max event delay must be positive");
        }
        this.dataFilePath = dataFilePath;
        this.maxDelayMsecs = maxEventDelaySecs * 1000;
        this.watermarkDelayMSecs = maxDelayMsecs < 10000 ? 10000 : maxDelayMsecs;
        this.servingSpeed = servingSpeedFactor;
    }


    @Override
    public void run(SourceContext<CameraWithCube> sourceContext) throws Exception {
        final Object lock = sourceContext.getCheckpointLock();
        Long prevDataStartTime = null;
        long cnt = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilePath)))) {
            String line;
            //skip emitted events
            while (cnt < eventCnt && reader.ready() && (line = reader.readLine()) != null) {
                cnt++;
                CameraWithCube cameraWithCube = CameraWithCube.fromString(line);
                // extract starting timestamp
                prevDataStartTime = getEventTime(cameraWithCube);
            }

            //emit all subsequent events proportional to their timestamp
            while (reader.ready() && (line = reader.readLine()) != null) {
                CameraWithCube cameraWithCube = CameraWithCube.fromString(line);
                // extract starting timestamp
                long dataStartTime = getEventTime(cameraWithCube);

                if (prevDataStartTime != null) {
                    final long diff = (dataStartTime - prevDataStartTime) / servingSpeed;
                    if (diff > 0) {
                        Thread.sleep(diff);
                    }
                }

                synchronized (lock) {
                    eventCnt++;
                    sourceContext.collectWithTimestamp(cameraWithCube, dataStartTime);
                    sourceContext.emitWatermark(new Watermark(dataStartTime - 1));
                }

                prevDataStartTime = dataStartTime;
            }
        }
    }


    private long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
        long dataDiff = eventTime - dataStartTime;
        return servingStartTime + (dataDiff / this.servingSpeed);
    }

    private long getEventTime(CameraWithCube cameraWithCube) {
        if (cameraWithCube.cameraKey != null) {
            DateTime ts = cameraWithCube.cameraKey.ts;
            return ts.getMillis();
        } else {
            return 0;
        }
    }

    private long getNormalDelayMsecs(Random rand) {
        long delay = -1;
        long x = maxDelayMsecs / 2;
        while (delay < 0 || delay > maxDelayMsecs) {
            delay = (long) (rand.nextGaussian() * x) + x;
        }
        return delay;
    }

    @Override
    public void cancel() {

    }
}
