package com.intel.flink.sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.intel.flink.datatypes.CameraWithCube;
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
public class CameraWithCubeSource implements SourceFunction<CameraWithCube> {
    private static final Logger logger = LoggerFactory.getLogger(CameraWithCubeSource.class);

    private final int maxDelayMsecs;
    private final int watermarkDelayMSecs;

    private final String dataFilePath;
    private final int servingSpeed;
    private static final transient DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-ddHH:mm:ss").withLocale(Locale.US).withZoneUTC();


    public CameraWithCubeSource(String dataFilePath) {
        this(dataFilePath, 0, 1);
    }

    public CameraWithCubeSource(String dataFilePath, int servingSpeedFactor) {
        this(dataFilePath, 0, servingSpeedFactor);
    }

    public CameraWithCubeSource(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
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
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilePath)))) {
            generateUnorderedStream(reader, sourceContext);
        }
    }

    private void generateUnorderedStream(BufferedReader reader, SourceContext<CameraWithCube> sourceContext) throws Exception {

        long servingStartTime = Calendar.getInstance().getTimeInMillis();
        long dataStartTime;

        Random rand = new Random(7452);
        PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<>(
                32,
                (o1, o2) -> o1.f0.compareTo(o2.f0));

        // read first CameraWithCube and insert it into emit schedule
        String line;
        CameraWithCube cameraWithCube;
        if (reader.ready() && (line = reader.readLine()) != null) {
            // read first CameraWithCube
            cameraWithCube = CameraWithCube.fromString(line);
            // extract starting timestamp
            dataStartTime = getEventTime(cameraWithCube);
            // get delayed time
            long delayedEventTime = dataStartTime + getNormalDelayMsecs(rand);

            emitSchedule.add(new Tuple2<>(delayedEventTime, cameraWithCube));
            // schedule next watermark
            long watermarkTime = dataStartTime + watermarkDelayMSecs;
            Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
            emitSchedule.add(new Tuple2<>(watermarkTime, nextWatermark));

        } else {
            return;
        }

        // peek at next inputMetadata
        if (reader.ready() && (line = reader.readLine()) != null) {
            cameraWithCube = CameraWithCube.fromString(line);
        }

        // read cameraWithCube's one-by-one and emit a random cameraWithCube from the buffer each time
        while (emitSchedule.size() > 0 || reader.ready()) {

            // insert all events into schedule that might be emitted next
            long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
            long cameraWithKeyEventTime = cameraWithCube != null ? getEventTime(cameraWithCube) : -1;
            while (
                // while there is a cameraWithCube AND
                    cameraWithCube != null && (
                            emitSchedule.isEmpty() || // and no inputMetadata in schedule OR
                                    cameraWithKeyEventTime < curNextDelayedEventTime + maxDelayMsecs) // not enough inputMetadatas in schedule
                    ) {
                // insert event into emit schedule
                long delayedEventTime = cameraWithKeyEventTime + getNormalDelayMsecs(rand);
                emitSchedule.add(new Tuple2<>(delayedEventTime, cameraWithCube));

                // read next inputMetadata
                if (reader.ready() && (line = reader.readLine()) != null) {
                    cameraWithCube = CameraWithCube.fromString(line);
                    cameraWithKeyEventTime = getEventTime(cameraWithCube);
                } else {
                    cameraWithCube = null;
                    cameraWithKeyEventTime = -1;
                }
            }

            // emit schedule is updated, emit next element in schedule
            Tuple2<Long, Object> head = emitSchedule.poll();
            long delayedEventTime = head.f0;

            long now = Calendar.getInstance().getTimeInMillis();
            long servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime);
            long waitTime = servingTime - now;

            Thread.sleep((waitTime > 0) ? waitTime : 0);

            if (head.f1 instanceof CameraWithCube) {
                CameraWithCube emitCameraWithCubeData = (CameraWithCube) head.f1;
                // emit inputMetadata
                sourceContext.collectWithTimestamp(emitCameraWithCubeData, getEventTime(emitCameraWithCubeData));
            } else if (head.f1 instanceof Watermark) {
                Watermark emitWatermark = (Watermark) head.f1;
                // emit watermark
                sourceContext.emitWatermark(emitWatermark);
                // schedule next watermark
                long watermarkTime = delayedEventTime + watermarkDelayMSecs;
                Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
                emitSchedule.add(new Tuple2<>(watermarkTime, nextWatermark));
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
