package com.intel.flink.sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.intel.flink.datatypes.InputMetadata;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * Reads InputMetadata records from a source csv file and emits a stream of InputMetadata events.
 * Source operates in event-time.
 *
 */
public class InputMetadataSource implements SourceFunction<InputMetadata> {
    private static final Logger logger = LoggerFactory.getLogger(InputMetadataSource.class);
    //causes each event to be randomly delayed within the specified max bound. Yields an out-of-order stream.
    private final int maxDelayMsecs;
    private final int watermarkDelayMSecs;

    private final String dataFilePath;
    //A speed up factor of 60 means that events that happened within a minute are served in 1 sec.
    private final int servingSpeed;

    //    private transient BufferedReader reader;
//    private transient InputStream gzipStream;
    //.withLocale(Locale.US).withZoneUTC()
    //private static final transient DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-ddHH:mm:ss");

    /**
     * Serves the InputMetadata records from the specified and ordered input file.
     * InputMetadata are served exactly in order of their time stamps
     * at the speed at which they were originally generated.
     *
     * @param dataFilePath The gzipped input file from which the InputMetadata records are read.
     */
    public InputMetadataSource(String dataFilePath) {
        this(dataFilePath, 0, 1);
    }

    /**
     * Serves the InputMetadata records from the specified and ordered input file.
     * InputMetadata are served exactly in order of their time stamps
     * in a serving speed which is proportional to the specified serving speed factor.
     *
     * @param dataFilePath       The gzipped input file from which the InputMetadata records are read.
     * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
     */
    public InputMetadataSource(String dataFilePath, int servingSpeedFactor) {
        this(dataFilePath, 0, servingSpeedFactor);
    }

    /**
     * Serves the InputMetadata records from the specified and ordered input file.
     * InputMetadata are served out-of time stamp order with specified maximum random delay
     * in a serving speed which is proportional to the specified serving speed factor.
     *
     * @param dataFilePath       The gzipped input file from which the InputMetadata records are read.
     * @param maxEventDelaySecs  The max time in seconds by which events are delayed.
     * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
     */
    public InputMetadataSource(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
        if (maxEventDelaySecs < 0) {
            throw new IllegalArgumentException("Max event delay must be positive");
        }
        this.dataFilePath = dataFilePath;
        this.maxDelayMsecs = maxEventDelaySecs * 1000;
        this.watermarkDelayMSecs = maxDelayMsecs < 10000 ? 10000 : maxDelayMsecs;
        this.servingSpeed = servingSpeedFactor;
    }

    @Override
    public void run(SourceContext<InputMetadata> sourceContext) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilePath)))) {
            generateUnorderedStream(reader, sourceContext);
        }
    }

    private void generateUnorderedStream(BufferedReader reader, SourceContext<InputMetadata> sourceContext) throws Exception {

        long servingStartTime = Calendar.getInstance().getTimeInMillis();
        long dataStartTime;

        Random rand = new Random(7452);
        PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<>(
                32,
                (o1, o2) -> o1.f0.compareTo(o2.f0));

        // read first inputMetadata and insert it into emit schedule
        String line;
        InputMetadata inputMetadata;
        if (reader.ready() && (line = reader.readLine()) != null) {
            // read first inputMetadata
            inputMetadata = InputMetadata.fromString(line);
            // extract starting timestamp
            dataStartTime = getEventTime(inputMetadata);
            // get delayed time
            long delayedEventTime = dataStartTime + getNormalDelayMsecs(rand);

            emitSchedule.add(new Tuple2<>(delayedEventTime, inputMetadata));
            // schedule next watermark
            long watermarkTime = dataStartTime + watermarkDelayMSecs;
            Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
            emitSchedule.add(new Tuple2<>(watermarkTime, nextWatermark));
        } else {
            return;
        }

        // peek at next inputMetadata
        if (reader.ready() && (line = reader.readLine()) != null) {
            inputMetadata = InputMetadata.fromString(line);
        } else {
            inputMetadata = null;
        }

        // read inputMetadatas one-by-one and emit a random inputMetadata from the buffer each time
        while (emitSchedule.size() > 0 || reader.ready()) {

            // insert all events into schedule that might be emitted next
            long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
            long inputMetadataEventTime = inputMetadata != null ? getEventTime(inputMetadata) : -1;
            while (
                // while there is a inputMetadata AND
                    inputMetadata != null && (
                            emitSchedule.isEmpty() || // and no inputMetadata in schedule OR
                                    inputMetadataEventTime < curNextDelayedEventTime + maxDelayMsecs) // not enough inputMetadatas in schedule
                    ) {
                // insert event into emit schedule
                long delayedEventTime = inputMetadataEventTime + getNormalDelayMsecs(rand);
                emitSchedule.add(new Tuple2<>(delayedEventTime, inputMetadata));

                // read next inputMetadata
                if (reader.ready() && (line = reader.readLine()) != null) {
                    inputMetadata = InputMetadata.fromString(line);
                    inputMetadataEventTime = getEventTime(inputMetadata);
                } else {
                    inputMetadata = null;
                    inputMetadataEventTime = -1;
                }
            }

            // emit schedule is updated, emit next element in schedule
            Tuple2<Long, Object> head = emitSchedule.poll();
            long delayedEventTime = head.f0;

            long now = Calendar.getInstance().getTimeInMillis();
            long servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime);
            long waitTime = servingTime - now;

            Thread.sleep((waitTime > 0) ? waitTime : 0);

            if (head.f1 instanceof InputMetadata) {
                InputMetadata emitinputMetadata = (InputMetadata) head.f1;
                // emit inputMetadata
                sourceContext.collectWithTimestamp(emitinputMetadata, getEventTime(emitinputMetadata));
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

    private long getEventTime(InputMetadata inputMetadata) {
        if (inputMetadata.inputMetadataKey != null) {
            DateTime ts = inputMetadata.inputMetadataKey.ts;
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
