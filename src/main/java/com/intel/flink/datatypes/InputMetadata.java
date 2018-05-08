package com.intel.flink.datatypes;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 *
 */
public class InputMetadata implements Comparable<InputMetadata> {
    private static final Logger logger = LoggerFactory.getLogger(InputMetadata.class);
    private static final transient org.joda.time.format.DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-ddHH:mm:ss").withLocale(Locale.US).withZoneUTC();

    public InputMetadata() {
    }

    public InputMetadata(InputMetadataKey inputMetadataKey, List<String> cameraLst) {
        this.inputMetadataKey = inputMetadataKey;
        this.cameraLst = cameraLst;
        this.count = getCount();
    }

    public InputMetadata(DateTime ts, String cube, List<String> cameraLst) {
        this.inputMetadataKey = new InputMetadataKey(ts, cube);
        this.cameraLst = cameraLst;
        this.count = getCount();
    }

    /**
     * Convert input line to InputMetadata
     * ts,cube,2,cam1,cam2
     *
     * @param line input line from file
     * @return InputMetadata object
     */
    public static InputMetadata fromString(String line) {
        String[] tokens = line.split(",");

        if (tokens.length < 4) {
            throw new RuntimeException("Invalid record: " + line);
        }
        InputMetadata inputMetadata = new InputMetadata();
        try {
            DateTime ts = DateTime.parse(tokens[0], timeFormatter);
            String cube = tokens[1];
            inputMetadata.inputMetadataKey = new InputMetadataKey(ts, cube);
            List<String> cameraLst = new ArrayList<>();
            int cameraCnt = Integer.parseInt(tokens[2]);
            cameraLst.addAll(Arrays.asList(tokens).subList(3, cameraCnt + 3));
            inputMetadata.cameraLst = cameraLst;
        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }
        return inputMetadata;
    }

    public static class InputMetadataKey {
        public DateTime ts;
        //TODO: co-ords x,y,z
        public String cube;

        public InputMetadataKey() {
        }

        public InputMetadataKey(DateTime ts, String cube) {
            this.ts = ts;
            this.cube = cube;
        }

        public DateTime getTs() {
            return ts;
        }

        public void setTs(DateTime ts) {
            this.ts = ts;
        }

        public String getCube() {
            return cube;
        }

        public void setCube(String cube) {
            this.cube = cube;
        }

        @Override
        public String toString() {
            return "InputMetadataKey{" +
                    "ts=" + ts +
                    ", cube='" + cube + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            InputMetadataKey that = (InputMetadataKey) o;

            if (ts != null ? !ts.equals(that.ts) : that.ts != null) {
                return false;
            }
            return cube != null ? cube.equals(that.cube) : that.cube == null;
        }

        @Override
        public int hashCode() {
            int result = ts != null ? ts.hashCode() : 0;
            result = 31 * result + (cube != null ? cube.hashCode() : 0);
            return result;
        }
    }

    public InputMetadataKey inputMetadataKey;
    public List<String> cameraLst;
    public long count;

    public InputMetadataKey getInputMetadataKey() {
        return inputMetadataKey;
    }

    public void setInputMetadataKey(InputMetadataKey inputMetadataKey) {
        this.inputMetadataKey = inputMetadataKey;
    }

    public List<String> getCameraLst() {
        return cameraLst;
    }

    public void setCameraLst(List<String> cameraLst) {
        this.cameraLst = cameraLst;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getCount() {
        if (cameraLst != null && cameraLst.size() > 0) {
            count = cameraLst.size();
        } else {
            count = 0;
        }
        return count;
    }

    @Override
    public int compareTo(InputMetadata other) {
        if (other == null) {
            return 1;
        } else {
            if (this.inputMetadataKey.ts == other.inputMetadataKey.ts) {
                return 0;
            } else if (this.inputMetadataKey.ts.isBefore(other.inputMetadataKey.ts)) {
                return -1;
            } else if (this.inputMetadataKey.ts.isAfter(other.inputMetadataKey.ts)) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InputMetadata that = (InputMetadata) o;

        if (!inputMetadataKey.equals(that.inputMetadataKey)) {
            return false;
        }
        return cameraLst.equals(that.cameraLst);
    }

    @Override
    public int hashCode() {
        int result = inputMetadataKey.hashCode();
        result = 31 * result + cameraLst.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "InputMetadata{" +
                "inputMetadataKey=" + inputMetadataKey +
                ", cameraLst=" + cameraLst +
                '}';
    }
}
