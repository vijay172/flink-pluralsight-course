package com.intel.flink.datatypes;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;

/**
 *
 */
public class CameraWithCube implements Comparable<CameraWithCube> {
    private static final Logger logger = LoggerFactory.getLogger(CameraWithCube.class);
    private static final transient org.joda.time.format.DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-ddHH:mm:ss").withLocale(Locale.US).withZoneUTC();

    public static class CameraKey {
        public DateTime ts;
        String cam;

        public CameraKey() {
        }

        public CameraKey(DateTime ts, String cam) {
            this.ts = ts;
            this.cam = cam;
        }

        @Override
        public String toString() {
            return "CameraKey{" +
                    "ts=" + ts +
                    ", cam='" + cam + '\'' +
                    '}';
        }

        public DateTime getTs() {
            return ts;
        }

        public void setTs(DateTime ts) {
            this.ts = ts;
        }

        public String getCam() {
            return cam;
        }

        public void setCam(String cam) {
            this.cam = cam;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CameraKey cameraKey = (CameraKey) o;

            if (ts != null ? !ts.equals(cameraKey.ts) : cameraKey.ts != null) {
                return false;
            }
            return cam != null ? cam.equals(cameraKey.cam) : cameraKey.cam == null;
        }

        @Override
        public int hashCode() {
            int result = ts != null ? ts.hashCode() : 0;
            result = 31 * result + (cam != null ? cam.hashCode() : 0);
            return result;
        }
    }


    public CameraKey cameraKey;
    public List<String> cubeLst;
    public boolean tileExists;

    public CameraWithCube() {
    }

    public CameraWithCube(CameraKey cameraKey, List<String> cubeLst, boolean tileExists) {
        this.cameraKey = cameraKey;
        this.cubeLst = cubeLst;
        this.tileExists = tileExists;
    }

    public CameraWithCube(DateTime ts, String cam, List<String> cubeLst, boolean tileExists) {
        this.cameraKey = new CameraKey(ts, cam);
        this.cubeLst = cubeLst;
        this.tileExists = tileExists;
    }

    public CameraKey getCameraKey() {
        return cameraKey;
    }

    public void setCameraKey(CameraKey cameraKey) {
        this.cameraKey = cameraKey;
    }

    public List<String> getCubeLst() {
        return cubeLst;
    }

    public void setCubeLst(List<String> cubeLst) {
        this.cubeLst = cubeLst;
    }

    public boolean isTileExists() {
        return tileExists;
    }

    public void setTileExists(boolean tileExists) {
        this.tileExists = tileExists;
    }

    @Override
    public String toString() {
        return "CameraWithCube{" +
                "cameraKey=" + cameraKey +
                ", cubeLst=" + cubeLst +
                ", tileExists=" + tileExists +
                '}';
    }

    /**
     * Convert input line to CameraWithCube
     * Timestamp,camera1
     * ts1,cam1
     *
     * @param line input line from file
     * @return converted CameraWithCube object
     */
    public static CameraWithCube fromString(String line) {
        String[] tokens = line.split(",");

        if (tokens.length < 2) {
            throw new RuntimeException("Invalid record: " + line);
        }
        CameraWithCube cameraWithCube = new CameraWithCube();
        try {
            DateTime ts = DateTime.parse(tokens[0], timeFormatter);
            String cam = tokens[1];
            cameraWithCube.cameraKey = new CameraKey(ts, cam);
        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }
        return cameraWithCube;
    }

    @Override
    public int compareTo(CameraWithCube other) {
        if (other == null) {
            return 1;
        } else {
            if (this.cameraKey.ts == other.cameraKey.ts) {
                return 0;
            } else if (this.cameraKey.ts.isBefore(other.cameraKey.ts)) {
                return -1;
            } else if (this.cameraKey.ts.isAfter(other.cameraKey.ts)) {
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

        CameraWithCube that = (CameraWithCube) o;

        if (tileExists != that.tileExists) {
            return false;
        }
        if (cameraKey != null ? !cameraKey.equals(that.cameraKey) : that.cameraKey != null) {
            return false;
        }
        return cubeLst != null ? cubeLst.equals(that.cubeLst) : that.cubeLst == null;
    }

    @Override
    public int hashCode() {
        int result = cameraKey != null ? cameraKey.hashCode() : 0;
        result = 31 * result + (cubeLst != null ? cubeLst.hashCode() : 0);
        result = 31 * result + (tileExists ? 1 : 0);
        return result;
    }
}
