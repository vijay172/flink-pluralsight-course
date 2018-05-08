package com.intel.flink.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.intel.flink.datatypes.CameraWithCube;
import com.intel.flink.datatypes.InputMetadata;
import com.intel.flink.sources.CameraWithCubeSource;
import com.intel.flink.sources.CheckpointedCameraWithCubeSource;
import com.intel.flink.sources.CheckpointedInputMetadataSource;
import com.intel.flink.sources.InputMetadataSource;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MapTwoStreamsProcess {
    private static final Logger logger = LoggerFactory.getLogger(MapTwoStreamsProcess.class);
    static final OutputTag<InputMetadata> UNMATCHED_INPUT_METADATA =
            new OutputTag<InputMetadata>("UNMATCHED_INPUT_METADATA") {};
    static final OutputTag<CameraWithCube> UNMATCHED_CAMERA_DATA =
            new OutputTag<CameraWithCube>("UNMATCHED_CAMERA_DATA") {};
    public static void main(String[] args) throws Exception {
        logger.info("args:", args);
        ParameterTool params = ParameterTool.fromArgs(args);
        //zipped files
        final String inputMetadataFile = params.getRequired("metadata");
        final String cameraFile = params.getRequired("camera");
        logger.info("inputMetadataFile:{}, cameraFile:{}", inputMetadataFile, cameraFile);
        //at most 60 secs of delay
        final int delay = 60;
        //30 mins of events served every secs
        final int servingSpeedFactor = 1800;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //setup checkpointing
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10, TimeUnit.SECONDS)));
        //TODO: what time to use
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<InputMetadata> inputMetadataDataStream = env
                .addSource(new CheckpointedInputMetadataSource(inputMetadataFile, delay, servingSpeedFactor))
                //.addSource(new InputMetadataSource(inputMetadataFile, delay, servingSpeedFactor))
                //.filter(inputMetadata -> inputMetadata.inputMetadataKey.cube.equals("cu1"))
                .keyBy((inputMetadata) ->
                        inputMetadata.inputMetadataKey != null ? inputMetadata.inputMetadataKey.ts : new Object());
        logger.info("past inputMetadataFile source");
        DataStream<CameraWithCube> cameraWithCubeDataStream = env
                .addSource(new CheckpointedCameraWithCubeSource(cameraFile, delay, servingSpeedFactor))
                //.addSource(new CameraWithCubeSource(cameraFile, delay, servingSpeedFactor))
                .keyBy((cameraWithCube) -> cameraWithCube.cameraKey != null ? cameraWithCube.cameraKey.ts : new Object());
        logger.info("past cameraFile source");
        SingleOutputStreamOperator processedStream = inputMetadataDataStream
                .connect(cameraWithCubeDataStream)
                .process(new EnrichmentProcessFunction());
        //Use sideoutput to print the discarded unmatched events that were stored in state for sometime but were
        //eventually discarded in the onTimer method because matching events never arrived
        processedStream.getSideOutput(UNMATCHED_INPUT_METADATA).print();
        processedStream.getSideOutput(UNMATCHED_CAMERA_DATA).print();
        env.execute("Process - Join InputMetadata feed with Camera feed from Tile");
    }

    /**
     * Enrichment CoProcess function for keyed managed store. Use timers available in CoProcessFunction to eventually clear
     * any unmatched state that is being kept.
     */
    static final class EnrichmentProcessFunction extends CoProcessFunction<InputMetadata, CameraWithCube, Tuple2<InputMetadata, CameraWithCube>> {
        //keyed, managed state
        //ts1,cube1, [c1,c2],count
        private MapState<InputMetadata.InputMetadataKey, InputMetadata> inputMetadataState;
        //ts1,c1
        private MapState<CameraWithCube.CameraKey, CameraWithCube> cameraWithCubeState;
        //in msec
        final int delay1 = 25 * 60 * 1000;

        @Override
        public void open(Configuration config) {
            logger.debug("EnrichmentProcessFunction Entered open");
            //TODO:
            MapStateDescriptor<InputMetadata.InputMetadataKey, InputMetadata> inputMetadataMapStateDescriptor =
                    new MapStateDescriptor<>("inputMetadataState",
                            InputMetadata.InputMetadataKey.class, InputMetadata.class);
            inputMetadataState = getRuntimeContext().getMapState(inputMetadataMapStateDescriptor);
            MapStateDescriptor<CameraWithCube.CameraKey, CameraWithCube> cameraMapStateDescriptor =
                    new MapStateDescriptor<>("cameraWithCubeState",
                            CameraWithCube.CameraKey.class, CameraWithCube.class);
            cameraWithCubeState = getRuntimeContext().getMapState(cameraMapStateDescriptor);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<InputMetadata, CameraWithCube>> collector) throws Exception {
            //TODO: how to tune firing of onTimer ??
            //get all keys with the timestamp and clear them out
            logger.info("~~~~~~~~~~~~~~Entered onTimer with timestamp:{}, ctx:{}", timestamp, ctx);
            clearKeysFromInputMetadataMapAfterExpiration(timestamp, ctx);
            clearKeysFromCameraStateMapAfterExpiration(timestamp, ctx);
        }

        private void clearKeysFromInputMetadataMapAfterExpiration(long timestamp, OnTimerContext ctx) throws Exception {
            logger.debug("Entered clearKeysFromInputMetadataMapAfterExpiration with timestamp:{}", timestamp);
            final Iterable<InputMetadata.InputMetadataKey> inputMetadataKeys = inputMetadataState.keys();
            final Iterator<InputMetadata.InputMetadataKey> inputMetadataKeyIterator =
                    inputMetadataKeys != null ? inputMetadataKeys.iterator() : null;
            for (; inputMetadataKeyIterator != null && inputMetadataKeyIterator.hasNext(); ) {
                InputMetadata.InputMetadataKey inputMetadataKey = inputMetadataKeyIterator.next();
                //if  inputMetadataKey.ts <= incoming timestamp, remove the key as it has waited long enough in the memory
                logger.debug("inputMetadataKey.ts.getMillis():{}, timestamp:{}", inputMetadataKey.ts.getMillis(), timestamp);
                if (inputMetadataKey.ts.getMillis() + delay1 == timestamp) {
                    logger.info("Removing matched TS for inputMetadataKey:{}", inputMetadataKey);
                    final InputMetadata retrievedInputMetadata = inputMetadataState.get(inputMetadataKey);
                    logger.info("retrievedInputMetadata.count:{}", retrievedInputMetadata.count);
                    //TODO: if count > 0, then output the data
                    if (retrievedInputMetadata.count > 0) {
                        ctx.output(UNMATCHED_INPUT_METADATA, inputMetadataState.get(inputMetadataKey));
                    }
                    //inputMetadataKeyIterator.remove();
                }
                inputMetadataKeyIterator.remove();
            }
        }

        private void clearKeysFromCameraStateMapAfterExpiration(long timestamp, OnTimerContext ctx) throws Exception {
            logger.debug("Entered clearKeysFromCameraStateMapAfterExpiration with timestamp:{}", timestamp);
            final Iterable<CameraWithCube.CameraKey> cameraKeys = cameraWithCubeState.keys();
            final Iterator<CameraWithCube.CameraKey> cameraKeyIterator =
                    cameraKeys != null ? cameraKeys.iterator() : null;
            for (; cameraKeyIterator != null && cameraKeyIterator.hasNext(); ) {
                CameraWithCube.CameraKey cameraKey = cameraKeyIterator.next();
                //if cameraKey.ts is <= incoming timestamp , remove the key as it has waited long enough in the memory
                logger.debug("cameraKey.ts.getMillis():{}, timestamp:{}", cameraKey.ts.getMillis(), timestamp);
                final CameraWithCube cameraWithCube = cameraWithCubeState.get(cameraKey);
                final boolean tileExists = cameraWithCube.tileExists;
                if (cameraKey.ts.getMillis() + delay1 == timestamp && tileExists) {
                    logger.info("Removing matched TS for cameraKey:{}", cameraKey);
                } else {
                    ctx.output(UNMATCHED_CAMERA_DATA, cameraWithCubeState.get(cameraKey));
                }
                //remove camera keys after timer fires
                cameraKeyIterator.remove();
            }
        }

        /**
         * Data comes in from Input Metadata with (TS1,Cube1) as key with values [Camera1, Camera2], count = 2
         * Insert into InputMetadata state 1st.
         * Then check CameraWithCubeState with key (TS1, Camera1) key for existence in Camera(cameraWithCubeState) state.
         * If it doesn't exist, insert into CameraWithCube state TS1, Camera1 as key, values- [CU1], tileExists=false
         * if Camera row exists with tileExists=true(Tile Camera input exists), reduce count for (TS1,CU1) etc in a loop for all cubeLst entries.
         * if Camera row exists with tileExists=false (No Tile Camera input), update Camera state with new CU2 in cubeLst entry.
         *
         * @param inputMetadata Input metadata coming in
         * @param context Context
         * @param collector Collector used
         * @throws Exception Exception thrown
         */
        @Override
        public void processElement1(InputMetadata inputMetadata, Context context, Collector<Tuple2<InputMetadata, CameraWithCube>> collector) throws Exception {
            logger.info("EnrichmentProcessFunction- [processElement1] with inputMetadata:{} count:{}", inputMetadata, inputMetadata.count);

            final List<String> inputMetaCameraLst = inputMetadata.cameraLst;
            inputMetadata.count = inputMetaCameraLst != null ? inputMetaCameraLst.size() : 0L;
            final InputMetadata.InputMetadataKey inputMetadataKey = inputMetadata.inputMetadataKey; //(TS1,CU1)
            final DateTime inputMetaTs = inputMetadataKey != null ? inputMetadataKey.ts : null;
            final String inputMetaCube = inputMetadataKey != null ? inputMetadataKey.cube : null;
            //Insert into InputMetadata state 1st.
            inputMetadataState.put(inputMetadataKey, inputMetadata);
            //wait up to 1 minutes for the corresponding InputMetadata event, then clear the state
//            final long checkTS = context.timerService().currentWatermark() + delay1;
//            context.timerService().registerEventTimeTimer(checkTS);
            context.timerService().registerEventTimeTimer(inputMetaTs.getMillis() + delay1);
            //check in a loop for incoming inputMetadata with TS1, C1 key against existing Camera state data - cameraWithCube Map entries
            Iterator<String> inputMetaCameraLstIterator = inputMetaCameraLst != null ? inputMetaCameraLst.iterator() : null;
            for (; inputMetaCameraLstIterator != null && inputMetaCameraLstIterator.hasNext(); ) {
                String inputMetaCam = inputMetaCameraLstIterator.next();
                //TS1,C1
                CameraWithCube.CameraKey cameraKeyFromInputMetadata = new CameraWithCube.CameraKey(inputMetaTs, inputMetaCam);
                //check with key in cameraWithCubeState
                CameraWithCube cameraWithCube = cameraWithCubeState.get(cameraKeyFromInputMetadata);
                if (cameraWithCube != null) {
                    //key exists - hence check if tileExists
                    if (cameraWithCube.tileExists) {
                        logger.debug("[processElement1] inputMetadata cameraWithCube tileExists:{}", cameraWithCube);
                        //reduce count in inputMetadata for TS1,CU1
                        List<String> existingCameraWithCubeLst = cameraWithCube.cubeLst;
                        //if tile exists & empty cubeLst, then reduce inputMetadata state count for ts1,cu1 by 1
                        if (existingCameraWithCubeLst != null && existingCameraWithCubeLst.size() == 0) {
                            //TODO: DUPLICATE CODE - REFACTOR LATER
                            final InputMetadata.InputMetadataKey existingMetadataKey = new InputMetadata.InputMetadataKey(inputMetaTs, inputMetaCube); //(TS1,CU1), (TS1,CU2)
                            final InputMetadata existingInputMetadata = inputMetadataState.get(existingMetadataKey); //(TS1,CU1)
                            if (existingInputMetadata != null) {
                                //reduce count by 1 for inputMetadata state
                                existingInputMetadata.count -= 1;
                                inputMetadataState.put(existingMetadataKey, existingInputMetadata);

                                if (existingInputMetadata.count == 0) {
                                    logger.info("$$$$$[processElement1] Release Countdown latch with inputMetadata Collecting existingInputMetadata:{}, cameraWithCube:{}", existingInputMetadata, cameraWithCube);
                                    collector.collect(new Tuple2(existingInputMetadata, cameraWithCube));
                                } else {
                                    logger.info("!!!!![processElement1] with inputMetadata reducing count:{} ,existingInputMetadata:{}, cameraWithCube:{}", existingInputMetadata.count, existingInputMetadata, cameraWithCube);
                                    DateTime eventTimeTs = existingMetadataKey.ts;
                                    //wait up to 1 minutes for the corresponding InputMetadata event, then clear the state
                                    context.timerService().registerEventTimeTimer(eventTimeTs.getMillis() + delay1);
                                }
                            }
                        } else {
                            //reduce count for (TS1,CU1) from InputMetadata etc in a loop for all cubeLst entries.
                            Iterator<String> existingCameraWithCubeIterator = existingCameraWithCubeLst.iterator();
                            for (; existingCameraWithCubeIterator.hasNext(); ) {
                                String existingCameraWithCube = existingCameraWithCubeIterator.next(); //CU1, CU2
                                // if incoming inputMetadata's cu1 (inputCube) matches existingCube, remove from cameraWithCubeState's cubeLst
                                //if cameraWithCubeState's cubeLst's size is 0, remove key from cameraWithCubeState
                                if (existingCameraWithCube.equals(inputMetaCube)) {
                                    //TODO: do we need to do this - remove existingCube
                                    existingCameraWithCubeIterator.remove();
                                }
                                //for tile exists condition, reduce count for [TS1,cu1] key of inputMetadataState
                                final InputMetadata.InputMetadataKey existingMetadataKey = new InputMetadata.InputMetadataKey(inputMetaTs, existingCameraWithCube); //(TS1,CU1), (TS1,CU2)
                                final InputMetadata existingInputMetadata = inputMetadataState.get(existingMetadataKey); //(TS1,CU1)
                                if (existingInputMetadata != null) {
                                    //reduce count by 1 for inputMetadata state
                                    existingInputMetadata.count -= 1;
                                    inputMetadataState.put(existingMetadataKey, existingInputMetadata);

                                    if (existingInputMetadata.count == 0) {
                                        logger.info("$$$$$[processElement1] Release Countdown latch with inputMetadata Collecting existingInputMetadata:{}, cameraWithCube:{}", existingInputMetadata, cameraWithCube);
                                        collector.collect(new Tuple2(existingInputMetadata, cameraWithCube));
                                    } else {
                                        logger.info("$$$$$[processElement1] with inputMetadata reducing count existingInputMetadata.count:{}, cameraWithCube:{}", existingInputMetadata.count, cameraWithCube);
                                        DateTime eventTimeTs = existingMetadataKey.ts;
                                        //wait up to 1 minutes for the corresponding InputMetadata event, then clear the state
                                        context.timerService().registerEventTimeTimer(eventTimeTs.getMillis() + delay1);
                                    }
                                }
                            }
                        }

                        if (existingCameraWithCubeLst != null && existingCameraWithCubeLst.size() == 0) {
                            //if no cubeLst and tileExists=true, remove the key from the camera state ???
                            //TODO: remove ?? cameraWithCubeState.remove(cameraKeyFromInputMetadata);
                            logger.info("[processElement1] inputMetadata if no cubeLst and tileExists=true, remove the cameraKeyFromInputMetadata key from the camera state :{}", cameraKeyFromInputMetadata);
                        } else {
                            //update state with reduced cubeLst
                            cameraWithCubeState.put(cameraKeyFromInputMetadata, cameraWithCube);
                        }
                    } else {
                        //update into CameraWithCube with updated cubeLst containing new inputCube from inputMetadata
                        List<String> existingCubeLst = cameraWithCube.cubeLst;
                        if (existingCubeLst == null) {
                            existingCubeLst = new ArrayList<>();
                        }
                        if (!existingCubeLst.contains(inputMetaCube)) {
                            existingCubeLst.add(inputMetaCube);
                        }
                        cameraWithCubeState.put(cameraKeyFromInputMetadata, cameraWithCube);
                    }
                } else {
                    //insert into CameraWithCube with tileExists=false - i.e waiting for TileDB camera input to come in
                    List<String> newCubeLst = new ArrayList<>(Collections.singletonList(inputMetaCube));
                    CameraWithCube newCameraWithCube = new CameraWithCube(inputMetaTs, inputMetaCam, newCubeLst, false);
                    cameraWithCubeState.put(cameraKeyFromInputMetadata, newCameraWithCube);
                }
            }
        }

        /**
         * Data comes in from the Camera feed(cameraWithCube) with (TS1, Camera1) as key
         * Check if key exists in CameraWithCubeState
         * If camera key doesn't exist, insert into CameraWithCubeState with key & value- { empty cubeLst and tileExists= true }
         * If camera key exists, update CameraWithCubeState with key & value having tileExists= true
         * For camera key exists, check if value has a non-empty cubeLst. If no, stop.
         * If value has a non-empty cubeLst, reduce count for (TS1,CU1) etc in a loop for all cubeLst entries of Camera feed
         *
         * @param cameraWithCube Incoming Camera data
         * @param collector      Output collector
         * @throws Exception Exception thrown
         */
        @Override
        public void processElement2(CameraWithCube cameraWithCube, Context context, 
                                    Collector<Tuple2<InputMetadata, CameraWithCube>> collector) throws Exception {
            logger.info("EnrichmentProcessFunction- [processElement2] with Camera data:{}", cameraWithCube);
            //TS1, C1
            final CameraWithCube.CameraKey cameraKey = cameraWithCube.cameraKey;
            final DateTime cameraTS = cameraKey.ts;
            final String cameraKeyCam = cameraKey.getCam();
            final CameraWithCube existingCameraWithCube = cameraWithCubeState.get(cameraKey);
            if (existingCameraWithCube != null) {
                boolean tileExists = existingCameraWithCube.tileExists;
                final List<String> existingCubeLst = existingCameraWithCube.cubeLst;
                if (!tileExists) {
                    //update tileExists to true in camera state
                    existingCameraWithCube.tileExists = true;
                    cameraWithCubeState.put(cameraKey, existingCameraWithCube);
                    //wait up to 1 minutes for the corresponding InputMetadata event, then clear the state
                    context.timerService().registerEventTimeTimer(cameraTS.getMillis() + delay1);
                }
                //if cubeLst exists
                logger.info("[processElement2] cameraWithCube existingCubeLst:{}", existingCubeLst);
                if (existingCubeLst != null && existingCubeLst.size() > 0) {
                    for (String existingCube : existingCubeLst) { //CU1, CU2
                        final InputMetadata.InputMetadataKey existingMetadataKey = new InputMetadata.InputMetadataKey(cameraTS, existingCube); //(TS1,CU1), (TS1,CU2)
                        final InputMetadata existingInputMetadata = inputMetadataState.get(existingMetadataKey);
                        if (existingInputMetadata != null) {
                            List<String> existingInputMetaCameraLst = existingInputMetadata.cameraLst;
                            for (Iterator<String> existingInputMetaCameraLstIterator = existingInputMetaCameraLst.iterator(); existingInputMetaCameraLstIterator.hasNext(); ) {
                                String existingInputMetaCam = existingInputMetaCameraLstIterator.next();
                                if (existingInputMetaCam.equals(cameraKeyCam)) {
                                    //want to keep existing inputMetaData & not remove incoming camera from cameraLst of inputMetadata state
                                    existingInputMetadata.count -= 1;
                                }
                            }

                            if (existingInputMetadata.count == 0) {
                                logger.info("$$$$$[processElement2]  Release Countdown latch with Camera data Collecting existingInputMetadata:{}, cameraWithCube:{}", existingInputMetadata, cameraWithCube);
                                collector.collect(new Tuple2(existingInputMetadata, cameraWithCube));
                                //remove from inputMetadata state if count is 0
                                //TODO: combine all inputMetadataState into 1 update operation for performance
                                //TODO:remove this inputMetadataState.remove(existingMetadataKey);
                            } else {
                                //updated reduced count in inputMetadata
                                inputMetadataState.put(existingMetadataKey, existingInputMetadata);
                                logger.info("!!!![processElement2] with Camera data reducing count of existingInputMetadata:{}", existingInputMetadata);
                                //TODO: check this logic using cameraTS
                                context.timerService().registerEventTimeTimer(cameraTS.getMillis() + delay1);
                            }
                        }
                    }
                }
            } else {
                //insert into CameraWithCubeState with key & value- { empty cubeLst and tileExists= true }
                CameraWithCube newCameraWithCube = new CameraWithCube(cameraKey, Collections.emptyList(), true);
                cameraWithCubeState.put(cameraKey, newCameraWithCube);
                //wait up to 1 minutes for the corresponding InputMetadata event, then clear the state
                context.timerService().registerEventTimeTimer(cameraTS.getMillis() + delay1);
            }
        }


    }
}
