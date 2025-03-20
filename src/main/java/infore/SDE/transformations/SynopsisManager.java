package infore.SDE.transformations;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import infore.SDE.messages.Request;
import infore.SDE.messages.Message;
import infore.SDE.messages.MessageType;

import java.util.ArrayList;

public class SynopsisManager extends KeyedProcessFunction<String, Request, Message> {

    private MapState<Integer, Request> synopsesState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<Integer, Request> descriptor = new MapStateDescriptor<>(
                "synopsesState",
                Types.INT,
                Types.POJO(Request.class)
        );
        synopsesState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(Request request, Context ctx, Collector<Message> out) throws Exception {
        int requestID = request.getRequestID();
        int uid = request.getUID();

        if (requestID == 1 || requestID == 4 || requestID == 5
        || requestID == 200 || requestID == 201 || requestID == 202) {
            // Initialize synopsis or load snapshot
            synopsesState.put(uid, request);
        } else if (requestID == 2) {
            // Delete synopsis
            synopsesState.remove(uid);
        } else if (requestID == 777) {
            // Return list of synopses, all in one message.
            StringBuilder synopses = new StringBuilder();
            int count = 0;
            for (Request synopsis : synopsesState.values()) {
                synopses.append("Syn_").append(count).append("_").append(synopsis.toJsonString()).append("\n");
                count++;
            }
            Message message = new Message(MessageType.RESPONSE, synopses.toString(), request.getExternalUID());
            out.collect(message);
//            for (Request synopsis : synopsesState.values()) {
//                Message message = new Message(MessageType.RESPONSE, "Existing Synopsis: " + synopsis.toJsonString(), request.getExternalUID());
//                out.collect(message);
//            }
        }
    }
}