package infore.SDE.experiments;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Datapoint;
import infore.SDE.sources.KafkaStringConsumer;
import infore.SDE.sources.kafkaProducerEstimation;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class RUNKafkaReadS {


    private static String kafkaDataInputTopic;
    private static String kafkaRequestInputTopic;
    private static String kafkaBrokersList;
    private static int parallelism;
    private static int multi;
    private static String kafkaOutputTopic;
    private static String Source;

    /**
     * @param args Program arguments. You have to provide 4 arguments otherwise
     *             DEFAULT values will be used.<br>
     *             <ol>
     *             <li>args[0]={@link #kafkaDataInputTopic} DEFAULT: "Forex")
     *             <li>args[1]={@link #kafkaRequestInputTopic} DEFAULT: "Requests")
     *             <li>args[2]={@link #kafkaBrokersList} (DEFAULT: "localhost:9092")
     *             <li>args[3]={@link #parallelism} Job parallelism (DEFAULT: "4")
     *             <li>args[4]={@link #kafkaOutputTopic} DEFAULT: "OUT")
     *             "O10")
     *             </ol>
     *
     */

    public static void main(String[] args) throws Exception {
        // Initialize Input Parameters
        initializeParameters(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        KafkaStringConsumer kc = new KafkaStringConsumer(kafkaBrokersList, kafkaDataInputTopic, true);
        KafkaStringConsumer requests = new  KafkaStringConsumer(kafkaBrokersList, kafkaRequestInputTopic, true);
        kafkaProducerEstimation kp = new kafkaProducerEstimation(kafkaBrokersList, kafkaOutputTopic);

        DataStream<String> datastream = env.addSource(kc.getFc());
        DataStream<String> RQ_stream = env.addSource(requests.getFc());

        //map kafka data input to tuple2<int,double>
        DataStream<Datapoint> dataStream = datastream
                .map(new MapFunction<String, Datapoint>() {
                    @Override
                    public Datapoint map(String node) throws IOException {
                        // TODO Auto-generated method stub
                        ObjectMapper objectMapper = new ObjectMapper();
                        Datapoint dp = objectMapper.readValue(node, Datapoint.class);
                        System.out.println(dp.toJsonString());
                        return dp;
                    }
                }).name("DATA_SOURCE").keyBy((KeySelector<Datapoint, String>) Datapoint::getKey);

        //




        //Multiplication IF NEEDED
        //DataStream<Datapoint> DataStream2 = DataStream.flatMap(new IngestionMultiplierFlatMap(multi));


        env.execute("Streaming SDE"+parallelism+"_"+multi+"_"+kafkaDataInputTopic);

    }

    private static void initializeParameters(String[] args) {

        if (args.length > 1) {

            System.out.println("[INFO] User Defined program arguments");
            //User defined program arguments
            kafkaDataInputTopic = args[0];
            kafkaRequestInputTopic = args[1];
            multi = Integer.parseInt(args[2]);
            parallelism = Integer.parseInt(args[3]);
            Source ="non";
            kafkaBrokersList = "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667";
            kafkaOutputTopic = "RAD_OUT";

        }else{

            System.out.println("[INFO] Default values");
            kafkaDataInputTopic = "RAD_REQUEST_N";
            kafkaRequestInputTopic = "RAD_REQUEST_5";
            Source ="non";
            multi = 10;
            parallelism = 4;
            //parallelism2 = 4;
            kafkaBrokersList = "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667";
            kafkaOutputTopic = "RAD_OUT";
        }
    }
}
