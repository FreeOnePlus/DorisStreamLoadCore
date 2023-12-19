import org.apache.doris.streamload.core.IConvertor;
import org.apache.doris.streamload.core.StreamLoad;
import org.apache.doris.streamload.core.exception.StreamLoadException;
import org.apache.doris.streamload.core.params.DorisContentParams;
import org.apache.doris.streamload.core.params.StreamLoadParams;
import org.apache.doris.streamload.core.params.StreamLoadResult;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class StreamLoadDemo {
    private DorisContentParams dorisContentParams;
    private IConvertor convertor;

    public StreamLoadDemo() {
        this.dorisContentParams = getDorisContentParams();
        this.convertor = getConvertor();
    }

    private DorisContentParams getDorisContentParams() {
        return new DorisContentParams(
                "127.0.0.1"
                , 8030
                , "demo"
                , "app_log"
                , "root"
                , ""
        );
    }

    private IConvertor getConvertor() {
        return new IConvertor() {
            public String convertorToCsv(Object input) {
                List<String> dataList = (List<String>) input;
                String data = dataList.stream().collect(Collectors.joining("\n"));
                return data;
            }

            public String convertorToCsvWithNames(Object input) {
                return null;
            }

            public String convertorToCsvWithNamesAndTypes(Object input) {
                return null;
            }

            public String convertorToJson(Object input) {
                String data = (String) input;
                return data;
            }
        };
    }

    public StreamLoadResult loadJsonData() {
        String jsonStr = "{id=1,name=\"张三\",age=17}";
        try {
            StreamLoad streamLoad = new StreamLoad(convertor);
            StreamLoadResult streamLoadResult = streamLoad.run(jsonStr, dorisContentParams
                    , new StreamLoadParams.Builder()
                            .setFormat("json")
                            .build()
            );
            return streamLoadResult;
        } catch (StreamLoadException e) {
            throw new RuntimeException(e);
        }
    }

    public StreamLoadResult loadCsvData() {
        List<String> csvList = new ArrayList<>();
        csvList.add("2,李四,19");
        csvList.add("3,赵六,20");
        try {
            StreamLoad streamLoad = new StreamLoad(convertor);
            StreamLoadResult streamLoadResult = streamLoad.run(csvList, dorisContentParams
                    , new StreamLoadParams.Builder()
                            .setFormat("csv")
                            .setColumnSeparator(",")
                            .build()
            );
            return streamLoadResult;
        } catch (StreamLoadException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        StreamLoadDemo streamLoadDemo = new StreamLoadDemo();

        StreamLoadResult csvResult = streamLoadDemo.loadCsvData();
        System.out.println(csvResult.toString());

        StreamLoadResult jsonResult = streamLoadDemo.loadJsonData();
        System.out.println(jsonResult.toString());
    }

}
