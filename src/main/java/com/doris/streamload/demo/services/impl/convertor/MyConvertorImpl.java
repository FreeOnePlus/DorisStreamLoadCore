package com.doris.streamload.demo.services.impl.convertor;

import com.doris.streamload.demo.beans.DataBean;
import org.apache.doris.streamload.core.IConvertor;

import java.util.List;
import java.util.stream.Collectors;

public class MyConvertorImpl implements IConvertor {
    @Override
    public String convertorToCsv(Object input) {
        List<DataBean> list = (List<DataBean>) input;
        return list.stream().map(DataBean::toString)
                .collect(Collectors.joining("\n"));
    }

    @Override
    public String convertorToCsvWithNames(Object input) {
        return null;
    }

    @Override
    public String convertorToCsvWithNamesAndTypes(Object input) {
        return null;
    }

    @Override
    public String convertorToJson(Object input) {
        return null;
    }
}
