package com.iflytek.edmp.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2018/5/25
 * Time: 13:31
 * Description
 * 用于read and write table rows时候进行序列化和反序列化
 */

public class SerDeDemo extends AbstractSerDe {

    // params
    private List<String> columnNames = null;
    private List<TypeInfo> columnTypes = null;
    private ObjectInspector objectInspector = null;
    // seperator
    private String nullString = null;
    private String lineSep = null;
    private String kvSep = null;

    @Override
    public void initialize(Configuration conf, Properties tbl)
            throws SerDeException {
        // Read sep
        lineSep = "#";
        kvSep = "=";
        nullString = tbl.getProperty(Constants.SERIALIZATION_NULL_FORMAT, "");

        // Read Column Names
        String columnNameProp = tbl.getProperty(Constants.LIST_COLUMNS);
        if (columnNameProp != null && columnNameProp.length() > 0) {
            columnNames = Arrays.asList(columnNameProp.split(","));
        } else {
            columnNames = new ArrayList<String>();
        }

        // Read Column Types
        String columnTypeProp = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
        // default all string
        if (columnTypeProp == null) {
            String[] types = new String[columnNames.size()];
            Arrays.fill(types, 0, types.length, Constants.STRING_TYPE_NAME);
            columnTypeProp = StringUtils.join(types, ":");
        }
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProp);

        // Check column and types equals
        if (columnTypes.size() != columnNames.size()) {
            throw new SerDeException("len(columnNames) != len(columntTypes)");
        }

        // Create ObjectInspectors from the type information for each column
        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>();
        ObjectInspector oi;
        for (int c = 0; c < columnNames.size(); c++) {
            oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(c));
            columnOIs.add(oi);
        }
        objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);

    }

    @Override
    public Object deserialize(Writable wr) throws SerDeException {
        // Split to kv pair
        if (wr == null)
            return null;
        Map<String, String> kvMap = new HashMap<String, String>();
        Text text = (Text) wr;
        for (String kv : text.toString().split(lineSep)) {
            String[] pair = kv.split(kvSep);
            if (pair.length == 2) {
                kvMap.put(pair[0], pair[1]);
            }
        }

        // Set according to col_names and col_types
        ArrayList<Object> row = new ArrayList<Object>();
        String colName = null;
        TypeInfo type_info = null;
        Object obj = null;
        for (int i = 0; i < columnNames.size(); i++) {
            colName = columnNames.get(i);
            type_info = columnTypes.get(i);
            obj = null;
            if (type_info.getCategory() == ObjectInspector.Category.PRIMITIVE) {
                PrimitiveTypeInfo p_type_info = (PrimitiveTypeInfo) type_info;
                switch (p_type_info.getPrimitiveCategory()) {
                    case STRING:
                        obj = StringUtils.defaultString(kvMap.get(colName), "");
                        break;
                    case LONG:
                    case INT:
                        try {
                            obj = Long.parseLong(kvMap.get(colName));
                        } catch (Exception e) {
                        }
                }
            }
            row.add(obj);
        }

        return row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    @Override
    public SerDeStats getSerDeStats() {
        // Not suppourt yet
        return null;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        // Not suppourt yet
        return Text.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector oi)
            throws SerDeException {

        //TODO 需要研究源码，开发代码支持获取传入的数据，以及对应的字段名称

        return new Text("user_id=20#event=2002");
    }

}
