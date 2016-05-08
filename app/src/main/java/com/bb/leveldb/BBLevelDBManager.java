package com.bb.leveldb;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Created by zb on 15/4/13.
 */
public class BBLevelDBManager {

    private static MyLogger bblogger = MyLogger.bblog();
    private static BBLevelDBManager instance = null;
    final int BLK_MAX_KEY_VALUE_LENGTH = 20;

    private Map<String, DB> dbMap = null;

    private BBLevelDBManager() {
        dbMap = new HashMap<>();
    }

    public static synchronized BBLevelDBManager getInstance() {
        if (instance == null) {
            instance = new BBLevelDBManager();
        }
        return instance;
    }

    public DB getDatabase(String dbName) {
        bblogger.d("db dbMap.size = " + dbMap.size() + " " + dbMap.toString());
        DB db = dbMap.get(dbName);
        if (db == null) {
            db = openDatabase(dbName);
        }

        return db;
    }

    /**
     * @param dbFile：context.getDatabasePath("数据库名")
     * @return
     */
    public DB openDatabase(String dbName, File dbFile) {

        if (!dbFile.exists()) {
            dbFile.mkdirs();
        }

        DB db = dbMap.get(dbName);
        if (db != null) {
            if (dbMap != null) {
                if (dbMap.size() > 0)
                    ;
            }
        } else {
            db = new DB(dbFile);
            db.open();
            dbMap.put(dbName, db);
        }
        bblogger.i("打开数据库" + dbFile.getName());
        return db;
    }

    public DB openDatabase(String absolutePath) {

        File dbFile = new File(absolutePath);
        bblogger.i("db path" + dbFile.getAbsolutePath());
        return openDatabase(absolutePath, dbFile);
    }

    public DB openDatabase(String directory, String dbname) {
        File dbFile = new File(directory, dbname);
        bblogger.i("db path" + dbFile.getAbsolutePath());
        return openDatabase(dbname, dbFile);
    }

    /**
     * @param
     */
    public void closeDatabase(String dbname) {
        DB db = dbMap.get(dbname);
        if (db != null) {
            db.close();
            dbMap.remove(dbname);
        }
    }

    public void removeDatabase(String dbname) {
        DB db = dbMap.get(dbname);
        if (db != null) {
            db.close();
            db.destroy();
            dbMap.remove(dbname);
        }
    }


    public void closeAllDataBase() {
        for (DB db : dbMap.values()) {
            if (db != null)
                db.close();
        }
        dbMap.clear();
    }

    /**
     * 判断key是否存在
     *
     * @param key
     * @return
     */
    public boolean isKeyExist(String dbName, String key) {
        if (getDatabase(dbName).get(bytes(key)) != null)
            return true;
        else
            return false;
    }

    /**
     * 转化byte数组
     *
     * @param str
     * @return
     */
    private byte[] bytes(String str) {

        try {
            return str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public void put(String dbName, String key, String value) {
        getDatabase(dbName).put(bytes(key), bytes(value));
    }

    /**
     * 添加object value
     *
     * @param key
     * @param value
     */
    public void putObject(String dbName, String key, Object value) {
        getDatabase(dbName).put(bytes(key), toByteArray(value));
    }

    public String get(String dbName, String key) {
        bblogger.i(key);
        if (key != null && key.length() > 0) {
            byte[] val = getDatabase(dbName).get(bytes(key));
            if (val == null) {
                System.out.print("val is null");
                return null;
            }
            bblogger.i(new String(val));
            return (new String(val));
        } else
            return null;
    }

    public Object getObject(String dbName, String key) {
        System.out.print("getObject" + key);
        byte[] val = getDatabase(dbName).get(bytes(key));

        if (val == null) {
            System.out.print("val is null");
            return null;
        }

        return toObject(val);
    }

    public ArrayList<Object> mGet(String dbName, ArrayList<String> keys) {
        ArrayList<Object> values = new ArrayList<Object>();

        for (String index : keys) {
            if (getDatabase(dbName).get(bytes(index)) != null)
                values.add(new String(getDatabase(dbName).get(bytes(index))));
            else
                continue;
        }
        return values;
    }

    public ArrayList<String> mGet(String dbName, String[] keys) {
        ArrayList<String> values = new ArrayList<String>();

        for (String index : keys) {
            if (getDatabase(dbName).get(bytes(index)) != null)
                values.add(new String(getDatabase(dbName).get(bytes(index))));
            else
                values.add(null);
        }
        return values;
    }

    public void mPut(String dbName, Map<Object, Object> map) {
        final WriteBatch batch = new WriteBatch();

        try {
            final ByteBuffer key = ByteBuffer.allocate(BLK_MAX_KEY_VALUE_LENGTH);
            Iterator it = map.keySet().iterator();
            Object Okey;
            Object OValue;
            while (it.hasNext()) {
                Okey = it.next();
                OValue = map.get(Okey);

                key.clear();
                key.put(bytes(Okey.toString()));
                key.flip();

                ByteBuffer val = ByteBuffer.allocate(OValue.toString().length() * 4);
                val.clear();
                val.put(bytes(OValue.toString()));
                val.flip();
                batch.put(key, val);
            }
            getDatabase(dbName).write(batch);
        } finally {
            batch.close();
        }
    }

    public void del(String dbName, String key) {
        if (key != null)
            getDatabase(dbName).delete(bytes(key));
    }

    /**
     * 删除多个
     *
     * @param keys
     */
    public void mDel(String dbName, ArrayList<String> keys) {
        for (String indexKey : keys) {
            if (indexKey != null) {
                if (isKeyExist(dbName, indexKey))
                    getDatabase(dbName).delete(bytes(indexKey));
            }
        }
    }

    public void append(String dbName, String key, Object value) {
        byte[] val = getDatabase(dbName).get(bytes(key));
        StringBuffer sb = new StringBuffer();
        Set<String> set = new HashSet();

        if (val != null) {
            String valStr = new String(val);
            String[] appendDb = valStr.split(",");
            for (String item : appendDb) {
                set.add(item);
            }
        }

        String temp = (String) value;
        String[] appendNew = temp.split(",");

        for (String item : appendNew) {
            set.add(item);
        }

        StringBuilder result = new StringBuilder();
        for (String item : set) {
            if (result.length() > 0)
                result.append(",");

            result.append(item);
        }

        bblogger.i("append key=" + key + "result " + result.toString());
        put(dbName, key, result.toString());
    }

    public void appendlist(String dbName, String key, Object value) {
        byte[] val = getDatabase(dbName).get(bytes(key));
        StringBuffer sb = new StringBuffer();
        List<String> list = new ArrayList<String>();

        if (val != null) {
            String valStr = new String(val);
            String[] appendDb = valStr.split(",");
            for (String item : appendDb) {
                list.add(item);
            }
        }

        String temp = (String) value;
        String[] appendNew = temp.split(",");

        for (String item : appendNew) {
            list.add(item);
        }

        StringBuilder result = new StringBuilder();
        for (String item : list) {
            if (result.length() > 0)
                result.append(",");

            result.append(item);
        }

        bblogger.i("append key=" + key + "result " + result.toString());
        put(dbName, key, result.toString());
    }

    public void substract(String dbName, String key, Object value) {
        byte[] val = getDatabase(dbName).get(bytes(key));
        StringBuffer sb = new StringBuffer();
        Set<String> set = new HashSet();

        if (val != null) {
            String valStr = new String(val);
            String[] appendDb = valStr.split(",");
            for (String item : appendDb) {
                set.add(item);
            }
        }

        String temp = (String) value;
        String[] appendNew = temp.split(",");

        for (String item : appendNew) {
            if (set.contains(item))
                set.remove(item);
        }

        StringBuilder result = new StringBuilder();
        for (String item : set) {
            if (result.length() > 0)
                result.append(",");

            result.append(item);
        }

        put(dbName, key, result.toString());
    }


    public byte[] toByteArray(Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return bytes;
    }

    public Object toObject(byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return obj;
    }


}
