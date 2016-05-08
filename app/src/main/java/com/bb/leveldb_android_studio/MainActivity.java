package com.bb.leveldb_android_studio;

import android.app.Activity;
import android.os.Bundle;

import com.bb.leveldb.BBLevelDBManager;

import java.io.File;


public class MainActivity extends Activity {

    MyLogger bblogger = MyLogger.bblog();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        String leveldbname = "leveldb_BB";
        String dir = "data/data/" + getPackageName() + "/databases/"+leveldbname;
        bblogger.i(dir);

        File dbfile = new File(dir);

        if (!dbfile.exists()) {
            if (dbfile.mkdirs())
                bblogger.i("db 创建成功");
            else
                bblogger.i("db 创建失败");
        }

        bblogger.i(dbfile.getAbsolutePath());


        BBLevelDBManager dbManager = BBLevelDBManager.getInstance();
        dbManager.openDatabase(dbfile.getAbsolutePath());
        dbManager.put(dbfile.getAbsolutePath(), "stuName", "bb8");
        bblogger.i(dbManager.get(dbfile.getAbsolutePath(), "stuName"));
    }

}
