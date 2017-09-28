package edu.buffalo.cse.cse486586.groupmessenger1;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.ContentUris;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.net.Uri;
import android.util.Log;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.SQLException;

/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 * 
 * Please read:
 * 
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 * 
 * before you start to get yourself familiarized with ContentProvider.
 * 
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 * 
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {

    private SQLiteDatabase SQLDB;
    private DatabaseHelper DBHelper;
    private static final String Database_Name = "GroupMessengerDB";
    private static final String Table_Name = "GroupMessengerTable";
    private static final int Database_Version = 3;
    public static final Uri Provider_Uri = Uri.parse( "content://edu.buffalo.cse.cse486586.groupmessenger1.provider");
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";


    private static class DatabaseHelper extends SQLiteOpenHelper {
        DatabaseHelper(Context context) {
            super(context, Database_Name, null, Database_Version);
        }

        private static final String TABLE_CREATE = "CREATE TABLE " + Table_Name + " (" + KEY_FIELD + " STRING, " + VALUE_FIELD + " STRING)";

        @Override
        public void onCreate(SQLiteDatabase DB) {
            DB.execSQL(TABLE_CREATE);  //Create a table.
        }

        @Override
        public void onUpgrade(SQLiteDatabase DB, int oldVersion, int newVersion) {
        }

    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         * 
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */

        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        SQLiteDatabase db = DBHelper.getWritableDatabase();
        qb.setTables(Table_Name);
        Cursor cursor = qb.query( SQLDB, null, "key='"+values.get(KEY_FIELD)+"'", null, null, null, null);

        if (cursor.getCount() <= 0){
            long id = db.insert(Table_Name, null, values);
            if (id > 0) {
                Uri insert_Uri = ContentUris.appendId(Provider_Uri.buildUpon(), id).build();
             //   getContext().getContentResolver().notifyChange(insert_Uri, null);
                Log.v("insert", values.toString());
                return insert_Uri;
            }
            throw new SQLException("Failed to insert row into " + uri);
        }
        else{
            cursor.moveToFirst();
            db.update(Table_Name, values, "key='"+cursor.getInt(0)+"'", null);
            return null;
        }


    }

    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.
        DBHelper = new DatabaseHelper(getContext());
        SQLDB = DBHelper.getWritableDatabase();

        if (SQLDB.isReadOnly()) {
            SQLDB.close();
            SQLDB = null;
        }
        return false;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */

        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        SQLiteDatabase db = DBHelper.getWritableDatabase();
        qb.setTables(Table_Name);
        Cursor cursor = qb.query( db, null, "key=" + "'" + selection + "'", null, null, null, null);
        //  cursor.setNotificationUri(getContext().getContentResolver(), uri);
        Log.v("query", selection);

        return cursor;

    }
}
