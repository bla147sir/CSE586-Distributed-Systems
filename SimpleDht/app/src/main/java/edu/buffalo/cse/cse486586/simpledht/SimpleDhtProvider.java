package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.Context;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.SQLException;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;


public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final int JOIN_NODE0 = 11108;
    int myPort;
    String myNodeID;
    ConcurrentHashMap<Integer, Message> hashMap_port = new ConcurrentHashMap<Integer, Message>();      //<key, value>
    ConcurrentHashMap<String, String> hashMap_query = new ConcurrentHashMap<String, String>();
    String  minID, maxID;
    int minPort, maxPort;
    int total_node = 1;
    boolean end_wait = false;

    private SQLiteDatabase SQLDB;
    private DatabaseHelper DBHelper;
    private static final String Database_Name = "DHTDB";
    private static final String Table_Name = "DHTtable";
    private static final int Database_Version = 1;
    public static final Uri Provider_Uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    ContentResolver contentResolver;

    private static class DatabaseHelper extends SQLiteOpenHelper {
        DatabaseHelper(Context context) {
            super(context, Database_Name, null, Database_Version);
        }
        private static final String TABLE_CREATE = "CREATE TABLE " + Table_Name + " (" + KEY_FIELD + " STRING, " + VALUE_FIELD + " STRING)";
        private static final String TABLE_DELETE = "DROP TABLE IF EXISTS " + Table_Name;
        @Override
        public void onCreate(SQLiteDatabase DB) {
            DB.execSQL(TABLE_CREATE);  //Create a table.
        }

        @Override
        public void onUpgrade(SQLiteDatabase DB, int oldVersion, int newVersion) {
            DB.execSQL(TABLE_DELETE);
            onCreate(DB);
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        SQLiteDatabase db = DBHelper.getWritableDatabase();
        Message message = hashMap_port.get(myPort);

        if (message.ID.equals(message.PredID)) { //there is only one node
            if (selection.equals("@") || selection.equals("*")){
                db.delete(Table_Name,null,null);
            }else {
                db.delete(Table_Name, "key=" + "'" + selection + "'", null);
            }
        }
        else{     //there are more then one nodes
            String[] s = selection.split("\\|");
            selection = s[0];
            String request;   // which node query
            if (s.length == 1){
                request = Integer.toString(myPort);
            } else {
                request = s[1];
            }

            if (selection.equals("@")){
                db.delete(Table_Name,null,null);
            }
            else if (selection.equals("*")){
                db.delete(Table_Name,null,null);
                if (myPort ==Integer.parseInt(request)){
                    String state = "forward";
                    String query_all = "DELETE_ALL|" + selection +"|"+Integer.toString(myPort);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query_all, state);
                }else if (hashMap_port.get(myPort).Succ != Integer.parseInt(request)){
                    String state = "forward";
                    String query_all = "DELETE_ALL|" + selection +"|"+request;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query_all, state);
                }
            }
            else if (!selection.equals("@") && !selection.equals("*")){
                String hash_key = "";
                try {
                    hash_key = genHash(selection);
                } catch (NoSuchAlgorithmException e){
                    Log.v(TAG, "can't create the hash key."+e.getMessage());
                }

                if ((hash_key.compareTo(message.ID) < 0 && hash_key.compareTo(message.PredID) > 0) ||
                        (message.ID.equals(minID) && hash_key.compareTo(message.ID) < 0) ||
                        (message.ID.equals(minID) && hash_key.compareTo(message.PredID) > 0))
                {          // the key is in this node
                    db.delete(Table_Name, "key=" + "'" + selection + "'", null);
                } else {
                    String state = "forward";
                    String query_all = "DELETE_Single|" + selection;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query_all, state);
                }

            }
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        SQLiteDatabase db = DBHelper.getWritableDatabase();
        qb.setTables(Table_Name);
        String key = values.getAsString(KEY_FIELD);
        String value = values.getAsString(VALUE_FIELD);
        Message message = hashMap_port.get(myPort);

        try {
            String key_hash = genHash(key);
            if ((key_hash.compareTo(message.PredID) > 0 && key_hash.compareTo(message.ID) <= 0) ||
                    (myPort == minPort && key_hash.compareTo(message.ID) < 0) ||
                    (myPort == minPort && key_hash.compareTo(message.PredID) > 0) || (message.ID.equals(message.PredID)))
            {  //find the correct node or there is only one node
                long id = db.insert(Table_Name, null, values);
                if (id > 0){
                    Uri insert_Uri = ContentUris.appendId(Provider_Uri.buildUpon(), id).build();
                    return insert_Uri;
                }
                throw new SQLException("Failed to insert row into " + uri);
            } else {
                String msg_forward = "FORWARD|" + key + "|" + value;
                String state = "forward";
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_forward, state);
            }
        } catch (NoSuchAlgorithmException e){
            Log.e(TAG, "Can't create insert key hash: "+e.getMessage());
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = Integer.parseInt(portStr) * 2;
        int myPred = myPort;
        int mySucc = myPort;
        String myPredID;
        String mySuccID;
        minPort = myPort;
        maxPort = myPort;

        // Create database
        DBHelper = new DatabaseHelper(getContext());
        SQLDB = DBHelper.getWritableDatabase();
        if (SQLDB.isReadOnly()) {
            SQLDB.close();
            SQLDB = null;
        }
        contentResolver = (this.getContext()).getContentResolver();

        // Setup server socket
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket: " + e.getMessage());
            return false;
        }

        // Obtain node id by hash function: genHash
        try {
            myNodeID = genHash(portStr);
            myPredID = myNodeID;
            mySuccID = myNodeID;
            minID = myNodeID;
            maxID = myNodeID;
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Can't create a node id: " + e.getMessage());
            return false;
        }

        String msg_Join = "JOIN|"+Integer.toString(myPort)+"|"+myNodeID+"|"+myPred+"|"+myPredID+"|"+mySucc+"|"+mySuccID;
        String[] s = msg_Join.split("\\|");
        Message m = new Message(s);
        hashMap_port.put(myPort, m);
        if (myPort != JOIN_NODE0){
            String state = "node_join";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_Join, state);
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        SQLiteDatabase db = DBHelper.getWritableDatabase();
        qb.setTables(Table_Name);
        Cursor cursor;
        Message message = hashMap_port.get(myPort);

        if (message.ID.equals(message.PredID)){   //there is only one node
            if (selection.equals("@") || selection.equals("*")) {    // @ and * will be same function if there is only one node
                cursor = qb.query(db, null, null, null, null, null, null);
            } else {
                cursor = qb.query(db, null, "key=" + "'" + selection + "'", null, null, null, null);
            }
            return cursor;
        } else {   // there are more than one nodes
            String[] s = selection.split("\\|");
            selection = s[0];
            String request;   // which node query
            if (s.length == 1){
                request = Integer.toString(myPort);
            } else {
                request = s[1];
            }

            if (selection.equals("@")){      // query all the rows in local db
                cursor = qb.query(db, null, null, null, null, null, null);
                return cursor;
            }
            else if (selection.equals("*")){    //query all the rows in every db
                cursor = query_all(selection, request);
                return cursor;
            }
            else if (!selection.equals("@") && !selection.equals("*")){
                cursor = query_specific(selection, request);
                return cursor;
            }
        }
        Log.v("query", selection);

        return null;
    }

    private Cursor query_specific(String selection, String request){
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        SQLiteDatabase db = DBHelper.getWritableDatabase();
        qb.setTables(Table_Name);
        Cursor cursor;
        Message message = hashMap_port.get(myPort);

        try {
            String hash_key = genHash(selection);

            if ((hash_key.compareTo(message.ID) < 0 && hash_key.compareTo(message.PredID) > 0) ||
                    (message.ID.equals(minID) && hash_key.compareTo(message.ID) < 0) ||
                    (message.ID.equals(minID) && hash_key.compareTo(message.PredID) > 0))
            {          // whether the key is in this node
                try {
                    cursor = qb.query(db, null, "key=" + "'" + selection + "'", null, null, null, null);
                    if (message.Port != Integer.parseInt(request) &&  cursor != null){
                       // Log.v(TAG,"Found query "+ selection+" in "+Integer.toString(myPort));

                        int key_column = cursor.getColumnIndex(KEY_FIELD);
                        int value_column  = cursor.getColumnIndex(VALUE_FIELD);
                        if (key_column == -1 || value_column == -1) {
                            Log.e(TAG, "Wrong columns");
                            cursor.close();
                            throw new Exception();
                        }
                        cursor.moveToFirst();
                        String key = cursor.getString(key_column);
                        String value = cursor.getString(value_column);
                        cursor.close();
                        sendquery_back(key, value, request);     // the key-value is in this node, and send back to the request
                        //return null;
                    } else{
                        return cursor;
                    }

                } catch (Exception e){
                    return null;
                }

            } else {      // the key-value is not in this node
                String[] col_name = {KEY_FIELD, VALUE_FIELD};
                MatrixCursor m = new MatrixCursor(col_name);
                String state = "forward";  //send the key to succ
                String query_key = "QUERY_ONE|" + selection + "|" + request;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query_key, state);    // send the key-value to the its succ

                if (myPort == Integer.parseInt(request)) {     // this node query for the key-value
                    String[] insert = new String[2];
                    insert[0] = selection;
                    insert[1] = hashMap_query.get(selection);
                    synchronized(hashMap_query) {
                        try {
                            while (hashMap_query.get(selection)==null) {
                                hashMap_query.wait();
                            }
                        } catch (InterruptedException e) {
                            Log.e(TAG, "Query interrupted.");
                        }

                        insert[1] = hashMap_query.get(selection);
                        hashMap_query.remove(selection);     //clean the hashmap
                        m.addRow(insert);
                        return m;
                    }
                } else
                    return null;
            }
        } catch (NoSuchAlgorithmException e){
            Log.v(TAG, "Can't create hash key:"+ e.getMessage());
        }
        return null;
    }

    private void sendquery_back(String sendkey, String sendvalue, String request){
        DataOutputStream outputMsg;
        String sendmsg;
        try{
            sendmsg = "QUERY_ONE|" + sendkey + "|" + request + "|" + sendvalue;
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(request));
            outputMsg = new DataOutputStream(socket.getOutputStream());
            outputMsg.writeUTF(sendmsg);
            socket.close();
        }
        catch (IOException e){
            Log.e(TAG, "Output message failed." + e.getMessage());
        }
    }

    private Cursor query_all(String selection, String request){
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        SQLiteDatabase db = DBHelper.getWritableDatabase();
        qb.setTables(Table_Name);
        Message message = hashMap_port.get(myPort);
        Cursor cursor;

        cursor = qb.query(db, null, null, null, null, null, null);   // get the rows from local db

        if (myPort ==Integer.parseInt(request)){        // this node query for the key-value
            String[] col_name = {KEY_FIELD, VALUE_FIELD};
            MatrixCursor m = new MatrixCursor(col_name);
            //send the selection all to every node
            String state = "forward";
            String query_all = "QUERY_ALL|" + selection + "|" + Integer.toString(myPort)+"|"+Integer.toString(message.Succ);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query_all, state);

            String[] insert = new String[2];
            synchronized(hashMap_query) {
                try {
                    while (!end_wait) {
                        hashMap_query.wait();
                    }
                } catch (InterruptedException e) {
                    Log.e(TAG, "Query interrupted.");
                }
                for (Entry<String, String> entry : hashMap_query.entrySet()){
                    insert[0] = entry.getKey();
                    insert[1] = entry.getValue();
                    m.addRow(insert);
                }
                MergeCursor mergeCursor = new MergeCursor(new Cursor[] {cursor, m});     // merge the node's local cursor with the received cursor
                hashMap_query.clear();   //clean all the entries in the hashmap
                //  Log.v(TAG, "cursor rowww:"+mergeCursor.getCount());
                return mergeCursor;
            }

        } else {
            String cursor_string = "";
            if (cursor.getCount() > 0) {
                int key_column = cursor.getColumnIndex(KEY_FIELD);
                int value_column = cursor.getColumnIndex(VALUE_FIELD);
                if (key_column == -1 || value_column == -1) {
                    Log.e(TAG, "Wrong columns");
                    cursor.close();
                }
                cursor.moveToFirst();
                while (!cursor.isAfterLast()) {
                    if (cursor_string.equals("")) {
                        cursor_string = cursor.getString(key_column) + "|" + cursor.getString(value_column);
                    } else {
                        cursor_string = cursor_string + "|" + cursor.getString(key_column) + "|" + cursor.getString(value_column);
                    }
                    cursor.moveToNext();
                }
                cursor.close();
            } else {
                cursor_string = "";
            }

            boolean finish_all = false;
            if (request.equals(Integer.toString(message.Succ))){
                finish_all = true;
            }else {
                // send the selection to next node
                String state = "forward";
                String query_all = "QUERY_ALL|" + selection + "|" + request + "|" + Integer.toString(message.Succ);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query_all, state);
            }
            // send all the local db rows back to the request
            sendquery_all(cursor_string, request, finish_all);
        }
        return null;
    }

    private void sendquery_all(String cursor_string, String request, boolean finish_all){
        DataOutputStream outputMsg;
        String sendmsg;
        try{
            if (!finish_all) {
                sendmsg = "QUERY_ALL|not_finish|request_message|" + cursor_string;
            } else{
                //finish_all: all node have send their local rows; request_message: only use to that the sendmsg length become odd number
                sendmsg = "QUERY_ALL|finish_all|request_message|" + cursor_string;
            }
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(request));
            outputMsg = new DataOutputStream(socket.getOutputStream());
            outputMsg.writeUTF(sendmsg);
            socket.close();
        }
        catch (IOException e){
            Log.e(TAG, "Output message failed." + e.getMessage());
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    // Use SHA-1 as our hash function to generate keys
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, Void, Void>{
        @Override
        protected Void doInBackground(ServerSocket... sockets){
            ServerSocket serverSocket = sockets[0];
            Socket socket;
            DataInputStream inputMsg;
            try{
                while(true){
                    socket = serverSocket.accept();
                    inputMsg = new DataInputStream(socket.getInputStream());
                    String msg = inputMsg.readUTF();
                    String[] s = msg.split("\\|");
                    String state = s[0];
                    if (state.equals("JOIN")){   // new node join the chord
                        join(s);
                    }
                    else if (state.equals("SETUP")){   // setup other node's hash table
                        Message message = new Message(s);
                        hashMap_port.put(message.Port, message);
                        minPort = Integer.parseInt(s[7]);
                        maxPort = Integer.parseInt(s[8]);
                        minID = s[9];
                        maxID = s[10];
                    }
                    else if (state.equals("FORWARD")){
                        ContentValues cv = new ContentValues();
                        cv.put(KEY_FIELD, s[1]);      // s[1] = key
                        cv.put(VALUE_FIELD, s[2]);    // s[2] = value
                        contentResolver.insert(Provider_Uri, cv);
                    }
                    else if (state.equals("QUERY_ONE")){
                        String query_key = s[1];
                        String request = s[2];
                        if (myPort != Integer.parseInt(request)) {
                            String selection = query_key + "|" + request;
                            Cursor cursor = contentResolver.query(Provider_Uri, null, selection, null, null);
                        }
                        else {
                            String query_value = s[3];
                            hashMap_query.put(query_key, query_value);    //key, value
                            synchronized(hashMap_query) {
                                hashMap_query.notify();
                            }
                        }
                    }
                    else if (state.equals("QUERY_ALL")){
                        if (s.length == 4){
                            String query_select = s[1];
                            String request = s[2];
                            String selection = query_select + "|" + request;
                            Cursor cursor = contentResolver.query(Provider_Uri, null, selection, null, null);
                        } else {
                            String can_notify = s[1];
                            String key;
                            String value;
                            int i = 3;
                            while (i < s.length){
                                key = s[i];
                                value = s[i+1];
                                hashMap_query.put(key, value);
                                i = i + 2;
                            }
                            if (can_notify.equals("finish_all")) {
                                synchronized (hashMap_query) {
                                    hashMap_query.notify();
                                    end_wait = true;
                                }
                            }
                        }
                    }
                    else if (state.equals("DELETE_ALL")){
                        String selection = s[1] + "|" + s[2];
                        delete(Provider_Uri, selection, null);
                    }
                    else if (state.equals("DELETE_Single")){
                        String selection = s[1];
                        delete(Provider_Uri, selection, null);
                    }
                }
            }
            catch (IOException e){
                Log.e(TAG, "Error in Server socket." + e.getMessage());
            }
            return null;
        }
    }
    private void join(String[] s){
        total_node ++;
        Message message = new Message(s);
        Message temp_min = hashMap_port.get(minPort);
        Message temp_max = hashMap_port.get(maxPort);

        if (message.ID.compareTo(temp_min.ID) < 0 || message.ID.compareTo(temp_max.ID) > 0){  // smaller than mini or greater than max
            if (message.ID.compareTo(temp_min.ID) < 0) {      // update min
                minPort = message.Port;
                minID = message.ID;
            //    System.out.println("change min = "+minPort+", max = "+maxPort);
            }else if (message.ID.compareTo(temp_max.ID) > 0){    //update max
                maxPort = message.Port;
                maxID = message.ID;
            //     System.out.println("min = "+minPort+",change max = "+maxPort);
            }

            // update the new joined node's pred and succ
            message.Succ = temp_min.Port;
            message.SuccID = temp_min.ID;
            message.Pred = temp_max.Port;
            message.PredID = temp_max.ID;
            hashMap_port.put(message.Port, message);

            //update front node
            Message front = hashMap_port.get(message.Succ);
            front.Pred = message.Port;
            front.PredID = message.ID;
            hashMap_port.put(front.Port, front);

            //update back node
            Message back = hashMap_port.get(message.Pred);
            back.Succ = message.Port;
            back.SuccID = message.ID;
            hashMap_port.put(back.Port, back);
            //System.out.println("myport= "+message.Port+", pre ="+ message.Pred+", succ="+message.Succ);
        }
        else{
            boolean finish = false;
            while (!finish){
                if (message.ID.compareTo(temp_min.ID) > 0 && message.ID.compareTo(temp_min.SuccID) < 0 ){
                    //update current node
                    message.Pred = temp_min.Port;
                    message.PredID = temp_min.ID;
                    message.Succ = temp_min.Succ;
                    message.SuccID = temp_min.SuccID;
                    hashMap_port.put(message.Port, message);

                    //update backward node
                    temp_min.Succ = message.Port;
                    temp_min.SuccID = message.ID;
                    hashMap_port.put(temp_min.Port, temp_min);

                    //update front node
                    Message front_node= hashMap_port.get(message.Succ);
                    front_node.Pred = message.Port;
                    front_node.PredID = message.ID;
                    hashMap_port.put(front_node.Port, front_node);
                    finish = true;   // finish finding the pred and succ for the new join node
                  //  System.out.println("myport= "+message.Port+", pre ="+ message.Pred+", succ="+message.Succ);
                  //  System.out.println("min = "+minPort+", max = "+maxPort);
                }
                else{
                    temp_min = hashMap_port.get(temp_min.Succ);
                }
            }
        }

        // send the update msg to every process
        String state = "joined_finish";
        String msg_update ="SETUP|"+Integer.toString(message.Port)+"|"+message.ID+"|"+message.Pred+"|"+message.PredID+"|"+message.Succ+"|"+message.SuccID+"|"+Integer.toString(minPort)+"|"+Integer.toString(maxPort)+"|"+minID+"|"+maxID;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_update, state);
        Message msg_pred = hashMap_port.get(message.Pred);
        msg_update ="SETUP|"+Integer.toString(msg_pred.Port)+"|"+msg_pred.ID+"|"+msg_pred.Pred+"|"+msg_pred.PredID+"|"+msg_pred.Succ+"|"+msg_pred.SuccID+"|"+Integer.toString(minPort)+"|"+Integer.toString(maxPort)+"|"+minID+"|"+maxID;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_update, state);
        Message msg_succ = hashMap_port.get(message.Succ);
        msg_update ="SETUP|"+Integer.toString(msg_succ.Port)+"|"+msg_succ.ID+"|"+msg_succ.Pred+"|"+msg_succ.PredID+"|"+msg_succ.Succ+"|"+msg_succ.SuccID+"|"+Integer.toString(minPort)+"|"+Integer.toString(maxPort)+"|"+minID+"|"+maxID;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_update, state);
    }

    private class ClientTask extends AsyncTask<String, Void, Void>{
        @Override
        protected Void doInBackground(String... msgs){
            try{
                String msgToSend = msgs[0];
                String state = msgs[1];
                DataOutputStream outputMsg;

                if (state.equals("node_join")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), JOIN_NODE0);
                    try{
                        outputMsg = new DataOutputStream(socket.getOutputStream());
                        outputMsg.writeUTF(msgToSend);
                    }
                    catch (IOException e){
                        Log.e(TAG, "Output message failed." + e.getMessage());
                    }
                    socket.close();
                }
                else if (state.equals("joined_finish")){      //send (the information of each node's pred and succ) to every node
                    Message temp = hashMap_port.get(minPort);
                    int count = 1;
                    while (count <= total_node){
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), temp.Port);
                        try{
                            outputMsg = new DataOutputStream(socket.getOutputStream());
                            outputMsg.writeUTF(msgToSend);
                        }
                        catch (IOException e){
                            Log.e(TAG, "Output message failed." + e.getMessage());
                        }
                        socket.close();
                        temp = hashMap_port.get(temp.Succ);
                        count++;
                    }
                }
                else if (state.equals("forward")){  //send the msg of (insert key-value) to its succ
                    Message temp = hashMap_port.get(myPort);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), temp.Succ);
                    try{
                        outputMsg = new DataOutputStream(socket.getOutputStream());
                        outputMsg.writeUTF(msgToSend);
                    }
                    catch (IOException e){
                        Log.e(TAG, "Output message failed." + e.getMessage());
                    }
                    socket.close();
                }
            }
            catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            }
            catch (IOException e){
                Log.e(TAG, "ClientTask socket IOException: \n" + e.getMessage());
            }
            return null;
        }
    }

    class Message{
        String state, ID, PredID, SuccID;
        int Port, Pred, Succ;
        Message(String[] inputmsg){
            state = inputmsg[0];
            Port = Integer.parseInt(inputmsg[1]);
            ID = inputmsg[2];
            Pred = Integer.parseInt(inputmsg[3]);
            PredID = inputmsg[4];
            Succ = Integer.parseInt(inputmsg[5]);
            SuccID = inputmsg[6];
        }
    }
}
