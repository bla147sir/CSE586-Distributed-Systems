package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.Context;
import android.content.ContentValues;
import android.os.AsyncTask;
import android.os.Bundle;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;
import android.widget.EditText;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map.Entry;
import java.sql.Timestamp;


/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */

public class GroupMessengerActivity extends Activity {

    public static final Uri Provider_Uri = Uri.parse( "content://edu.buffalo.cse.cse486586.groupmessenger2.provider");
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    int myPort;

    int sql_id = 0;
    int counter = 0;
    TimeComparator timeComparator = new TimeComparator();
    BlockingQueue<Message> hold_back_queue = new PriorityBlockingQueue<Message>(200, timeComparator);
    ConcurrentHashMap<Integer, Signal> hashMap_msg_signal = new ConcurrentHashMap<Integer, Signal>(200);      //<key, value>
    ConcurrentHashMap<Integer, Integer> hashMap_signal_count = new ConcurrentHashMap<Integer, Integer>(200);
    ConcurrentHashMap<Integer, Integer> hashMap_time = new ConcurrentHashMap<Integer, Integer>(10);      //<myport, time>
    int[] msg_count = new int[200];
    int index = 0;
    boolean failure = false;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = Integer.parseInt(portStr) * 2;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        final EditText editText = (EditText)findViewById(R.id.editText1);

        String msg = editText.getText().toString();    //get text from EditText and convert to string
        tv.setText(msg);     //set string from EditText to TextView

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        final Button SendButton = (Button) findViewById(R.id.button4);
        SendButton.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                // Perform action on click
                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.
                TextView localTextView = (TextView) findViewById(R.id.textView1);
                localTextView.append("\t" + msg + "\n"); // This is one way to display a string.

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            }
        });//setonclickListener end

    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            // server code that receives messages and passes them to onProgressUpdate().
            Socket socket;
            DataInputStream inputMsg;

            try {
                while(true){
                    socket = serverSocket.accept();
                  //  socket.setSoTimeout(10000);
                  //  try {
                        inputMsg = new DataInputStream(socket.getInputStream());
                        String msg = inputMsg.readUTF();     // reads characters encoded with modified UTF-8
                        String[] s = msg.split("\\|");
                        String state = s[1];

                        if (state.equals("original")) {
                            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                            int time = (int) (timestamp.getTime() % 10000000);

                            Message message = new Message(s, time);
                            hold_back_queue.add(message);    //put the message class to the hold-back queue based on message id

                            if (failure == false) {
                                hashMap_time.put(myPort, time);
                                for (int i = 11108; i <= 11124; i = i + 4) {
                                    if (hashMap_time.get(i)!=null) {
                                        int time2 = hashMap_time.get(i);
                                       // System.out.println("time2 " + time2);
                                        if ((int) (timestamp.getTime() % 10000000) - time2 > 1000) {
                                            failure = true;
//                                            System.out.println("time2 " + failure);

                                        }
                                    }
                                }
                            }


                            //send the id and sequencer back to the sender
                            try {
                                Socket socket_sendback = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), message.sender);

                                try {
                                    String msgSendBack = message.msg + "|signal|" + Integer.toString(message.msgid)
                                            + "|" + Integer.toString(message.receiver) + "|" + Integer.toString(message.time);
                                    DataOutputStream sendback_output = new DataOutputStream(socket_sendback.getOutputStream());
                                    sendback_output.writeUTF(msgSendBack);

                                } catch (IOException e) {
                                    Log.e(TAG, "Send back signal failed");
                                }
                                socket_sendback.close();

                            } catch (IOException e) {
                                Log.e(TAG, "Socket for send back IOException");
                            }

                        } else if (state.equals("signal")) {
                            Signal signal = new Signal(s);

                            boolean contains = hashMap_msg_signal.containsKey(signal.msgid);

                            if (!contains) {    //==false
                                hashMap_msg_signal.put(signal.msgid, signal);
                                msg_count[index]++;
                                hashMap_signal_count.put(signal.msgid, index);
                                index++;
                            } else if (contains) {    //==true
                                int count_index;

                                count_index = hashMap_signal_count.get(signal.msgid);
                                msg_count[count_index]++;

                                if (signal.time > hashMap_msg_signal.get(signal.msgid).time) {
                                    hashMap_msg_signal.put(signal.msgid, signal);
                                }

                            }

                            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                            if (failure == false) {
                                hashMap_time.put(myPort, (int)(timestamp.getTime() % 10000000));
                                for (int i = 11108; i <= 11124; i = i + 4) {
                                    if (hashMap_time.get(i)!=null) {
                                        int time2 = hashMap_time.get(i);
                                        // System.out.println("time2 " + time2);
                                        if ((int) (timestamp.getTime() % 10000000) - time2 > 1000) {
                                            failure = true;
//                                            System.out.println("time2 " + failure);

                                        }
                                    }
                                }
                            }

                            publishProgress(signal.state);

                        } else if (state.equals("deliverable")) {
                            Signal signal = new Signal(s);
                            Message hold_poll;
                            PriorityBlockingQueue<Message> temp_queue = new PriorityBlockingQueue<Message>(200, timeComparator);

                            while (hold_back_queue.peek() != null) {
                                hold_poll = hold_back_queue.poll();
                                if (hold_poll.msgid == signal.msgid) {
                                    if (hold_poll.time < signal.time) {
                                        hold_poll.time = signal.time;
                                        hold_poll.receiver = signal.receiver;
                                    }
                                    hold_poll.state = signal.state;
                                }
                                temp_queue.add(hold_poll);
                            }

                            while (temp_queue.peek() != null) {
                                hold_back_queue.add(temp_queue.poll());
                            }

                            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                            if (failure == false) {
                                hashMap_time.put(myPort, (int)(timestamp.getTime() % 10000000));
                                for (int i = 11108; i <= 11124; i = i + 4) {
                                    if (hashMap_time.get(i)!=null) {
                                        int time2 = hashMap_time.get(i);
                                        // System.out.println("time2 " + time2);
                                        if ((int) (timestamp.getTime() % 10000000) - time2 > 1000) {
                                            failure = true;
//                                            System.out.println("time2 " + failure);

                                        }
                                    }
                                }
                            }

                            publishProgress(signal.state);
                        }
                   // }catch (SocketTimeoutException ste){
                     //   failure = true;
                     //   Log.v(TAG, "socket time out");
                   // }


                }
            }
            catch (Exception e) {
                Log.v(TAG, "Error in ServerTask::"+e.getMessage());
            }

            return null;
        }

        protected void onProgressUpdate(String...strings) {

            String state = strings[0].trim();

            if (state.equals("signal"))
                new DeliverTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);

            if (state.equals("deliverable")){
                new SendoutTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
            }

        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Socket[] socket = new Socket[5];

                for (int i = 0; i < 5; i++)
                    socket[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT[i]));

                String msgToSend = msgs[0];
                /*
                 * client code that sends out a message.
                 */
                DataOutputStream outputMsg;
                try {
                    counter++;
                    int msgid = myPort * 100 + counter;
                    msgToSend = msgToSend + "|original|" +Integer.toString(msgid) + "|" + Integer.toString(myPort);

                    for (int i = 0; i < 5; i++){
                        outputMsg = new DataOutputStream(socket[i].getOutputStream());
                        outputMsg.writeUTF(msgToSend);
                    }

                }
                catch (IOException e) {
                    Log.e(TAG, "Output message failed");
                }
                for (int i = 0; i < 5; i++)
                    socket[i].close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }

    }


    private class DeliverTask extends AsyncTask<Void, Void, Void>{
        @Override
        protected Void doInBackground(Void... voids){

            int[] temp = msg_count;
            int index_5, key_id;
            Integer[] INT_count = new Integer[temp.length];

            for (int i = 0; i < temp.length; i++)
                INT_count[i] = Integer.valueOf(temp[i]);     //convert int[] to Integer[]

            boolean contains5 = Arrays.asList(INT_count).contains(5);
            while (contains5 && !failure){
                index_5 = Arrays.asList(INT_count).indexOf(5);
                msg_count[index_5] = 0;
                for (Entry<Integer, Integer> entry : hashMap_signal_count.entrySet()) {
                    if (index_5 == entry.getValue()) {
                        key_id = entry.getKey();
                        Signal signal_to_deliverable = hashMap_msg_signal.get(key_id);

                        // multicast the highest sequence number and receiver
                        try {
                            Socket[] socket = new Socket[5];

                            for (int i = 0; i < 5; i++)
                                socket[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT[i]));


                            try {
                                String msgToSend =
                                        signal_to_deliverable.msg + "|deliverable|" + Integer.toString(signal_to_deliverable.msgid) +
                                        "|"+ Integer.toString(signal_to_deliverable.receiver) + "|" + Integer.toString(signal_to_deliverable.time);

                                DataOutputStream outputMsg;

                                for (int i = 0; i < 5; i++){
                                    outputMsg = new DataOutputStream(socket[i].getOutputStream());
                                    outputMsg.writeUTF(msgToSend);
                                }

                            } catch (IOException e) {
                                Log.e(TAG, "Multicast signal failed");
                            }
                            for (int i = 0; i < 5; i++)
                                socket[i].close();

                        } catch (IOException e) {
                            Log.e(TAG, "Socket for multicasting signal IOException");
                        }
                    }
                }
                temp = msg_count;
                INT_count = new Integer[temp.length];
                for (int i = 0; i < temp.length; i++)
                    INT_count[i] = Integer.valueOf(temp[i]);

                contains5 = Arrays.asList(INT_count).contains(5);
            }

            // failure handling--------------
            boolean contains4 = Arrays.asList(INT_count).contains(4);
            while (failure && (contains5 || contains4)){
                int working_process = 0;

                if (contains4)
                    working_process = 4;
                else{
                    if (contains5)
                        working_process = 5;
                }

                index_5 = Arrays.asList(INT_count).indexOf(working_process);
                msg_count[index_5] = 0;
                for (Entry<Integer, Integer> entry : hashMap_signal_count.entrySet()) {
                    if (index_5 == entry.getValue()) {
                        key_id = entry.getKey();
                        Signal signal_to_deliverable = hashMap_msg_signal.get(key_id);

                        // multicast the highest sequence number and receiver
                        try {
                            Socket[] socket = new Socket[5];

                            for (int i = 0; i < 5; i++)
                                socket[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT[i]));

                            try {
                                String msgToSend =
                                        signal_to_deliverable.msg + "|deliverable|" + Integer.toString(signal_to_deliverable.msgid) +
                                                "|"+ Integer.toString(signal_to_deliverable.receiver) + "|" + Integer.toString(signal_to_deliverable.time);

                                DataOutputStream outputMsg;

                                for (int i = 0; i < 5; i++){
                                    outputMsg = new DataOutputStream(socket[i].getOutputStream());
                                    outputMsg.writeUTF(msgToSend);
                                }

                            } catch (IOException e) {
                                Log.e(TAG, "Multicast signal failed");
                            }
                            for (int i = 0; i < 5; i++)
                                socket[i].close();

                        } catch (IOException e) {
                            Log.e(TAG, "Socket for multicasting signal IOException");
                        }
                    }
                }
                temp = msg_count;
                INT_count = new Integer[temp.length];
                for (int i = 0; i < temp.length; i++)
                    INT_count[i] = Integer.valueOf(temp[i]);

                contains4 = Arrays.asList(INT_count).contains(4);
                contains5 = Arrays.asList(INT_count).contains(5);
            }

            return null;
        }

    }


    private class SendoutTask extends AsyncTask<Void, String, Void>{
        @Override
        protected Void doInBackground(Void... voids){

            Message sendmsg;
            try{
                while (hold_back_queue.peek().state.equals("deliverable")){
                    sendmsg = hold_back_queue.poll();
                    publishProgress(sendmsg.msg);
                }

            }catch (Exception e){
                Log.v(TAG, "Error in SendoutTask::"+e.getMessage());
            }

            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();

            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\t\n");

            //insert the msg to sqlite database
            try {
                ContentValues cv = new ContentValues();
                cv.put("key", Integer.toString(sql_id));
                cv.put("value", strReceived);
                sql_id++;

                getContentResolver().insert(Provider_Uri, cv);

            } catch (Exception e) {
                Log.e(TAG, "Insert message failed:" + e.toString());
            }
        }
    }

    class Message{
        int sender, msgid, receiver, time;
        String msg;
        String state;

        Message(String[] inputmsg, int time){
            msg = inputmsg[0];
            state = inputmsg[1];
            msgid = Integer.parseInt(inputmsg[2]);           //the counter of the send-process
            sender = Integer.parseInt(inputmsg[3]);           //send from which process
            receiver = myPort;
            this.time = time;
        }
    }

    class Signal{
        int msgid, receiver, time;
        String msg, state;

        Signal(String[] inputmsg){
            msg = inputmsg[0];
            state = inputmsg[1];
            msgid = Integer.parseInt(inputmsg[2]);
            receiver = Integer.parseInt(inputmsg[3]);
            time = Integer.parseInt(inputmsg[4]);
        }
    }

    class TimeComparator implements Comparator<Message>{
        public int compare(Message msg1, Message msg2){
            if (msg1.time < msg2.time)  return -1;
            if (msg1.time > msg2.time)  return 1;
       /*     if (msg1.time == msg2.time){
                if (msg1.receiver < msg2.receiver)  return -1;
                if (msg1.receiver > msg2.receiver)  return 1;
                else return 0;
            }
         */   else return 0;
        }
    }


}
