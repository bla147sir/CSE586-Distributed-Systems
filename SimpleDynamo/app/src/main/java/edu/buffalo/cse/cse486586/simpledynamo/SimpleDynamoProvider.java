package edu.buffalo.cse.cse486586.simpledynamo;

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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
	static final String[] AVD_PORT = {"5554", "5556", "5558", "5560", "5562"};
	static final int SERVER_PORT = 10000;
	int myPort, maxPort, minPort;
	ConcurrentHashMap<Integer, Node> hashMap_port = new ConcurrentHashMap<Integer, Node>(5);   //<key, value>
	ConcurrentHashMap<String, String> hashMap_query = new ConcurrentHashMap<String, String>();
	ConcurrentHashMap<String, Integer> hashMap_query_specific = new ConcurrentHashMap<String, Integer>();
	int query_specific_seq = 0;
	boolean end_wait_query_specific = false;
	int query_all_return_num = 0;
	boolean end_wait_query_all = false;


	private SQLiteDatabase SQLDB;
	private DatabaseHelper DBHelper;
	private static final String Database_Name = "DynamoDB";
	private static final String Table_Name = "DynamoTable";
	private static final int Database_Version = 1;
	public static final Uri Provider_Uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
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
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		SQLiteDatabase db = DBHelper.getWritableDatabase();
		Node node = hashMap_port.get(myPort);

		if (selection.equals("@")) {
			db.delete(Table_Name, null, null);
		} else if (selection.equals("*")) {
			// send delete msg to all the nodes
			String state = "all_nodes";
			String msgtosend = "DELETE|" + selection;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend, state);
		} else if (!selection.equals("@") && !selection.equals("*")) {
			String hash_key;
			boolean found = false;
			try {
				hash_key = genHash(selection);
				while (!found) {
					if ((hash_key.compareTo(node.ID) < 0 && hash_key.compareTo(node.PredID) > 0) ||
							(node.NodePort == minPort && hash_key.compareTo(node.ID) < 0) || (node.NodePort == minPort && hash_key.compareTo(node.PredID) > 0)) {   // the key is in this node
						found = true;
						String msg_forward = "DELETE|" + selection;
						String state = "forward_3_nodes";
						String receiver = Integer.toString(node.NodePort);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_forward, state, receiver);
					} else {
						node = hashMap_port.get(node.SuccPort);
					}
				}
			} catch (NoSuchAlgorithmException e) {
				Log.v(TAG, "can't create the hash key." + e.getMessage());
			}


		}
		return 0;
	}

	private int LocalDelete(String selection) {
		SQLiteDatabase db = DBHelper.getWritableDatabase();
		if (selection.equals("*")) {
			db.delete(Table_Name, null, null);
		} else {
			db.delete(Table_Name, "key=" + "'" + selection + "'", null);
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
		synchronized (this) {
			SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
			SQLiteDatabase db = DBHelper.getWritableDatabase();
			qb.setTables(Table_Name);
			String key = values.getAsString(KEY_FIELD);
			String value = values.getAsString(VALUE_FIELD);
			Node node = hashMap_port.get(myPort);
			try {
				String key_hash = genHash(key);
				boolean found = false;
				while (!found) {
					if ((key_hash.compareTo(node.PredID) > 0 && key_hash.compareTo(node.ID) <= 0) ||
							(node.NodePort == minPort && key_hash.compareTo(node.ID) < 0) || (node.NodePort == minPort && key_hash.compareTo(node.PredID) > 0)) {  // key should be in this node
						found = true;
						// send the insert msg to this node, succ node, and succ node's succ node
						String msg_forward = "INSERT|" + key + "|" + value;
						String state = "forward_3_nodes";
						String receiver = Integer.toString(node.NodePort);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_forward, state, receiver);
					} else {
						// find the succ node
						node = hashMap_port.get(node.SuccPort);
					}
				}
			} catch (NoSuchAlgorithmException e) {
				Log.e(TAG, "Can't create insert key hash: " + e.getMessage());
			}
			return null;
		}
	}

	private Uri LocalInsert(Uri uri, String key, String value) {
		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		SQLiteDatabase db = DBHelper.getWritableDatabase();
		qb.setTables(Table_Name);

		ContentValues cv = new ContentValues();
		cv.put(KEY_FIELD, key);
		cv.put(VALUE_FIELD, value);
		Cursor cursor = qb.query(SQLDB, null, "key='" + key + "'", null, null, null, null);

		if (cursor.getCount() <= 0) {    // there is no same key in db
			long id = db.insert(Table_Name, null, cv);
			if (id > 0) {
				Uri insert_Uri = ContentUris.appendId(Provider_Uri.buildUpon(), id).build();
				Log.e(TAG, " insert: key = " + key + ", value = " + value);
				return insert_Uri;
			}
			throw new SQLException("Failed to insert row into " + uri);
		} else {   // there is same key in db, so update to this one
			cursor.moveToFirst();
			db.update(Table_Name, cv, "key='" + key + "'", null);     // getInt(0)=get the value in column 0
			return null;
		}
	}

	private Void SetupRing() {
		try {
			int nodePort, predPort, succPort;
			String nodeID, predID, succID;
			maxPort = Integer.parseInt(REMOTE_PORT[0]);   // 11108
			minPort = Integer.parseInt(REMOTE_PORT[0]);
			String maxID = genHash(AVD_PORT[0]);      // hash 5554
			String minID = maxID;

			nodePort = Integer.parseInt(REMOTE_PORT[0]);
			nodeID = genHash(AVD_PORT[0]);
			Node node = new Node(nodePort, nodeID, nodePort, nodeID, nodePort, nodeID);     // nodeport, nodeID, predport, predID, succport, succID
			hashMap_port.put(node.NodePort, node);

			for (int i = 1; i < 5; i++) {
				nodePort = Integer.parseInt(REMOTE_PORT[i]);
				predPort = nodePort;
				succPort = nodePort;
				nodeID = genHash(AVD_PORT[i]);
				predID = nodeID;
				succID = nodeID;
				node = new Node(nodePort, nodeID, predPort, predID, succPort, succID);
				hashMap_port.put(node.NodePort, node);

				if (node.ID.compareTo(minID) < 0 || node.ID.compareTo(maxID) > 0) {   // smaller than mini or greater than max
					// update the new node's pred and succ
					node.PredPort = maxPort;
					node.PredID = maxID;
					node.SuccPort = minPort;
					node.SuccID = minID;
					hashMap_port.put(nodePort, node);

					// update front node (succ node)
					Node front = hashMap_port.get(node.SuccPort);
					front.PredPort = node.NodePort;
					front.PredID = node.ID;
					hashMap_port.put(front.NodePort, front);

					// update back node (pred node)
					Node back = hashMap_port.get(node.PredPort);
					back.SuccPort = node.NodePort;
					back.SuccID = node.ID;
					hashMap_port.put(back.NodePort, back);

					if (node.ID.compareTo(minID) < 0) { // update min
						minPort = node.NodePort;
						minID = node.ID;
					} else if (node.ID.compareTo(maxID) > 0) {    // update max
						maxPort = node.NodePort;
						maxID = node.ID;
					}
				} else {
					boolean finish = false;
					Node temp = hashMap_port.get(minPort);
					while (!finish) {
						if (node.ID.compareTo(temp.ID) > 0 && node.ID.compareTo(temp.SuccID) < 0) {
							// update new node
							node.PredPort = temp.NodePort;
							node.PredID = temp.ID;
							node.SuccPort = temp.SuccPort;
							node.SuccID = temp.SuccID;
							hashMap_port.put(node.NodePort, node);

							// update front node
							Node front = hashMap_port.get(node.SuccPort);
							front.PredPort = node.NodePort;
							front.PredID = node.ID;
							hashMap_port.put(front.NodePort, front);

							// update back node (pred node)
							Node back = hashMap_port.get(node.PredPort);
							back.SuccPort = node.NodePort;
							back.SuccID = node.ID;
							hashMap_port.put(back.NodePort, back);
							finish = true;
						} else {
							temp = hashMap_port.get(temp.SuccPort);
						}
					}
				}
			}
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "Can't create a node id:" + e.getMessage());
		}
		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = Integer.parseInt(portStr) * 2;

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

		// Set up ring
		SetupRing();

		// recover db
		// send msg to ask for reinsert the rows
		String sendmsg = "RECOVER_INSERT|" + Integer.toString(myPort);
		String state = "forward_recover";
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendmsg, state);

		return false;
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		SQLiteDatabase db = DBHelper.getWritableDatabase();
		qb.setTables(Table_Name);
		Cursor cursor;
		String[] s = selection.split("\\|");
		selection = s[0];
		String request;   // which node query

		if (s.length == 1) {
			request = Integer.toString(myPort);
		} else {
			request = s[1];
		}

		if (selection.equals("@")) {      // query all the rows in local db
			cursor = qb.query(db, null, null, null, null, null, null);
			Log.v("query: ", selection);
			return cursor;
		} else if (selection.equals("*")) {    //query all the rows in every db
			cursor = query_all(selection, request);
			Log.v("query: ", selection);
			return cursor;
		} else if (!selection.equals("@") && !selection.equals("*")) {
			cursor = query_specific(selection, request);
			Log.v("query: ", selection);
			return cursor;
		}

		Log.v("query: ", selection);
		return null;
	}

	private Cursor query_specific(String selection, String request) {
		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		SQLiteDatabase db = DBHelper.getWritableDatabase();
		qb.setTables(Table_Name);
		Cursor cursor;
		Node node = hashMap_port.get(myPort);
		try {
			String hash_key = genHash(selection);
			String sendmsg, state, receiver;
			String[] col_name = {KEY_FIELD, VALUE_FIELD};
			String[] insert = new String[2];
			boolean found = false;
			while (!found) {
				if ((hash_key.compareTo(node.ID) < 0 && hash_key.compareTo(node.PredID) > 0) ||
						(node.NodePort == minPort && hash_key.compareTo(node.ID) < 0) ||
						(node.NodePort == minPort && hash_key.compareTo(node.PredID) > 0)) {   // the key-value is in this node
					found = true;
					try {
						if ((node.NodePort != myPort) && (myPort == Integer.parseInt(request))) {    // send the selection to the node who have the key
							// only request node can send the msg
							query_specific_seq++;
							sendmsg = "QUERY_ONE|" + selection + "|" + request + "|" + Integer.toString(query_specific_seq);
							state = "forward_3_nodes";
							receiver = Integer.toString(node.NodePort);
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendmsg, state, receiver);

							// wait for the result to come back
							MatrixCursor m = new MatrixCursor(col_name);
							insert[0] = selection;
							insert[1] = hashMap_query.get(selection);
							synchronized (hashMap_query) {
								try {
									while (!end_wait_query_specific) {
										hashMap_query.wait();
									}
								} catch (InterruptedException e) {
									Log.e(TAG, "Query interrupted.");
								}

								insert[1] = hashMap_query.get(selection);
								Log.e(TAG, "get result = " + insert[1]);
								m.addRow(insert);
								end_wait_query_specific = false;
								return m;
							}
						} else if (node.NodePort == Integer.parseInt(request)) {    // the result node is the request
							cursor = qb.query(db, null, "key=" + "'" + selection + "'", null, null, null, null);
							return cursor;
						}
					} catch (Exception e) {
						return null;
					}

				} else {
					node = hashMap_port.get(node.SuccPort);
				}
			}
		} catch (NoSuchAlgorithmException e) {
			Log.v(TAG, "Can't create hash key:" + e.getMessage());
		}
		return null;
	}

	private Void LocalQuery(String selection, String request, int seq) {
		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		SQLiteDatabase db = DBHelper.getWritableDatabase();
		qb.setTables(Table_Name);
		Cursor cursor;
		try {
			cursor = qb.query(db, null, "key=" + "'" + selection + "'", null, null, null, null);
			int key_column = cursor.getColumnIndex(KEY_FIELD);
			int value_column = cursor.getColumnIndex(VALUE_FIELD);
			if (key_column == -1 || value_column == -1) {
				Log.e(TAG, "Wrong columns");
				cursor.close();
				throw new Exception();
			}
			cursor.moveToFirst();
			String key = cursor.getString(key_column);
			String value = cursor.getString(value_column);
			cursor.close();

			// send the result back to the request node,
			//Log.e(TAG, "send (" + key + ")back result to:" + request);
			String sendmsg = "QUERY_ONE|" + key + "|" + request + "|" + Integer.toString(seq) + "|" + value;
			String state = "specific";
			String receiver = request;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendmsg, state, receiver);
		} catch (Exception e) {
			return null;
		}
		return null;
	}

	private Cursor query_all(String selection, String request) {
		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		SQLiteDatabase db = DBHelper.getWritableDatabase();
		qb.setTables(Table_Name);
		Node node = hashMap_port.get(myPort);
		Cursor cursor;

		cursor = qb.query(db, null, null, null, null, null, null);   // get the rows from local db

		if (myPort == Integer.parseInt(request)) {        // this node query for the key-value
			String[] col_name = {KEY_FIELD, VALUE_FIELD};
			MatrixCursor m = new MatrixCursor(col_name);

			//send the selection all to every node
			String state = "random_3nodes";
			String query_all = "QUERY_ALL|" + selection + "|" + Integer.toString(myPort);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query_all, state);

			String[] insert = new String[2];
			synchronized (hashMap_query) {
				try {
					Log.e(TAG, "start waiting query all");
					while (!end_wait_query_all) {
						hashMap_query.wait(1000);
					}
					Log.e(TAG, "finish waiting query all");
				} catch (InterruptedException e) {
					Log.e(TAG, "Query interrupted.");
				}
				for (Entry<String, String> entry : hashMap_query.entrySet()) {
					insert[0] = entry.getKey();
					insert[1] = entry.getValue();
					m.addRow(insert);
				}
				MergeCursor mergeCursor = new MergeCursor(new Cursor[]{cursor, m});     // merge the node's local cursor with the received cursor
				hashMap_query.clear();   //clean all the entries in the hashmap
				query_all_return_num = 0;
				end_wait_query_all = false;
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
			// send all local db rows back to request
			String state = "specific";
			String msgtosend = "QUERY_ALL|" + Integer.toString(myPort) + "|" + request + "|" + cursor_string;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend, state, request);

		}
		return null;
	}

	private Void send_recover_insert(Cursor cursor, String request) {
		String cursor_string = "";
		if (cursor.getCount() > 0) {
			int key_column = cursor.getColumnIndex(KEY_FIELD);
			int value_column = cursor.getColumnIndex(VALUE_FIELD);
			if (key_column == -1 || value_column == -1) {
				Log.e(TAG, "Wrong columns");
				cursor.close();
			}
			try {
				cursor.moveToFirst();
				int check = 0;
				String key_hash;
				boolean recover_row;
				Node node;
				while (!cursor.isAfterLast()) {
					key_hash = genHash(cursor.getString(key_column));
					recover_row = false;
					node = hashMap_port.get(Integer.parseInt(request));

					// check if this key-value should be inserted to the recovered node
					for (int i = 0; i < 3; i++) {
						if ((key_hash.compareTo(node.PredID) > 0 && key_hash.compareTo(node.ID) <= 0) ||
								(node.NodePort == minPort && key_hash.compareTo(node.ID) < 0) ||
								(node.NodePort == minPort && key_hash.compareTo(node.PredID) > 0)) {
							recover_row = true;
						}
						node = hashMap_port.get(node.PredPort);
					}

					if (recover_row) {
						if (cursor_string.equals("")) {
							cursor_string = cursor.getString(key_column) + "|" + cursor.getString(value_column);
						} else {
							cursor_string = cursor_string + "|" + cursor.getString(key_column) + "|" + cursor.getString(value_column);
						}
						check++;
					}
					cursor.moveToNext();
				}
				cursor.close();
			} catch (NoSuchAlgorithmException e) {
				Log.e(TAG, "Can't hash key column value:" + e.getMessage());
			}
			String state = "specific";
			String msgtosend = "RECOVER_INSERT|" + request + "|" + Integer.toString(myPort) + "|" + cursor_string;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend, state, request);
		}
		cursor.close();
		return null;
	}

	private class ServerTask extends AsyncTask<ServerSocket, Void, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			Socket socket;
			DataInputStream inputMsg;
			String msg, state;
			String[] s;

			String query_select, selection, key, value, request, sender;
			Cursor cursor;
			try {
				while (true) {
					socket = serverSocket.accept();
					inputMsg = new DataInputStream(socket.getInputStream());
					msg = inputMsg.readUTF();
					s = msg.split("\\|");
					state = s[0];
					if (state.equals("INSERT")) {
						LocalInsert(Provider_Uri, s[1], s[2]);   //(key and value)
					} else if (state.equals("QUERY_ONE")) {
						key = s[1];
						request = s[2];
						int seq = Integer.parseInt(s[3]);
						if (myPort != Integer.parseInt(request) || s.length == 4) {
							LocalQuery(key, request, seq);
						} else if (s.length == 5) {   // request node get the result
							value = s[4];
							if (hashMap_query_specific.get(key) == null || hashMap_query_specific.get(key) < seq) {
								hashMap_query.put(key, value);
								hashMap_query_specific.put(key, seq);
								synchronized (hashMap_query) {
									hashMap_query.notify();
									end_wait_query_specific = true;
								}

							}
						}
					} else if (state.equals("QUERY_ALL")) {
						request = s[2];
						if (myPort != Integer.parseInt(request)) {
							query_select = s[1];
							Log.e(TAG, "collecting query all result for:" + request);
							selection = query_select + "|" + request;
							contentResolver.query(Provider_Uri, null, selection, null, null);    // there is no return cursor for here
						} else {
							sender = s[1];
							Log.e(TAG, "receiving query all result from:" + sender);
							int i = 3;
							while (i < s.length) {
								key = s[i];
								value = s[i + 1];
								hashMap_query.put(key, value);
								i = i + 2;
							}
							query_all_return_num++;
							if (query_all_return_num >= 2) {
								synchronized (hashMap_query) {
									hashMap_query.notify();
									end_wait_query_all = true;
								}
							}
						}
					} else if (state.equals("DELETE")) {
						selection = s[1];
						LocalDelete(selection);
					} else if (state.equals("RECOVER_INSERT")) {
						request = s[1];
						if (s.length == 2) {
							selection = "@";
							cursor = contentResolver.query(Provider_Uri, null, selection, null, null);
							send_recover_insert(cursor, request);
							cursor.close();
						} else if (myPort == Integer.parseInt(request)) {
							if (s.length - 3 > 0) {
								synchronized (this) {
									for (int i = 3; i < s.length; i = i + 2) {
										key = s[i];
										value = s[i + 1];
										LocalInsert(Provider_Uri, key, value);
									}
								}
							}
						}
					}
				}
			} catch (IOException e) {
				Log.e(TAG, "Error in Server socket." + e.getMessage());
			}
			return null;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {
		@Override
		protected Void doInBackground(String... msgs) {
			try {
				String msgToSend = msgs[0];
				String state = msgs[1];
				DataOutputStream outputMsg;

				if (state.equals("forward")) {  //send the msg of (insert key-value) to its succ
					Node node = hashMap_port.get(myPort);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), node.SuccPort);
					try {
						outputMsg = new DataOutputStream(socket.getOutputStream());
						outputMsg.writeUTF(msgToSend);
					} catch (IOException e) {
						Log.e(TAG, "Output message failed." + e.getMessage());
					}
					socket.close();
				} else if (state.equals("forward_3_nodes")) {
					String receiver = msgs[2];   //send to which node
					Node[] node = new Node[3];
					node[0] = hashMap_port.get(Integer.parseInt(receiver));
					node[1] = hashMap_port.get(node[0].SuccPort);
					node[2] = hashMap_port.get(node[1].SuccPort);

					for (int i = 0; i < 3; i++) {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), node[i].NodePort);
						try {
							outputMsg = new DataOutputStream(socket.getOutputStream());
							outputMsg.writeUTF(msgToSend);
						} catch (IOException e) {
							Log.e(TAG, "Output message failed." + e.getMessage());
						}
						socket.close();
					}
				} else if (state.equals("all_nodes")) {   //send to all nodes
					for (int i = 0; i < 5; i++) {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT[i]));
						try {
							outputMsg = new DataOutputStream(socket.getOutputStream());
							outputMsg.writeUTF(msgToSend);
						} catch (IOException e) {
							Log.e(TAG, "Output message failed." + e.getMessage());
						}
						socket.close();
					}
				} else if (state.equals("specific")) {
					String receiver = msgs[2];
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(receiver));
					try {
						outputMsg = new DataOutputStream(socket.getOutputStream());
						outputMsg.writeUTF(msgToSend);
					} catch (IOException e) {
						Log.e(TAG, "Output message failed." + e.getMessage());
					}
					socket.close();

				} else if (state.equals("forward_recover")) {
					Node node = hashMap_port.get(myPort);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), node.PredPort);
					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), node.SuccPort);
					try {
						outputMsg = new DataOutputStream(socket.getOutputStream());
						outputMsg.writeUTF(msgToSend);
						outputMsg = new DataOutputStream(socket1.getOutputStream());
						outputMsg.writeUTF(msgToSend);
					} catch (IOException e) {
						Log.e(TAG, "Output message failed." + e.getMessage());
					}
					socket.close();
					socket1.close();
				} else if (state.equals("random_3nodes")) {    // send query all to random three nodes not including itself
					Node node = hashMap_port.get(myPort);
					for (int i = 0; i < 3; i++) {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), node.SuccPort);
						try {
							outputMsg = new DataOutputStream(socket.getOutputStream());
							outputMsg.writeUTF(msgToSend);
						} catch (IOException e) {
							Log.e(TAG, "Output message failed." + e.getMessage());
						}
						node = hashMap_port.get(node.SuccPort);
						socket.close();
					}
				}
			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException: \n" + e.getMessage());
			}
			return null;

		}
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	class Node {
		int NodePort, PredPort, SuccPort;
		String ID, PredID, SuccID;

		Node(int NodePort, String ID, int PredPort, String PredID, int SuccPort, String SuccID) {
			this.NodePort = NodePort;
			this.ID = ID;
			this.PredPort = PredPort;
			this.PredID = PredID;
			this.SuccPort = SuccPort;
			this.SuccID = SuccID;
		}
	}
}