package org.apache.pig.backend.storm.state;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.pig.backend.storm.oper.TriBasicPersist.TriBasicPersistState;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.Pair;

public class WindowBundle<T extends Writable> implements Writable {

	private int maxSize;
	private HashMap<Integer, Window> closed;
	private Window openWin;
	private int closedAge = 10000;
	Random r = new Random();

	public String toString() {
		return "WindowBundle@" + hashCode() + " open: " + openWin + " closed: " + closed;
	}
	
	public static class Window {
		int cur_id;
		boolean isClosed;
		TriBasicPersistState contents;
		int itemCount;
		long closedTS;
		
		public String toString() {
			return "Window@" + cur_id + "[" + isClosed + "] -> " + contents.toString();
		}
	}
	
	public WindowBundle() {
		
	}
	
	public void merge(WindowBundle other) {
		// Pull the closed windows in.
		for (Object o : other.closed.values()) {
			Window w = (Window) o;
			
			if (closed.containsKey(w.cur_id)) {
				// Create a new closed window with a new id.
				Window nw = new Window();
				nw.closedTS = w.closedTS;
				nw.cur_id = getId();
				nw.isClosed = true;
				nw.itemCount = w.itemCount;
				nw.contents = w.contents;
				
				closed.put(nw.cur_id, nw);
			} else {
				closed.put(w.cur_id, w);
			}
		}
		
		// Merge the tuples from the other open window.
		if (other.openWin != null) {
			// If we have no open window, clone the id from the merging value.
			if (openWin == null) {
				openNewWindow();
				openWin.cur_id = other.openWin.cur_id;
			}
			
			
			for (Entry<Writable, Writable> ent : other.openWin.contents.entrySet()) {
				IntWritable c = (IntWritable) ent.getValue();
				int delta = 1;
				int count = c.get();
				if (c.get() < 0) {
					delta = -1;
					count = -count;
				}
				// Unroll things so we don't end up with windows with too much data.
				for (int i = 0; i < count; i++) {
					update(ent.getKey(), delta);
				}
			}
		}
	}
	
	public WindowBundle(int maxSize) {
		this(maxSize, -1);
	}
	
	public WindowBundle(int maxSize, int closedAge) {
		init(maxSize, closedAge);
	}
	
	void closeWindow() {
		openWin.isClosed = true;
		openWin.closedTS = System.currentTimeMillis();
		closed.put(openWin.cur_id, openWin);
		openWin = null;
	}
	
	int getId() {
		while (true) {
			int i = r.nextInt();
			if (!closed.containsKey(i)) {
				return i;
			}
		}
	}
	
	void openNewWindow() {		
		openWin = new Window();
		
		openWin.cur_id = getId();
		
		openWin.isClosed = false;
		openWin.contents = new TriBasicPersistState();
		openWin.itemCount = 0;
	}
	
	void init(int maxSize, int closedAge) {
		this.maxSize = maxSize;
		if (closedAge > 0) {
			this.closedAge  = closedAge;
		}
		closed = new HashMap<Integer, Window>();
	}
	
	void update(Writable o, int c) {
		if (openWin == null) {
			openNewWindow();
		}
		
		IntWritable iw = (IntWritable) openWin.contents.get(o);
		if (iw == null) {
			iw = new IntWritable(c);
			openWin.contents.put(o, iw);
		} else {
			iw.set(iw.get() + c);
			if (iw.get() == 0) {
				openWin.contents.remove(o);
			}
		}
		
		// FIXME: This is incorrect for c != +/- 1.
		if (c > 0 && iw.get() > 0) {
			openWin.itemCount += 1;
		} else if (c < 0 && openWin.itemCount > 0){
			openWin.itemCount -= 1;
		}
		
		if (openWin.itemCount == maxSize) {
			closeWindow();
		}
	}
	
	public boolean isEmpty() {
		return (openWin == null || openWin.itemCount == 0) && (closed.size() == 0);
	}
	
	public void push(Writable o) {
		update(o, 1);
	}
	
	public void remove(Writable o) {
		update(o, -1);
	}
	
	public void runAgeoff(long ts) {
		List<Integer> delKeys = new ArrayList<Integer>();
		
		for (Entry<Integer, Window> ent : closed.entrySet()) {
			if (ent.getValue().closedTS + closedAge < ts) {
				delKeys.add(ent.getKey());
			}		
		}
//		if (delKeys.size() > 0) {
//			System.out.println("runAgeoff: Removing " + delKeys.size() + " items...");
//		}
		
		for (Integer i : delKeys) {
			closed.remove(i);
		}
	}
	
	void writeWindow(Window w, DataOutput out) throws IOException {
		out.writeInt(w.cur_id);
		out.writeInt(w.itemCount);
		out.writeLong(w.closedTS);
		w.contents.write(out);		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// Write the max size and ageoff
		out.writeInt(maxSize);
		out.writeInt(this.closedAge);
		
		// Write if there is an open window.
		out.writeBoolean(openWin != null);
		if (openWin != null) {
			// Write the window.
			writeWindow(openWin, out);
		}

		// Write any closed windows.
		out.writeInt(closed.size());
		if (closed.size() > 0) {
			for (Window w : closed.values()) {
				writeWindow(w, out);
			}
		}
	}

	Window readWindow(DataInput in) throws IOException {
		Window w = new Window();
		w.cur_id = in.readInt();
		w.itemCount = in.readInt();
		w.closedTS = in.readLong();
		w.contents = new TriBasicPersistState();
		w.contents.readFields(in);
		return w;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// Read the max size and ageoff
		maxSize = in.readInt();
		closedAge = in.readInt();
		init(maxSize, closedAge);

		// Read if there is an open window.
		if (in.readBoolean()) {
			// Read the window.
			openWin = readWindow(in);
		}

		// Read any closed windows.
		int c = in.readInt();
		while (closed.size() < c) {
			Window w = readWindow(in);
			closed.put(w.cur_id, w);
		}
	}

	public static List<Pair<List<NullableTuple>, List<NullableTuple>>> getTuples(WindowBundle<NullableTuple> lastWB, WindowBundle<NullableTuple> curWB) {
		ArrayList<Pair<List<NullableTuple>, List<NullableTuple>>> ret = new ArrayList<Pair<List<NullableTuple>, List<NullableTuple>>>();
		
		// We will cycle through current and develop the necessary pairs.
		
		// Start with the open window.
		if (curWB.openWin != null) {
			List<NullableTuple> lastTups = null;
			if (lastWB != null && lastWB.openWin != null && lastWB.openWin.cur_id == curWB.openWin.cur_id) {
				lastTups = lastWB.openWin.contents.getTuples(null);
			}
			ret.add(new Pair<List<NullableTuple>, List<NullableTuple>>(lastTups, curWB.openWin.contents.getTuples(null)));
		}
		
		// Now look through the closed windows.
		for (Window cur_w : curWB.closed.values()) {
			// We will only add a pair for this window if it was
			// previously open or didn't exist at all.  Once closed, windows
			// don't change, thus ReduceDelta would not produce results.

			// Closed in both cases, ignore.
			if (lastWB != null && lastWB.closed.containsKey(cur_w.cur_id)) {
				continue;
			}

			List<NullableTuple> lastTups = null;
			// Track tuples from last time if the window was previously open.
			if (lastWB != null && lastWB.openWin != null && lastWB.openWin.cur_id == cur_w.cur_id) {
				lastTups = lastWB.openWin.contents.getTuples(null);
			}
						
			ret.add(new Pair<List<NullableTuple>, List<NullableTuple>>(lastTups, cur_w.contents.getTuples(null)));
		}
		
		return ret;
	}
}
