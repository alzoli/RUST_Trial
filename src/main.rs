use std::{
    collections::HashMap,
    sync::{mpsc, Arc, Mutex},
    thread,
    time::Duration,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    id: u32,
    payload: String,
}

fn main() {
    // Shared, thread-safe cache accessible to both components
    let cache: Arc<Mutex<HashMap<u32, String>>> = Arc::new(Mutex::new(HashMap::new()));

    // 1) Direct ownership transfer between components via channel
    let (tx, rx) = mpsc::channel::<Message>();

    let cache_consumer = Arc::clone(&cache);
    let consumer_thread = thread::spawn(move || {
        // Component B: consumes Message values (ownership moved through the channel)
        while let Ok(msg) = rx.recv() {
            println!("Component B received (ownership transfer): {:?}", msg);
            let mut c = cache_consumer.lock().unwrap();
            c.insert(msg.id, msg.payload);
        }
        println!("Component B (ownership) exiting: sender closed");
    });

    let producer_thread = thread::spawn(move || {
        // Component A: produces messages and sends them (moves ownership)
        for i in 0..3 {
            let msg = Message {
                id: i,
                payload: format!("payload {}", i),
            };
            tx.send(msg).expect("failed to send message");
            thread::sleep(Duration::from_millis(50));
        }
        println!("Component A (ownership) done sending");
        // tx dropped here when thread exits -> consumer will see channel closed
    });

    producer_thread.join().unwrap();
    // wait a little for consumer to process
    thread::sleep(Duration::from_millis(100));

    // 2) Serialized transfer (simulating crossing a process boundary or network)
    let (tx2, rx2) = mpsc::channel::<Vec<u8>>();

    let cache_consumer2 = Arc::clone(&cache);
    let consumer_serialized = thread::spawn(move || {
        // Component B: receives serialized bytes, deserializes safely
        while let Ok(bytes) = rx2.recv() {
            match serde_json::from_slice::<Message>(&bytes) {
                Ok(msg) => {
                    println!("Component B received (serialized): {:?}", msg);
                    let mut c = cache_consumer2.lock().unwrap();
                    // store with offset key to differentiate
                    c.insert(msg.id + 100, msg.payload);
                }
                Err(e) => eprintln!("Deserialization error: {}", e),
            }
        }
        println!("Component B (serialized) exiting: sender closed");
    });

    let producer_serialized = thread::spawn(move || {
        // Component A: serializes Message into JSON bytes and sends
        for i in 10..13 {
            let msg = Message {
                id: i,
                payload: format!("serialized {}", i),
            };
            let bytes = serde_json::to_vec(&msg).expect("failed to serialize");
            tx2.send(bytes).expect("failed to send bytes");
            thread::sleep(Duration::from_millis(50));
        }
        println!("Component A (serialized) done sending");
        // tx2 dropped here
    });

    producer_serialized.join().unwrap();
    thread::sleep(Duration::from_millis(100));

    // Inspect final shared cache
    {
        let c = cache.lock().unwrap();
        println!("Final shared cache: {:#?}", *c);
    }

    // Join consumer threads — they exit when corresponding senders are dropped
    consumer_thread.join().unwrap();
    consumer_serialized.join().unwrap();
}
