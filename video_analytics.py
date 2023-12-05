import cv2
import json
import sqlite3
import threading
from datetime import datetime

# Load configuration
from config import camera_streams, video_duration, database_config

# Initialize database
conn = sqlite3.connect(database_config["database_name"])
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS batches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id INTEGER,
        starting_frame_id INTEGER,
        ending_frame_id INTEGER,
        timestamp TEXT
    )
''')
conn.commit()

# Placeholder for thread safety, using Lock for synchronization
lock = threading.Lock()

def process_frame(camera_id, frame_id, frame):
    """
    Process a single frame and generate metadata.
    """
    # Placeholder for frame processing logic
    geo_location = get_geo_location()  # Placeholder for getting geo-location
    image_path = f"output/camera_{camera_id}_frame_{frame_id}.jpg"

    # Save one frame per second as an image file
    if frame_id % 25 == 0:
        cv2.imwrite(image_path, frame)

    # Create JSON object for the frame
    frame_data = {
        "camera_id": camera_id,
        "frame_id": frame_id,
        "geo_location": geo_location,
        "image_path": image_path
    }

    # Write frame information to a JSON file
    write_frame_to_json(frame_data)

    return frame_data

def write_frame_to_json(frame_data):
    """
    Write frame information to a JSON file.
    """
    with lock:
        with open("frame_info.json", 'a') as json_file:
            json.dump(frame_data, json_file)
            json_file.write('\n')

def process_batch(batch_id, starting_frame_id, ending_frame_id, timestamp):
    """
    Process a batch of frames and log the information in the database.
    """
    # Placeholder for batch processing logic
    cursor.execute('''
        INSERT INTO batches (batch_id, starting_frame_id, ending_frame_id, timestamp)
        VALUES (?, ?, ?, ?)
    ''', (batch_id, starting_frame_id, ending_frame_id, timestamp))
    conn.commit()

def process_camera_stream(camera_info):
    """
    Process a camera stream, handle frames and batches.
    """
    camera_id = camera_info["camera_id"]
    video_source = camera_info["source"]
    cap = cv2.VideoCapture(video_source)

    frame_id = 0
    batch_id = 1

    while True:
        ret, frame = cap.read()

        if not ret:
            break

        frame_data = process_frame(camera_id, frame_id, frame)

        frame_id += 1

        if frame_id % 25 == 0:  # Process batches every second
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            process_batch(batch_id, frame_id - 24, frame_id, timestamp)
            batch_id += 1

    cap.release()

def main():
    """
    Main function to initiate concurrent processing of camera streams.
    """
    # Process multiple camera streams concurrently
    threads = []
    for camera_info in camera_streams:
        thread = threading.Thread(target=process_camera_stream, args=(camera_info,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
