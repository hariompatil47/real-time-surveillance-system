import json
import cv2
import sqlite3
from datetime import datetime, timedelta

# Load configuration
from config import database_config

def generate_output_video(frames_info, output_video_path):
    """
    Generate an output video using the provided frame information.
    """
    # Define the codec and create a VideoWriter object
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    video_writer = cv2.VideoWriter(output_video_path, fourcc, 25.0, (640, 480))

    # Read frames and write to the output video
    for frame_info in frames_info:
        frame = cv2.imread(frame_info["image_path"])
        video_writer.write(frame)

    # Release the VideoWriter object
    video_writer.release()

def get_frames_info_from_json(timestamp, duration):
    """
    Retrieve frame information from the JSON file based on the user's input.
    """
    frames_info = []

    with open("frame_info.json", 'r') as json_file:
        for line in json_file:
            frame_info = json.loads(line)
            frame_timestamp = datetime.strptime(frame_info["timestamp"], '%Y-%m-%d %H:%M:%S')

            if timestamp <= frame_timestamp <= timestamp + timedelta(seconds=duration):
                frames_info.append(frame_info)

    return frames_info

def get_batch_info_from_database(timestamp, duration):
    """
    Retrieve batch information from the database based on the user's input.
    """
    conn = sqlite3.connect(database_config["database_name"])
    cursor = conn.cursor()

    cursor.execute('''
        SELECT *
        FROM batches
        WHERE timestamp >= ? AND timestamp <= ?
    ''', (timestamp, timestamp + timedelta(seconds=duration)))

    batch_info = cursor.fetchall()

    conn.close()

    return batch_info

def create_sample_database():
    """
    Create a sample SQLite database for testing purposes.
    """
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

    # Insert sample data
    cursor.execute('''
        INSERT INTO batches (batch_id, starting_frame_id, ending_frame_id, timestamp)
        VALUES (1, 1, 25, '2023-01-01 00:00:00'),
               (2, 26, 50, '2023-01-01 00:00:01'),
               (3, 51, 75, '2023-01-01 00:00:02')
    ''')

    conn.commit()
    conn.close()

def create_sample_json():
    """
    Create a sample JSON file for testing purposes.
    """
    sample_frame_data = {
        "camera_id": 1,
        "frame_id": 1,
        "geo_location": "Sample Location",
        "image_path": "output/camera_1_frame_1.jpg",
        "timestamp": "2023-01-01 00:00:00"
    }

    with open("frame_info.json", 'w') as json_file:
        json.dump(sample_frame_data, json_file)
        json_file.write('\n')

def main():
    """
    Main function to execute the user-driven program.
    """
    try:
        # Create sample database and JSON file for testing
        create_sample_database()
        create_sample_json()

        # User input
        user_timestamp_str = input("Enter TIMESTAMP (YYYY-MM-DD HH:MM:SS): ")
        user_duration_str = input("Enter DURATION OF THE VIDEO FILE (in seconds): ")

        # Convert user input to datetime and integer
        user_timestamp = datetime.strptime(user_timestamp_str, '%Y-%m-%d %H:%M:%S')
        user_duration = int(user_duration_str)

        # Retrieve batch information from the database
        batch_info = get_batch_info_from_database(user_timestamp, user_duration)

        if not batch_info:
            print("No relevant batches found.")
            return

        # Retrieve frame information from the JSON file
        frames_info = get_frames_info_from_json(user_timestamp, user_duration)

        if not frames_info:
            print("No relevant frames found.")
            return

        # Generate an output video
        output_video_path = f"output_videos/user_output_video.mp4"
        generate_output_video(frames_info, output_video_path)

        print(f"Output video generated successfully: {output_video_path}")

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
