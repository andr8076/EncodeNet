import socket
import tqdm
import subprocess
import os
import json
import threading

# ASCII Art Placeholder
print("ASCII ART HERE")

# Configuration prompts
server_port = int(input("Enter the server port for this NodeSlave: "))

# Function to get available encoders from ffmpeg
def get_available_encoders():
    try:
        command = ["ffmpeg", "-encoders"]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        encoders = []
        for line in result.stdout.splitlines():
            if "V" in line[:2]:
                encoders.append(line.split()[1])
        return encoders
    except Exception as e:
        print(f"Error getting available encoders: {e}")
        return []

# Function to encode video
def encode_video(input_path, output_path, encoder):
    command = [
        "ffmpeg",
        "-i", input_path,
        "-c:v", encoder,
        "-crf", "28",
        "-preset", "fast",
        "-y", output_path
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return result

# Function to encode a sample segment of the video
def encode_sample_segment(input_path, output_path, duration=30, encoder='libx265'):
    try:
        # Get the duration of the video
        command = [
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            input_path
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        video_duration = float(result.stdout.strip())

        # Calculate the start time for the middle segment
        start_time = (video_duration / 2) - (duration / 2)

        # Ensure the start time is valid
        start_time = max(0, start_time)

        # Compress the segment from the middle
        command = [
            "ffmpeg",
            "-ss", str(start_time),
            "-i", input_path,
            "-t", str(duration),
            "-c:v", encoder,  # Use the selected encoder
            "-crf", "28",  # Adjust quality here
            "-y", output_path
        ]
        subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        print(f"Error compressing sample segment for {input_path}: {e}")

# Function to handle client connections
def handle_client_connection(client_socket, address, slave_name):
    with client_socket:
        message = client_socket.recv(1024).decode()
        if message.startswith("READY?"):
            slave_name = message.split(" ")[1]
            client_socket.sendall(b"READY")
            print(f"NodeSlave {slave_name} ({address[0]}:{address[1]}) is ready.")
            return

        task_data = json.loads(message)
        input_path = task_data['input_path']
        output_path = task_data['output_path']
        encoder = task_data['encoder']
        task_type = task_data['task_type']

        # Update status to Receiving
        print(f"{slave_name}: Receiving video file...")
        with open(input_path, 'wb') as f:
            while True:
                bytes_read = client_socket.recv(4096)
                if not bytes_read:
                    break
                f.write(bytes_read)

        # Update status to Encoding
        print(f"{slave_name}: Encoding video file...")
        total_size = os.path.getsize(input_path)
        progress = tqdm.tqdm(total=total_size, unit="B", unit_scale=True, unit_divisor=1024, desc=f"Encoding {os.path.basename(input_path)}")
        if task_type == 'sample':
            encode_sample_segment(input_path, output_path, encoder=encoder)
        else:
            encode_video(input_path, output_path, encoder=encoder)
        progress.update(total_size)
        progress.close()

        # Update status to Sending
        print(f"{slave_name}: Sending encoded video file...")
        with open(output_path, 'rb') as f:
            while True:
                bytes_read = f.read(4096)
                if not bytes_read:
                    break
                client_socket.sendall(bytes_read)

        print(f"{slave_name}: Task completed and file sent back.")

# Main function to start NodeSlave
def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('0.0.0.0', server_port))
        s.listen()
        print(f"Listening on port {server_port} for incoming video data...")

        while True:
            client_socket, addr = s.accept()
            slave_name = "Unknown"
            client_handler = threading.Thread(
                target=handle_client_connection,
                args=(client_socket, addr, slave_name)
            )
            client_handler.start()

if __name__ == "__main__":
    main()
