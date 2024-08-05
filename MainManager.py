import os
import pandas as pd
import shutil
from tqdm import tqdm
import socket
import json

# Function to get the size of a file in a human-readable format
def format_size(size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f}{unit}"
        size /= 1024

# Function to get the size of a file
def get_file_size(filepath):
    return os.path.getsize(filepath)

# Function to send task to NodeMaster
def send_task_to_nodemaster(socket_path, task_data):
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(socket_path)
            message = json.dumps(task_data)
            s.sendall(message.encode('utf-8'))
            # Receive response
            response = s.recv(1024).decode('utf-8')
            print(f"Received from NodeMaster: {response}")
    except Exception as e:
        print(f"Error communicating with NodeMaster: {e}")

# Function to get the duration of a video
def get_video_duration(filepath):
    command = [
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        filepath
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return float(result.stdout)

# Function to process a movie for sampling
def process_sample_movie(movie_path, sample_dir):
    try:
        original_size = get_file_size(movie_path)
        video_duration = get_video_duration(movie_path)
        estimated_compressed_size = (original_size / video_duration) * 30  # Simplified estimation

        return original_size, estimated_compressed_size
    except Exception as e:
        print(f"Error processing sample movie {movie_path}: {e}")
        return None, None

# Function to get all movie files in a directory, including subfolders
def get_all_sample_movie_files(directory):
    movie_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(('.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv')):
                movie_files.append(os.path.join(root, file))
    return movie_files

# Function to back up the original file to a specified location
def backup_original_file(original_path, target_dir):
    try:
        target_path = os.path.join(target_dir, os.path.relpath(original_path, movies_dir))
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        shutil.copy2(original_path, target_path)
    except Exception as e:
        print(f"Error backing up original file {original_path} to {target_dir}: {e}")

# Main script execution
try:
    # Get user inputs for directories
    while True:
        movies_dir = input("Enter the directory containing your movies: ")
        if os.path.isdir(movies_dir):
            break
        else:
            print("Invalid directory. Please enter a valid directory.")

    # Get Unix socket path for NodeMaster
    nodemaster_socket_path = input("Enter the Unix socket path for NodeMaster: ")

    # Get all movie files in the directory and subdirectories
    movie_files = get_all_sample_movie_files(movies_dir)
    if not movie_files:
        raise Exception("No movie files found in the specified directory.")

    # Prompt user about the total number of movies
    print(f"\nFound {len(movie_files)} movie files.")

    # Ask the user for the preferred action with the new compressed files
    print("Choose what to do with the new compressed files:")
    print("1. Replace the files with the new compressed")
    print("2. Replace the files with the new compressed, and back up the original to a new location")
    print("3. Place the new compressed files in a new location, leaving the original in their place")
    
    while True:
        try:
            action = int(input("Enter the number of your choice (1, 2, or 3): "))
            if action in [1, 2, 3]:
                break
            else:
                print("Please enter 1, 2, or 3.")
        except ValueError:
            print("Invalid input. Please enter a number.")

    if action == 2:
        while True:
            backup_dir = input("Enter the directory to back up original movies: ")
            if os.path.isdir(backup_dir):
                break
            else:
                print("Invalid directory. Please enter a valid directory.")
        compressed_dir = movies_dir
    elif action == 3:
        while True:
            alternate_compressed_dir = input("Enter the directory to save the new compressed movies: ")
            if os.path.isdir(alternate_compressed_dir):
                break
            else:
                print("Invalid directory. Please enter a valid directory.")
        compressed_dir = alternate_compressed_dir
    else:
        compressed_dir = movies_dir

    # Ensure the necessary directories exist
    os.makedirs(compressed_dir, exist_ok=True)

    # Ask if the user wants to add the encoder name suffix to the filenames
    add_encoder_suffix = input("Do you want to add the encoder name suffix to the filenames? (y/n): ").strip().lower() == 'y'

    # If action is 2, back up original files to the backup directory
    if action == 2:
        for movie_path in tqdm(movie_files, desc="Backing up originals"):
            backup_original_file(movie_path, backup_dir)
    
    # Create a temporary directory for sample compression files
    sample_dir = os.path.join(movies_dir, "sample_compression")
    os.makedirs(sample_dir, exist_ok=True)

    # Perform sample encoding
    print("#" * 50)
    print("Starting Sample Encoding")
    print("#" * 50)
    movie_names = []
    original_sizes = []
    estimated_sizes = []
    size_reductions = []
    percent_reductions = []

    for movie_path in tqdm(movie_files, desc="Sample encoding"):
        original_size, estimated_compressed_size = process_sample_movie(movie_path, sample_dir)
        if original_size is None or estimated_compressed_size is None:
            continue
        size_reduction = original_size - estimated_compressed_size
        percent_reduction = (size_reduction / original_size) * 100

        movie_names.append(movie_path)
        original_sizes.append(original_size)
        estimated_sizes.append(estimated_compressed_size)
        size_reductions.append(size_reduction)
        percent_reductions.append(percent_reduction)

    # Clean up sample compression files and folder
    shutil.rmtree(sample_dir, ignore_errors=True)

    # Create a DataFrame to store the results
    data = {
        "Movie": movie_names,
        "Original Size": original_sizes,
        "Estimated Compressed Size": estimated_sizes,
        "Size Reduction": size_reductions,
        "Percent Reduction (%)": percent_reductions
    }

    df = pd.DataFrame(data)

    # Group by percent reduction to summarize data
    summary_df = df.groupby("Percent Reduction (%)").agg(
        files=pd.NamedAgg(column="Movie", aggfunc="count"),
        total_original_size=pd.NamedAgg(column="Original Size", aggfunc="sum"),
        total_compressed_size=pd.NamedAgg(column="Estimated Compressed Size", aggfunc="sum"),
        total_size_reduction=pd.NamedAgg(column="Size Reduction", aggfunc="sum")
    ).reset_index()

    # Display the summarized data in an effective method
    print("\nCompression Options:")
    for index, row in summary_df.iterrows():
        percent = row["Percent Reduction (%)"]
        files = row["files"]
        original_size = format_size(row["total_original_size"])
        compressed_size = format_size(row["total_compressed_size"])
        size_reduction = format_size(row["total_size_reduction"])
        print(f"{int(percent)}%: {files} files, was {original_size}, becomes {compressed_size}, saves {size_reduction}")

    # Ask user for the compression options to proceed with
    selected_options = input("\nEnter the compression options to proceed with (e.g., 1,2,4-6): ")
    selected_percentages = []
    for part in selected_options.split(','):
        if '-' in part:
            start, end = map(int, part.split('-'))
            selected_percentages.extend(range(start, end + 1))
        else:
            selected_percentages.append(int(part))

    # Filter movies based on the selected percentages
    selected_df = df[df["Percent Reduction (%)"].isin(selected_percentages)]

    # Print summary statistics for the chosen options
    total_original_size = format_size(selected_df["Original Size"].sum())
    total_estimated_size = format_size(selected_df["Estimated Compressed Size"].sum())
    total_savings = format_size(selected_df["Size Reduction"].sum())

    print(f"\nSelected Options: {selected_options}")
    print(f"Number of Movies: {len(selected_df)}")
    print(f"Total Original Size: {total_original_size}")
    print(f"Total Estimated Compressed Size: {total_estimated_size}")
    print(f"Total Savings: {total_savings}\n")

    # Iterate through all selected movie files with a progress bar for full encoding
    print("#" * 50)
    print("Starting Full Encoding")
    print("#" * 50)
    for index, row in tqdm(selected_df.iterrows(), desc="Full encoding", total=len(selected_df)):
        movie_path = row["Movie"]
        if action == 3:
            output_compressed_path = os.path.join(alternate_compressed_dir, os.path.relpath(movie_path, movies_dir))
        else:
            output_compressed_path = movie_path

        if add_encoder_suffix:
            output_compressed_path = f"{os.path.splitext(output_compressed_path)[0]}-encoded{os.path.splitext(output_compressed_path)[1]}"

        # Ensure the output directory exists
        os.makedirs(os.path.dirname(output_compressed_path), exist_ok=True)

        # Send the encoding task to NodeMaster
        task_data = {
            'input_path': movie_path,
            'output_path': output_compressed_path,
            'encoder': 'libx265',
            'task_type': 'full'
        }
        send_task_to_nodemaster(nodemaster_socket_path, task_data)

        if action == 1:
            # Remove the original file after compression
            os.remove(movie_path)
        elif action == 2:
            # Replace the original file with the compressed file
            shutil.move(output_compressed_path, movie_path)

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Ensure cleanup of sample directory in case of any errors
    if os.path.exists(sample_dir):
        shutil.rmtree(sample_dir, ignore_errors=True)