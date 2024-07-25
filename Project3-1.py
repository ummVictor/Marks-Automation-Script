import csv
import subprocess
import json
import xlsxwriter
import os
import argparse

from io import BytesIO
from frameioclient import FrameioClient
from pymongo import MongoClient



class MongoDBManager:
    def __init__(self, host='localhost', port=27017, db_name='my_database'):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.client = None
        self.db = None

    def connect(self):
        self.client = MongoClient(self.host, self.port)
        self.db = self.client[self.db_name]

    def clear_database(self):
        if self.db is not None:
            self.client.drop_database(self.db_name)

    def create_collections(self):
        if self.db is not None:
            self.db.create_collection('xytech_collection')
            self.db.create_collection('baselight_collection')
            
    def upload_data(self, data, collection_name):
        if self.db is not None:
            collection = self.db[collection_name]
            if collection_name == 'xytech_collection':
                for location in data['Locations']:
                    record = {
                        "Title": data["Title"],
                        "Producer": data["Producer"],
                        "Operator": data["Operator"],
                        "Job": data["Job"],
                        "Location": location,
                        "Notes": data["Notes"]
                    }
                    collection.insert_one(record)
                print(f"Inserted data into {collection_name}")
            else:
                converted_data = [{"path": item[0], "range": item[1]} for item in data]
                collection.insert_many(converted_data)
                print(f"Uploaded {len(data)} documents to {collection_name}")
    
    def print_collections(self):
        if self.db is not None:
            collections = self.db.list_collection_names()
            print("Collections in the database:")
            for collection_name in collections:
                print(collection_name)
                print("Documents in collection:")
                collection = self.db[collection_name]
                for document in collection.find():
                    print(document)
    
    def get_records_within_max_frame(self, max_frame):
        result = []
        if self.db is not None:
            collection = self.db['baselight_collection']
            
            for document in collection.find():
                frame_rangess = document['range']
                frames = []
                if '-' in frame_rangess:
                    ranges = frame_rangess.split('-')
                    start = int(ranges[0])
                    end = int(ranges[1])
                    frames.extend(range(start, end + 1))
                    if all(frame <= max_frame for frame in frames):
                        result.append(document)

        return result
    
    def get_meta_data(self):
        if self.db is not None:
            collection = self.db['xytech_collection']
            return collection.find()[0]
 
class Workorder:
    def __init__(self, title, producer, operator, job, locations, notes):
        self.title = title
        self.producer = producer
        self.operator = operator
        self.job = job
        self.locations = locations
        self.notes = notes

    def __str__(self):
        return f"Title: {self.title}\nProducer: {self.producer}\nOperator: {self.operator}\nJob: {self.job}\nLocations: {', '.join(self.locations)}\nNotes: {self.notes}"

    def get_diction(self):
        return {"Title": self.title, "Producer": self.producer, "Operator": self.operator, "Job": self.job, "Locations": self.locations, "Notes": self.notes}
    

def xytechParser(file_path):
    with open(file_path, 'r') as file:
        title = file.readline().strip().split()[2] 
        
        file.readline()

        producer = file.readline().strip().split(": ")[1]

        operator = file.readline().strip().split(": ")[1]

        job = file.readline().strip().split(": ")[1]

        file.readline()

        locations = []
        while True:
            line = file.readline().strip()
            if line == "Notes:":
                break
            if line and line != "Location:":
                locations.append(line)

        notes = file.read().strip()

    return Workorder(title, producer, operator, job, locations, notes)

def baselightParser(file_path):
    parsed_data = []
    with open(file_path, 'r') as file:
        for line in file:
            line_data = line.strip().split()
            path = line_data[0]
            numbers = []
            for num in line_data[1:]:
                if num.isdigit():
                    numbers.append(int(num))
            parsed_data.append((path, rangeConverter(numbers)))
    return parsed_data

def rangeConverter(numbers):
    ranges = []
    i = 0
    while i < len(numbers):
        start = numbers[i]
        end = start
        while i + 1 < len(numbers) and numbers[i + 1] == end + 1:
            end = numbers[i + 1]
            i += 1
        if start == end:
            ranges.append(str(start))
        else:
            ranges.append(f"{start}-{end}")
        i += 1
    return ranges

def make_map(class_info, tuple_info):
    mapping = {}
    for class_dir in class_info:
        class_postfix_parts = class_dir.split('/')[-4:]  
        for tuple_dir, _ in tuple_info:
            tuple_postfix_parts = tuple_dir.split('/')[-4:]  
            if class_postfix_parts == tuple_postfix_parts:
                mapping[tuple_dir] = class_dir
                break
    return mapping

def make_output(map, list):
    output_dirs = [] 
    for tp in list:
        str = map[tp[0]]
        for num in tp[1]:
            output_dirs.append((str, num))
    return output_dirs


def generate_matrix(class_info, list):
    mat = [["Producer", "Operator", "Job", "Notes"], [class_info.producer, class_info.operator, class_info.job, class_info.notes], ["Location", "Frames to Fix", " ", " "]]

    for item in list:
        mat.append([item[0], item[1], " ", " "])
    return mat

def export_csv(matrix, file_path):
    with open(file_path, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(matrix)
        
def getMidpoint(start, end):
    return int((start + end) / 2)

def totalFrames(video_path):
    ffprobe_cmd = [
        "ffprobe",
        "-v", "error",
        "-count_frames", "-select_streams", "v:0",
        "-show_entries", "stream=nb_read_frames",
        "-of", "json",
        video_path
    ]
    result = subprocess.run(ffprobe_cmd, capture_output=True, text=True)

    if result.returncode == 0:
        data = json.loads(result.stdout)
        total_frames = int(data['streams'][0]['nb_read_frames'])
        return total_frames
    else:
        print("Error running ffprobe command.")
        return None

def frame_timecode(frame_number, fps=60.0):
    total_seconds = int(frame_number) / fps
    seconds = int(total_seconds)
    fractional_seconds = int((total_seconds - seconds) * fps)
    return f"{seconds:02d}.{fractional_seconds:02d}"

def FrameRange_TimecodeString(frame_range, fps=60.0):
    if '-' in frame_range:
        ranges = frame_range.split('-')
        start = int(ranges[0])
        end = int(ranges[1])
        return frameTimecodeString(start) + "-" + frameTimecodeString(end)
    else:
        return frameTimecodeString(int(frame_range))

def frameTimecodeString(frame_number, fps=60.0):
    total_seconds = frame_number / fps
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)
    frames = int((total_seconds - int(total_seconds)) * fps)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}:{frames:02d}"

def frameToImage(frame, video_path, image_name):
    seconds = frame_timecode(frame)
    print(seconds)
    ffmpeg_command = [
        "ffmpeg",
        "-i", video_path,
        "-ss", f"{seconds}",
        "-vframes", "1",
        "-vf", "scale=96:74",
        image_name
    ]

    try:
        subprocess.run(ffmpeg_command, check=True)
        print(f"Image generated successfully: {image_name}")
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

def createThumbnailRange(frameRange, video_path, image_name):
    if '-' in frameRange:
        ranges = frameRange.split('-')
        start = int(ranges[0])
        end = int(ranges[1])
        middle = getMidpoint(start, end)
        frameToImage(middle, video_path, image_name)
    else:
        frameToImage(frameRange, video_path, image_name)
        
import subprocess

def generateClip(frame_range, video_path, output_path):
    frames = frame_range.split('-')
    start_frame = int(frames[0])
    end_frame = int(frames[1])
    print(start_frame)
    print(end_frame)
    ffmpeg_command = [
        "ffmpeg",
        "-ss", frame_timecode(start_frame),
        "-i", video_path,
        "-to", frame_timecode(end_frame - start_frame),  
        "-c:v", "copy",
        "-c:a", "copy",
        output_path
    ]
    try:
        subprocess.run(ffmpeg_command, check=True)
        print(f"Video clip generated successfully: {output_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

def formXls(meta_data, map_list, video_path, output_path="Output.xlsx"):
    client = FrameioClient("fio-u-r56N2OqoMkYChYZoYCTiJhoFSrPLPcYqmrEnmYSFoHq9Fi4CFbW9VvKdcaTDpJGS")
    
    workbook = xlsxwriter.Workbook(output_path)
    worksheet = workbook.add_worksheet("output")
    worksheet.write('A1', 'Producer')
    worksheet.write('B1', 'Operator')
    worksheet.write('C1', 'Job')
    worksheet.write('D1', 'Notes')
    
    worksheet.write('A2', meta_data['Producer'])
    worksheet.write('B2', meta_data['Operator'])
    worksheet.write('C2', meta_data['Job'])
    worksheet.write('D2', meta_data['Notes'])
    
    worksheet.write('A3', "Show Location")
    worksheet.write('B3', "Frames to Fix")
    worksheet.write('C3', "Timecodes")
    worksheet.write('D3', "Thumbnail")
    i = 4
    for item in map_list:
        
        path = item['path']
        frame_range = item['range']
        image_name = 'image' + str(i) + '.jpg'
        video_name = 'video' + str(i) + '.mp4'
        createThumbnailRange(frame_range, video_path, image_name)
        worksheet.write('A' + str(i), path)
        worksheet.write('B' + str(i), frame_range)
        worksheet.write('C' + str(i), FrameRange_TimecodeString(frame_range))
        file = open(image_name, 'rb')
        data = BytesIO(file.read())
        worksheet.insert_image('D' + str(i), image_name, {'image_data': data})
        file.close()
        if '-' in frame_range:
            generateClip(frame_range, video_path, video_name)
            client.assets.upload("83bcc10d-f77f-4ec9-bc9f-be19463991b5", video_name)
        i+=1
        
    workbook.close()

def generateXls(baselight_data, xytech, video_path):
    manager = MongoDBManager()
    manager.connect()
    manager.clear_database()
    manager.create_collections()
    manager.upload_data(baselight_data, 'baselight_collection')
    manager.upload_data(xytech, 'xytech_collection')
    manager.print_collections()
    maxFrame = totalFrames(video_path)
    records_within_range = manager.get_records_within_max_frame(maxFrame)
    meta_data = manager.get_meta_data()
    formXls(meta_data, records_within_range, video_path)

def main():
    parser = argparse.ArgumentParser(description="Process baselight and xytech files and generate XLS with thumbnails and video clips.")
    parser.add_argument("--baselight", help="Path to Baselight file", required=True)
    parser.add_argument("--xytech", help="Path to XYTech file", required=True)
    parser.add_argument("--process", help="Path to video file for processing", required=True)
    parser.add_argument("--xls", help="Output XLS file path", action="store_true")
    args = parser.parse_args()
    
    class_info = xytechParser(args.xytech)
    tuple_info = baselightParser(args.baselight)
    map = make_map(class_info.locations, tuple_info)
    list = make_output(map, tuple_info)
    #print(list)
    mat = generate_matrix(class_info, list)
    if args.xls:
        generateXls(list, class_info.get_diction(), args.process)
    
    export_csv(mat, "output.csv")
    
if __name__ == "__main__":
    main()