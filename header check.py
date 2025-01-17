import csv

def check_csv_header(csv_path):
    with open(csv_path, 'r', newline='') as file:
        reader = csv.reader(file)
        header = next(reader)  # Read the first row (header)
        return header

# Example usage:
playlist_csv_path = r"C:\Users\langi longi\Desktop\New folder\Neo4j\importcsvneo4j\playlist.csv"
header = check_csv_header(playlist_csv_path)
print("CSV header:", header)
