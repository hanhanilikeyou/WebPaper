from WebTextCleaner.pipeline import process_html
from WebTextCleaner.utils import read_file, write_file
import sys
import os

def main():
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    model_path = sys.argv[3]

    html = read_file(input_file)
    result = process_html(html, model_path)
    write_file(output_file, result)

if __name__ == "__main__":
    main()
