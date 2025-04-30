import argparse
from extractor import extract_texts_parallel

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Academic-like text extractor")
    parser.add_argument("--input", required=True, help="Path to input JSON file")
    parser.add_argument("--output", required=True, help="Path to output JSON file")
    parser.add_argument("--workers", type=int, default=1, help="Number of parallel workers")

    args = parser.parse_args()
    extract_texts_parallel(args.input, args.output, args.workers)