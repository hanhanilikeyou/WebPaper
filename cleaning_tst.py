# cleaning.py
import argparse


def main():
    parser = argparse.ArgumentParser(description='Test argument parsing.')
    parser.add_argument('-i', '--input', required=True, help='Input JSONL file')
    parser.add_argument('-o', '--output', required=True, help='Output JSON file')
    parser.add_argument('-p', '--profile', choices=['medical', 'legal', 'social'], help='Optional profile')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose mode')

    args = parser.parse_args()

    print("Input file:", args.input)
    print("Output file:", args.output)
    print("Profile:", args.profile)
    print("Verbose mode:", args.verbose)


if __name__ == '__main__':
    main()