def read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_file(path, text_list):
    with open(path, "w", encoding="utf-8") as f:
        for line in text_list:
            f.write(line.strip() + "\n")
