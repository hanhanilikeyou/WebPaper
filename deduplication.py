from datasketch import MinHash, MinHashLSH

def get_minhash(text, num_perm=128):
    m = MinHash(num_perm=num_perm)
    for word in text.split():
        m.update(word.encode("utf8"))
    return m

def deduplicate_texts(texts, threshold=0.9):
    lsh = MinHashLSH(threshold=threshold, num_perm=128)
    unique = []
    for i, text in enumerate(texts):
        mh = get_minhash(text)
        if not lsh.query(mh):
            lsh.insert(f"text_{i}", mh)
            unique.append(text)
    return unique
