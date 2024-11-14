

def process_ner_in_chunks(data, column_name, chunk_size=1000):
    scanner = NERScanner()
    return scanner.scan(data)
    