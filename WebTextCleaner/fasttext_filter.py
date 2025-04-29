import fasttext

class FastTextFilter:
    def __init__(self, model_path):
        self.model = fasttext.load_model(model_path)

    def filter_paragraphs(self, paragraphs):
        return [p for p in paragraphs if self.model.predict(p)[0][0] == "__label__keep"]
