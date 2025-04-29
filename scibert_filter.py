from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

class SciBERTScorer:
    def __init__(self, model_name="allenai/scibert_scivocab_uncased"):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)

    def score_paragraphs(self, paragraphs):
        scored = []
        for text in paragraphs:
            inputs = self.tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
            with torch.no_grad():
                outputs = self.model(**inputs)
                score = torch.softmax(outputs.logits, dim=1)[0][1].item()
                scored.append((text, score))
        return [text for text, score in scored if score > 0.5]
