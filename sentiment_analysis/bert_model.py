# import torch
# from transformers import AutoModelForSequenceClassification, AutoTokenizer
# from config import MODEL_NAME, MODEL_SAVE_PATH, DEVICE

# class SentimentModel:
#     def __init__(self, trained=False):
#         self.tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
#         if trained:
#             self.model = AutoModelForSequenceClassification.from_pretrained(MODEL_SAVE_PATH).to(DEVICE)
#         else:
#             self.model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, num_labels=2).to(DEVICE)

#     def predict(self, text):
#         inputs = self.tokenizer(text, return_tensors="pt", padding=True, truncation=True, max_length=512).to(DEVICE)
#         with torch.no_grad():
#             outputs = self.model(**inputs)
#         scores = outputs.logits.softmax(dim=-1).tolist()[0]
#         sentiment = scores.index(max(scores))  # 0 = Negative, 1 = Positive
#         return sentiment

# # Load trained model instance
# trained_model = SentimentModel(trained=True)
