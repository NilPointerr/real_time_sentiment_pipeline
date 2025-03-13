# import torch
# from torch.utils.data import DataLoader, Dataset
# from transformers import AutoTokenizer, AutoModelForSequenceClassification, AdamW
# from config import MODEL_NAME, MODEL_SAVE_PATH, BATCH_SIZE, EPOCHS, DEVICE
# import pandas as pd

# # Load Data (Replace with real dataset)
# df = pd.read_csv("sentiment_analysis/dataset.csv")  # Assume cleaned Reddit data

# class SentimentDataset(Dataset):
#     def __init__(self, data):
#         self.tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
#         self.texts = data["text"].tolist()
#         self.labels = data["label"].tolist()

#     def __len__(self):
#         return len(self.texts)

#     def __getitem__(self, idx):
#         inputs = self.tokenizer(self.texts[idx], return_tensors="pt", padding="max_length", truncation=True, max_length=512)
#         inputs = {k: v.squeeze(0) for k, v in inputs.items()}  # Remove batch dim
#         inputs["labels"] = torch.tensor(self.labels[idx], dtype=torch.long)
#         return inputs

# dataset = SentimentDataset(df)
# dataloader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True)

# # Load Model
# model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, num_labels=2).to(DEVICE)
# optimizer = AdamW(model.parameters(), lr=5e-5)

# # Training Loop
# for epoch in range(EPOCHS):
#     model.train()
#     for batch in dataloader:
#         batch = {k: v.to(DEVICE) for k, v in batch.items()}
#         outputs = model(**batch)
#         loss = outputs.loss
#         loss.backward()
#         optimizer.step()
#         optimizer.zero_grad()
#     print(f"Epoch {epoch + 1} - Loss: {loss.item()}")

# # Save Trained Model
# model.save_pretrained(MODEL_SAVE_PATH)
# print("✅ Model Training Completed & Saved!")
