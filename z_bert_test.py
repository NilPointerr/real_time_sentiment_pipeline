# import json
# import torch
# from sklearn.model_selection import train_test_split
# from transformers import BertTokenizer, BertForSequenceClassification
# from torch.utils.data import DataLoader, Dataset
# from torch.optim import Adam
# from tqdm import tqdm
# import torch.nn.functional as F

# # Load JSON data
# with open('sentiment_data.json', 'r') as f:
#     data = json.load(f)

# # Preprocessing
# def preprocess_data(data):
#     texts = [item['text'] for item in data]
#     labels = [1 if item['comments'] > 0 else 0 for item in data] # Simple sentiment label
#     return texts, labels

# texts, labels = preprocess_data(data)

# # Train-test split
# train_texts, test_texts, train_labels, test_labels = train_test_split(texts, labels, test_size=0.2, random_state=42)

# # Tokenizer
# tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')

# class RedditDataset(Dataset):
#     def __init__(self, texts, labels):
#         self.texts = texts
#         self.labels = labels

#     def __len__(self):
#         return len(self.texts)

#     def __getitem__(self, idx):
#         encoding = tokenizer(self.texts[idx], truncation=True, padding='max_length', max_length=512, return_tensors='pt')
#         return {
#             'input_ids': encoding['input_ids'].squeeze(0),
#             'attention_mask': encoding['attention_mask'].squeeze(0),
#             'label': torch.tensor(self.labels[idx], dtype=torch.long)
#         }

# # Create DataLoader
# train_dataset = RedditDataset(train_texts, train_labels)
# test_dataset = RedditDataset(test_texts, test_labels)

# train_loader = DataLoader(train_dataset, batch_size=8, shuffle=True)
# test_loader = DataLoader(test_dataset, batch_size=8, shuffle=False)

# # Load Pretrained BERT Model
# model = BertForSequenceClassification.from_pretrained('bert-base-uncased', num_labels=2)
# device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
# model.to(device)

# # Optimizer
# optimizer = Adam(model.parameters(), lr=2e-5)

# # Training
# model.train()
# for epoch in range(3):
#     loop = tqdm(train_loader, leave=True)
#     for batch in loop:
#         input_ids = batch['input_ids'].to(device)
#         attention_mask = batch['attention_mask'].to(device)
#         labels = batch['label'].to(device)

#         optimizer.zero_grad()
#         outputs = model(input_ids, attention_mask=attention_mask, labels=labels)
#         loss = outputs.loss
#         loss.backward()
#         optimizer.step()
        
#         loop.set_description(f'Epoch {epoch+1}')
#         loop.set_postfix(loss=loss.item())

# # Testing
# model.eval()
# correct = 0
# total = 0
# with torch.no_grad():
#     for batch in test_loader:
#         input_ids = batch['input_ids'].to(device)
#         attention_mask = batch['attention_mask'].to(device)
#         labels = batch['label'].to(device)

#         outputs = model(input_ids, attention_mask=attention_mask)
#         predictions = torch.argmax(outputs.logits, dim=-1)
#         correct += (predictions == labels).sum().item()
#         total += labels.size(0)

# accuracy = correct / total
# print(f'Test Accuracy: {accuracy * 100:.2f}%')



sum_of_squares = sum(x*x for x in range(100))
print(sum_of_squares)